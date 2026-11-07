#include "kv_cache_manager/common/test/secure_valkey_test_environment.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <netinet/in.h>
#include <openssl/mem.h>
#include <openssl/sha.h>
#include <signal.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace kv_cache_manager::test {
namespace {

void CleanseString(std::string &value) noexcept {
    if (!value.empty()) {
        OPENSSL_cleanse(value.data(), value.size());
    }
    value.clear();
}

struct TemporaryRandomBytes {
    ~TemporaryRandomBytes() noexcept { CleanseString(value); }

    std::string value;
};

struct TemporaryCredentialFileContents {
    ~TemporaryCredentialFileContents() noexcept {
        CleanseString(username);
        CleanseString(password);
    }

    std::string username;
    std::string password;
};

bool WriteFile(const std::string &path, const std::string &contents, mode_t mode = 0600) {
    const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, mode);
    if (fd < 0) {
        return false;
    }
    std::size_t offset = 0;
    bool ok = true;
    while (offset < contents.size()) {
        const ssize_t written = write(fd, contents.data() + offset, contents.size() - offset);
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            ok = false;
            break;
        }
    }
    if (close(fd) != 0) {
        ok = false;
    }
    return ok;
}

bool CopyFile(const std::string &source, const std::string &destination, const mode_t mode = 0600) {
    std::error_code error;
    std::filesystem::copy_file(source, destination, std::filesystem::copy_options::overwrite_existing, error);
    if (error) {
        return false;
    }
    return chmod(destination.c_str(), mode) == 0;
}

bool RunProcess(const std::vector<std::string> &arguments) {
    if (arguments.empty()) {
        return false;
    }
    const pid_t child = fork();
    if (child < 0) {
        return false;
    }
    if (child == 0) {
        const int null_fd = open("/dev/null", O_WRONLY);
        if (null_fd >= 0) {
            dup2(null_fd, STDOUT_FILENO);
            dup2(null_fd, STDERR_FILENO);
            close(null_fd);
        }
        std::vector<char *> argv;
        argv.reserve(arguments.size() + 1);
        for (const std::string &argument : arguments) {
            argv.push_back(const_cast<char *>(argument.c_str()));
        }
        argv.push_back(nullptr);
        execvp(argv.front(), argv.data());
        _exit(127);
    }
    int status = 0;
    while (waitpid(child, &status, 0) < 0) {
        if (errno != EINTR) {
            return false;
        }
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

std::string RandomHex(std::size_t byte_count) {
    TemporaryRandomBytes bytes;
    bytes.value.resize(byte_count, '\0');
    const int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return {};
    }
    std::size_t offset = 0;
    while (offset < bytes.value.size()) {
        const ssize_t read_size = read(fd, bytes.value.data() + offset, bytes.value.size() - offset);
        if (read_size > 0) {
            offset += static_cast<std::size_t>(read_size);
        } else if (read_size < 0 && errno == EINTR) {
            continue;
        } else {
            close(fd);
            return {};
        }
    }
    if (close(fd) != 0) {
        return {};
    }

    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.value.size() * 2);
    for (const unsigned char byte : bytes.value) {
        result.push_back(kHex[byte >> 4]);
        result.push_back(kHex[byte & 0x0f]);
    }
    return result;
}

std::string Sha256Hex(const std::string &value) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(value.data()), value.size(), digest);
    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (const unsigned char byte : digest) {
        stream << std::setw(2) << static_cast<unsigned int>(byte);
    }
    std::string result = stream.str();
    OPENSSL_cleanse(digest, sizeof(digest));
    return result;
}

int FindLoopbackPort() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    struct sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(fd, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) != 0) {
        close(fd);
        return 0;
    }
    socklen_t address_size = sizeof(address);
    if (getsockname(fd, reinterpret_cast<struct sockaddr *>(&address), &address_size) != 0) {
        close(fd);
        return 0;
    }
    const int port = ntohs(address.sin_port);
    close(fd);
    return port;
}

bool ProcessExited(pid_t &process) {
    while (true) {
        int status = 0;
        const pid_t result = waitpid(process, &status, WNOHANG);
        if (result == process || (result < 0 && errno == ECHILD)) {
            process = -1;
            return true;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

bool IsPrivateDirectory(const std::string &path) {
    struct stat status {};
    return !path.empty() && lstat(path.c_str(), &status) == 0 && S_ISDIR(status.st_mode) &&
           status.st_uid == geteuid() && (status.st_mode & 0077) == 0;
}

bool FileContainsReadyMarker(const std::string &path) {
    static constexpr char kReadyMarker[] = "Ready to accept connections";
    constexpr std::size_t kMaximumLogBytes = 64 * 1024;
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return false;
    }

    std::string contents;
    contents.reserve(kMaximumLogBytes);
    char buffer[4096];
    while (contents.size() < kMaximumLogBytes) {
        const std::size_t remaining = kMaximumLogBytes - contents.size();
        const std::size_t requested = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
        const ssize_t read_size = read(fd, buffer, requested);
        if (read_size > 0) {
            contents.append(buffer, static_cast<std::size_t>(read_size));
        } else if (read_size < 0 && errno == EINTR) {
            continue;
        } else {
            break;
        }
    }
    close(fd);
    return contents.find(kReadyMarker) != std::string::npos;
}

bool ConnectToLoopback(const int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    struct sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<uint16_t>(port));
    const bool connected = connect(fd, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) == 0;
    close(fd);
    return connected;
}

bool WaitForChildReady(pid_t &process, const int port, const std::string &log_path) {
    for (int attempt = 0; attempt < 200; ++attempt) {
        if (ProcessExited(process)) {
            return false;
        }
        if (FileContainsReadyMarker(log_path) && !ProcessExited(process) && ConnectToLoopback(port)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return !ProcessExited(process);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

bool GenerateServerCertificate(const std::string &root,
                               const std::string &name,
                               const std::string &common_name,
                               const std::string &subject_alt_names,
                               bool create_serial) {
    const std::string key = root + "/" + name + ".key";
    const std::string request = root + "/" + name + ".csr";
    const std::string certificate = root + "/" + name + ".pem";
    const std::string extensions = root + "/" + name + ".ext";
    if (!WriteFile(extensions,
                   "subjectAltName=" + subject_alt_names +
                       "\n"
                       "extendedKeyUsage=serverAuth\n")) {
        return false;
    }
    if (!RunProcess({"openssl",
                     "req",
                     "-new",
                     "-newkey",
                     "rsa:2048",
                     "-nodes",
                     "-keyout",
                     key,
                     "-out",
                     request,
                     "-subj",
                     "/CN=" + common_name})) {
        return false;
    }
    std::vector<std::string> sign_arguments{
        "openssl", "x509", "-req", "-in", request, "-CA", root + "/ca.pem", "-CAkey", root + "/ca.key"};
    if (create_serial) {
        sign_arguments.push_back("-CAcreateserial");
    } else {
        sign_arguments.push_back("-CAserial");
        sign_arguments.push_back(root + "/ca.srl");
    }
    sign_arguments.insert(sign_arguments.end(), {"-out", certificate, "-days", "1", "-sha256", "-extfile", extensions});
    return RunProcess(sign_arguments);
}

} // namespace

SecureValkeyTestServer::SecureValkeyTestServer(std::string storage_identity, const int64_t database)
    : storage_identity_(std::move(storage_identity)), database_(database) {}

SecureValkeyTestServer::~SecureValkeyTestServer() {
    Stop();
    CleanseString(username_);
    CleanseString(password_);
    if (!root_dir_.empty()) {
        std::error_code error;
        std::filesystem::remove_all(root_dir_, error);
        if (error) {
            std::fprintf(
                stderr, "SecureValkeyTestServer failed to remove its private test root: %s\n", error.message().c_str());
        }
    }
}

bool SecureValkeyTestServer::Prepare() {
    if (prepared_) {
        return true;
    }
    char directory_template[] = "/tmp/kvcm-secure-valkey-XXXXXX";
    const char *created = mkdtemp(directory_template);
    if (created == nullptr) {
        return false;
    }
    root_dir_ = created;
    security_dir_ = root_dir_ + "/security";
    if (mkdir(security_dir_.c_str(), 0700) != 0) {
        return false;
    }

    if (!RunProcess({"openssl",
                     "req",
                     "-x509",
                     "-newkey",
                     "rsa:2048",
                     "-nodes",
                     "-keyout",
                     root_dir_ + "/ca.key",
                     "-out",
                     root_dir_ + "/ca.pem",
                     "-days",
                     "1",
                     "-sha256",
                     "-subj",
                     "/CN=KVCache Test CA"}) ||
        !RunProcess({"openssl",
                     "req",
                     "-x509",
                     "-newkey",
                     "rsa:2048",
                     "-nodes",
                     "-keyout",
                     root_dir_ + "/wrong-ca.key",
                     "-out",
                     root_dir_ + "/wrong-ca.pem",
                     "-days",
                     "1",
                     "-sha256",
                     "-subj",
                     "/CN=KVCache Wrong Test CA"}) ||
        !GenerateServerCertificate(root_dir_, "server", "localhost", "DNS:localhost,IP:127.0.0.1", true) ||
        !GenerateServerCertificate(root_dir_, "mismatch-server", "mismatch.invalid", "DNS:mismatch.invalid", false) ||
        !CopyFile(root_dir_ + "/ca.pem", security_dir_ + "/ca.pem", 0444)) {
        return false;
    }
    if (!WriteCredentials() || !WriteAcl()) {
        return false;
    }
    prepared_ = true;
    return true;
}

bool SecureValkeyTestServer::IsPreparedForPreservedStart() const {
    static constexpr char kPrivateRootPrefix[] = "/tmp/kvcm-secure-valkey-";
    return prepared_ && port_ > 0 && root_dir_.rfind(kPrivateRootPrefix, 0) == 0 &&
           security_dir_ == root_dir_ + "/security" && IsPrivateDirectory(root_dir_) &&
           IsPrivateDirectory(security_dir_);
}

bool SecureValkeyTestServer::WriteCredentials() {
    CleanseString(username_);
    CleanseString(password_);
    username_ = "kvcm_test_" + RandomHex(8);
    password_ = RandomHex(32);
    TemporaryCredentialFileContents contents;
    contents.username = username_ + "\n";
    contents.password = password_ + "\n";
    const std::string username_path = security_dir_ + "/username";
    const std::string password_path = security_dir_ + "/password";
    if ((unlink(username_path.c_str()) != 0 && errno != ENOENT) ||
        (unlink(password_path.c_str()) != 0 && errno != ENOENT) || username_.size() <= sizeof("kvcm_test_") - 1 ||
        password_.empty() || !WriteFile(username_path, contents.username) ||
        !WriteFile(password_path, contents.password)) {
        return false;
    }
    return chmod(username_path.c_str(), 0444) == 0 && chmod(password_path.c_str(), 0600) == 0;
}

bool SecureValkeyTestServer::WriteAcl() const {
    const std::string key_pattern =
        "kvcm:valkey:" + std::to_string(storage_identity_.size()) + ":" + storage_identity_ + ":*";
    std::string acl = "user default off\nuser " + username_ + " reset on #" + Sha256Hex(password_) + " ~" +
                      key_pattern + " -@all +ping +select +get +set +del +exists +getrange +eval_ro\n";
    const bool written = WriteFile(root_dir_ + "/users.acl", acl);
    CleanseString(acl);
    return written;
}

bool SecureValkeyTestServer::Start(const bool tls,
                                   const bool mismatched_certificate,
                                   const char *server_executable,
                                   const PortMode port_mode,
                                   const bool authenticated) {
    const int preserved_port = port_;
    if (port_mode == PortMode::PRESERVE && !IsPreparedForPreservedStart()) {
        return false;
    }
    Stop();
    if (port_mode == PortMode::FRESH && !Prepare()) {
        return false;
    }

    constexpr int kMaximumStartupAttempts = 5;
    const int startup_attempts = port_mode == PortMode::FRESH ? kMaximumStartupAttempts : 1;
    const std::string config_path = root_dir_ + "/valkey.conf";
    const std::string log_path = root_dir_ + "/valkey.log";
    for (int startup_attempt = 0; startup_attempt < startup_attempts; ++startup_attempt) {
        port_ = port_mode == PortMode::FRESH ? FindLoopbackPort() : preserved_port;
        if (port_ == 0) {
            continue;
        }

        std::ostringstream config;
        config << "bind 127.0.0.1\n"
               << "protected-mode yes\n"
               << "daemonize no\n"
               << "databases 16\n"
               << "save \"\"\n"
               << "appendonly no\n";
        if (authenticated) {
            config << "aclfile " << root_dir_ << "/users.acl\n";
        }
        config << "dir " << root_dir_ << "\n"
               << "logfile " << log_path << "\n";
        if (tls) {
            const std::string certificate_name = mismatched_certificate ? "mismatch-server" : "server";
            config << "port 0\n"
                   << "tls-port " << port_ << "\n"
                   << "tls-cert-file " << root_dir_ << "/" << certificate_name << ".pem\n"
                   << "tls-key-file " << root_dir_ << "/" << certificate_name << ".key\n"
                   << "tls-ca-cert-file " << root_dir_ << "/ca.pem\n"
                   << "tls-auth-clients no\n";
        } else {
            config << "port " << port_ << "\n"
                   << "tls-port 0\n";
        }
        if (!WriteFile(config_path, config.str())) {
            if (port_mode == PortMode::FRESH) {
                port_ = 0;
            }
            return false;
        }
        if (!WriteFile(log_path, "")) {
            if (port_mode == PortMode::FRESH) {
                port_ = 0;
            }
            return false;
        }

        server_pid_ = fork();
        if (server_pid_ < 0) {
            server_pid_ = -1;
            if (port_mode == PortMode::FRESH) {
                port_ = 0;
            }
            continue;
        }
        if (server_pid_ == 0) {
            const char *configured_server = std::getenv("REDIS_SERVER_BIN");
            const char *server = server_executable != nullptr
                                     ? server_executable
                                     : (configured_server == nullptr ? "valkey-server" : configured_server);
            execlp(server, server, config_path.c_str(), static_cast<char *>(nullptr));
            _exit(127);
        }
        if (WaitForChildReady(server_pid_, port_, log_path)) {
            return true;
        }
        Stop();
        if (port_mode == PortMode::FRESH) {
            port_ = 0;
        }
    }
    if (port_mode == PortMode::PRESERVE) {
        std::fprintf(stderr, "SecureValkeyTestServer failed to restart on stable loopback port %d\n", preserved_port);
    }
    return false;
}

bool SecureValkeyTestServer::StartTls() { return Start(true, false); }

bool SecureValkeyTestServer::StartPlaintext() { return Start(false, false); }

bool SecureValkeyTestServer::StartPlaintextWithoutAuthentication() {
    return Start(false, false, nullptr, PortMode::FRESH, false);
}

bool SecureValkeyTestServer::StartTlsWithServerExecutableForTest(const std::string &server_executable) {
    return Start(true, false, server_executable.c_str());
}

bool SecureValkeyTestServer::RestartTlsWithRotatedCredentials() {
    if (!IsPreparedForPreservedStart()) {
        return false;
    }
    Stop();
    if (!WriteCredentials() || !WriteAcl()) {
        return false;
    }
    return Start(true, false, nullptr, PortMode::PRESERVE);
}

bool SecureValkeyTestServer::RestartTlsWithMismatchedCertificate() {
    return Start(true, true, nullptr, PortMode::PRESERVE);
}

void SecureValkeyTestServer::Stop() {
    if (server_pid_ <= 0) {
        return;
    }
    kill(server_pid_, SIGTERM);
    for (int attempt = 0; attempt < 200; ++attempt) {
        int status = 0;
        const pid_t result = waitpid(server_pid_, &status, WNOHANG);
        if (result == server_pid_ || (result < 0 && errno == ECHILD)) {
            server_pid_ = -1;
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    kill(server_pid_, SIGKILL);
    while (waitpid(server_pid_, nullptr, 0) < 0 && errno == EINTR) {}
    server_pid_ = -1;
}

std::string SecureValkeyTestServer::Endpoint(const std::string &host, const bool secure) const {
    return Endpoint(host, secure, database_);
}

std::string SecureValkeyTestServer::Endpoint(const std::string &host, const bool secure, const int64_t database) const {
    if (port_ <= 0) {
        return {};
    }
    return std::string(secure ? "valkeys://" : "valkey://") + host + ":" + std::to_string(port_) +
           "/?db=" + std::to_string(database);
}

bool SecureValkeyTestServer::CreateSecurityDirectory(const SecurityMaterialVariant variant,
                                                     std::string &directory) const {
    if (!prepared_) {
        return false;
    }
    directory = root_dir_ + "/material-" + RandomHex(8);
    if (directory.size() == root_dir_.size() + sizeof("/material-") - 1 || mkdir(directory.c_str(), 0700) != 0) {
        return false;
    }

    const std::string ca_path = directory + "/ca.pem";
    const std::string username_path = directory + "/username";
    const std::string password_path = directory + "/password";
    if (variant != SecurityMaterialVariant::MISSING_CA) {
        const std::string ca_source =
            variant == SecurityMaterialVariant::WRONG_CA ? root_dir_ + "/wrong-ca.pem" : root_dir_ + "/ca.pem";
        if (!CopyFile(ca_source, ca_path)) {
            return false;
        }
    }
    TemporaryCredentialFileContents contents;
    contents.username = username_ + "\n";
    contents.password = password_ + "\n";
    if (variant == SecurityMaterialVariant::WRONG_USERNAME) {
        CleanseString(contents.username);
        contents.username = "kvcm_test_" + RandomHex(8) + "\n";
    } else if (variant == SecurityMaterialVariant::WRONG_PASSWORD) {
        CleanseString(contents.password);
        contents.password = RandomHex(32) + "\n";
    } else if (variant == SecurityMaterialVariant::EMPTY_USERNAME) {
        CleanseString(contents.username);
    } else if (variant == SecurityMaterialVariant::MALFORMED_USERNAME) {
        contents.username.insert(contents.username.size() / 2, "\n");
    } else if (variant == SecurityMaterialVariant::INVALID_CA) {
        if (!WriteFile(ca_path, RandomHex(32))) {
            return false;
        }
    } else if (variant == SecurityMaterialVariant::OVERSIZED_PASSWORD) {
        CleanseString(contents.password);
        contents.password = RandomHex(2050);
    }
    if (variant != SecurityMaterialVariant::MISSING_USERNAME && !WriteFile(username_path, contents.username)) {
        return false;
    }
    if (variant != SecurityMaterialVariant::MISSING_PASSWORD && !WriteFile(password_path, contents.password)) {
        return false;
    }

    if (variant != SecurityMaterialVariant::MISSING_CA && chmod(ca_path.c_str(), 0444) != 0) {
        return false;
    }
    if (variant != SecurityMaterialVariant::MISSING_USERNAME && chmod(username_path.c_str(), 0444) != 0) {
        return false;
    }
    if (variant != SecurityMaterialVariant::MISSING_PASSWORD && chmod(password_path.c_str(), 0600) != 0) {
        return false;
    }

    if (variant == SecurityMaterialVariant::UNREADABLE_PASSWORD) {
        return chmod(password_path.c_str(), 0000) == 0;
    }
    if (variant == SecurityMaterialVariant::SYMLINK_CA) {
        return unlink(ca_path.c_str()) == 0 && symlink((security_dir_ + "/ca.pem").c_str(), ca_path.c_str()) == 0;
    }
    if (variant == SecurityMaterialVariant::SYMLINK_USERNAME) {
        return unlink(username_path.c_str()) == 0 &&
               symlink((security_dir_ + "/username").c_str(), username_path.c_str()) == 0;
    }
    if (variant == SecurityMaterialVariant::SYMLINK_PASSWORD) {
        return unlink(password_path.c_str()) == 0 &&
               symlink((security_dir_ + "/password").c_str(), password_path.c_str()) == 0;
    }
    if (variant == SecurityMaterialVariant::UNSAFE_DIRECTORY_MODE) {
        return chmod(directory.c_str(), 0720) == 0;
    }
    if (variant == SecurityMaterialVariant::UNSAFE_CA_MODE) {
        return chmod(ca_path.c_str(), 0664) == 0;
    }
    if (variant == SecurityMaterialVariant::UNSAFE_USERNAME_MODE) {
        return chmod(username_path.c_str(), 0642) == 0;
    }
    if (variant == SecurityMaterialVariant::UNSAFE_PASSWORD_MODE) {
        return chmod(password_path.c_str(), 0620) == 0;
    }
    if (variant == SecurityMaterialVariant::PASSWORD_GROUP_READABLE) {
        return chmod(password_path.c_str(), 0640) == 0;
    }
    return true;
}

} // namespace kv_cache_manager::test
