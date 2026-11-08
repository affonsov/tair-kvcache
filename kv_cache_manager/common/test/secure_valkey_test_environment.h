#pragma once

#include <cstdint>
#include <string>
#include <sys/types.h>

namespace kv_cache_manager::test {

enum class SecurityMaterialVariant {
    VALID,
    WRONG_CA,
    WRONG_USERNAME,
    WRONG_PASSWORD,
    MISSING_CA,
    MISSING_USERNAME,
    MISSING_PASSWORD,
    EMPTY_USERNAME,
    MALFORMED_USERNAME,
    UNREADABLE_PASSWORD,
    INVALID_CA,
    OVERSIZED_PASSWORD,
    SYMLINK_CA,
    SYMLINK_USERNAME,
    SYMLINK_PASSWORD,
    UNSAFE_DIRECTORY_MODE,
    UNSAFE_CA_MODE,
    UNSAFE_USERNAME_MODE,
    UNSAFE_PASSWORD_MODE,
    PASSWORD_GROUP_READABLE,
};

class SecureValkeyTestServer {
public:
    explicit SecureValkeyTestServer(std::string storage_identity, int64_t database = 11);
    ~SecureValkeyTestServer();

    SecureValkeyTestServer(const SecureValkeyTestServer &) = delete;
    SecureValkeyTestServer &operator=(const SecureValkeyTestServer &) = delete;

    bool StartTls();
    bool StartPlaintext();
    // Plaintext loopback server with no ACL file, so the default user needs no
    // password. Mirrors integration_test/testlib/redis_server.py for plaintext
    // valkey:// coverage that cannot carry credentials to an authenticated server.
    bool StartPlaintextWithoutAuthentication();
    bool StartTlsWithServerExecutableForTest(const std::string &server_executable);
    bool RestartTlsWithRotatedCredentials();
    bool RestartTlsWithMismatchedCertificate();
    void Stop();

    std::string Endpoint(const std::string &host = "localhost", bool secure = true) const;
    std::string Endpoint(const std::string &host, bool secure, int64_t database) const;
    const std::string &security_dir() const { return security_dir_; }
    const std::string &root_dir_for_test() const { return root_dir_; }
    int port_for_test() const { return port_; }
    pid_t server_pid_for_test() const { return server_pid_; }
    bool CreateSecurityDirectory(SecurityMaterialVariant variant, std::string &directory) const;

private:
    enum class PortMode {
        FRESH,
        PRESERVE
    };

    bool Prepare();
    bool IsPreparedForPreservedStart() const;
    bool Start(bool tls,
               bool mismatched_certificate,
               const char *server_executable = nullptr,
               PortMode port_mode = PortMode::FRESH,
               bool authenticated = true);
    bool WriteCredentials();
    bool WriteAcl() const;

    std::string storage_identity_;
    int64_t database_;
    int port_ = 0;
    pid_t server_pid_ = -1;
    std::string root_dir_;
    std::string security_dir_;
    std::string username_;
    std::string password_;
    bool prepared_ = false;
};

} // namespace kv_cache_manager::test
