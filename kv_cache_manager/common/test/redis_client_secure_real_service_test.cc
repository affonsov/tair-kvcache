#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/common/test/secure_valkey_test_environment.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {
namespace {

constexpr char kStorageIdentity[] = "secure-client-test";

static_assert(!std::is_move_constructible_v<RedisClient>);
static_assert(!std::is_move_assignable_v<RedisClient>);

RedisConnectionOptions SecureOptions(const std::string &endpoint, const std::string &security_dir) {
    StandardUri uri = StandardUri::FromUri(endpoint);
    uri.SetParam("timeout_ms", "1000");
    uri.SetParam("retry_count", "2");
    RedisConnectionOptions options;
    options.endpoint = std::move(uri);
    options.security.transport = RedisTransportSecurity::TLS_NAMED_ACL;
    options.security.directory = security_dir;
    return options;
}

std::string AllowedKey(const std::string &suffix) {
    return "kvcm:valkey:" + std::to_string(sizeof(kStorageIdentity) - 1) + ":" + kStorageIdentity + ":" + suffix;
}

class ScopedOwnerDirectoryModeRestorer {
public:
    explicit ScopedOwnerDirectoryModeRestorer(std::string directory) : directory_(std::move(directory)) {}
    ~ScopedOwnerDirectoryModeRestorer() { (void)chmod(directory_.c_str(), 0700); }

    ScopedOwnerDirectoryModeRestorer(const ScopedOwnerDirectoryModeRestorer &) = delete;
    ScopedOwnerDirectoryModeRestorer &operator=(const ScopedOwnerDirectoryModeRestorer &) = delete;

private:
    std::string directory_;
};

class ScopedFd {
public:
    explicit ScopedFd(const int fd) : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    ScopedFd(const ScopedFd &) = delete;
    ScopedFd &operator=(const ScopedFd &) = delete;

    bool valid() const { return fd_ >= 0; }
    int get() const { return fd_; }

private:
    int fd_;
};

class ScopedLoopbackPortReservation {
public:
    explicit ScopedLoopbackPortReservation(const int port) {
        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd_ < 0) {
            return;
        }
        const int reuse_address = 1;
        if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) != 0) {
            Close();
            return;
        }
        struct sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<uint16_t>(port));
        if (bind(fd_, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) != 0 || listen(fd_, 1) != 0) {
            Close();
        }
    }

    ~ScopedLoopbackPortReservation() { Close(); }

    ScopedLoopbackPortReservation(const ScopedLoopbackPortReservation &) = delete;
    ScopedLoopbackPortReservation &operator=(const ScopedLoopbackPortReservation &) = delete;

    bool valid() const { return fd_ >= 0; }

    bool AcceptsLoopbackConnection(const int port) const {
        const int client = socket(AF_INET, SOCK_STREAM, 0);
        if (client < 0) {
            return false;
        }
        struct sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<uint16_t>(port));
        const bool connected = connect(client, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) == 0;
        close(client);
        return connected;
    }

private:
    void Close() {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
    }

    int fd_ = -1;
};

class InspectableRedisClient : public RedisClient {
public:
    using RedisClient::RedisClient;

    std::vector<ReplyUPtr> Execute(const std::vector<CmdArgs> &commands) { return CommandPipeline(commands); }
};

class RedisClientSecureRealServiceTest : public TESTBASE {
public:
    RedisClientSecureRealServiceTest() : server_(kStorageIdentity) {}

protected:
    test::SecureValkeyTestServer server_;
};

TEST_F(RedisClientSecureRealServiceTest, VerifiesDnsAndIpIdentityAndEnforcesAclScope) {
    ASSERT_TRUE(server_.StartTls());
    for (const std::string &host : {"localhost", "127.0.0.1"}) {
        InspectableRedisClient client(SecureOptions(server_.Endpoint(host), server_.security_dir()));
        ASSERT_TRUE(client.Open()) << host;

        const std::string key = AllowedKey(host);
        const std::string binary_value("secure\0value\xff", 13);
        EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.SetStrings({key}, {binary_value}));
        std::vector<std::string> values;
        EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({key}, values));
        EXPECT_EQ((std::vector<std::string>{binary_value}), values);
        std::vector<bool> exists;
        EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.Exists({key}, exists));
        EXPECT_EQ((std::vector<bool>{true}), exists);

        auto denied_key = client.Execute({{"SET", "outside-the-storage-prefix", "value"}});
        ASSERT_EQ(1u, denied_key.size());
        ASSERT_NE(nullptr, denied_key.front());
        EXPECT_EQ(REDIS_REPLY_ERROR, denied_key.front()->type);

        auto denied_command = client.Execute({{"DBSIZE"}});
        ASSERT_EQ(1u, denied_command.size());
        ASSERT_NE(nullptr, denied_command.front());
        EXPECT_EQ(REDIS_REPLY_ERROR, denied_command.front()->type);

        auto select_behavior = client.Execute({{"SELECT", "12"}, {"GET", key}, {"SELECT", "11"}});
        ASSERT_EQ(3u, select_behavior.size());
        ASSERT_NE(nullptr, select_behavior[0]);
        ASSERT_NE(nullptr, select_behavior[1]);
        ASSERT_NE(nullptr, select_behavior[2]);
        EXPECT_EQ(REDIS_REPLY_STATUS, select_behavior[0]->type);
        EXPECT_EQ("OK", std::string(select_behavior[0]->str, select_behavior[0]->len));
        EXPECT_EQ(REDIS_REPLY_NIL, select_behavior[1]->type);
        EXPECT_EQ(REDIS_REPLY_STATUS, select_behavior[2]->type);
        EXPECT_EQ("OK", std::string(select_behavior[2]->str, select_behavior[2]->len));

        EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.Delete({key}));
        client.Close();
    }
}

TEST_F(RedisClientSecureRealServiceTest, RejectsWrongCaIdentityAndNamedAclCredentials) {
    ASSERT_TRUE(server_.StartTls());
    const std::string endpoint = server_.Endpoint();
    for (const auto variant : {test::SecurityMaterialVariant::WRONG_CA,
                               test::SecurityMaterialVariant::WRONG_USERNAME,
                               test::SecurityMaterialVariant::WRONG_PASSWORD}) {
        std::string directory;
        ASSERT_TRUE(server_.CreateSecurityDirectory(variant, directory));
        RedisClient client(SecureOptions(endpoint, directory));
        EXPECT_FALSE(client.Open());
    }

    const pid_t original_server_pid = server_.server_pid_for_test();
    ASSERT_GT(original_server_pid, 0);
    ASSERT_TRUE(server_.RestartTlsWithMismatchedCertificate());
    ASSERT_EQ(endpoint, server_.Endpoint());
    EXPECT_GT(server_.server_pid_for_test(), 0);
    EXPECT_NE(original_server_pid, server_.server_pid_for_test());
    RedisClient mismatch(SecureOptions(endpoint, server_.security_dir()));
    EXPECT_FALSE(mismatch.Open());
}

TEST_F(RedisClientSecureRealServiceTest, RejectsMissingUnreadableEmptyOversizedAndMalformedFiles) {
    ASSERT_TRUE(server_.StartTls());
    for (const auto variant : {test::SecurityMaterialVariant::MISSING_CA,
                               test::SecurityMaterialVariant::MISSING_USERNAME,
                               test::SecurityMaterialVariant::MISSING_PASSWORD,
                               test::SecurityMaterialVariant::EMPTY_USERNAME,
                               test::SecurityMaterialVariant::MALFORMED_USERNAME,
                               test::SecurityMaterialVariant::UNREADABLE_PASSWORD,
                               test::SecurityMaterialVariant::INVALID_CA,
                               test::SecurityMaterialVariant::OVERSIZED_PASSWORD}) {
        std::string directory;
        ASSERT_TRUE(server_.CreateSecurityDirectory(variant, directory));
        RedisClient client(SecureOptions(server_.Endpoint(), directory));
        EXPECT_FALSE(client.Open());
    }

    RedisClient relative_directory(SecureOptions(server_.Endpoint(), "relative/security"));
    EXPECT_FALSE(relative_directory.Open());
}

TEST_F(RedisClientSecureRealServiceTest, RejectsSymlinkDirectoryAndFixedFiles) {
    ASSERT_TRUE(server_.StartTls());

    std::string valid_directory;
    ASSERT_TRUE(server_.CreateSecurityDirectory(test::SecurityMaterialVariant::VALID, valid_directory));
    const std::string directory_symlink = valid_directory + "-symlink";
    ASSERT_EQ(0, symlink(valid_directory.c_str(), directory_symlink.c_str()));
    RedisClient symlink_directory(SecureOptions(server_.Endpoint(), directory_symlink));
    EXPECT_FALSE(symlink_directory.Open());
    RedisClient symlink_directory_with_trailing_slash(SecureOptions(server_.Endpoint(), directory_symlink + "/"));
    EXPECT_FALSE(symlink_directory_with_trailing_slash.Open());

    for (const auto variant : {test::SecurityMaterialVariant::SYMLINK_CA,
                               test::SecurityMaterialVariant::SYMLINK_USERNAME,
                               test::SecurityMaterialVariant::SYMLINK_PASSWORD}) {
        std::string directory;
        ASSERT_TRUE(server_.CreateSecurityDirectory(variant, directory));
        RedisClient client(SecureOptions(server_.Endpoint(), directory));
        EXPECT_FALSE(client.Open());
    }
}

TEST_F(RedisClientSecureRealServiceTest, RejectsUnsafeModesAndAcceptsReadOnlyMountedMaterial) {
    ASSERT_TRUE(server_.StartTls());
    for (const auto variant : {test::SecurityMaterialVariant::UNSAFE_DIRECTORY_MODE,
                               test::SecurityMaterialVariant::UNSAFE_CA_MODE,
                               test::SecurityMaterialVariant::UNSAFE_USERNAME_MODE,
                               test::SecurityMaterialVariant::UNSAFE_PASSWORD_MODE,
                               test::SecurityMaterialVariant::PASSWORD_GROUP_READABLE}) {
        std::string directory;
        ASSERT_TRUE(server_.CreateSecurityDirectory(variant, directory));
        RedisClient client(SecureOptions(server_.Endpoint(), directory));
        EXPECT_FALSE(client.Open());
    }

    std::string read_only_directory;
    ASSERT_TRUE(server_.CreateSecurityDirectory(test::SecurityMaterialVariant::VALID, read_only_directory));
    ASSERT_EQ(0, chmod((read_only_directory + "/password").c_str(), 0400));
    RedisClient read_only_client(SecureOptions(server_.Endpoint(), read_only_directory));
    EXPECT_TRUE(read_only_client.Open());
}

TEST_F(RedisClientSecureRealServiceTest, NonWritableDirectoryPreventsCaReplacement) {
    if (geteuid() == 0) {
        GTEST_SKIP() << "root bypasses directory write permission checks";
    }
    ASSERT_TRUE(server_.StartTls());
    std::string directory;
    ASSERT_TRUE(server_.CreateSecurityDirectory(test::SecurityMaterialVariant::VALID, directory));
    {
        ScopedOwnerDirectoryModeRestorer mode_restorer(directory);
        std::error_code copy_error;
        std::filesystem::copy_file(directory + "/ca.pem", directory + "/replacement-ca.pem", copy_error);
        ASSERT_FALSE(copy_error);
        ASSERT_EQ(0, chmod(directory.c_str(), 0500));

        errno = 0;
        EXPECT_EQ(-1, rename((directory + "/replacement-ca.pem").c_str(), (directory + "/ca.pem").c_str()));
        EXPECT_TRUE(errno == EACCES || errno == EPERM);

        RedisClient client(SecureOptions(server_.Endpoint(), directory));
        EXPECT_TRUE(client.Open());
    }

    struct stat directory_status {};
    ASSERT_EQ(0, stat(directory.c_str(), &directory_status));
    EXPECT_EQ(static_cast<mode_t>(0700), directory_status.st_mode & 0777);
}

TEST(SecureValkeyTestServerTest, ReapedStartupChildClearsOwnedPidBeforeStop) {
    test::SecureValkeyTestServer server("reaped-child-test");
    EXPECT_FALSE(server.StartTlsWithServerExecutableForTest("/bin/false"));
    EXPECT_EQ(-1, server.server_pid_for_test());
    server.Stop();
    EXPECT_EQ(-1, server.server_pid_for_test());
}

TEST(SecureValkeyTestServerTest, RestartRequiresSuccessfulStartWithoutCreatingFixtureState) {
    test::SecureValkeyTestServer credential_rotation_server("unprepared-rotation-test");
    EXPECT_FALSE(credential_rotation_server.RestartTlsWithRotatedCredentials());
    EXPECT_EQ(-1, credential_rotation_server.server_pid_for_test());
    EXPECT_EQ(0, credential_rotation_server.port_for_test());
    EXPECT_TRUE(credential_rotation_server.Endpoint().empty());
    EXPECT_TRUE(credential_rotation_server.root_dir_for_test().empty());
    EXPECT_TRUE(credential_rotation_server.security_dir().empty());

    test::SecureValkeyTestServer mismatched_certificate_server("unprepared-mismatch-test");
    EXPECT_FALSE(mismatched_certificate_server.RestartTlsWithMismatchedCertificate());
    EXPECT_EQ(-1, mismatched_certificate_server.server_pid_for_test());
    EXPECT_EQ(0, mismatched_certificate_server.port_for_test());
    EXPECT_TRUE(mismatched_certificate_server.Endpoint().empty());
    EXPECT_TRUE(mismatched_certificate_server.root_dir_for_test().empty());
    EXPECT_TRUE(mismatched_certificate_server.security_dir().empty());
}

TEST(SecureValkeyTestServerTest, OccupiedPreservedPortRejectsUnrelatedListener) {
    test::SecureValkeyTestServer server("occupied-preserved-port-test");
    ASSERT_TRUE(server.StartTls());
    const std::string endpoint = server.Endpoint();
    const int preserved_port = server.port_for_test();
    ASSERT_FALSE(endpoint.empty());
    ASSERT_GT(preserved_port, 0);

    server.Stop();
    ASSERT_EQ(-1, server.server_pid_for_test());
    ASSERT_EQ(endpoint, server.Endpoint());
    ScopedLoopbackPortReservation reservation(preserved_port);
    ASSERT_TRUE(reservation.valid());

    EXPECT_FALSE(server.RestartTlsWithMismatchedCertificate());
    EXPECT_EQ(-1, server.server_pid_for_test());
    EXPECT_EQ(preserved_port, server.port_for_test());
    EXPECT_EQ(endpoint, server.Endpoint());
    EXPECT_TRUE(reservation.AcceptsLoopbackConnection(preserved_port));
}

TEST_F(RedisClientSecureRealServiceTest, NeverFallsBackBetweenTlsAndPlaintext) {
    ASSERT_TRUE(server_.StartTls());
    RedisClient plaintext(StandardUri::FromUri(server_.Endpoint("127.0.0.1", false)));
    EXPECT_FALSE(plaintext.Open());

    ASSERT_TRUE(server_.StartPlaintext());
    RedisClient secure(SecureOptions(server_.Endpoint("127.0.0.1", true), server_.security_dir()));
    EXPECT_FALSE(secure.Open());
}

TEST_F(RedisClientSecureRealServiceTest, ReconnectRepeatsTlsAuthSelectPingAndRereadsFiles) {
    ASSERT_TRUE(server_.StartTls());
    const std::string endpoint = server_.Endpoint();
    RedisClient client(SecureOptions(endpoint, server_.security_dir()));
    ASSERT_TRUE(client.Open());
    const std::string key = AllowedKey("rotation");
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK}), client.SetStrings({key}, {"before"}));

    const pid_t original_server_pid = server_.server_pid_for_test();
    ASSERT_GT(original_server_pid, 0);
    const std::string password_path = server_.security_dir() + "/password";
    ScopedFd original_password_fd(open(password_path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    ASSERT_TRUE(original_password_fd.valid());
    struct stat original_password_status {};
    ASSERT_EQ(0, fstat(original_password_fd.get(), &original_password_status));

    ASSERT_TRUE(server_.RestartTlsWithRotatedCredentials());
    ASSERT_EQ(endpoint, server_.Endpoint());
    const pid_t restarted_server_pid = server_.server_pid_for_test();
    ASSERT_GT(restarted_server_pid, 0);
    ASSERT_NE(original_server_pid, restarted_server_pid);
    struct stat rotated_password_status {};
    ASSERT_EQ(0, stat(password_path.c_str(), &rotated_password_status));
    ASSERT_TRUE(original_password_status.st_dev != rotated_password_status.st_dev ||
                original_password_status.st_ino != rotated_password_status.st_ino);

    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.SetStrings({key}, {"after"}));
    std::vector<std::string> values;
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({key}, values));
    EXPECT_EQ((std::vector<std::string>{"after"}), values);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.Delete({key}));
}

} // namespace
} // namespace kv_cache_manager
