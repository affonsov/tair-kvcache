#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <fcntl.h>
#include <functional>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>

#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {
namespace {

using namespace std::chrono_literals;

static_assert(!std::is_move_constructible_v<RedisClient>);
static_assert(!std::is_move_assignable_v<RedisClient>);

constexpr std::size_t kMaxRespLineBytes = 64;
constexpr std::size_t kMaxRespArrayElements = 16;
constexpr std::size_t kMaxRespBulkBytes = 2 * 1024 * 1024;
constexpr auto kServerIoTimeout = 2s;
constexpr auto kDeadlineBudget = 300ms;
constexpr auto kMaxDeadlineOperationDuration = 200ms;
constexpr auto kLegacyReplyDelay = 750ms;
constexpr int kConfiguredTimeoutMs = 1500;
static_assert(kLegacyReplyDelay > kDeadlineBudget);
static_assert(kLegacyReplyDelay < std::chrono::milliseconds(kConfiguredTimeoutMs));

bool ParseSize(const std::string &text, const std::size_t maximum, std::size_t &value) {
    value = 0;
    if (text.empty()) {
        return false;
    }
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc() && result.ptr == text.data() + text.size() && value <= maximum;
}

bool ReadExact(int fd, char *data, std::size_t size) {
    while (size != 0) {
        const ssize_t count = recv(fd, data, size, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        data += count;
        size -= static_cast<std::size_t>(count);
    }
    return true;
}

bool ReadLine(int fd, std::string &line) {
    line.clear();
    char byte = 0;
    while (line.size() <= kMaxRespLineBytes) {
        const ssize_t count = recv(fd, &byte, 1, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count != 1) {
            return false;
        }
        if (byte == '\r') {
            return ReadExact(fd, &byte, 1) && byte == '\n';
        }
        if (line.size() == kMaxRespLineBytes) {
            return false;
        }
        line.push_back(byte);
    }
    return false;
}

bool ReadCommand(int fd, std::vector<std::string> &command) {
    command.clear();
    char marker = 0;
    std::string line;
    std::size_t count = 0;
    if (!ReadExact(fd, &marker, 1) || marker != '*' || !ReadLine(fd, line) ||
        !ParseSize(line, kMaxRespArrayElements, count)) {
        return false;
    }
    command.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        std::size_t size = 0;
        if (!ReadExact(fd, &marker, 1) || marker != '$' || !ReadLine(fd, line) ||
            !ParseSize(line, kMaxRespBulkBytes, size)) {
            command.clear();
            return false;
        }
        std::string argument(size, '\0');
        if (!ReadExact(fd, argument.data(), size)) {
            command.clear();
            return false;
        }
        char terminator[2]{};
        if (!ReadExact(fd, terminator, sizeof(terminator)) || terminator[0] != '\r' || terminator[1] != '\n') {
            command.clear();
            return false;
        }
        command.push_back(std::move(argument));
    }
    return true;
}

bool SendAll(int fd, const std::string &data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t count = send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    return true;
}

std::string BoundedReply(bool exists, const std::string &value) {
    return "*2\r\n:" + std::to_string(exists ? 1 : 0) + "\r\n$" + std::to_string(value.size()) + "\r\n" + value +
           "\r\n";
}

struct CapturedLoggerOutput {
    std::string stdout_text;
    std::string stderr_text;

    bool Contains(const std::string &text) const {
        return stdout_text.find(text) != std::string::npos || stderr_text.find(text) != std::string::npos;
    }
};

CapturedLoggerOutput CaptureLoggerOutput(const std::function<void()> &operation) {
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    operation();
    CapturedLoggerOutput output;
    output.stderr_text = testing::internal::GetCapturedStderr();
    output.stdout_text = testing::internal::GetCapturedStdout();
    return output;
}

class LoopbackRespServer {
public:
    using Handler = std::function<void(int, std::size_t)>;

    explicit LoopbackRespServer(Handler handler) : handler_(std::move(handler)) {
        listener_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        EXPECT_GE(listener_, 0);
        int reuse = 1;
        EXPECT_EQ(0, setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        EXPECT_EQ(0, bind(listener_, reinterpret_cast<const sockaddr *>(&address), sizeof(address)));
        EXPECT_EQ(0, listen(listener_, 8));
        socklen_t length = sizeof(address);
        EXPECT_EQ(0, getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &length));
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { Run(); });
    }

    ~LoopbackRespServer() {
        stopping_.store(true, std::memory_order_release);
        shutdown(listener_, SHUT_RDWR);
        close(listener_);
        {
            std::lock_guard<std::mutex> lock(active_client_mutex_);
            if (active_client_ >= 0) {
                shutdown(active_client_, SHUT_RDWR);
            }
        }
        thread_.join();
    }

    int Port() const { return port_; }
    std::size_t Connections() const { return connections_.load(std::memory_order_acquire); }

private:
    void Run() {
        while (!stopping_.load(std::memory_order_acquire)) {
            const int client = accept(listener_, nullptr, nullptr);
            if (client < 0) {
                return;
            }
            timeval timeout{};
            timeout.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(kServerIoTimeout).count();
            EXPECT_EQ(0, setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
            EXPECT_EQ(0, setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)));
            {
                std::lock_guard<std::mutex> lock(active_client_mutex_);
                if (stopping_.load(std::memory_order_acquire)) {
                    close(client);
                    return;
                }
                active_client_ = client;
            }
            const std::size_t index = connections_.fetch_add(1, std::memory_order_acq_rel);
            handler_(client, index);
            {
                std::lock_guard<std::mutex> lock(active_client_mutex_);
                active_client_ = -1;
            }
            shutdown(client, SHUT_RDWR);
            close(client);
        }
    }

    Handler handler_;
    int listener_ = -1;
    int port_ = 0;
    std::atomic<std::size_t> connections_{0};
    std::atomic<bool> stopping_{false};
    std::mutex active_client_mutex_;
    int active_client_ = -1;
    std::thread thread_;
};

StandardUri TestUri(int port, int retry_count = 2, const std::string &host = "127.0.0.1", int timeout_ms = 2000) {
    return StandardUri::FromUri("redis://" + host + ":" + std::to_string(port) + "?timeout_ms=" +
                                std::to_string(timeout_ms) + "&retry_count=" + std::to_string(retry_count));
}

class NullAdoptRedisClient : public RedisClient {
public:
    explicit NullAdoptRedisClient(const StandardUri &uri) : RedisClient(uri) {}

    int AdoptedFd() const { return adopted_fd_; }

protected:
    redisContext *AdoptConnectedSocket(const int socket_fd) override {
        adopted_fd_ = socket_fd;
        return nullptr;
    }

private:
    int adopted_fd_ = -1;
};

class FailFirstAppendRedisClient : public RedisClient {
public:
    explicit FailFirstAppendRedisClient(const StandardUri &uri) : RedisClient(uri) {}

protected:
    bool AppendDeadlineCommand(const CmdArgs &cmd) override {
        if (!failed_) {
            failed_ = true;
            return false;
        }
        return RedisClient::AppendDeadlineCommand(cmd);
    }

private:
    bool failed_ = false;
};

bool CompleteHandshake(int fd) {
    std::vector<std::string> command;
    return ReadCommand(fd, command) && command == std::vector<std::string>{"PING"} && SendAll(fd, "+PONG\r\n");
}

TEST(RedisClientDeadlineTest, RestoresConfiguredTimeoutBeforeLegacyCommand) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ("EVAL_RO", command[0]);
        std::this_thread::sleep_for(25ms);
        ASSERT_TRUE(SendAll(fd, BoundedReply(true, "deadline")));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", "legacy"}), command);
        std::this_thread::sleep_for(kLegacyReplyDelay);
        ASSERT_TRUE(SendAll(fd, "$6\r\nlegacy\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 1, "127.0.0.1", kConfiguredTimeoutMs));
    ASSERT_TRUE(client.Prepare());

    const auto deadline_start = std::chrono::steady_clock::now();
    const auto deadline_result = client.GetStringsUntil({"deadline"}, {8}, deadline_start + kDeadlineBudget);
    const auto deadline_elapsed = std::chrono::steady_clock::now() - deadline_start;
    ASSERT_EQ(EC_OK, deadline_result.operation_error);
    ASSERT_EQ((std::vector<std::string>{"deadline"}), deadline_result.values);
    ASSERT_LT(deadline_elapsed, kMaxDeadlineOperationDuration);

    std::vector<std::string> values;
    const auto legacy_start = std::chrono::steady_clock::now();
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({"legacy"}, values));
    EXPECT_GE(std::chrono::steady_clock::now() - legacy_start, kLegacyReplyDelay);
    EXPECT_EQ((std::vector<std::string>{"legacy"}), values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, SetRestoresConfiguredTimeoutBeforeLegacyCommand) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", "deadline", "value"}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", "legacy"}), command);
        std::this_thread::sleep_for(kLegacyReplyDelay);
        ASSERT_TRUE(SendAll(fd, "$6\r\nlegacy\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 1, "127.0.0.1", kConfiguredTimeoutMs));
    ASSERT_TRUE(client.Prepare());

    const auto deadline_start = std::chrono::steady_clock::now();
    const auto deadline_result = client.SetStringsUntil({"deadline"}, {"value"}, deadline_start + kDeadlineBudget);
    const auto deadline_elapsed = std::chrono::steady_clock::now() - deadline_start;
    ASSERT_EQ(EC_OK, deadline_result.operation_error);
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK}), deadline_result.errors);
    ASSERT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::CONFIRMED}), deadline_result.certainties);
    ASSERT_LT(deadline_elapsed, kMaxDeadlineOperationDuration);

    std::vector<std::string> values;
    const auto legacy_start = std::chrono::steady_clock::now();
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({"legacy"}, values));
    EXPECT_GE(std::chrono::steady_clock::now() - legacy_start, kLegacyReplyDelay);
    EXPECT_EQ((std::vector<std::string>{"legacy"}), values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyAuthRequiresExactOkStatusWhenPeerEchoesCredential) {
    std::atomic<bool> command_after_auth{false};
    bool opened = true;
    {
        LoopbackRespServer server([&](int fd, std::size_t connection) {
            EXPECT_EQ(0u, connection);
            std::vector<std::string> command;
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "-NOAUTH Authentication required\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_EQ(2u, command.size());
            EXPECT_EQ("AUTH", command[0]);
            ASSERT_TRUE(SendAll(fd, "+" + command[1] + "\r\n"));

            pollfd descriptor{fd, POLLIN, 0};
            if (poll(&descriptor, 1, 200) > 0 && ReadCommand(fd, command)) {
                command_after_auth.store(true, std::memory_order_release);
                (void)SendAll(fd, "+PONG\r\n");
            }
        });
        const std::string credential = "auth-" + std::to_string(server.Port());
        RedisClient client(StandardUri::FromUri("redis://" + credential + "@127.0.0.1:" +
                                                std::to_string(server.Port()) + "?timeout_ms=1000&retry_count=1"));
        opened = client.Open();
    }

    EXPECT_FALSE(opened);
    EXPECT_FALSE(command_after_auth.load(std::memory_order_acquire));
}

TEST(RedisClientDeadlineTest, LegacyAuthErrorDoesNotLogEchoedCredential) {
    std::string credential;
    bool opened = true;
    CapturedLoggerOutput output;
    {
        LoopbackRespServer server([](int fd, std::size_t connection) {
            EXPECT_EQ(0u, connection);
            std::vector<std::string> command;
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "-NOAUTH Authentication required\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_EQ(2u, command.size());
            EXPECT_EQ("AUTH", command[0]);
            ASSERT_TRUE(SendAll(fd, "-ERR rejected " + command[1] + "\r\n"));
        });
        credential = "auth-error-" + std::to_string(server.Port());
        RedisClient client(StandardUri::FromUri("redis://" + credential + "@127.0.0.1:" +
                                                std::to_string(server.Port()) + "?timeout_ms=1000&retry_count=1"));
        output = CaptureLoggerOutput([&] { opened = client.Open(); });
    }

    EXPECT_FALSE(opened);
    EXPECT_TRUE(output.Contains("auth fail"));
    EXPECT_FALSE(output.Contains(credential));
}

TEST(RedisClientDeadlineTest, LegacyPostAuthPingEchoDoesNotLogCredential) {
    std::string credential;
    bool opened = true;
    CapturedLoggerOutput output;
    {
        LoopbackRespServer server([](int fd, std::size_t connection) {
            EXPECT_EQ(0u, connection);
            std::vector<std::string> command;
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "-NOAUTH Authentication required\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_EQ(2u, command.size());
            EXPECT_EQ("AUTH", command[0]);
            const std::string submitted_credential = command[1];
            ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "+" + submitted_credential + "\r\n"));
        });
        credential = "post-auth-ping-" + std::to_string(server.Port());
        RedisClient client(StandardUri::FromUri("redis://" + credential + "@127.0.0.1:" +
                                                std::to_string(server.Port()) + "?timeout_ms=1000&retry_count=1"));
        output = CaptureLoggerOutput([&] { opened = client.Open(); });
    }

    EXPECT_FALSE(opened);
    EXPECT_TRUE(output.Contains("ping fail after auth"));
    EXPECT_FALSE(output.Contains(credential));
}

TEST(RedisClientDeadlineTest, LegacyAuthenticatedSelectEchoDoesNotLogCredential) {
    std::string credential;
    bool opened = true;
    CapturedLoggerOutput output;
    {
        LoopbackRespServer server([](int fd, std::size_t connection) {
            EXPECT_EQ(0u, connection);
            std::vector<std::string> command;
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "-NOAUTH Authentication required\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_EQ(2u, command.size());
            EXPECT_EQ("AUTH", command[0]);
            const std::string submitted_credential = command[1];
            ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"PING"}), command);
            ASSERT_TRUE(SendAll(fd, "+PONG\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_EQ(2u, command.size());
            EXPECT_EQ("SELECT", command[0]);
            ASSERT_TRUE(SendAll(fd, "+" + submitted_credential + "\r\n"));
        });
        credential = "select-auth-" + std::to_string(server.Port());
        RedisClient client(StandardUri::FromUri("redis://" + credential +
                                                "@127.0.0.1:" + std::to_string(server.Port()) +
                                                "/?db=1&timeout_ms=1000&retry_count=1"));
        output = CaptureLoggerOutput([&] { opened = client.Open(); });
    }

    EXPECT_FALSE(opened);
    EXPECT_TRUE(output.Contains("fail to select redis db"));
    EXPECT_FALSE(output.Contains(credential));
}

TEST(RedisClientDeadlineTest, RespParserRejectsMalformedAndOversizedLengths) {
    const auto rejects = [](const std::string &payload) {
        int sockets[2]{-1, -1};
        ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, sockets));
        ASSERT_TRUE(SendAll(sockets[0], payload));
        shutdown(sockets[0], SHUT_WR);
        std::vector<std::string> command;
        EXPECT_FALSE(ReadCommand(sockets[1], command));
        close(sockets[0]);
        close(sockets[1]);
    };

    rejects("*not-a-number\r\n");
    rejects("*17\r\n");
    rejects("*1\r\n$2097153\r\n");
    rejects("*00000000000000000000000000000000000000000000000000000000000000000\r\n");
}

TEST(RedisClientDeadlineTest, ServerTeardownInterruptsActiveClientRead) {
    int client = -1;
    const auto start = std::chrono::steady_clock::now();
    {
        LoopbackRespServer server([](int fd, std::size_t) {
            std::vector<std::string> command;
            EXPECT_FALSE(ReadCommand(fd, command));
        });
        client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ASSERT_GE(client, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<uint16_t>(server.Port()));
        ASSERT_EQ(0, connect(client, reinterpret_cast<const sockaddr *>(&address), sizeof(address)));
        const auto wait_until = std::chrono::steady_clock::now() + 500ms;
        while (server.Connections() == 0 && std::chrono::steady_clock::now() < wait_until) {
            std::this_thread::yield();
        }
        ASSERT_EQ(1u, server.Connections());
    }
    close(client);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);
}

TEST(RedisClientDeadlineTest, EagerResolutionAndPreexpiredDeadlineDoNoIo) {
    LoopbackRespServer server([](int, std::size_t) { ADD_FAILURE() << "preexpired operation opened a connection"; });
    RedisClient client(TestUri(server.Port(), 1, "localhost"));
    ASSERT_TRUE(client.Prepare());
    const auto result = client.SetStringsUntil({"key"}, {"value"}, RedisClient::Deadline::min());
    EXPECT_EQ(EC_TIMEOUT, result.operation_error);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::NOT_SENT}), result.certainties);
    EXPECT_EQ(0u, server.Connections());

    RedisClient unresolvable(TestUri(server.Port(), 1, "256.256.256.256"));
    EXPECT_FALSE(unresolvable.Prepare());
}

TEST(RedisClientDeadlineTest, DelayedHandshakeCannotReturnSuccess) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"PING"}), command);
        std::this_thread::sleep_for(250ms);
        (void)SendAll(fd, "+PONG\r\n");
    });
    RedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Prepare());

    const auto result =
        client.SetStringsUntil({"key"}, {"value"}, std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
    EXPECT_EQ(EC_TIMEOUT, result.operation_error);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::NOT_SENT}), result.certainties);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, NullContextAdoptionClosesOriginalSocket) {
    LoopbackRespServer server([](int fd, std::size_t) {
        std::vector<std::string> command;
        EXPECT_FALSE(ReadCommand(fd, command));
    });
    NullAdoptRedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Prepare());
    const auto result =
        client.SetStringsUntil({"key"}, {"value"}, std::chrono::steady_clock::now() + std::chrono::seconds(1));
    EXPECT_EQ(EC_IO_ERROR, result.operation_error);
    ASSERT_GE(client.AdoptedFd(), 0);
    errno = 0;
    EXPECT_EQ(-1, fcntl(client.AdoptedFd(), F_GETFD));
    EXPECT_EQ(EBADF, errno);
}

TEST(RedisClientDeadlineTest, SetRetriesOnlyPreNetworkAppendFailure) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        if (connection == 0) {
            char byte = 0;
            EXPECT_LE(recv(fd, &byte, 1, 0), 0);
            return;
        }
        ASSERT_EQ(1u, connection);
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", "key", "value"}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    FailFirstAppendRedisClient client(TestUri(server.Port(), 2));
    ASSERT_TRUE(client.Prepare());
    const auto result =
        client.SetStringsUntil({"key"}, {"value"}, std::chrono::steady_clock::now() + std::chrono::seconds(2));
    EXPECT_EQ(EC_OK, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), result.errors);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::CONFIRMED}), result.certainties);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, SetMapsConfirmedAmbiguousAndNotSentWithoutReplay) {
    std::mutex mutex;
    std::condition_variable shutdown_observed;
    bool observed = false;
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < 64; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ("SET", command[0]);
        }
        ASSERT_TRUE(SendAll(fd, "+OK\r\n+"));
        while (send(fd, "x", 1, MSG_NOSIGNAL) == 1) {
            std::this_thread::sleep_for(20ms);
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            observed = true;
        }
        shutdown_observed.notify_one();
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());
    std::vector<std::string> keys(65);
    std::vector<std::string> values(65, "value");
    for (std::size_t i = 0; i < keys.size(); ++i) {
        keys[i] = "key-" + std::to_string(i);
    }
    const auto start = std::chrono::steady_clock::now();
    const auto result = client.SetStringsUntil(keys, values, start + 250ms);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
    EXPECT_EQ(EC_OK, result.errors[0]);
    EXPECT_EQ(ValkeyWriteCertainty::CONFIRMED, result.certainties[0]);
    for (std::size_t i = 1; i < 64; ++i) {
        EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, result.certainties[i]);
    }
    EXPECT_EQ(ValkeyWriteCertainty::NOT_SENT, result.certainties[64]);
    EXPECT_EQ(1u, result.confirmed_count);
    EXPECT_EQ(63u, result.ambiguous_count);
    EXPECT_EQ(1u, result.not_sent_count);
    EXPECT_EQ(1u, server.Connections());
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(shutdown_observed.wait_for(lock, 1s, [&] { return observed; }));
}

TEST(RedisClientDeadlineTest, DelayedSetCompletionTimesOutWithoutReplayingOrLosingCertainty) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < 64; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
        }
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
        std::this_thread::sleep_for(250ms);
        (void)SendAll(fd, "+OK\r\n");
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());
    std::vector<std::string> keys(65);
    std::vector<std::string> values(65, "value");
    for (std::size_t i = 0; i < keys.size(); ++i) {
        keys[i] = "delayed-key-" + std::to_string(i);
    }

    const auto start = std::chrono::steady_clock::now();
    const auto result = client.SetStringsUntil(keys, values, start + 100ms);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);
    EXPECT_EQ(EC_TIMEOUT, result.operation_error);
    EXPECT_EQ(EC_OK, result.errors[0]);
    EXPECT_EQ(ValkeyWriteCertainty::CONFIRMED, result.certainties[0]);
    for (std::size_t i = 1; i < 64; ++i) {
        EXPECT_EQ(EC_TIMEOUT, result.errors[i]);
        EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, result.certainties[i]);
    }
    EXPECT_EQ(EC_TIMEOUT, result.errors[64]);
    EXPECT_EQ(ValkeyWriteCertainty::NOT_SENT, result.certainties[64]);
    EXPECT_EQ(1u, result.confirmed_count);
    EXPECT_EQ(63u, result.ambiguous_count);
    EXPECT_EQ(1u, result.not_sent_count);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, DelayedBoundedReadReturnsTimeoutAndReconnectsFresh) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        if (connection == 0) {
            std::this_thread::sleep_for(250ms);
            (void)SendAll(fd, BoundedReply(true, "late"));
            return;
        }
        ASSERT_EQ(1u, connection);
        ASSERT_TRUE(SendAll(fd, BoundedReply(true, "fresh")));
    });
    RedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Prepare());

    const auto start = std::chrono::steady_clock::now();
    const auto late = client.GetStringsUntil({"key"}, {4}, start + 100ms);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);
    EXPECT_EQ(EC_TIMEOUT, late.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_TIMEOUT}), late.errors);
    EXPECT_EQ((std::vector<std::string>{""}), late.values);

    const auto fresh = client.GetStringsUntil({"key"}, {5}, std::chrono::steady_clock::now() + std::chrono::seconds(1));
    EXPECT_EQ(EC_OK, fresh.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), fresh.errors);
    EXPECT_EQ((std::vector<std::string>{"fresh"}), fresh.values);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, BoundedReadRetriesTransportAndUsesExactEvalShape) {
    const std::string key("key\0binary", 10);
    const std::string value("v\0x", 3);
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_EQ(5u, command.size());
        EXPECT_EQ("EVAL_RO", command[0]);
        EXPECT_NE(std::string::npos, command[1].find("EXISTS"));
        EXPECT_NE(std::string::npos, command[1].find("GETRANGE"));
        EXPECT_EQ("1", command[2]);
        EXPECT_EQ(key, command[3]);
        EXPECT_EQ("3", command[4]);
        if (connection == 0) {
            return;
        }
        ASSERT_EQ(1u, connection);
        EXPECT_TRUE(SendAll(fd, BoundedReply(true, value)));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());
    const auto result = client.GetStringsUntil({key}, {value.size()}, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ(EC_OK, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), result.errors);
    EXPECT_EQ((std::vector<std::string>{value}), result.values);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, BoundedReadDistinguishesMissingEmptyAndSizeMismatch) {
    LoopbackRespServer server([](int fd, std::size_t) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        const std::vector<std::string> replies{BoundedReply(false, ""),
                                               BoundedReply(true, ""),
                                               BoundedReply(true, "abc"),
                                               BoundedReply(true, "ab"),
                                               BoundedReply(true, "abcd")};
        for (const std::string &reply : replies) {
            ASSERT_TRUE(ReadCommand(fd, command));
            ASSERT_TRUE(SendAll(fd, reply));
        }
    });
    RedisClient client(TestUri(server.Port()));
    ASSERT_TRUE(client.Prepare());
    const auto result = client.GetStringsUntil(
        {"missing", "empty", "exact", "short", "oversized"}, {0, 0, 3, 3, 3}, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ(EC_OK, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT, EC_OK, EC_OK, EC_MISMATCH, EC_MISMATCH}), result.errors);
    EXPECT_EQ("", result.values[1]);
    EXPECT_EQ("abc", result.values[2]);
}

TEST(RedisClientDeadlineTest, BoundedReadCommandErrorRetainsContextWithConfiguredTimeout) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        std::this_thread::sleep_for(25ms);
        ASSERT_TRUE(SendAll(fd, "-NOPERM EVAL_RO denied\r\n"));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", "legacy"}), command);
        std::this_thread::sleep_for(kLegacyReplyDelay);
        ASSERT_TRUE(SendAll(fd, "$6\r\nlegacy\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 1, "127.0.0.1", kConfiguredTimeoutMs));
    ASSERT_TRUE(client.Prepare());

    const auto deadline_start = std::chrono::steady_clock::now();
    const auto result = client.GetStringsUntil({"key"}, {1}, deadline_start + kDeadlineBudget);
    const auto deadline_elapsed = std::chrono::steady_clock::now() - deadline_start;
    EXPECT_EQ(EC_ERROR, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_ERROR}), result.errors);
    ASSERT_LT(deadline_elapsed, kMaxDeadlineOperationDuration);

    std::vector<std::string> values;
    const auto legacy_start = std::chrono::steady_clock::now();
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({"legacy"}, values));
    EXPECT_GE(std::chrono::steady_clock::now() - legacy_start, kLegacyReplyDelay);
    EXPECT_EQ((std::vector<std::string>{"legacy"}), values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, BoundedReadRejectsMalformedReplyAndDiscardsContext) {
    LoopbackRespServer server([](int fd, std::size_t) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, "*1\r\n:1\r\n"));
        char byte = 0;
        EXPECT_LE(recv(fd, &byte, 1, 0), 0);
    });
    RedisClient client(TestUri(server.Port()));
    ASSERT_TRUE(client.Prepare());
    const auto result = client.GetStringsUntil({"key"}, {1}, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ(EC_CORRUPTION, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_CORRUPTION}), result.errors);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, BoundedReadParserErrorDoesNotRetry) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, "$invalid\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());
    const auto result = client.GetStringsUntil({"key"}, {1}, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ(EC_CORRUPTION, result.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_CORRUPTION}), result.errors);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, BoundedReadValidatesExpectedSizeRangeAndRejectsOverflowWithoutIo) {
    const std::string max_valid = std::to_string(std::numeric_limits<int64_t>::max() - 1);
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_EQ(5u, command.size());
        EXPECT_EQ(max_valid, command[4]);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
    });
    RedisClient client(TestUri(server.Port()));
    ASSERT_TRUE(client.Prepare());
    if (std::numeric_limits<std::size_t>::max() >= static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
        const auto valid_result =
            client.GetStringsUntil({"key"},
                                   {static_cast<std::size_t>(std::numeric_limits<int64_t>::max() - 1)},
                                   std::chrono::steady_clock::now() + 1s);
        EXPECT_EQ(EC_OK, valid_result.operation_error);
        EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT}), valid_result.errors);

        const auto invalid_result =
            client.GetStringsUntil({"key"},
                                   {static_cast<std::size_t>(std::numeric_limits<int64_t>::max())},
                                   std::chrono::steady_clock::now() + 1s);
        EXPECT_EQ(EC_BADARGS, invalid_result.operation_error);
        EXPECT_EQ(1u, server.Connections());
    }
}

TEST(RedisClientDeadlineTest, BoundedReadChargesInclusiveByteAndSaturatesNearInt64Limit) {
    std::atomic<bool> unexpected_pipelining{false};
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        pollfd descriptor{fd, POLLIN, 0};

        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(ReadCommand(fd, command));
        unexpected_pipelining.store(poll(&descriptor, 1, 100) > 0, std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));

        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_EQ(5u, command.size());
        EXPECT_EQ(std::to_string(std::numeric_limits<int64_t>::max() - 1), command[4]);
        descriptor.revents = 0;
        unexpected_pipelining.store(unexpected_pipelining.load(std::memory_order_acquire) ||
                                        poll(&descriptor, 1, 100) > 0,
                                    std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
    });
    RedisClient client(TestUri(server.Port()));
    ASSERT_TRUE(client.Prepare());

    constexpr std::size_t kResponseLimit = 1024 * 1024;
    const auto boundary = client.GetStringsUntil({"half-1", "half-2", "tail"},
                                                 {kResponseLimit / 2 - 1, kResponseLimit / 2 - 1, 0},
                                                 std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ(std::vector<ErrorCode>(3, EC_NOENT), boundary.errors);

    if (std::numeric_limits<std::size_t>::max() >= static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
        const auto near_limit =
            client.GetStringsUntil({"near-limit", "near-limit-tail"},
                                   {static_cast<std::size_t>(std::numeric_limits<int64_t>::max() - 1), 0},
                                   std::chrono::steady_clock::now() + 3s);
        EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT, EC_NOENT}), near_limit.errors);
    }
    EXPECT_FALSE(unexpected_pipelining.load(std::memory_order_acquire));
}

TEST(RedisClientDeadlineTest, BoundedReadChunksAtCommandAndResponseLimits) {
    std::atomic<bool> extra_before_first_reply{false};
    LoopbackRespServer server([&](int fd, std::size_t) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < 64; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
        }
        pollfd descriptor{fd, POLLIN, 0};
        extra_before_first_reply.store(poll(&descriptor, 1, 100) > 0, std::memory_order_release);
        for (std::size_t i = 0; i < 64; ++i) {
            ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        }
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));

        ASSERT_TRUE(ReadCommand(fd, command));
        descriptor.revents = 0;
        extra_before_first_reply.store(extra_before_first_reply.load(std::memory_order_acquire) ||
                                           poll(&descriptor, 1, 100) > 0,
                                       std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(true, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(true, "")));

        ASSERT_TRUE(ReadCommand(fd, command));
        descriptor.revents = 0;
        extra_before_first_reply.store(extra_before_first_reply.load(std::memory_order_acquire) ||
                                           poll(&descriptor, 1, 100) > 0,
                                       std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));

        ASSERT_TRUE(ReadCommand(fd, command));
        descriptor.revents = 0;
        extra_before_first_reply.store(extra_before_first_reply.load(std::memory_order_acquire) ||
                                           poll(&descriptor, 1, 100) > 0,
                                       std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));

        ASSERT_TRUE(ReadCommand(fd, command));
        descriptor.revents = 0;
        extra_before_first_reply.store(extra_before_first_reply.load(std::memory_order_acquire) ||
                                           poll(&descriptor, 1, 100) > 0,
                                       std::memory_order_release);
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_TRUE(SendAll(fd, BoundedReply(false, "")));
    });
    RedisClient client(TestUri(server.Port()));
    ASSERT_TRUE(client.Prepare());
    std::vector<std::string> keys(65);
    std::vector<std::size_t> sizes(65, 0);
    for (std::size_t i = 0; i < keys.size(); ++i) {
        keys[i] = "count-" + std::to_string(i);
    }
    auto result = client.GetStringsUntil(keys, sizes, std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ(std::vector<ErrorCode>(65, EC_NOENT), result.errors);

    result =
        client.GetStringsUntil({"large-1", "large-2"}, {600 * 1024, 600 * 1024}, std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_MISMATCH, EC_MISMATCH}), result.errors);

    const std::string large_key_1(600 * 1024, 'a');
    const std::string large_key_2(600 * 1024, 'b');
    result = client.GetStringsUntil({large_key_1, large_key_2}, {0, 0}, std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT, EC_NOENT}), result.errors);

    result = client.GetStringsUntil(
        {"oversized-response", "response-tail"}, {1024 * 1024 + 1, 0}, std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT, EC_NOENT}), result.errors);

    const std::string oversized_key(1024 * 1024 + 1, 'z');
    result = client.GetStringsUntil({oversized_key, "request-tail"}, {0, 0}, std::chrono::steady_clock::now() + 3s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_NOENT, EC_NOENT}), result.errors);
    EXPECT_FALSE(extra_before_first_reply.load(std::memory_order_acquire));
}

} // namespace
} // namespace kv_cache_manager
