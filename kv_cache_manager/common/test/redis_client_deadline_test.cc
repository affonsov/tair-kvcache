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
constexpr auto kNoNextChunkObservation = 500ms;
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

bool HasSocketEvent(int fd, std::chrono::milliseconds timeout) {
    pollfd descriptor{fd, POLLIN, 0};
    int result = 0;
    do {
        result = poll(&descriptor, 1, static_cast<int>(timeout.count()));
    } while (result < 0 && errno == EINTR);
    EXPECT_GE(result, 0);
    return result != 0;
}

std::string BulkReply(const std::string &value) { return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n"; }

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

bool CompleteHandshake(int fd) {
    std::vector<std::string> command;
    return ReadCommand(fd, command) && command == std::vector<std::string>{"PING"} && SendAll(fd, "+PONG\r\n");
}

TEST(RedisClientDeadlineTest, CommandViewsReferenceCallerOwnedBuffers) {
    const std::string key("key\0binary", 10);
    const std::string value("value\0binary", 12);
    const std::string script = "return redis.call('GET',KEYS[1])";
    const std::string expected_size = std::to_string(value.size());

    const auto set_command = redis_client_internal::BuildCommandView("SET", key, value);
    ASSERT_EQ(3, set_command.argc);
    EXPECT_EQ(key.data(), set_command.argv[1]);
    EXPECT_EQ(key.size(), set_command.lengths[1]);
    EXPECT_EQ(value.data(), set_command.argv[2]);
    EXPECT_EQ(value.size(), set_command.lengths[2]);

    const auto get_command = redis_client_internal::BuildCommandView("GET", key);
    ASSERT_EQ(2, get_command.argc);
    EXPECT_EQ(key.data(), get_command.argv[1]);
    EXPECT_EQ(key.size(), get_command.lengths[1]);

    const auto eval_command = redis_client_internal::BuildCommandView("EVAL_RO", script, "1", key, expected_size);
    ASSERT_EQ(5, eval_command.argc);
    EXPECT_EQ(script.data(), eval_command.argv[1]);
    EXPECT_EQ(script.size(), eval_command.lengths[1]);
    EXPECT_EQ(key.data(), eval_command.argv[3]);
    EXPECT_EQ(key.size(), eval_command.lengths[3]);
    EXPECT_EQ(expected_size.data(), eval_command.argv[4]);
    EXPECT_EQ(expected_size.size(), eval_command.lengths[4]);
}

TEST(RedisClientDeadlineTest, StringPipelinesPreserveBinaryBuffers) {
    std::string large_value(256 * 1024, 'v');
    large_value[17] = '\0';
    large_value[large_value.size() - 1] = static_cast<char>(0xff);
    const std::vector<std::string> legacy_keys{std::string("legacy\0key", 10)};
    const std::vector<std::string> legacy_values{large_value};
    const std::vector<std::string> deadline_keys{std::string("deadline\0key", 12)};
    const std::vector<std::string> deadline_values{large_value};
    const std::vector<std::size_t> expected_sizes{large_value.size()};

    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", legacy_keys[0], legacy_values[0]}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", legacy_keys[0]}), command);
        ASSERT_TRUE(SendAll(fd, "$" + std::to_string(large_value.size()) + "\r\n" + large_value + "\r\n"));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", deadline_keys[0], deadline_values[0]}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

        ASSERT_TRUE(ReadCommand(fd, command));
        ASSERT_EQ(5u, command.size());
        EXPECT_EQ("EVAL_RO", command[0]);
        EXPECT_EQ("1", command[2]);
        EXPECT_EQ(deadline_keys[0], command[3]);
        EXPECT_EQ(std::to_string(expected_sizes[0]), command[4]);
        ASSERT_TRUE(SendAll(fd, BoundedReply(true, large_value)));
    });
    RedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Prepare());
    ASSERT_TRUE(client.Open());

    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.SetStrings(legacy_keys, legacy_values));
    std::vector<std::string> values;
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings(legacy_keys, values));
    EXPECT_EQ(legacy_values, values);

    const auto set_result =
        client.SetStringsUntil(deadline_keys, deadline_values, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), set_result.errors);

    const auto get_result =
        client.GetStringsUntil(deadline_keys, expected_sizes, std::chrono::steady_clock::now() + 2s);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), get_result.errors);
    EXPECT_EQ(deadline_values, get_result.values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyStringPipelinesChunkAndPreserveReplyAlignment) {
    constexpr std::size_t kChunkSize = 64;
    constexpr std::size_t kItemCount = kChunkSize + 1;
    constexpr std::size_t kErrorIndex = 7;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kItemCount);
    values.reserve(kItemCount);
    for (std::size_t i = 0; i < kItemCount; ++i) {
        keys.push_back("key-" + std::to_string(i));
        values.push_back("value-" + std::to_string(i));
    }

    std::atomic<bool> extra_command_before_reply{false};
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;

        for (std::size_t i = 0; i < kChunkSize; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", keys[i], values[i]}), command);
        }
        extra_command_before_reply.store(HasSocketEvent(fd, kNoNextChunkObservation), std::memory_order_release);
        for (std::size_t i = 0; i < kChunkSize; ++i) {
            ASSERT_TRUE(SendAll(fd, i == kErrorIndex ? "-ERR rejected\r\n" : "+OK\r\n"));
        }
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", keys.back(), values.back()}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

        for (std::size_t i = 0; i < kChunkSize; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"GET", keys[i]}), command);
        }
        extra_command_before_reply.store(extra_command_before_reply.load(std::memory_order_acquire) ||
                                             HasSocketEvent(fd, kNoNextChunkObservation),
                                         std::memory_order_release);
        for (std::size_t i = 0; i < kChunkSize; ++i) {
            const std::string reply =
                i == kErrorIndex ? "$-1\r\n" : "$" + std::to_string(values[i].size()) + "\r\n" + values[i] + "\r\n";
            ASSERT_TRUE(SendAll(fd, reply));
        }
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", keys.back()}), command);
        ASSERT_TRUE(SendAll(fd, "$" + std::to_string(values.back().size()) + "\r\n" + values.back() + "\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Open());

    std::vector<ErrorCode> expected_errors(kItemCount, EC_OK);
    expected_errors[kErrorIndex] = EC_ERROR;
    EXPECT_EQ(expected_errors, client.SetStrings(keys, values));

    expected_errors[kErrorIndex] = EC_NOENT;
    std::vector<std::string> actual_values;
    EXPECT_EQ(expected_errors, client.GetStrings(keys, actual_values));
    values[kErrorIndex].clear();
    EXPECT_EQ(values, actual_values);
    EXPECT_FALSE(extra_command_before_reply.load(std::memory_order_acquire));
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacySetRequiresExactOkAndPreservesMixedReplyAlignment) {
    const std::vector<std::string> keys{std::string("status\0key", 10), "integer-key", "error-key", "ok-key"};
    const std::vector<std::string> values{
        std::string("status\0value", 12), "integer-value", "error-value", std::string("ok\0value", 8)};

    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", keys[i], values[i]}), command);
        }

        ASSERT_TRUE(SendAll(fd, std::string("+OK\0suffix\r\n", sizeof("+OK\0suffix\r\n") - 1)));
        ASSERT_TRUE(SendAll(fd, ":1\r\n"));
        ASSERT_TRUE(SendAll(fd, "-ERR rejected\r\n"));
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Open());

    EXPECT_EQ((std::vector<ErrorCode>{EC_ERROR, EC_ERROR, EC_ERROR, EC_OK}), client.SetStrings(keys, values));
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacySetPartialRepliesDoNotReplayWrites) {
    const std::vector<std::string> keys{"first", std::string("second\0key", 10), "third"};
    const std::vector<std::string> values{"first-value", std::string("second\0value", 12), "third-value"};

    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", keys[i], values[i]}), command);
        }
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Open());

    EXPECT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), client.SetStrings(keys, values));
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyGetPreservesMixedReplyAlignmentAcrossChunks) {
    constexpr std::size_t kChunkSize = 64;
    constexpr std::size_t kItemCount = kChunkSize + 1;
    std::vector<std::string> keys;
    std::vector<std::string> expected_values(kItemCount);
    keys.reserve(kItemCount);
    for (std::size_t i = 0; i < kItemCount; ++i) {
        keys.push_back("mixed-key-" + std::to_string(i));
        if (i != 1 && i != 2 && i != 3) {
            expected_values[i] = "mixed-value-" + std::to_string(i);
        }
    }
    keys[0] = std::string("bulk\0key", 8);
    expected_values[0] = std::string("bulk\0value", 10);
    keys.back() = std::string("next\0key", 8);
    expected_values.back() = std::string("next\0value", 10);

    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (std::size_t i = 0; i < kChunkSize; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"GET", keys[i]}), command);
        }
        for (std::size_t i = 0; i < kChunkSize; ++i) {
            if (i == 1) {
                ASSERT_TRUE(SendAll(fd, "-ERR rejected\r\n"));
            } else if (i == 2) {
                ASSERT_TRUE(SendAll(fd, "$-1\r\n"));
            } else if (i == 3) {
                ASSERT_TRUE(SendAll(fd, ":7\r\n"));
            } else {
                ASSERT_TRUE(SendAll(fd, BulkReply(expected_values[i])));
            }
        }
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", keys.back()}), command);
        ASSERT_TRUE(SendAll(fd, BulkReply(expected_values.back())));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Open());

    std::vector<ErrorCode> expected_errors(kItemCount, EC_OK);
    expected_errors[1] = EC_ERROR;
    expected_errors[2] = EC_NOENT;
    expected_errors[3] = EC_ERROR;
    std::vector<std::string> actual_values;
    EXPECT_EQ(expected_errors, client.GetStrings(keys, actual_values));
    EXPECT_EQ(expected_values, actual_values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyGetPartialRepliesReplayWholeChunkFresh) {
    const std::vector<std::string> keys{
        "first", std::string("second\0key", 10), "third", std::string("fourth\0key", 10)};
    const std::vector<std::string> expected_values{
        std::string("fresh\0first", 11), "fresh-second", std::string("fresh\0third", 11), "fresh-fourth"};

    LoopbackRespServer server([&](int fd, std::size_t connection) {
        ASSERT_LE(connection, 1u);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        for (const std::string &key : keys) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"GET", key}), command);
        }
        if (connection == 0) {
            ASSERT_TRUE(SendAll(fd, BulkReply(std::string("stale\0first", 11))));
            return;
        }
        for (const std::string &value : expected_values) {
            ASSERT_TRUE(SendAll(fd, BulkReply(value)));
        }
    });
    RedisClient client(TestUri(server.Port(), 2));
    ASSERT_TRUE(client.Open());

    std::vector<std::string> actual_values;
    EXPECT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), client.GetStrings(keys, actual_values));
    EXPECT_EQ(expected_values, actual_values);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, StringPipelinesEnforcePayloadBoundaryOverLoopbackResp) {
    constexpr std::size_t kRequestPayloadLimit = 1024 * 1024;

    const std::string set_boundary_key_1("set-boundary\0a", sizeof("set-boundary\0a") - 1);
    const std::string set_boundary_key_2 = "set-boundary-b";
    std::string set_boundary_value_2(
        kRequestPayloadLimit - 2 * (sizeof("SET") - 1) - set_boundary_key_1.size() - set_boundary_key_2.size(), 'b');
    set_boundary_value_2[17] = '\0';
    set_boundary_value_2.back() = static_cast<char>(0xff);
    std::string oversized_set_value(kRequestPayloadLimit + 1, 'o');
    oversized_set_value[23] = '\0';
    oversized_set_value.back() = static_cast<char>(0x80);
    const std::vector<std::string> set_keys{
        set_boundary_key_1, set_boundary_key_2, "set-oversized", std::string("set-tail\0", 9)};
    const std::vector<std::string> set_values{"", set_boundary_value_2, oversized_set_value, "tail-value"};

    const std::string get_boundary_key_1("get-boundary\0a", sizeof("get-boundary\0a") - 1);
    std::string get_boundary_key_2(kRequestPayloadLimit - 2 * (sizeof("GET") - 1) - get_boundary_key_1.size(), 'g');
    get_boundary_key_2[19] = '\0';
    get_boundary_key_2.back() = static_cast<char>(0xfe);
    std::string oversized_get_key(kRequestPayloadLimit - (sizeof("GET") - 1) + 1, 'k');
    oversized_get_key[29] = '\0';
    oversized_get_key.back() = static_cast<char>(0x81);
    const std::vector<std::string> get_keys{
        get_boundary_key_1, get_boundary_key_2, oversized_get_key, std::string("get-tail\0", 9)};
    const std::vector<std::string> get_values{
        std::string("first\0value", 11), "second-value", std::string("oversized\0value", 15), "tail-value"};

    ASSERT_EQ(kRequestPayloadLimit,
              2 * (sizeof("SET") - 1) + set_keys[0].size() + set_values[0].size() + set_keys[1].size() +
                  set_values[1].size());
    ASSERT_GT((sizeof("SET") - 1) + set_keys[2].size() + set_values[2].size(), kRequestPayloadLimit);
    ASSERT_EQ(kRequestPayloadLimit, 2 * (sizeof("GET") - 1) + get_keys[0].size() + get_keys[1].size());
    ASSERT_GT((sizeof("GET") - 1) + get_keys[2].size(), kRequestPayloadLimit);

    std::atomic<bool> premature_next_chunk{false};
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        EXPECT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;

        auto observe_no_next_chunk = [&] {
            if (HasSocketEvent(fd, kNoNextChunkObservation)) {
                premature_next_chunk.store(true, std::memory_order_release);
            }
        };
        auto serve_set = [&](const bool reject_second_boundary_item) {
            for (std::size_t i = 0; i < 2; ++i) {
                ASSERT_TRUE(ReadCommand(fd, command));
                EXPECT_EQ((std::vector<std::string>{"SET", set_keys[i], set_values[i]}), command);
            }
            observe_no_next_chunk();
            ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
            ASSERT_TRUE(SendAll(fd, reject_second_boundary_item ? "-ERR rejected\r\n" : "+OK\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", set_keys[2], set_values[2]}), command);
            observe_no_next_chunk();
            ASSERT_TRUE(SendAll(fd, "+OK\r\n"));

            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", set_keys[3], set_values[3]}), command);
            ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
        };

        serve_set(true);

        for (std::size_t i = 0; i < 2; ++i) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"GET", get_keys[i]}), command);
        }
        observe_no_next_chunk();
        ASSERT_TRUE(SendAll(fd, BulkReply(get_values[0])));
        ASSERT_TRUE(SendAll(fd, BulkReply(get_values[1])));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", get_keys[2]}), command);
        observe_no_next_chunk();
        ASSERT_TRUE(SendAll(fd, BulkReply(get_values[2])));

        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", get_keys[3]}), command);
        ASSERT_TRUE(SendAll(fd, BulkReply(get_values[3])));

        serve_set(false);
    });
    RedisClient client(TestUri(server.Port(), 1));
    ASSERT_TRUE(client.Prepare());
    ASSERT_TRUE(client.Open());

    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_ERROR, EC_OK, EC_OK}), client.SetStrings(set_keys, set_values));

    std::vector<std::string> actual_values;
    EXPECT_EQ(std::vector<ErrorCode>(get_keys.size(), EC_OK), client.GetStrings(get_keys, actual_values));
    EXPECT_EQ(get_values, actual_values);

    const auto deadline_result = client.SetStringsUntil(set_keys, set_values, std::chrono::steady_clock::now() + 5s);
    EXPECT_EQ(EC_OK, deadline_result.operation_error);
    EXPECT_EQ(std::vector<ErrorCode>(set_keys.size(), EC_OK), deadline_result.errors);
    EXPECT_EQ(std::vector<ValkeyWriteCertainty>(set_keys.size(), ValkeyWriteCertainty::CONFIRMED),
              deadline_result.certainties);
    EXPECT_EQ(set_keys.size(), deadline_result.confirmed_count);
    EXPECT_EQ(0u, deadline_result.RejectedCount());
    EXPECT_EQ(0u, deadline_result.ambiguous_count);
    EXPECT_EQ(0u, deadline_result.not_sent_count);

    EXPECT_FALSE(premature_next_chunk.load(std::memory_order_acquire));
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyGetProtocolErrorDoesNotReconnectOrReplay) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_EQ(0u, connection);
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", "malformed"}), command);
        ASSERT_TRUE(SendAll(fd, "$invalid\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Open());

    std::vector<std::string> values;
    EXPECT_EQ((std::vector<ErrorCode>{EC_CORRUPTION}), client.GetStrings({"malformed"}, values));
    EXPECT_EQ((std::vector<std::string>{""}), values);
    EXPECT_EQ(1u, server.Connections());
}

TEST(RedisClientDeadlineTest, LegacyGetEofReconnectsAndSucceedsFresh) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"GET", "transport"}), command);
        if (connection == 0) {
            return;
        }
        ASSERT_EQ(1u, connection);
        ASSERT_TRUE(SendAll(fd, "$5\r\nfresh\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 2));
    ASSERT_TRUE(client.Open());

    std::vector<std::string> values;
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), client.GetStrings({"transport"}, values));
    EXPECT_EQ((std::vector<std::string>{"fresh"}), values);
    EXPECT_EQ(2u, server.Connections());
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

void VerifySetErrorReplyIsRejected(const std::string &error_reply) {
    LoopbackRespServer server([&](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        if (connection == 0) {
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", "rejected", "value"}), command);
            ASSERT_TRUE(ReadCommand(fd, command));
            EXPECT_EQ((std::vector<std::string>{"SET", "ambiguous", "value"}), command);
            ASSERT_TRUE(SendAll(fd, error_reply));
            return;
        }
        ASSERT_EQ(1u, connection);
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", "fresh", "value"}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());

    const auto rejected =
        client.SetStringsUntil({"rejected", "ambiguous"}, {"value", "value"}, std::chrono::steady_clock::now() + 1s);
    EXPECT_EQ(EC_ERROR, rejected.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_ERROR, EC_IO_ERROR}), rejected.errors);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::REJECTED, ValkeyWriteCertainty::AMBIGUOUS}),
              rejected.certainties);
    EXPECT_EQ(0u, rejected.confirmed_count);
    EXPECT_EQ(1u, rejected.RejectedCount());
    EXPECT_EQ(1u, rejected.ambiguous_count);
    EXPECT_EQ(0u, rejected.not_sent_count);
    EXPECT_EQ(1u, server.Connections());

    const auto fresh = client.SetStringsUntil({"fresh"}, {"value"}, std::chrono::steady_clock::now() + 1s);
    EXPECT_EQ(EC_OK, fresh.operation_error);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::CONFIRMED}), fresh.certainties);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, NopermSetErrorIsRejectedWithoutRetryAndReconnectsFresh) {
    VerifySetErrorReplyIsRejected("-NOPERM SET denied\r\n");
}

TEST(RedisClientDeadlineTest, ReadonlySetErrorIsRejectedWithoutRetryAndReconnectsFresh) {
    VerifySetErrorReplyIsRejected("-READONLY replica rejects writes\r\n");
}

TEST(RedisClientDeadlineTest, SetUnexpectedReplyCorruptsOnlyCurrentCommandAndReconnectsFresh) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        if (connection == 0) {
            for (std::size_t i = 0; i < 64; ++i) {
                ASSERT_TRUE(ReadCommand(fd, command));
                EXPECT_EQ((std::vector<std::string>{"SET", "malformed-" + std::to_string(i), "value"}), command);
            }
            ASSERT_TRUE(SendAll(fd, "+OK\r\n:1\r\n"));
            char byte = 0;
            EXPECT_EQ(0, recv(fd, &byte, 1, 0));
            return;
        }
        ASSERT_EQ(1u, connection);
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", "fresh", "value"}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());

    std::vector<std::string> keys(65);
    std::vector<std::string> values(keys.size(), "value");
    for (std::size_t i = 0; i < keys.size(); ++i) {
        keys[i] = "malformed-" + std::to_string(i);
    }
    const auto malformed =
        client.SetStringsUntil(keys, values, std::chrono::steady_clock::now() + std::chrono::seconds(1));

    EXPECT_EQ(EC_CORRUPTION, malformed.operation_error);
    EXPECT_EQ(EC_OK, malformed.errors[0]);
    EXPECT_EQ(ValkeyWriteCertainty::CONFIRMED, malformed.certainties[0]);
    EXPECT_EQ(EC_CORRUPTION, malformed.errors[1]);
    EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, malformed.certainties[1]);
    for (std::size_t i = 2; i < 64; ++i) {
        EXPECT_EQ(EC_IO_ERROR, malformed.errors[i]);
        EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, malformed.certainties[i]);
    }
    EXPECT_EQ(EC_IO_ERROR, malformed.errors[64]);
    EXPECT_EQ(ValkeyWriteCertainty::NOT_SENT, malformed.certainties[64]);
    EXPECT_EQ(1u, malformed.confirmed_count);
    EXPECT_EQ(0u, malformed.RejectedCount());
    EXPECT_EQ(63u, malformed.ambiguous_count);
    EXPECT_EQ(1u, malformed.not_sent_count);
    EXPECT_EQ(1u, server.Connections());

    const auto fresh = client.SetStringsUntil({"fresh"}, {"value"}, std::chrono::steady_clock::now() + 1s);
    EXPECT_EQ(EC_OK, fresh.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), fresh.errors);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::CONFIRMED}), fresh.certainties);
    EXPECT_EQ(2u, server.Connections());
}

TEST(RedisClientDeadlineTest, SetMalformedWireReplyCorruptsOnlyParserFailedCommandAndReconnectsFresh) {
    LoopbackRespServer server([](int fd, std::size_t connection) {
        ASSERT_TRUE(CompleteHandshake(fd));
        std::vector<std::string> command;
        if (connection == 0) {
            for (std::size_t i = 0; i < 64; ++i) {
                ASSERT_TRUE(ReadCommand(fd, command));
                EXPECT_EQ((std::vector<std::string>{"SET", "malformed-wire-" + std::to_string(i), "value"}), command);
            }
            ASSERT_TRUE(SendAll(fd, "+OK\r\n$invalid\r\n"));
            char byte = 0;
            EXPECT_EQ(0, recv(fd, &byte, 1, 0));
            return;
        }
        ASSERT_EQ(1u, connection);
        ASSERT_TRUE(ReadCommand(fd, command));
        EXPECT_EQ((std::vector<std::string>{"SET", "fresh", "value"}), command);
        ASSERT_TRUE(SendAll(fd, "+OK\r\n"));
    });
    RedisClient client(TestUri(server.Port(), 3));
    ASSERT_TRUE(client.Prepare());

    std::vector<std::string> keys(65);
    std::vector<std::string> values(keys.size(), "value");
    for (std::size_t i = 0; i < keys.size(); ++i) {
        keys[i] = "malformed-wire-" + std::to_string(i);
    }
    const auto malformed =
        client.SetStringsUntil(keys, values, std::chrono::steady_clock::now() + std::chrono::seconds(1));

    EXPECT_EQ(EC_CORRUPTION, malformed.operation_error);
    EXPECT_EQ(EC_OK, malformed.errors[0]);
    EXPECT_EQ(ValkeyWriteCertainty::CONFIRMED, malformed.certainties[0]);
    EXPECT_EQ(EC_CORRUPTION, malformed.errors[1]);
    EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, malformed.certainties[1]);
    for (std::size_t i = 2; i < 64; ++i) {
        EXPECT_EQ(EC_IO_ERROR, malformed.errors[i]);
        EXPECT_EQ(ValkeyWriteCertainty::AMBIGUOUS, malformed.certainties[i]);
    }
    EXPECT_EQ(EC_IO_ERROR, malformed.errors[64]);
    EXPECT_EQ(ValkeyWriteCertainty::NOT_SENT, malformed.certainties[64]);
    EXPECT_EQ(1u, malformed.confirmed_count);
    EXPECT_EQ(0u, malformed.RejectedCount());
    EXPECT_EQ(63u, malformed.ambiguous_count);
    EXPECT_EQ(1u, malformed.not_sent_count);
    EXPECT_EQ(1u, server.Connections());

    const auto fresh = client.SetStringsUntil({"fresh"}, {"value"}, std::chrono::steady_clock::now() + 1s);
    EXPECT_EQ(EC_OK, fresh.operation_error);
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), fresh.errors);
    EXPECT_EQ((std::vector<ValkeyWriteCertainty>{ValkeyWriteCertainty::CONFIRMED}), fresh.certainties);
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
