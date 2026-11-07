#include "kv_cache_manager/common/redis_client.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_set>

#include "kv_cache_manager/common/logger.h"
#include "unistd.h"

namespace kv_cache_manager {

#define KVCM_REDIS_LOG_INFO(format, ...)                                                                               \
    KVCM_LOG_INFO("redis client addr[%s:%ld] " format, host_.c_str(), port_, ##__VA_ARGS__)
#define KVCM_REDIS_LOG_WARN(format, ...)                                                                               \
    KVCM_LOG_WARN("redis client addr[%s:%ld] " format, host_.c_str(), port_, ##__VA_ARGS__)
#define KVCM_REDIS_LOG_ERROR(format, ...)                                                                              \
    KVCM_LOG_ERROR("redis client addr[%s:%ld] " format, host_.c_str(), port_, ##__VA_ARGS__)

namespace {

class ScopedSocket {
public:
    explicit ScopedSocket(const int fd) : fd_(fd) {}
    ~ScopedSocket() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    ScopedSocket(const ScopedSocket &) = delete;
    ScopedSocket &operator=(const ScopedSocket &) = delete;

    int Get() const { return fd_; }
    int Release() {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

// Cap per-pipeline hiredis command/reply bookkeeping while retaining useful
// batching for Valkey block transfers.
constexpr std::size_t kStringPipelineMaxCommandCount = 64;
// Cap copied raw request arguments at 1 MiB per pipeline. This counts SET plus
// key/value bytes or GET plus key bytes; RESP framing is not copied here.
constexpr std::size_t kStringPipelineMaxRequestPayloadBytes = 1024 * 1024;

std::size_t SaturatingAdd(const std::size_t lhs, const std::size_t rhs) noexcept {
    const auto max = std::numeric_limits<std::size_t>::max();
    return rhs > max - lhs ? max : lhs + rhs;
}

std::size_t SetStringRequestPayloadBytes(const std::string &key, const std::string &value) noexcept {
    return SaturatingAdd(SaturatingAdd(sizeof("SET") - 1, key.size()), value.size());
}

std::size_t GetStringRequestPayloadBytes(const std::string &key) noexcept {
    return SaturatingAdd(sizeof("GET") - 1, key.size());
}

bool IsExactStatus(const redisReply *reply, const char *status) {
    const std::size_t length = strlen(status);
    return reply && reply->type == REDIS_REPLY_STATUS && reply->str && reply->len == length &&
           memcmp(reply->str, status, length) == 0;
}

bool IsRetryableHiredisTransportError(const int error) noexcept {
    return error == REDIS_ERR_IO || error == REDIS_ERR_EOF || error == REDIS_ERR_TIMEOUT;
}

template <typename RequestPayloadBytes>
std::size_t StringPipelineChunkEnd(const std::size_t item_count,
                                   const std::size_t begin,
                                   RequestPayloadBytes request_payload_bytes) {
    std::size_t end = begin;
    std::size_t chunk_payload_bytes = 0;
    while (end < item_count && end - begin < kStringPipelineMaxCommandCount) {
        const std::size_t item_payload_bytes = request_payload_bytes(end);
        if (end != begin && (chunk_payload_bytes > kStringPipelineMaxRequestPayloadBytes ||
                             item_payload_bytes > kStringPipelineMaxRequestPayloadBytes - chunk_payload_bytes)) {
            break;
        }
        chunk_payload_bytes = SaturatingAdd(chunk_payload_bytes, item_payload_bytes);
        ++end;
        if (chunk_payload_bytes > kStringPipelineMaxRequestPayloadBytes) {
            // A valid oversized item runs alone instead of being rejected.
            break;
        }
    }
    return end;
}

constexpr std::size_t kValkeyBatchMaxResponsePayloadBytes = 1024 * 1024;
constexpr auto kReconnectDelay = std::chrono::milliseconds(50);
// A compliant Valkey server returns at most expected_size + 1 bytes. Stock
// hiredis still trusts a RESP bulk length supplied by an authenticated server;
// this command cannot defend against a malicious server lying in that length.
constexpr char kBoundedGetScript[] = "local e=redis.call('EXISTS',KEYS[1]); if e==0 then return {0,''} end; "
                                     "return {1,redis.call('GETRANGE',KEYS[1],0,ARGV[1])}";

std::chrono::milliseconds RemainingMilliseconds(const RedisClient::Deadline deadline) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
        return std::chrono::milliseconds::zero();
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) + std::chrono::milliseconds(1);
}

bool FillSocketAddress(
    const int family, const std::string &address, const int64_t port, sockaddr_storage &storage, socklen_t &length) {
    memset(&storage, 0, sizeof(storage));
    if (family == AF_INET) {
        auto *addr = reinterpret_cast<sockaddr_in *>(&storage);
        addr->sin_family = AF_INET;
        addr->sin_port = htons(static_cast<uint16_t>(port));
        length = sizeof(*addr);
        return inet_pton(AF_INET, address.c_str(), &addr->sin_addr) == 1;
    }
    if (family == AF_INET6) {
        auto *addr = reinterpret_cast<sockaddr_in6 *>(&storage);
        addr->sin6_family = AF_INET6;
        addr->sin6_port = htons(static_cast<uint16_t>(port));
        length = sizeof(*addr);
        return inet_pton(AF_INET6, address.c_str(), &addr->sin6_addr) == 1;
    }
    return false;
}

std::size_t BoundedGetRequestPayloadBytes(const std::string &key, const std::string &expected_size) noexcept {
    return SaturatingAdd(
        SaturatingAdd(
            SaturatingAdd(SaturatingAdd(sizeof("EVAL_RO") - 1, sizeof(kBoundedGetScript) - 1), sizeof("1") - 1),
            key.size()),
        expected_size.size());
}

std::size_t BoundedGetChunkEnd(const std::vector<std::string> &keys,
                               const std::vector<std::size_t> &expected_sizes,
                               const std::vector<std::string> &expected_size_args,
                               const std::size_t begin) {
    std::size_t end = begin;
    std::size_t request_bytes = 0;
    std::size_t response_bytes = 0;
    while (end < keys.size() && end - begin < kStringPipelineMaxCommandCount) {
        const std::size_t item_request_bytes = BoundedGetRequestPayloadBytes(keys[end], expected_size_args[end]);
        const std::size_t item_response_bytes = SaturatingAdd(expected_sizes[end], 1);
        if (end != begin && (request_bytes > kStringPipelineMaxRequestPayloadBytes ||
                             item_request_bytes > kStringPipelineMaxRequestPayloadBytes - request_bytes ||
                             response_bytes > kValkeyBatchMaxResponsePayloadBytes ||
                             item_response_bytes > kValkeyBatchMaxResponsePayloadBytes - response_bytes)) {
            break;
        }
        request_bytes = SaturatingAdd(request_bytes, item_request_bytes);
        response_bytes = SaturatingAdd(response_bytes, item_response_bytes);
        ++end;
        if (request_bytes > kStringPipelineMaxRequestPayloadBytes ||
            response_bytes > kValkeyBatchMaxResponsePayloadBytes) {
            break;
        }
    }
    return end;
}

} // namespace

class RedisClient::DeadlineWatchdog {
public:
    DeadlineWatchdog(RedisClient &client, const Deadline deadline)
        : client_(client)
        , deadline_(deadline)
        , uncaught_exceptions_(std::uncaught_exceptions())
        , thread_([this] { Run(); }) {}

    ~DeadlineWatchdog() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
            cv_.notify_all();
        }
        thread_.join();
        std::lock_guard<std::mutex> lock(mutex_);
        CloseDuplicate();
        if (std::uncaught_exceptions() > uncaught_exceptions_) {
            client_.Disconnect();
        }
    }

    bool Arm(const int socket_fd) {
        const int duplicate = fcntl(socket_fd, F_DUPFD_CLOEXEC, 0);
        if (duplicate < 0) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        CloseDuplicate();
        if (fired_ || stop_) {
            close(duplicate);
            return false;
        }
        duplicate_fd_ = duplicate;
        cv_.notify_all();
        return true;
    }

    void Disarm() {
        std::lock_guard<std::mutex> lock(mutex_);
        CloseDuplicate();
    }

    bool DidExpire() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return fired_;
    }

private:
    void Run() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_until(lock, deadline_, [this] { return stop_; })) {
            fired_ = true;
            if (duplicate_fd_ >= 0) {
                (void)shutdown(duplicate_fd_, SHUT_RDWR);
            }
        }
    }

    void CloseDuplicate() {
        if (duplicate_fd_ >= 0) {
            close(duplicate_fd_);
            duplicate_fd_ = -1;
        }
    }

    RedisClient &client_;
    Deadline deadline_;
    int uncaught_exceptions_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    int duplicate_fd_ = -1;
    bool fired_ = false;
    bool stop_ = false;
    std::thread thread_;
};

RedisClient::RedisClient(const StandardUri &storage_uri)
    : user_info_(storage_uri.GetUserInfo()), host_(storage_uri.GetHostName()), port_(storage_uri.GetPort()) {
    int64_t tmp_db = 0;
    storage_uri.GetParamAs("db", tmp_db);
    if (tmp_db >= 0) {
        db_ = tmp_db;
    }
    int64_t tmp_timeout_ms = 0;
    storage_uri.GetParamAs("timeout_ms", tmp_timeout_ms);
    if (tmp_timeout_ms > 0) {
        timeout_ms_ = tmp_timeout_ms;
    }
    int64_t tmp_retry_count = 0;
    storage_uri.GetParamAs("retry_count", tmp_retry_count);
    if (tmp_retry_count > 0) {
        retry_count_ = tmp_retry_count;
    }
    int64_t tmp_randomkey_batch_num = 0;
    storage_uri.GetParamAs("randomkey_batch_num", tmp_randomkey_batch_num);
    if (tmp_randomkey_batch_num > 0) {
        randomkey_batch_num_ = tmp_randomkey_batch_num;
    }
}

RedisClient::~RedisClient() { Close(); }

bool RedisClient::Prepare() {
    numeric_addresses_.clear();
    prepared_ = false;
    if (port_ <= 0 || port_ > std::numeric_limits<uint16_t>::max()) {
        KVCM_LOG_ERROR("valkey prepare stage[resolve] host[%s] failed: invalid port", host_.c_str());
        return false;
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICSERV;
    addrinfo *addresses = nullptr;
    const std::string service = std::to_string(port_);
    const int resolve_result = getaddrinfo(host_.c_str(), service.c_str(), &hints, &addresses);
    if (resolve_result != 0) {
        KVCM_LOG_ERROR(
            "valkey prepare stage[resolve] host[%s] failed[%s]", host_.c_str(), gai_strerror(resolve_result));
        return false;
    }

    std::unordered_set<std::string> seen;
    for (const addrinfo *address = addresses; address != nullptr; address = address->ai_next) {
        if (address->ai_family != AF_INET && address->ai_family != AF_INET6) {
            continue;
        }
        char numeric_host[NI_MAXHOST]{};
        if (getnameinfo(address->ai_addr,
                        address->ai_addrlen,
                        numeric_host,
                        sizeof(numeric_host),
                        nullptr,
                        0,
                        NI_NUMERICHOST) != 0) {
            continue;
        }
        const std::string identity = std::to_string(address->ai_family) + ":" + numeric_host;
        if (seen.insert(identity).second) {
            numeric_addresses_.push_back({address->ai_family, numeric_host});
        }
    }
    freeaddrinfo(addresses);
    prepared_ = !numeric_addresses_.empty();
    if (!prepared_) {
        KVCM_LOG_ERROR("valkey prepare stage[resolve] host[%s] failed: no usable IPv4/IPv6 address", host_.c_str());
        return false;
    }
    KVCM_LOG_INFO("valkey prepare stage[resolve] host[%s] addresses[%zu] outcome[ready]",
                  host_.c_str(),
                  numeric_addresses_.size());
    return true;
}

bool RedisClient::ApplyRemainingSocketTimeout(const Deadline deadline) {
    if (!context_) {
        return false;
    }
    const auto remaining = RemainingMilliseconds(deadline);
    if (remaining <= std::chrono::milliseconds::zero()) {
        return false;
    }
    timeval timeout{};
    timeout.tv_sec = remaining.count() / 1000;
    timeout.tv_usec = (remaining.count() % 1000) * 1000;
    return redisSetTimeout(context_, timeout) == REDIS_OK;
}

void RedisClient::RestoreConfiguredSocketTimeoutOrDisconnect() {
    if (!context_) {
        return;
    }
    timeval timeout{};
    timeout.tv_sec = timeout_ms_ / 1000;
    timeout.tv_usec = (timeout_ms_ % 1000) * 1000;
    if (context_->err || redisSetTimeout(context_, timeout) != REDIS_OK) {
        KVCM_REDIS_LOG_WARN("failed to restore configured socket timeout; disconnecting");
        Disconnect();
    }
}

bool RedisClient::ConnectNumericAddressUntil(const NumericAddress &numeric_address,
                                             const Deadline deadline,
                                             DeadlineWatchdog &watchdog) {
    if (std::chrono::steady_clock::now() >= deadline) {
        return false;
    }

    sockaddr_storage socket_address{};
    socklen_t socket_address_length = 0;
    if (!FillSocketAddress(
            numeric_address.family, numeric_address.address, port_, socket_address, socket_address_length)) {
        return false;
    }

    ScopedSocket socket_fd(socket(numeric_address.family, SOCK_STREAM, IPPROTO_TCP));
    if (socket_fd.Get() < 0) {
        return false;
    }
    (void)fcntl(socket_fd.Get(), F_SETFD, fcntl(socket_fd.Get(), F_GETFD, 0) | FD_CLOEXEC);
    const int original_flags = fcntl(socket_fd.Get(), F_GETFL, 0);
    if (original_flags < 0 || fcntl(socket_fd.Get(), F_SETFL, original_flags | O_NONBLOCK) != 0 ||
        !watchdog.Arm(socket_fd.Get())) {
        return false;
    }

    int connect_result =
        connect(socket_fd.Get(), reinterpret_cast<const sockaddr *>(&socket_address), socket_address_length);
    if (connect_result != 0 && errno == EINPROGRESS) {
        while (std::chrono::steady_clock::now() < deadline) {
            const auto remaining = RemainingMilliseconds(deadline);
            const auto max_poll = std::chrono::milliseconds(std::numeric_limits<int>::max());
            const int poll_timeout = static_cast<int>(std::min(remaining, max_poll).count());
            pollfd descriptor{socket_fd.Get(), POLLOUT, 0};
            connect_result = poll(&descriptor, 1, poll_timeout);
            if (connect_result < 0 && errno == EINTR) {
                continue;
            }
            if (connect_result > 0) {
                int socket_error = 0;
                socklen_t socket_error_length = sizeof(socket_error);
                connect_result = getsockopt(socket_fd.Get(), SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_length);
                if (connect_result == 0 && socket_error != 0) {
                    errno = socket_error;
                    connect_result = -1;
                }
            } else if (connect_result == 0) {
                errno = ETIMEDOUT;
                connect_result = -1;
            }
            break;
        }
    }
    if (connect_result != 0 || std::chrono::steady_clock::now() >= deadline ||
        fcntl(socket_fd.Get(), F_SETFL, original_flags) != 0) {
        watchdog.Disarm();
        return false;
    }

    context_ = AdoptConnectedSocket(socket_fd.Get());
    if (context_) {
        (void)socket_fd.Release();
    }

    auto fail_connection = [this, &watchdog] {
        Disconnect();
        watchdog.Disarm();
        return false;
    };
    try {
        if (!context_ || context_->err || !watchdog.Arm(context_->fd) || !ApplyRemainingSocketTimeout(deadline)) {
            return fail_connection();
        }

        auto completed_in_time = [&watchdog, deadline] {
            return std::chrono::steady_clock::now() < deadline && !watchdog.DidExpire();
        };

        ReplyUPtr ping_reply = ExecuteCommand(redis_client_internal::BuildCommandView("PING"));
        if (!completed_in_time() || !ping_reply) {
            return fail_connection();
        }
        if (ping_reply->type == REDIS_REPLY_ERROR) {
            const std::string error(ping_reply->str ? ping_reply->str : "", ping_reply->len);
            if (error.compare(0, sizeof("NOAUTH") - 1, "NOAUTH") != 0) {
                return fail_connection();
            }
            if (!ApplyRemainingSocketTimeout(deadline)) {
                return fail_connection();
            }
            ReplyUPtr auth_reply = ExecuteCommand(redis_client_internal::BuildCommandView("AUTH", user_info_));
            if (!completed_in_time() || !IsExactStatus(auth_reply.get(), "OK")) {
                return fail_connection();
            }
            if (!ApplyRemainingSocketTimeout(deadline)) {
                return fail_connection();
            }
            ping_reply = ExecuteCommand(redis_client_internal::BuildCommandView("PING"));
            if (!completed_in_time()) {
                return fail_connection();
            }
        }
        if (!IsExactStatus(ping_reply.get(), "PONG")) {
            return fail_connection();
        }

        if (db_ > 0) {
            if (!ApplyRemainingSocketTimeout(deadline)) {
                return fail_connection();
            }
            ReplyUPtr select_reply =
                ExecuteCommand(redis_client_internal::BuildCommandView("SELECT", std::to_string(db_)));
            if (!completed_in_time() || !IsExactStatus(select_reply.get(), "OK")) {
                return fail_connection();
            }
        }
        if (!completed_in_time()) {
            return fail_connection();
        }
        return true;
    } catch (...) {
        (void)fail_connection();
        throw;
    }
}

bool RedisClient::ConnectUntil(const Deadline deadline, DeadlineWatchdog &watchdog) {
    Disconnect();
    watchdog.Disarm();
    for (const NumericAddress &numeric_address : numeric_addresses_) {
        if (ConnectNumericAddressUntil(numeric_address, deadline, watchdog)) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
    }
    Disconnect();
    watchdog.Disarm();
    return false;
}

bool RedisClient::ReconnectUntil(const Deadline deadline, DeadlineWatchdog &watchdog) {
    for (int64_t attempt = 0; attempt < retry_count_ && std::chrono::steady_clock::now() < deadline; ++attempt) {
        if (ConnectUntil(deadline, watchdog)) {
            return true;
        }
        if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_until(std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
        }
    }
    KVCM_LOG_WARN("valkey operation stage[reconnect] host[%s] attempts[%ld] deadline_expired[%d] outcome[failed]",
                  host_.c_str(),
                  retry_count_,
                  std::chrono::steady_clock::now() >= deadline);
    return false;
}

bool RedisClient::IsContextOk() const {
    if (!context_ || context_->err) {
        std::string msg = (context_ ? context_->errstr : "Cannot allocate redis context");
        KVCM_REDIS_LOG_WARN("redis context error[%s]", msg.c_str());
        return false;
    }
    return true;
}

bool RedisClient::IsReplyOk(const redisReply *reply) const {
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        std::string msg = (reply ? reply->str : "invalid nullptr redis reply");
        KVCM_REDIS_LOG_WARN("redis reply error[%s]", msg.c_str());
        return false;
    }
    return true;
}

bool RedisClient::CheckReplyInteger(const redisReply *reply) const {
    if (!reply) {
        KVCM_REDIS_LOG_ERROR("redis check reply integer, reply is nullptr");
        return false;
    }
    if (reply->type != REDIS_REPLY_INTEGER || reply->integer < 0) {
        KVCM_REDIS_LOG_ERROR(
            "redis reply check integer type fail, type[%d] integer[%lld] ", reply->type, reply->integer);
        return false;
    }
    return true;
}

bool RedisClient::CheckReplyArray(const redisReply *reply) const {
    if (!reply) {
        KVCM_REDIS_LOG_ERROR("redis check reply array fail, reply is nullptr");
        return false;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        KVCM_REDIS_LOG_ERROR("redis reply check array type fail, type[%d]", reply->type);
        return false;
    }
    return true;
}

// out_str is empty if it is nil
bool RedisClient::GetReplyStrOrNil(const redisReply *reply, std::string &out_str) const {
    out_str.clear();
    if (!reply) {
        KVCM_REDIS_LOG_ERROR("redis get reply str or nil fail, reply is nullptr");
        return false;
    }
    if (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_NIL) {
        KVCM_REDIS_LOG_ERROR("redis get reply str or nil fail, unexpected reply type[%d]", reply->type);
        return false;
    }
    out_str = (reply->type == REDIS_REPLY_STRING ? std::string(reply->str) : std::string());
    return true;
}

bool RedisClient::Connect() {
    Disconnect();

    struct timeval timeout;
    timeout.tv_sec = timeout_ms_ / 1000;
    timeout.tv_usec = (timeout_ms_ % 1000) * 1000;
    context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    if (!IsContextOk()) {
        KVCM_REDIS_LOG_WARN("fail to connect to redis");
        return false;
    }

    ReplyUPtr ping_reply_1((redisReply *)redisCommand(context_, "PING"), freeReplyObject);
    if (!ping_reply_1) {
        KVCM_REDIS_LOG_WARN("want to ping first to check if auth needed, but ping fail, get nullptr reply");
        return false;
    }
    if (IsReplyOk(ping_reply_1.get())) {
        // no need to auth, do nothing
    } else {
        assert(ping_reply_1->type == REDIS_REPLY_ERROR);
        std::string error_str(ping_reply_1->str);
        static const std::string auth_str = "NOAUTH";
        if (error_str.size() >= auth_str.size() && error_str.compare(0, auth_str.size(), auth_str.c_str()) == 0) {
            // do auth
            ReplyUPtr auth_reply((redisReply *)redisCommand(context_, "AUTH %s", user_info_.c_str()), freeReplyObject);
            if (!IsExactStatus(auth_reply.get(), "OK")) {
                KVCM_REDIS_LOG_WARN("auth fail");
                return false;
            }
            ReplyUPtr ping_reply_2((redisReply *)redisCommand(context_, "PING"), freeReplyObject);
            if (!IsExactStatus(ping_reply_2.get(), "PONG")) {
                KVCM_REDIS_LOG_WARN("ping fail after auth");
                return false;
            }
            KVCM_REDIS_LOG_INFO("connect to redis, auth successfully");
        } else {
            KVCM_REDIS_LOG_WARN("unexpected ping error str[%s]", error_str.c_str());
            return false;
        }
    }

    if (db_ > 0) {
        ReplyUPtr select_reply((redisReply *)redisCommand(context_, "SELECT %ld", db_), freeReplyObject);
        if (!IsExactStatus(select_reply.get(), "OK")) {
            KVCM_REDIS_LOG_WARN("fail to select redis db");
            return false;
        }
    }

    KVCM_REDIS_LOG_INFO("connect to redis db[%ld] timeout_ms[%ld] retry_count[%ld]", db_, timeout_ms_, retry_count_);
    return true;
}

void RedisClient::Disconnect() {
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
}

bool RedisClient::Reconnect() {
    for (int32_t count = 0; count < retry_count_; ++count) {
        if (Connect()) {
            return true;
        }
        usleep(50 * 1000);
    }
    KVCM_REDIS_LOG_ERROR("fail to reconnect redis after [%ld] times", retry_count_);
    return false;
}

// return empty vector if failed
std::vector<RedisClient::ReplyUPtr> RedisClient::TryExecPipeline(const std::vector<CmdArgs> &cmds) {
    std::vector<ReplyUPtr> replies;
    for (const CmdArgs &cmd : cmds) {
        std::vector<const char *> argv;
        std::vector<size_t> argvlen;
        argv.reserve(cmd.size());
        argvlen.reserve(cmd.size());
        for (const auto &cmd_arg : cmd) {
            argv.push_back(cmd_arg.data());
            argvlen.push_back(cmd_arg.size());
        }
        if (redisAppendCommandArgv(context_, (int)argv.size(), argv.data(), argvlen.data()) != REDIS_OK) {
            KVCM_REDIS_LOG_WARN("redisAppendCommandArgv failed: %s", context_->errstr);
            return replies;
        }
    }

    replies.reserve(cmds.size());
    for (size_t i = 0; i < cmds.size(); ++i) {
        redisReply *r = nullptr;
        if (redisGetReply(context_, (void **)&r) != REDIS_OK) {
            KVCM_REDIS_LOG_WARN("redisGetReply failed: %s", context_->errstr);
            freeReplyObject(r);
            replies.clear();
            return replies;
        }
        replies.emplace_back(r, freeReplyObject);
    }
    return replies;
}

// return empty vector if failed
std::vector<RedisClient::ReplyUPtr> RedisClient::CommandPipeline(const std::vector<CmdArgs> &cmds) {
    std::vector<ReplyUPtr> replies;
    (void)CommandPipeline(cmds, replies);
    return replies;
}

ErrorCode RedisClient::CommandPipeline(const std::vector<CmdArgs> &cmds, std::vector<ReplyUPtr> &out_replies) {
    out_replies.clear();
    if (cmds.empty()) {
        return EC_BADARGS;
    }
    for (int32_t count = 0; count < retry_count_; ++count) {
        if (!IsContextOk() && !Reconnect()) {
            KVCM_REDIS_LOG_ERROR("fail to reconnect before pipeline, try count[%d]", count);
            return EC_IO_ERROR;
        }

        out_replies = TryExecPipeline(cmds);
        if (!out_replies.empty()) {
            return EC_OK;
        }
        if (IsContextOk()) {
            KVCM_REDIS_LOG_ERROR("pipeline fail but connection is ok, try count[%d]", count);
            return EC_ERROR;
        }
        KVCM_REDIS_LOG_WARN("pipeline fail, connection not ok, try count[%d]", count);
        usleep(50 * 1000);
    }

    KVCM_REDIS_LOG_ERROR("pipeline all fail, try count[%ld]", retry_count_);
    return EC_IO_ERROR;
}

std::vector<ErrorCode> RedisClient::BatchWrite(const std::vector<CmdArgs> &cmds, bool &out_all_ok) {
    out_all_ok = false;
    auto replies = CommandPipeline(cmds);
    if (replies.size() != cmds.size()) {
        return std::vector<ErrorCode>(cmds.size(), EC_ERROR);
    }
    std::vector<ErrorCode> error_codes;
    error_codes.reserve(replies.size());
    bool has_error = false;
    for (auto &reply : replies) {
        if (!IsReplyOk(reply.get()) || !CheckReplyInteger(reply.get())) {
            error_codes.push_back(EC_ERROR);
            has_error = true;
        } else {
            error_codes.push_back(EC_OK);
        }
    }
    out_all_ok = !has_error;
    return error_codes;
}

bool RedisClient::Open() {
    if (!Reconnect()) {
        KVCM_REDIS_LOG_ERROR("fail to connect in open");
        return false;
    }
    return true;
}

void RedisClient::Close() { Disconnect(); }

std::vector<ErrorCode> RedisClient::SetStrings(const std::vector<std::string> &keys,
                                               const std::vector<std::string> &values) {
    if (keys.size() != values.size()) {
        KVCM_REDIS_LOG_ERROR("redis set strings fail, keys.size[%lu] != values.size[%lu]", keys.size(), values.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }
    if (keys.empty()) {
        return {};
    }

    std::vector<ErrorCode> ec_per_key(keys.size(), EC_ERROR);
    for (std::size_t chunk_begin = 0; chunk_begin < keys.size();) {
        const std::size_t chunk_end = StringPipelineChunkEnd(keys.size(), chunk_begin, [&](const std::size_t index) {
            return SetStringRequestPayloadBytes(keys[index], values[index]);
        });

        std::vector<ReplyUPtr> set_replies;
        const ErrorCode pipeline_ec =
            ExecuteStringPipeline(StringCommand::SET, keys, &values, chunk_begin, chunk_end, set_replies);
        if (pipeline_ec != EC_OK || set_replies.size() != chunk_end - chunk_begin) {
            KVCM_REDIS_LOG_ERROR("redis set strings fail, chunk begin[%zu] commands[%zu] replies[%zu] pipeline_ec[%d]",
                                 chunk_begin,
                                 chunk_end - chunk_begin,
                                 set_replies.size(),
                                 static_cast<int>(pipeline_ec));
            break;
        }

        for (std::size_t i = 0; i < set_replies.size(); ++i) {
            const redisReply *reply = set_replies[i].get();
            if (IsReplyOk(reply) && reply->type == REDIS_REPLY_STATUS && reply->str && reply->len == 2 &&
                memcmp(reply->str, "OK", 2) == 0) {
                ec_per_key[chunk_begin + i] = EC_OK;
            }
        }
        chunk_begin = chunk_end;
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::GetStrings(const std::vector<std::string> &keys,
                                               std::vector<std::string> &out_values) {
    out_values.assign(keys.size(), std::string());
    if (keys.empty()) {
        return {};
    }

    std::vector<ErrorCode> ec_per_key(keys.size(), EC_ERROR);
    for (std::size_t chunk_begin = 0; chunk_begin < keys.size();) {
        const std::size_t chunk_end = StringPipelineChunkEnd(keys.size(), chunk_begin, [&](const std::size_t index) {
            return GetStringRequestPayloadBytes(keys[index]);
        });

        std::vector<ReplyUPtr> get_replies;
        const ErrorCode pipeline_ec =
            ExecuteStringPipeline(StringCommand::GET, keys, nullptr, chunk_begin, chunk_end, get_replies);
        if (pipeline_ec != EC_OK || get_replies.size() != chunk_end - chunk_begin) {
            KVCM_REDIS_LOG_ERROR("redis get strings fail, chunk begin[%zu] commands[%zu] replies[%zu] pipeline_ec[%d]",
                                 chunk_begin,
                                 chunk_end - chunk_begin,
                                 get_replies.size(),
                                 static_cast<int>(pipeline_ec));
            const ErrorCode failure = pipeline_ec == EC_OK ? EC_ERROR : pipeline_ec;
            std::fill(ec_per_key.begin() + chunk_begin, ec_per_key.begin() + chunk_end, failure);
            break;
        }

        for (std::size_t i = 0; i < get_replies.size(); ++i) {
            const redisReply *reply = get_replies[i].get();
            if (!IsReplyOk(reply)) {
                continue;
            }
            const std::size_t result_index = chunk_begin + i;
            if (reply->type == REDIS_REPLY_NIL) {
                ec_per_key[result_index] = EC_NOENT;
            } else if (reply->type == REDIS_REPLY_STRING && reply->str) {
                out_values[result_index].assign(reply->str, reply->len);
                ec_per_key[result_index] = EC_OK;
            }
        }
        chunk_begin = chunk_end;
    }
    return ec_per_key;
}

bool RedisClient::AppendCommand(const redis_client_internal::CommandView &command) {
    std::array<const char *, redis_client_internal::kMaxCommandArguments> argv = command.argv;
    return redisAppendCommandArgv(context_, command.argc, argv.data(), command.lengths.data()) == REDIS_OK;
}

RedisClient::ReplyUPtr RedisClient::ExecuteCommand(const redis_client_internal::CommandView &command) {
    std::array<const char *, redis_client_internal::kMaxCommandArguments> argv = command.argv;
    return ReplyUPtr(
        static_cast<redisReply *>(redisCommandArgv(context_, command.argc, argv.data(), command.lengths.data())),
        freeReplyObject);
}

ErrorCode RedisClient::ExecuteStringPipeline(const StringCommand command,
                                             const std::vector<std::string> &keys,
                                             const std::vector<std::string> *values,
                                             const std::size_t begin,
                                             const std::size_t end,
                                             std::vector<ReplyUPtr> &out_replies) {
    out_replies.clear();
    out_replies.reserve(end - begin);
    for (int32_t attempt = 0; attempt < retry_count_; ++attempt) {
        if (!IsContextOk() && !Reconnect()) {
            KVCM_REDIS_LOG_ERROR("fail to reconnect before string pipeline, try count[%d]", attempt);
            return EC_IO_ERROR;
        }

        bool append_ok = true;
        int hiredis_error = REDIS_ERR_OTHER;
        for (std::size_t i = begin; i < end; ++i) {
            redis_client_internal::CommandView command_view;
            if (command == StringCommand::SET) {
                assert(values != nullptr);
                command_view = redis_client_internal::BuildCommandView("SET", keys[i], (*values)[i]);
            } else {
                command_view = redis_client_internal::BuildCommandView("GET", keys[i]);
            }
            if (!AppendCommand(command_view)) {
                append_ok = false;
                hiredis_error = context_ ? context_->err : REDIS_ERR_OTHER;
                break;
            }
        }
        if (!append_ok) {
            Disconnect();
            if (!IsRetryableHiredisTransportError(hiredis_error)) {
                return hiredis_error == REDIS_ERR_PROTOCOL ? EC_CORRUPTION : EC_ERROR;
            }
            if (attempt + 1 < retry_count_) {
                usleep(50 * 1000);
            }
            continue;
        }

        bool replies_ok = true;
        for (std::size_t i = begin; i < end; ++i) {
            redisReply *raw_reply = nullptr;
            if (redisGetReply(context_, reinterpret_cast<void **>(&raw_reply)) != REDIS_OK || !raw_reply) {
                hiredis_error = context_ ? context_->err : REDIS_ERR_OTHER;
                KVCM_REDIS_LOG_WARN("redisGetReply failed with hiredis error[%d]", hiredis_error);
                freeReplyObject(raw_reply);
                replies_ok = false;
                break;
            }
            out_replies.emplace_back(raw_reply, freeReplyObject);
        }
        if (replies_ok) {
            return EC_OK;
        }

        out_replies.clear();
        Disconnect();
        const bool retryable_transport = IsRetryableHiredisTransportError(hiredis_error);
        if (command == StringCommand::SET || !retryable_transport) {
            if (hiredis_error == REDIS_ERR_PROTOCOL) {
                return EC_CORRUPTION;
            }
            return retryable_transport ? EC_IO_ERROR : EC_ERROR;
        }
        if (attempt + 1 < retry_count_) {
            usleep(50 * 1000);
        }
    }

    KVCM_REDIS_LOG_ERROR("string pipeline all fail, try count[%ld]", retry_count_);
    return EC_IO_ERROR;
}

bool RedisClient::AppendDeadlineCommand(const CmdArgs &cmd) {
    std::vector<const char *> argv;
    std::vector<std::size_t> lengths;
    argv.reserve(cmd.size());
    lengths.reserve(cmd.size());
    for (const std::string &argument : cmd) {
        argv.push_back(argument.data());
        lengths.push_back(argument.size());
    }
    return redisAppendCommandArgv(context_, static_cast<int>(argv.size()), argv.data(), lengths.data()) == REDIS_OK;
}

redisContext *RedisClient::AdoptConnectedSocket(const int socket_fd) { return redisConnectFd(socket_fd); }

ValkeySetStringsResult RedisClient::SetStringsUntil(const std::vector<std::string> &keys,
                                                    const std::vector<std::string> &values,
                                                    const Deadline deadline) {
    ValkeySetStringsResult result;
    result.errors.assign(keys.size(), EC_IO_ERROR);
    result.certainties.assign(keys.size(), ValkeyWriteCertainty::NOT_SENT);
    auto finish = [&result]() {
        result.confirmed_count = 0;
        result.ambiguous_count = 0;
        result.not_sent_count = 0;
        for (const ValkeyWriteCertainty certainty : result.certainties) {
            switch (certainty) {
            case ValkeyWriteCertainty::CONFIRMED:
                ++result.confirmed_count;
                break;
            case ValkeyWriteCertainty::REJECTED:
                break;
            case ValkeyWriteCertainty::AMBIGUOUS:
                ++result.ambiguous_count;
                break;
            case ValkeyWriteCertainty::NOT_SENT:
                ++result.not_sent_count;
                break;
            }
        }
        return result;
    };

    if (keys.size() != values.size()) {
        result.operation_error = EC_BADARGS;
        std::fill(result.errors.begin(), result.errors.end(), EC_BADARGS);
        return finish();
    }
    if (keys.empty()) {
        return finish();
    }
    if (!prepared_) {
        result.operation_error = EC_CONFIG_ERROR;
        std::fill(result.errors.begin(), result.errors.end(), EC_CONFIG_ERROR);
        KVCM_LOG_ERROR("valkey set stage[admission] items[%zu] outcome[not_prepared]", keys.size());
        return finish();
    }
    if (std::chrono::steady_clock::now() >= deadline) {
        result.operation_error = EC_TIMEOUT;
        std::fill(result.errors.begin(), result.errors.end(), EC_TIMEOUT);
        KVCM_LOG_WARN("valkey set stage[admission] items[%zu] deadline_expired[1] outcome[not_sent]", keys.size());
        return finish();
    }

    DeadlineWatchdog watchdog(*this, deadline);
    bool abort = false;
    for (std::size_t chunk_begin = 0; chunk_begin < keys.size() && !abort;) {
        if (std::chrono::steady_clock::now() >= deadline) {
            result.operation_error = EC_TIMEOUT;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), EC_TIMEOUT);
            break;
        }
        const std::size_t chunk_end = StringPipelineChunkEnd(keys.size(), chunk_begin, [&](const std::size_t index) {
            return SetStringRequestPayloadBytes(keys[index], values[index]);
        });

        bool appended = false;
        for (int64_t attempt = 0; attempt < retry_count_ && std::chrono::steady_clock::now() < deadline; ++attempt) {
            if (!context_ || context_->err) {
                if (!ReconnectUntil(deadline, watchdog)) {
                    break;
                }
            } else if (!watchdog.Arm(context_->fd) || !ApplyRemainingSocketTimeout(deadline)) {
                Disconnect();
                watchdog.Disarm();
                if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_until(
                        std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
                }
                continue;
            }

            appended = true;
            try {
                for (std::size_t i = chunk_begin; i < chunk_end; ++i) {
                    const auto command = redis_client_internal::BuildCommandView("SET", keys[i], values[i]);
                    if (!AppendCommand(command)) {
                        appended = false;
                        break;
                    }
                }
            } catch (...) {
                Disconnect();
                watchdog.Disarm();
                throw;
            }
            if (appended && std::chrono::steady_clock::now() < deadline) {
                break;
            }
            // redisAppendCommandArgv only formats into hiredis' output buffer. No
            // flush has started, so this chunk remains safe to retry fresh.
            Disconnect();
            watchdog.Disarm();
            appended = false;
            if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_until(std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
            }
        }
        if (!appended) {
            result.operation_error = std::chrono::steady_clock::now() >= deadline ? EC_TIMEOUT : EC_IO_ERROR;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), result.operation_error);
            break;
        }

        for (std::size_t i = chunk_begin; i < chunk_end; ++i) {
            result.certainties[i] = ValkeyWriteCertainty::AMBIGUOUS;
            result.errors[i] = EC_IO_ERROR;
        }
        for (std::size_t reply_index = 0; reply_index < chunk_end - chunk_begin; ++reply_index) {
            redisReply *raw_reply = nullptr;
            const int reply_result = redisGetReply(context_, reinterpret_cast<void **>(&raw_reply));
            ReplyUPtr reply(raw_reply, freeReplyObject);
            const bool completed_in_time = std::chrono::steady_clock::now() < deadline && !watchdog.DidExpire();
            if (reply_result != REDIS_OK || !reply) {
                const std::size_t result_index = chunk_begin + reply_index;
                if (!completed_in_time) {
                    result.operation_error = EC_TIMEOUT;
                    std::fill(result.errors.begin() + result_index, result.errors.end(), EC_TIMEOUT);
                } else if (context_ && context_->err == REDIS_ERR_PROTOCOL) {
                    result.operation_error = EC_CORRUPTION;
                    result.errors[result_index] = EC_CORRUPTION;
                } else {
                    result.operation_error = EC_IO_ERROR;
                    std::fill(result.errors.begin() + result_index, result.errors.end(), EC_IO_ERROR);
                }
                Disconnect();
                watchdog.Disarm();
                abort = true;
                break;
            }

            const std::size_t result_index = chunk_begin + reply_index;
            if (IsExactStatus(reply.get(), "OK")) {
                result.errors[result_index] = EC_OK;
                result.certainties[result_index] = ValkeyWriteCertainty::CONFIRMED;
                if (!completed_in_time) {
                    result.operation_error = EC_TIMEOUT;
                    std::fill(result.errors.begin() + result_index + 1, result.errors.end(), EC_TIMEOUT);
                    Disconnect();
                    watchdog.Disarm();
                    abort = true;
                    break;
                }
                continue;
            }

            if (reply->type == REDIS_REPLY_ERROR) {
                result.certainties[result_index] = ValkeyWriteCertainty::REJECTED;
                result.errors[result_index] = EC_ERROR;
                result.operation_error = completed_in_time ? EC_ERROR : EC_TIMEOUT;
                if (!completed_in_time) {
                    std::fill(result.errors.begin() + result_index + 1, result.errors.end(), EC_TIMEOUT);
                }
            } else if (completed_in_time) {
                result.operation_error = EC_CORRUPTION;
                result.errors[result_index] = EC_CORRUPTION;
            } else {
                result.operation_error = EC_TIMEOUT;
                std::fill(result.errors.begin() + result_index, result.errors.end(), EC_TIMEOUT);
            }
            Disconnect();
            watchdog.Disarm();
            abort = true;
            break;
        }
        if (!abort) {
            chunk_begin = chunk_end;
        }
    }
    watchdog.Disarm();
    const bool deadline_expired = watchdog.DidExpire() || std::chrono::steady_clock::now() >= deadline;
    if (deadline_expired) {
        Disconnect();
        if (result.operation_error == EC_OK) {
            result.operation_error = EC_TIMEOUT;
        }
    } else {
        RestoreConfiguredSocketTimeoutOrDisconnect();
    }
    const ValkeySetStringsResult final_result = finish();
    KVCM_LOG_INFO("valkey set stage[complete] items[%zu] confirmed[%zu] rejected[%zu] ambiguous[%zu] not_sent[%zu] "
                  "operation_error[%d] deadline_expired[%d]",
                  keys.size(),
                  final_result.confirmed_count,
                  final_result.RejectedCount(),
                  final_result.ambiguous_count,
                  final_result.not_sent_count,
                  static_cast<int>(final_result.operation_error),
                  deadline_expired);
    return final_result;
}

ValkeyGetStringsResult RedisClient::GetStringsUntil(const std::vector<std::string> &keys,
                                                    const std::vector<std::size_t> &expected_sizes,
                                                    const Deadline deadline) {
    ValkeyGetStringsResult result;
    result.errors.assign(keys.size(), EC_IO_ERROR);
    result.values.resize(keys.size());
    if (keys.size() != expected_sizes.size()) {
        result.operation_error = EC_BADARGS;
        std::fill(result.errors.begin(), result.errors.end(), EC_BADARGS);
        return result;
    }
    if (keys.empty()) {
        return result;
    }
    std::vector<std::string> expected_size_args;
    expected_size_args.reserve(expected_sizes.size());
    const uint64_t max_expected_size = static_cast<uint64_t>(std::numeric_limits<int64_t>::max() - 1);
    for (const std::size_t expected_size : expected_sizes) {
        if (static_cast<uint64_t>(expected_size) > max_expected_size) {
            result.operation_error = EC_BADARGS;
            std::fill(result.errors.begin(), result.errors.end(), EC_BADARGS);
            return result;
        }
        expected_size_args.push_back(std::to_string(expected_size));
    }
    if (!prepared_) {
        result.operation_error = EC_CONFIG_ERROR;
        std::fill(result.errors.begin(), result.errors.end(), EC_CONFIG_ERROR);
        KVCM_LOG_ERROR("valkey get stage[admission] items[%zu] outcome[not_prepared]", keys.size());
        return result;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
        result.operation_error = EC_TIMEOUT;
        std::fill(result.errors.begin(), result.errors.end(), EC_TIMEOUT);
        KVCM_LOG_WARN("valkey get stage[admission] items[%zu] deadline_expired[1] outcome[not_sent]", keys.size());
        return result;
    }

    DeadlineWatchdog watchdog(*this, deadline);
    bool abort = false;
    for (std::size_t chunk_begin = 0; chunk_begin < keys.size() && !abort;) {
        if (std::chrono::steady_clock::now() >= deadline) {
            result.operation_error = EC_TIMEOUT;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), EC_TIMEOUT);
            break;
        }
        const std::size_t chunk_end = BoundedGetChunkEnd(keys, expected_sizes, expected_size_args, chunk_begin);

        bool completed = false;
        bool deadline_failure = false;
        bool non_retryable_failure = false;
        ErrorCode non_retryable_error = EC_ERROR;
        std::vector<ReplyUPtr> replies;
        // Allocate reply bookkeeping before hiredis can buffer any command.
        replies.reserve(chunk_end - chunk_begin);
        for (int64_t attempt = 0; attempt < retry_count_ && std::chrono::steady_clock::now() < deadline; ++attempt) {
            replies.clear();
            if (!context_ || context_->err) {
                if (!ReconnectUntil(deadline, watchdog)) {
                    break;
                }
            } else if (!watchdog.Arm(context_->fd) || !ApplyRemainingSocketTimeout(deadline)) {
                Disconnect();
                watchdog.Disarm();
                if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_until(
                        std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
                }
                continue;
            }

            bool append_ok = true;
            try {
                for (std::size_t i = chunk_begin; i < chunk_end; ++i) {
                    const auto command = redis_client_internal::BuildCommandView(
                        "EVAL_RO", kBoundedGetScript, "1", keys[i], expected_size_args[i]);
                    if (!AppendCommand(command)) {
                        append_ok = false;
                        break;
                    }
                }
            } catch (...) {
                Disconnect();
                watchdog.Disarm();
                throw;
            }
            if (!append_ok || std::chrono::steady_clock::now() >= deadline) {
                Disconnect();
                watchdog.Disarm();
                if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_until(
                        std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
                }
                continue;
            }

            bool transport_ok = true;
            for (std::size_t i = chunk_begin; i < chunk_end; ++i) {
                redisReply *raw_reply = nullptr;
                const int reply_result = redisGetReply(context_, reinterpret_cast<void **>(&raw_reply));
                const bool completed_in_time = std::chrono::steady_clock::now() < deadline && !watchdog.DidExpire();
                if (reply_result != REDIS_OK || !raw_reply) {
                    freeReplyObject(raw_reply);
                    transport_ok = false;
                    if (!completed_in_time) {
                        deadline_failure = true;
                        break;
                    }
                    const int hiredis_error = context_ ? context_->err : REDIS_ERR_OTHER;
                    const bool retryable_transport = hiredis_error == REDIS_ERR_IO || hiredis_error == REDIS_ERR_EOF ||
                                                     hiredis_error == REDIS_ERR_TIMEOUT;
                    if (!retryable_transport) {
                        non_retryable_failure = true;
                        non_retryable_error = hiredis_error == REDIS_ERR_PROTOCOL ? EC_CORRUPTION : EC_ERROR;
                    }
                    break;
                }
                if (!completed_in_time) {
                    freeReplyObject(raw_reply);
                    transport_ok = false;
                    deadline_failure = true;
                    break;
                }
                replies.emplace_back(raw_reply, freeReplyObject);
            }
            if (transport_ok && replies.size() == chunk_end - chunk_begin &&
                std::chrono::steady_clock::now() < deadline && !watchdog.DidExpire()) {
                completed = true;
                break;
            }
            if (transport_ok) {
                deadline_failure = true;
            }
            Disconnect();
            watchdog.Disarm();
            if (deadline_failure || non_retryable_failure) {
                break;
            }
            // EVAL_RO is read-only, so a transport failure may replay the whole
            // chunk on a fresh numeric-address connection within this deadline.
            if (attempt + 1 < retry_count_ && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_until(std::min(deadline, std::chrono::steady_clock::now() + kReconnectDelay));
            }
        }
        if (deadline_failure) {
            result.operation_error = EC_TIMEOUT;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), EC_TIMEOUT);
            abort = true;
            break;
        }
        if (non_retryable_failure) {
            result.operation_error = non_retryable_error;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), non_retryable_error);
            abort = true;
            break;
        }
        if (!completed) {
            result.operation_error =
                watchdog.DidExpire() || std::chrono::steady_clock::now() >= deadline ? EC_TIMEOUT : EC_IO_ERROR;
            std::fill(result.errors.begin() + chunk_begin, result.errors.end(), result.operation_error);
            break;
        }

        bool protocol_error = false;
        bool command_error = false;
        for (std::size_t i = 0; i < replies.size(); ++i) {
            const redisReply *reply = replies[i].get();
            const std::size_t result_index = chunk_begin + i;
            if (reply->type == REDIS_REPLY_ERROR) {
                result.errors[result_index] = EC_ERROR;
                command_error = true;
                continue;
            }
            if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 || !reply->element || !reply->element[0] ||
                !reply->element[1] || reply->element[0]->type != REDIS_REPLY_INTEGER ||
                (reply->element[0]->integer != 0 && reply->element[0]->integer != 1) ||
                reply->element[1]->type != REDIS_REPLY_STRING || !reply->element[1]->str) {
                result.errors[result_index] = EC_CORRUPTION;
                protocol_error = true;
                continue;
            }
            const bool exists = reply->element[0]->integer == 1;
            const redisReply *value = reply->element[1];
            if (!exists) {
                if (value->len != 0) {
                    result.errors[result_index] = EC_CORRUPTION;
                    protocol_error = true;
                } else {
                    result.errors[result_index] = EC_NOENT;
                }
                continue;
            }
            if (value->len != expected_sizes[result_index]) {
                result.errors[result_index] = EC_MISMATCH;
                continue;
            }
            result.values[result_index].assign(value->str, value->len);
            result.errors[result_index] = EC_OK;
        }
        const bool deadline_expired = watchdog.DidExpire() || std::chrono::steady_clock::now() >= deadline;
        if (deadline_expired) {
            result.operation_error = EC_TIMEOUT;
            std::fill(result.errors.begin() + chunk_begin, result.errors.begin() + chunk_end, EC_TIMEOUT);
            std::fill(result.values.begin() + chunk_begin, result.values.begin() + chunk_end, std::string());
            Disconnect();
            watchdog.Disarm();
            abort = true;
        } else if (protocol_error) {
            result.operation_error = EC_CORRUPTION;
            Disconnect();
            watchdog.Disarm();
            abort = true;
        } else if (command_error) {
            result.operation_error = EC_ERROR;
            abort = true;
        } else {
            chunk_begin = chunk_end;
        }
    }
    watchdog.Disarm();
    const bool deadline_expired = watchdog.DidExpire() || std::chrono::steady_clock::now() >= deadline;
    if (deadline_expired) {
        Disconnect();
        if (result.operation_error == EC_OK) {
            result.operation_error = EC_TIMEOUT;
            std::fill(result.errors.begin(), result.errors.end(), EC_TIMEOUT);
            std::fill(result.values.begin(), result.values.end(), std::string());
        }
    } else {
        RestoreConfiguredSocketTimeoutOrDisconnect();
    }
    KVCM_LOG_INFO("valkey get stage[complete] items[%zu] operation_error[%d] deadline_expired[%d]",
                  keys.size(),
                  static_cast<int>(result.operation_error),
                  deadline_expired);
    return result;
}

// --- Static command builders ---

void RedisClient::BuildSetCmds(const std::vector<std::string> &keys,
                               const std::vector<std::map<std::string, std::string>> &field_maps,
                               std::vector<CmdArgs> &out_cmds) {
    assert(keys.size() == field_maps.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        out_cmds.push_back({"DEL", keys[i]});
        if (!field_maps[i].empty()) {
            CmdArgs hset_cmd;
            hset_cmd.reserve(field_maps[i].size() * 2 + 2);
            hset_cmd.emplace_back("HSET");
            hset_cmd.emplace_back(keys[i]);
            for (const auto &[field_name, field_value] : field_maps[i]) {
                hset_cmd.emplace_back(field_name);
                hset_cmd.emplace_back(field_value);
            }
            out_cmds.emplace_back(std::move(hset_cmd));
        }
    }
}

void RedisClient::BuildHashSetCmds(const std::vector<std::string> &keys,
                                   const std::vector<std::map<std::string, std::string>> &field_maps,
                                   std::vector<CmdArgs> &out_cmds) {
    assert(keys.size() == field_maps.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (field_maps[i].empty())
            continue;
        CmdArgs hset_cmd;
        hset_cmd.reserve(field_maps[i].size() * 2 + 2);
        hset_cmd.emplace_back("HSET");
        hset_cmd.emplace_back(keys[i]);
        for (const auto &[field_name, field_value] : field_maps[i]) {
            hset_cmd.emplace_back(field_name);
            hset_cmd.emplace_back(field_value);
        }
        out_cmds.emplace_back(std::move(hset_cmd));
    }
}

void RedisClient::BuildDeleteCmds(const std::vector<std::string> &keys, std::vector<CmdArgs> &out_cmds) {
    for (const auto &key : keys) {
        out_cmds.push_back({"DEL", key});
    }
}

void RedisClient::BuildHashDeleteCmds(const std::vector<std::string> &keys,
                                      const std::vector<std::vector<std::string>> &field_names_vec,
                                      std::vector<CmdArgs> &out_cmds) {
    assert(keys.size() == field_names_vec.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (field_names_vec[i].empty())
            continue;
        CmdArgs hdel_cmd;
        hdel_cmd.reserve(field_names_vec[i].size() + 2);
        hdel_cmd.emplace_back("HDEL");
        hdel_cmd.emplace_back(keys[i]);
        for (const auto &field_name : field_names_vec[i]) {
            hdel_cmd.emplace_back(field_name);
        }
        out_cmds.emplace_back(std::move(hdel_cmd));
    }
}

// cover old key-fields
std::vector<ErrorCode> RedisClient::Set(const std::vector<std::string> &keys,
                                        const std::vector<std::map<std::string, std::string>> &field_maps) {
    if (keys.size() != field_maps.size()) {
        KVCM_REDIS_LOG_ERROR("redis set fail, keys.size[%lu] != field_maps.size[%lu]", keys.size(), field_maps.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }

    std::vector<CmdArgs> cmds;
    cmds.reserve(keys.size() * 2);
    BuildSetCmds(keys, field_maps, cmds);

    std::vector<ReplyUPtr> replies = CommandPipeline(cmds);
    if (cmds.size() != replies.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis set fail, pipeline cmds.size[%lu] != replies.size[%lu]", cmds.size(), replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    size_t reply_idx = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        const ReplyUPtr &del_reply = replies[reply_idx++];
        if (!IsReplyOk(del_reply.get()) || !CheckReplyInteger(del_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis set fail, key[%s] DEL fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
            if (!field_maps[i].empty()) {
                ++reply_idx;
            }
        } else if (!field_maps[i].empty()) {
            const ReplyUPtr &hset_reply = replies[reply_idx++];
            if (!IsReplyOk(hset_reply.get()) || !CheckReplyInteger(hset_reply.get())) {
                KVCM_REDIS_LOG_ERROR("redis set fail, key[%s] HSET fail", keys[i].c_str());
                ec_per_key.emplace_back(EC_ERROR);
            } else {
                ec_per_key.emplace_back(EC_OK);
            }
        } else {
            ec_per_key.emplace_back(EC_OK);
        }
    }
    return ec_per_key;
}

// return EC_NOENT if key not exist
std::vector<ErrorCode> RedisClient::Update(const std::vector<std::string> &keys,
                                           const std::vector<std::map<std::string, std::string>> &field_maps) {
    if (keys.size() != field_maps.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis update fail, keys.size[%lu] != field_maps.size[%lu]", keys.size(), field_maps.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }

    std::vector<bool> is_exist_vec;
    std::vector<ErrorCode> ec_per_key = Exists(keys, is_exist_vec);
    std::vector<CmdArgs> hset_cmds;
    hset_cmds.reserve(keys.size());
    size_t exist_count = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] != EC_OK) {
            KVCM_REDIS_LOG_ERROR("redis update fail, key[%s] fail in exists", keys[i].c_str());
        } else if (!is_exist_vec[i]) {
            ec_per_key[i] = EC_NOENT;
        } else if (field_maps[i].empty()) {
            // nothing to update
        } else {
            const std::map<std::string, std::string> &field_map = field_maps[i];
            CmdArgs hset_cmd;
            hset_cmd.reserve((field_map.size() + 1) * 2);
            hset_cmd.emplace_back("HSET");
            hset_cmd.emplace_back(keys[i]);
            for (const auto &[field_name, field_value] : field_map) {
                hset_cmd.emplace_back(field_name);
                hset_cmd.emplace_back(field_value);
            }
            hset_cmds.emplace_back(std::move(hset_cmd));
            ++exist_count;
        }
    }
    if (exist_count <= 0) {
        return ec_per_key;
    }

    std::vector<ReplyUPtr> hset_replies = CommandPipeline(hset_cmds);
    if (hset_cmds.size() != hset_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis update fail, pipeline hset_cmds.size[%lu] != hset_replies.size[%lu]",
                             hset_cmds.size(),
                             hset_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    size_t hset_reply_index = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] != EC_OK || field_maps[i].empty()) {
            continue;
        }
        const ReplyUPtr &hset_reply = hset_replies[hset_reply_index++];
        if (!IsReplyOk(hset_reply.get()) || !CheckReplyInteger(hset_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis update fail, key[%s] HSET fail", keys[i].c_str());
            ec_per_key[i] = EC_ERROR;
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::Upsert(const std::vector<std::string> &keys,
                                           const std::vector<std::map<std::string, std::string>> &field_maps) {
    if (keys.size() != field_maps.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis upsert fail, keys.size[%lu] != field_maps.size[%lu]", keys.size(), field_maps.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }

    std::vector<CmdArgs> hset_cmds;
    hset_cmds.reserve(keys.size());
    BuildHashSetCmds(keys, field_maps, hset_cmds);

    std::vector<ReplyUPtr> hset_replies = CommandPipeline(hset_cmds);
    if (hset_cmds.size() != hset_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis upsert fail, pipeline hset_cmds.size[%lu] != hset_replies.size[%lu]",
                             hset_cmds.size(),
                             hset_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    size_t reply_idx = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (field_maps[i].empty()) {
            ec_per_key.emplace_back(EC_OK);
            continue;
        }
        const ReplyUPtr &hset_reply = hset_replies[reply_idx++];
        if (!IsReplyOk(hset_reply.get()) || !CheckReplyInteger(hset_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis upsert fail, key[%s] HSET fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
        } else {
            ec_per_key.emplace_back(EC_OK);
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::Delete(const std::vector<std::string> &keys) {
    std::vector<CmdArgs> del_cmds;
    del_cmds.reserve(keys.size());
    BuildDeleteCmds(keys, del_cmds);

    std::vector<ReplyUPtr> del_replies = CommandPipeline(del_cmds);
    if (del_cmds.size() != del_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis delete fail, pipeline del_cmds.size[%lu] != del_replies.size[%lu]",
                             del_cmds.size(),
                             del_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const ReplyUPtr &del_reply = del_replies[i];
        if (!IsReplyOk(del_reply.get()) || !CheckReplyInteger(del_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis delete fail, key[%s] DEL fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
        } else if (del_reply->integer == 0) {
            ec_per_key.emplace_back(EC_NOENT);
        } else {
            ec_per_key.emplace_back(EC_OK);
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::DeleteFields(const std::vector<std::string> &keys,
                                                 const std::vector<std::vector<std::string>> &field_names_vec) {
    if (keys.size() != field_names_vec.size()) {
        KVCM_REDIS_LOG_ERROR("redis delete fields fail, keys.size[%lu] != field_names_vec.size[%lu]",
                             keys.size(),
                             field_names_vec.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }

    std::vector<CmdArgs> hdel_cmds;
    std::vector<size_t> original_indexes;
    std::vector<ErrorCode> ec_per_key(keys.size(), EC_OK);
    hdel_cmds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (field_names_vec[i].empty()) {
            continue;
        }
        original_indexes.emplace_back(i);
    }
    BuildHashDeleteCmds(keys, field_names_vec, hdel_cmds);
    if (hdel_cmds.empty()) {
        return ec_per_key;
    }

    std::vector<ReplyUPtr> hdel_replies = CommandPipeline(hdel_cmds);
    if (hdel_cmds.size() != hdel_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis delete fields fail, pipeline hdel_cmds.size[%lu] != hdel_replies.size[%lu]",
                             hdel_cmds.size(),
                             hdel_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    assert(original_indexes.size() == hdel_replies.size());
    for (size_t i = 0; i < original_indexes.size(); ++i) {
        const size_t original_idx = original_indexes[i];
        const ReplyUPtr &hdel_reply = hdel_replies[i];
        if (!IsReplyOk(hdel_reply.get()) || !CheckReplyInteger(hdel_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis delete fields fail, key[%s] HDEL fail", keys[original_idx].c_str());
            ec_per_key[original_idx] = EC_ERROR;
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::Get(const std::vector<std::string> &keys,
                                        const std::vector<std::string> &field_names,
                                        std::vector<std::map<std::string, std::string>> &out_field_maps) {
    if (field_names.empty()) {
        KVCM_REDIS_LOG_ERROR("invalid empty field names");
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }
    out_field_maps = std::vector<std::map<std::string, std::string>>(keys.size());

    std::vector<CmdArgs> hmget_cmds;
    hmget_cmds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        CmdArgs hmget_cmd;
        hmget_cmd.reserve(field_names.size() + 2);
        hmget_cmd.emplace_back("HMGET");
        hmget_cmd.emplace_back(keys[i]);
        for (const auto &field_name : field_names) {
            hmget_cmd.emplace_back(field_name);
        }
        hmget_cmds.emplace_back(std::move(hmget_cmd));
    }
    std::vector<ReplyUPtr> hmget_replies = CommandPipeline(hmget_cmds);
    if (keys.size() != hmget_replies.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis get fail, pipeline keys.size[%lu] != hmget_replies.size[%lu]", keys.size(), hmget_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const ReplyUPtr &hmget_reply = hmget_replies[i];
        if (!IsReplyOk(hmget_reply.get()) || !CheckReplyArray(hmget_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis get fail, key[%s] HMGET fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }
        if (hmget_reply->elements != field_names.size()) {
            KVCM_REDIS_LOG_ERROR("redis get fail, key[%s] HMGET reply elements[%lu] != field_names[%lu]",
                                 keys[i].c_str(),
                                 hmget_reply->elements,
                                 field_names.size());
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }

        bool hasError = false;
        std::map<std::string, std::string> &out_field_map = out_field_maps[i];
        for (size_t j = 0; j < hmget_reply->elements; ++j) {
            const std::string &field_name = field_names[j];
            const redisReply *field_value_reply = hmget_reply->element[j];
            std::string field_value;
            if (!GetReplyStrOrNil(field_value_reply, field_value)) {
                KVCM_REDIS_LOG_ERROR(
                    "redis get fail, key[%s] field_name[%s] get reply str fail", keys[i].c_str(), field_name.c_str());
                out_field_map.clear();
                hasError = true;
                break;
            }
            // Skip nil values: treat them as non-existent fields
            if (!field_value.empty()) {
                out_field_map.emplace(field_name, std::move(field_value));
            }
        }
        if (hasError) {
            ec_per_key.emplace_back(EC_ERROR);
        } else if (out_field_map.empty()) {
            // All fields are nil: key does not exist or has no matching fields
            ec_per_key.emplace_back(EC_NOENT);
        } else {
            ec_per_key.emplace_back(EC_OK);
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::Get(const std::vector<std::string> &keys,
                                        const std::vector<std::vector<std::string>> &field_names_vec,
                                        std::vector<std::map<std::string, std::string>> &out_field_maps) {
    if (keys.size() != field_names_vec.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis get fail, keys.size[%lu] != field_names_vec.size[%lu]", keys.size(), field_names_vec.size());
        return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
    }

    out_field_maps = std::vector<std::map<std::string, std::string>>(keys.size());

    std::vector<CmdArgs> hmget_cmds;
    hmget_cmds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::vector<std::string> &field_names = field_names_vec[i];
        if (field_names.empty()) {
            KVCM_REDIS_LOG_ERROR("invalid empty field names for key[%s]", keys[i].c_str());
            return std::vector<ErrorCode>(keys.size(), EC_BADARGS);
        }
        CmdArgs hmget_cmd;
        hmget_cmd.reserve(field_names.size() + 2);
        hmget_cmd.emplace_back("HMGET");
        hmget_cmd.emplace_back(keys[i]);
        for (const auto &field_name : field_names) {
            hmget_cmd.emplace_back(field_name);
        }
        hmget_cmds.emplace_back(std::move(hmget_cmd));
    }
    std::vector<ReplyUPtr> hmget_replies = CommandPipeline(hmget_cmds);
    if (keys.size() != hmget_replies.size()) {
        KVCM_REDIS_LOG_ERROR(
            "redis get fail, pipeline keys.size[%lu] != hmget_replies.size[%lu]", keys.size(), hmget_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::vector<std::string> &field_names = field_names_vec[i];
        const ReplyUPtr &hmget_reply = hmget_replies[i];
        if (!IsReplyOk(hmget_reply.get()) || !CheckReplyArray(hmget_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis get fail, key[%s] HMGET fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }
        if (hmget_reply->elements != field_names.size()) {
            KVCM_REDIS_LOG_ERROR("redis get fail, key[%s] HMGET reply elements[%lu] != field_names[%lu]",
                                 keys[i].c_str(),
                                 hmget_reply->elements,
                                 field_names.size());
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }

        bool hasError = false;
        std::map<std::string, std::string> &out_field_map = out_field_maps[i];
        for (size_t j = 0; j < hmget_reply->elements; ++j) {
            const std::string &field_name = field_names[j];
            const redisReply *field_value_reply = hmget_reply->element[j];
            std::string field_value;
            if (!GetReplyStrOrNil(field_value_reply, field_value)) {
                KVCM_REDIS_LOG_ERROR(
                    "redis get fail, key[%s] field_name[%s] get reply str fail", keys[i].c_str(), field_name.c_str());
                out_field_map.clear();
                hasError = true;
                break;
            }
            if (!field_value.empty()) {
                out_field_map.emplace(field_name, std::move(field_value));
            }
        }
        if (hasError) {
            ec_per_key.emplace_back(EC_ERROR);
        } else if (out_field_map.empty()) {
            ec_per_key.emplace_back(EC_NOENT);
        } else {
            ec_per_key.emplace_back(EC_OK);
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::GetAllFields(const std::vector<std::string> &keys,
                                                 std::vector<std::map<std::string, std::string>> &out_field_maps) {
    out_field_maps = std::vector<std::map<std::string, std::string>>(keys.size());

    std::vector<CmdArgs> hgetall_cmds;
    hgetall_cmds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        CmdArgs hgetall_cmd{"HGETALL", keys[i]};
        hgetall_cmds.emplace_back(std::move(hgetall_cmd));
    }
    std::vector<ReplyUPtr> hgetall_replies = CommandPipeline(hgetall_cmds);
    if (hgetall_cmds.size() != hgetall_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis get all fields fail, pipeline hgetall_cmds.size[%lu] != hgetall_replies.size[%lu]",
                             hgetall_cmds.size(),
                             hgetall_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const ReplyUPtr &hgetall_reply = hgetall_replies[i];
        if (!IsReplyOk(hgetall_reply.get()) || !CheckReplyArray(hgetall_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis get all fields fail, key[%s] HGETALL fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }
        if (hgetall_reply->elements == 0) {
            ec_per_key.emplace_back(EC_NOENT);
            continue;
        }
        if ((hgetall_reply->elements) & 1) { // elements should be even number
            KVCM_REDIS_LOG_ERROR("redis get all fields fail, key[%s] HGETALL reply elements[%lu] is not even",
                                 keys[i].c_str(),
                                 hgetall_reply->elements);
            ec_per_key.emplace_back(EC_ERROR);
            continue;
        }

        bool hasError = false;
        std::map<std::string, std::string> &out_field_map = out_field_maps[i];
        for (size_t j = 0; j < hgetall_reply->elements; j += 2) {
            const redisReply *field_name_reply = hgetall_reply->element[j];
            std::string field_name;
            if (!GetReplyStrOrNil(field_name_reply, field_name)) {
                KVCM_REDIS_LOG_ERROR(
                    "redis get all fields fail, key[%s] field name idx[%lu] get reply str fail", keys[i].c_str(), j);
                ec_per_key[i] = EC_ERROR;
                out_field_map.clear();
                break;
            }
            const redisReply *field_value_reply = hgetall_reply->element[j + 1];
            std::string field_value;
            if (!GetReplyStrOrNil(field_value_reply, field_value)) {
                KVCM_REDIS_LOG_ERROR(
                    "redis get all fields fail, key[%s] field value idx[%lu] get reply str fail", keys[i].c_str(), j);
                hasError = true;
                out_field_map.clear();
                break;
            }
            out_field_map.emplace(std::move(field_name), std::move(field_value));
        }
        ec_per_key.emplace_back(hasError ? EC_ERROR : EC_OK);
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::Exists(const std::vector<std::string> &keys, std::vector<bool> &out_is_exist_vec) {
    out_is_exist_vec.assign(keys.size(), false);

    std::vector<CmdArgs> exists_cmds;
    exists_cmds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        CmdArgs exist_cmd{"EXISTS", keys[i]};
        exists_cmds.emplace_back(std::move(exist_cmd));
    }

    std::vector<ReplyUPtr> exists_replies = CommandPipeline(exists_cmds);
    if (exists_cmds.size() != exists_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis exists fail, pipeline exists_cmds.size[%lu] != exists_replies.size[%lu]",
                             exists_cmds.size(),
                             exists_replies.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    std::vector<ErrorCode> ec_per_key;
    ec_per_key.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const ReplyUPtr &exists_reply = exists_replies[i];
        if (!IsReplyOk(exists_reply.get()) || !CheckReplyInteger(exists_reply.get())) {
            KVCM_REDIS_LOG_ERROR("redis exists fail, key[%s] EXISTS fail", keys[i].c_str());
            ec_per_key.emplace_back(EC_ERROR);
            out_is_exist_vec[i] = false;
        } else {
            ec_per_key.emplace_back(EC_OK);
            out_is_exist_vec[i] = (exists_reply->integer > 0);
        }
    }
    return ec_per_key;
}

std::vector<ErrorCode> RedisClient::ExistsFieldWithPrefix(const std::vector<std::string> &keys,
                                                          const std::string &field_prefix,
                                                          std::vector<bool> &out_exists_vec) {
    out_exists_vec.assign(keys.size(), false);

    // First check which keys exist. Non-existent keys get EC_NOENT immediately.
    std::vector<bool> key_exists_vec;
    std::vector<ErrorCode> ec_per_key = Exists(keys, key_exists_vec);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] == EC_OK && !key_exists_vec[i]) {
            ec_per_key[i] = EC_NOENT;
        }
    }

    const std::string pattern = field_prefix + "*";
    const std::string count_hint = "1000";
    struct PendingKey {
        size_t original_index;
        std::string cursor;
    };
    std::vector<PendingKey> pending;
    pending.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] == EC_OK) {
            pending.push_back({i, "0"});
        }
    }
    while (!pending.empty()) {
        std::vector<CmdArgs> hscan_cmds;
        hscan_cmds.reserve(pending.size());
        for (const auto &entry : pending) {
            hscan_cmds.push_back(
                {"HSCAN", keys[entry.original_index], entry.cursor, "MATCH", pattern, "COUNT", count_hint});
        }

        std::vector<ReplyUPtr> replies = CommandPipeline(hscan_cmds);
        if (replies.size() != hscan_cmds.size()) {
            KVCM_REDIS_LOG_ERROR(
                "redis exists field with prefix fail, pipeline hscan_cmds.size[%lu] != replies.size[%lu]",
                hscan_cmds.size(),
                replies.size());
            for (auto &entry : pending) {
                ec_per_key[entry.original_index] = EC_ERROR;
            }
            break;
        }

        std::vector<PendingKey> next_pending;
        for (size_t i = 0; i < pending.size(); ++i) {
            size_t original_idx = pending[i].original_index;
            const ReplyUPtr &hscan_reply = replies[i];
            if (!IsReplyOk(hscan_reply.get()) || !CheckReplyArray(hscan_reply.get()) || hscan_reply->elements != 2) {
                KVCM_REDIS_LOG_ERROR("redis exists field with prefix fail, key[%s] HSCAN fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }

            const redisReply *next_cursor_reply = hscan_reply->element[0];
            const redisReply *fields_reply = hscan_reply->element[1];
            std::string next_cursor;
            if (!GetReplyStrOrNil(next_cursor_reply, next_cursor)) {
                KVCM_REDIS_LOG_ERROR("redis exists field with prefix fail, key[%s] get next cursor fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }
            if (!CheckReplyArray(fields_reply)) {
                KVCM_REDIS_LOG_ERROR("redis exists field with prefix fail, key[%s] check fields reply fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }
            // HSCAN returns [field1, value1, field2, value2, ...].
            // Skip tombstones (empty value) — they are not valid locations.
            bool found_non_empty = false;
            for (size_t j = 0; j + 1 < fields_reply->elements; j += 2) {
                std::string value;
                if (GetReplyStrOrNil(fields_reply->element[j + 1], value) && !value.empty()) {
                    found_non_empty = true;
                    break;
                }
            }
            if (found_non_empty) {
                out_exists_vec[original_idx] = true;
            } else if (next_cursor != "0") {
                next_pending.push_back({original_idx, std::move(next_cursor)});
            }
        }
        pending = std::move(next_pending);
    }
    return ec_per_key;
}

std::vector<ErrorCode>
RedisClient::GetFieldNamesWithPrefix(const std::vector<std::string> &keys,
                                     const std::string &field_prefix,
                                     std::vector<std::vector<std::string>> &out_field_names_vec) {
    out_field_names_vec.resize(keys.size());

    // First check which keys exist. Non-existent keys get EC_NOENT immediately.
    std::vector<bool> key_exists_vec;
    std::vector<ErrorCode> ec_per_key = Exists(keys, key_exists_vec);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] == EC_OK && !key_exists_vec[i]) {
            ec_per_key[i] = EC_NOENT;
        }
    }

    const std::string pattern = field_prefix + "*";
    const std::string count_hint = "1000";
    struct PendingKey {
        size_t original_index;
        std::string cursor;
    };
    std::vector<PendingKey> pending;
    pending.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (ec_per_key[i] == EC_OK) {
            pending.push_back({i, "0"});
        }
    }
    while (!pending.empty()) {
        std::vector<CmdArgs> hscan_cmds;
        hscan_cmds.reserve(pending.size());
        for (const auto &entry : pending) {
            hscan_cmds.push_back(
                {"HSCAN", keys[entry.original_index], entry.cursor, "MATCH", pattern, "COUNT", count_hint});
        }

        std::vector<ReplyUPtr> replies = CommandPipeline(hscan_cmds);
        if (replies.size() != hscan_cmds.size()) {
            KVCM_REDIS_LOG_ERROR(
                "redis list field names with prefix fail, pipeline hscan_cmds.size[%lu] != replies.size[%lu]",
                hscan_cmds.size(),
                replies.size());
            for (auto &entry : pending) {
                ec_per_key[entry.original_index] = EC_ERROR;
            }
            break;
        }

        std::vector<PendingKey> next_pending;
        for (size_t i = 0; i < pending.size(); ++i) {
            size_t original_idx = pending[i].original_index;
            const ReplyUPtr &hscan_reply = replies[i];
            if (!IsReplyOk(hscan_reply.get()) || !CheckReplyArray(hscan_reply.get()) || hscan_reply->elements != 2) {
                KVCM_REDIS_LOG_ERROR("redis list field names with prefix fail, key[%s] HSCAN fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }

            const redisReply *next_cursor_reply = hscan_reply->element[0];
            const redisReply *fields_reply = hscan_reply->element[1];
            std::string next_cursor;
            if (!GetReplyStrOrNil(next_cursor_reply, next_cursor)) {
                KVCM_REDIS_LOG_ERROR("redis list field names with prefix fail, key[%s] get next cursor fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }
            if (!CheckReplyArray(fields_reply)) {
                KVCM_REDIS_LOG_ERROR("redis list field names with prefix fail, key[%s] check fields reply fail",
                                     keys[original_idx].c_str());
                ec_per_key[original_idx] = EC_ERROR;
                continue;
            }
            // HSCAN returns [field1, value1, field2, value2, ...]; collect only
            // field names (even indices) whose value is non-empty (skip tombstones).
            for (size_t j = 0; j + 1 < fields_reply->elements; j += 2) {
                std::string field_name;
                std::string field_value;
                if (GetReplyStrOrNil(fields_reply->element[j], field_name) && !field_name.empty() &&
                    GetReplyStrOrNil(fields_reply->element[j + 1], field_value) && !field_value.empty()) {
                    out_field_names_vec[original_idx].emplace_back(std::move(field_name));
                }
            }
            if (next_cursor != "0") {
                next_pending.push_back({original_idx, std::move(next_cursor)});
            }
        }
        pending = std::move(next_pending);
    }
    return ec_per_key;
}

ErrorCode RedisClient::Scan(const std::string &matching_prefix,
                            const std::string &cursor,
                            const int64_t limit,
                            std::string &out_next_cursor,
                            std::vector<std::string> &out_keys) {
    out_next_cursor.clear();
    out_keys.clear();

    std::string pattern = matching_prefix + "*";
    CmdArgs scan_cmd{"SCAN", cursor, "MATCH", pattern, "COUNT", std::to_string(limit)};
    std::vector<ReplyUPtr> scan_replies = CommandPipeline({scan_cmd});
    if (/*scan_cmds.size()*/ 1 != scan_replies.size()) {
        KVCM_REDIS_LOG_ERROR("redis scan fail, pipeline [1] != scan_replies.size[%lu]", scan_replies.size());
        return EC_ERROR;
    }

    const ReplyUPtr &scan_reply = scan_replies[0];
    if (!IsReplyOk(scan_reply.get()) || !CheckReplyArray(scan_reply.get())) {
        KVCM_REDIS_LOG_ERROR("redis scan fail");
        return EC_ERROR;
    }
    if ((scan_reply->elements) != 2) { // [next_cursor, [keys...]]
        KVCM_REDIS_LOG_ERROR("redis scan fail, scan reply elements[%lu] is not 2", scan_reply->elements);
        return EC_ERROR;
    }

    const redisReply *next_cursor_reply = scan_reply->element[0];
    const redisReply *keys_reply = scan_reply->element[1];
    if (!IsReplyOk(next_cursor_reply) || !GetReplyStrOrNil(next_cursor_reply, out_next_cursor)) {
        KVCM_REDIS_LOG_ERROR("redis scan fail, get next cursor fail");
        out_next_cursor.clear();
        return EC_ERROR;
    }
    if (!IsReplyOk(keys_reply) || !CheckReplyArray(keys_reply)) {
        KVCM_REDIS_LOG_ERROR("redis scan fail, check keys reply fail");
        out_next_cursor.clear();
        return EC_ERROR;
    }
    for (size_t i = 0; i < keys_reply->elements; ++i) {
        const redisReply *key_reply = keys_reply->element[i];
        std::string key;
        if (!IsReplyOk(key_reply) || !GetReplyStrOrNil(key_reply, key) || key.empty()) {
            KVCM_REDIS_LOG_ERROR("redis scan fail, get key from reply fail");
            out_next_cursor.clear();
            out_keys.clear();
            return EC_ERROR;
        }
        out_keys.emplace_back(std::move(key));
    }
    return EC_OK;
}

ErrorCode
RedisClient::Rand(const std::string &matching_prefix, const int64_t count, std::vector<std::string> &out_keys) {
    out_keys.clear();

    std::vector<CmdArgs> randomkey_cmds(randomkey_batch_num_, CmdArgs{"RANDOMKEY"});
    std::unordered_set<std::string> seen;
    size_t consecutive_misses = 0;
    while (out_keys.size() < count && consecutive_misses < 3) {
        std::vector<ReplyUPtr> randomkey_replies = CommandPipeline(randomkey_cmds);
        if (randomkey_cmds.size() != randomkey_replies.size()) {
            KVCM_REDIS_LOG_ERROR("redis rand fail, pipeline randomkey_cmds.size[%lu] != randomkey_replies.size[%lu]",
                                 randomkey_cmds.size(),
                                 randomkey_replies.size());
            out_keys.clear();
            return EC_ERROR;
        }
        bool found = false;
        for (size_t i = 0; i < randomkey_replies.size(); ++i) {
            const ReplyUPtr &randomkey_reply = randomkey_replies[i];
            std::string key;
            if (!IsReplyOk(randomkey_reply.get()) || !GetReplyStrOrNil(randomkey_reply.get(), key)) {
                KVCM_REDIS_LOG_ERROR("redis rand fail, get rand key from reply fail");
                out_keys.clear();
                return EC_ERROR;
            }
            if (key.size() >= matching_prefix.size() && key.compare(0, matching_prefix.size(), matching_prefix) == 0) {
                if (seen.insert(key).second) {
                    out_keys.emplace_back(std::move(key));
                    found = true;
                    if (out_keys.size() >= count) {
                        break;
                    }
                }
            }
        }
        if (found) {
            consecutive_misses = 0;
        } else {
            ++consecutive_misses;
        }
    }
    return EC_OK;
}

#undef KVCM_REDIS_LOG_INFO
#undef KVCM_REDIS_LOG_WARN
#undef KVCM_REDIS_LOG_ERROR

} // namespace kv_cache_manager
