#pragma once

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace kv_cache_manager::test {

class LoopbackRespServer {
public:
    struct Response {
        std::string bytes;
        bool close_connection = false;
        bool allow_send_failure = false;

        static Response Reply(std::string value, bool allow_failure = false) {
            return {std::move(value), false, allow_failure};
        }

        static Response Close() { return {{}, true, true}; }
    };

    using Command = std::vector<std::string>;
    using Handler = std::function<Response(const Command &)>;

    explicit LoopbackRespServer(Handler handler, int64_t expected_database = 0)
        : handler_(std::move(handler)), expected_database_(expected_database) {
        Start();
    }

    ~LoopbackRespServer() { Stop(); }

    LoopbackRespServer(const LoopbackRespServer &) = delete;
    LoopbackRespServer &operator=(const LoopbackRespServer &) = delete;

    bool Ok() const {
        std::lock_guard lock(state_mutex_);
        return error_.empty();
    }

    std::string Error() const {
        std::lock_guard lock(state_mutex_);
        return error_;
    }

    int Port() const { return port_; }
    std::size_t CommandCount() const { return command_count_.load(std::memory_order_acquire); }
    std::size_t AcceptedConnectionCount() const { return accepted_connection_count_.load(std::memory_order_acquire); }
    std::size_t CompletedHandshakeCount() const { return completed_handshake_count_.load(std::memory_order_acquire); }

    bool WaitForCommands(std::size_t count, std::chrono::milliseconds timeout) const {
        std::unique_lock lock(progress_mutex_);
        return progress_cv_.wait_for(
            lock, timeout, [this, count] { return command_count_.load(std::memory_order_acquire) >= count; });
    }

    bool WaitForHandled(std::size_t count, std::chrono::milliseconds timeout) const {
        std::unique_lock lock(progress_mutex_);
        return progress_cv_.wait_for(
            lock, timeout, [this, count] { return handled_count_.load(std::memory_order_acquire) >= count; });
    }

    bool WaitForCompletedHandshakes(std::size_t count, std::chrono::milliseconds timeout) const {
        std::unique_lock lock(progress_mutex_);
        return progress_cv_.wait_for(lock, timeout, [this, count] {
            return completed_handshake_count_.load(std::memory_order_acquire) >= count;
        });
    }

    void SetReplyGates(std::vector<std::size_t> command_ordinals) {
        std::lock_guard lock(reply_gate_mutex_);
        reply_gate_ordinals_ = std::move(command_ordinals);
        next_reply_gate_ = 0;
        active_reply_gate_ = 0;
        active_reply_gate_client_ = -1;
        release_current_reply_gate_ = false;
        release_all_reply_gates_ = false;
    }

    bool WaitForReplyGate(std::size_t command_ordinal, std::chrono::milliseconds timeout) const {
        std::unique_lock lock(reply_gate_mutex_);
        return reply_gate_cv_.wait_for(lock, timeout, [this, command_ordinal] {
            return active_reply_gate_ == command_ordinal || stopping_.load(std::memory_order_acquire);
        }) && active_reply_gate_ == command_ordinal;
    }

    bool NoPendingInputAtReplyGate(std::chrono::milliseconds observation) const {
        std::lock_guard lock(reply_gate_mutex_);
        if (active_reply_gate_client_ < 0) {
            return false;
        }
        pollfd descriptor{active_reply_gate_client_, POLLIN, 0};
        int result = 0;
        do {
            result = poll(&descriptor, 1, static_cast<int>(observation.count()));
        } while (result < 0 && errno == EINTR);
        return result == 0;
    }

    void ReleaseReplyGate() {
        std::lock_guard lock(reply_gate_mutex_);
        release_current_reply_gate_ = true;
        reply_gate_cv_.notify_all();
    }

    void ReleaseAllReplyGates() {
        std::lock_guard lock(reply_gate_mutex_);
        release_current_reply_gate_ = true;
        release_all_reply_gates_ = true;
        reply_gate_cv_.notify_all();
    }

    static std::string BulkReply(const std::string &value) {
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    }

    static std::string BoundedGetReply(bool exists, const std::string &value) {
        return "*2\r\n:" + std::to_string(exists ? 1 : 0) + "\r\n" + BulkReply(value);
    }

private:
    enum class HandshakeState {
        EXPECT_PING,
        EXPECT_SELECT,
        COMPLETE
    };

    static constexpr std::size_t kMaximumLineBytes = 64;
    static constexpr std::size_t kMaximumArrayElements = 16;
    static constexpr std::size_t kMaximumBulkBytes = 2 * 1024 * 1024;
    static constexpr auto kIoTimeout = std::chrono::seconds(5);

    void Start() {
        listener_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listener_ < 0) {
            SetError("socket failed");
            return;
        }
        int reuse = 1;
        if (setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
            SetError("setsockopt SO_REUSEADDR failed");
            close(listener_);
            listener_ = -1;
            return;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listener_, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0 ||
            listen(listener_, 16) != 0) {
            SetError("loopback bind or listen failed");
            close(listener_);
            listener_ = -1;
            return;
        }
        socklen_t length = sizeof(address);
        if (getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &length) != 0) {
            SetError("getsockname failed");
            close(listener_);
            listener_ = -1;
            return;
        }
        port_ = ntohs(address.sin_port);
        const int listener = listener_;
        accept_thread_ = std::thread([this, listener] { AcceptLoop(listener); });
    }

    void Stop() {
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        ReleaseAllReplyGates();
        if (listener_ >= 0) {
            (void)shutdown(listener_, SHUT_RDWR);
        }
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        if (listener_ >= 0) {
            close(listener_);
            listener_ = -1;
        }
        {
            std::lock_guard lock(clients_mutex_);
            for (const int client : active_clients_) {
                (void)shutdown(client, SHUT_RDWR);
            }
        }
        for (auto &thread : client_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    void AcceptLoop(int listener) {
        while (!stopping_.load(std::memory_order_acquire)) {
            const int client = accept(listener, nullptr, nullptr);
            if (client < 0) {
                if ((errno == EINTR || errno == ECONNABORTED) && !stopping_.load(std::memory_order_acquire)) {
                    continue;
                }
                if (!stopping_.load(std::memory_order_acquire)) {
                    SetError("accept failed");
                }
                return;
            }
            timeval timeout{};
            timeout.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(kIoTimeout).count();
            if (setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
                setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
                SetError("client socket timeout setup failed");
                close(client);
                continue;
            }
            {
                std::lock_guard lock(clients_mutex_);
                active_clients_.push_back(client);
                accepted_connection_count_.fetch_add(1, std::memory_order_acq_rel);
                NotifyProgress();
                client_threads_.emplace_back([this, client] { ServeClient(client); });
            }
        }
    }

    void ServeClient(int client) {
        HandshakeState handshake_state = HandshakeState::EXPECT_PING;
        Command command;
        while (!stopping_.load(std::memory_order_acquire) && ReadCommand(client, command)) {
            Response response;
            bool handled_command = false;
            bool handshake_completed = false;
            bool protocol_error = false;
            std::size_t command_ordinal = 0;

            if (handshake_state == HandshakeState::EXPECT_PING) {
                if (command != Command{"PING"}) {
                    SetError("connection did not send PING first");
                    response = Response::Reply("-ERR PING required first\r\n");
                    protocol_error = true;
                } else {
                    response = Response::Reply("+PONG\r\n");
                    if (expected_database_ > 0) {
                        handshake_state = HandshakeState::EXPECT_SELECT;
                    } else {
                        handshake_state = HandshakeState::COMPLETE;
                        handshake_completed = true;
                    }
                }
            } else if (handshake_state == HandshakeState::EXPECT_SELECT) {
                if (command != Command{"SELECT", std::to_string(expected_database_)}) {
                    SetError("connection did not SELECT the expected database after PING");
                    response = Response::Reply("-ERR expected SELECT\r\n");
                    protocol_error = true;
                } else {
                    response = Response::Reply("+OK\r\n");
                    handshake_state = HandshakeState::COMPLETE;
                    handshake_completed = true;
                }
            } else {
                handled_command = true;
                command_ordinal = command_count_.fetch_add(1, std::memory_order_acq_rel) + 1;
                NotifyProgress();
                try {
                    response = handler_(command);
                } catch (const std::exception &exception) {
                    SetError(std::string("handler exception: ") + exception.what());
                    MarkHandled();
                    break;
                } catch (...) {
                    SetError("handler threw an unknown exception");
                    MarkHandled();
                    break;
                }
            }

            if (response.close_connection) {
                if (handled_command) {
                    MarkHandled();
                }
                break;
            }
            if (handled_command) {
                WaitAtReplyGate(client, command_ordinal);
            }
            if (!SendAll(client, response.bytes) && !response.allow_send_failure &&
                !stopping_.load(std::memory_order_acquire)) {
                SetError("send failed");
                if (handled_command) {
                    MarkHandled();
                }
                break;
            }
            if (handshake_completed) {
                completed_handshake_count_.fetch_add(1, std::memory_order_acq_rel);
                NotifyProgress();
            }
            if (handled_command) {
                MarkHandled();
            }
            if (protocol_error) {
                break;
            }
        }
        {
            std::lock_guard lock(clients_mutex_);
            for (auto it = active_clients_.begin(); it != active_clients_.end(); ++it) {
                if (*it == client) {
                    active_clients_.erase(it);
                    break;
                }
            }
        }
        (void)shutdown(client, SHUT_RDWR);
        close(client);
    }

    void WaitAtReplyGate(int client, std::size_t command_ordinal) {
        std::unique_lock lock(reply_gate_mutex_);
        if (release_all_reply_gates_ || next_reply_gate_ >= reply_gate_ordinals_.size() ||
            reply_gate_ordinals_[next_reply_gate_] != command_ordinal) {
            return;
        }
        active_reply_gate_ = command_ordinal;
        active_reply_gate_client_ = client;
        release_current_reply_gate_ = false;
        reply_gate_cv_.notify_all();
        reply_gate_cv_.wait(lock, [this] {
            return release_current_reply_gate_ || release_all_reply_gates_ || stopping_.load(std::memory_order_acquire);
        });
        active_reply_gate_ = 0;
        active_reply_gate_client_ = -1;
        if (release_all_reply_gates_) {
            next_reply_gate_ = reply_gate_ordinals_.size();
        } else {
            ++next_reply_gate_;
        }
        reply_gate_cv_.notify_all();
    }

    static bool ParseSize(const std::string &text, std::size_t maximum, std::size_t &value) {
        value = 0;
        if (text.empty()) {
            return false;
        }
        const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
        return result.ec == std::errc() && result.ptr == text.data() + text.size() && value <= maximum;
    }

    static bool ReadExact(int fd, char *data, std::size_t size) {
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

    static bool ReadLine(int fd, std::string &line) {
        line.clear();
        char byte = 0;
        while (line.size() <= kMaximumLineBytes) {
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
            if (line.size() == kMaximumLineBytes) {
                return false;
            }
            line.push_back(byte);
        }
        return false;
    }

    static bool ReadCommand(int fd, Command &command) {
        command.clear();
        char marker = 0;
        std::string line;
        std::size_t count = 0;
        if (!ReadExact(fd, &marker, 1) || marker != '*' || !ReadLine(fd, line) ||
            !ParseSize(line, kMaximumArrayElements, count)) {
            return false;
        }
        command.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            std::size_t size = 0;
            if (!ReadExact(fd, &marker, 1) || marker != '$' || !ReadLine(fd, line) ||
                !ParseSize(line, kMaximumBulkBytes, size)) {
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

    static bool SendAll(int fd, const std::string &data) {
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

    void MarkHandled() {
        handled_count_.fetch_add(1, std::memory_order_acq_rel);
        NotifyProgress();
    }

    // Waiters check their predicates under progress_mutex_, so taking it here
    // orders each counter update before the wakeup and prevents a lost notify.
    void NotifyProgress() {
        { std::lock_guard lock(progress_mutex_); }
        progress_cv_.notify_all();
    }

    void SetError(std::string error) {
        std::lock_guard lock(state_mutex_);
        if (error_.empty()) {
            error_ = std::move(error);
        }
    }

    Handler handler_;
    int64_t expected_database_ = 0;
    int listener_ = -1;
    int port_ = 0;
    std::atomic<bool> stopping_{false};
    std::atomic<std::size_t> command_count_{0};
    std::atomic<std::size_t> handled_count_{0};
    std::atomic<std::size_t> accepted_connection_count_{0};
    std::atomic<std::size_t> completed_handshake_count_{0};
    mutable std::mutex progress_mutex_;
    mutable std::condition_variable progress_cv_;
    mutable std::mutex state_mutex_;
    std::string error_;
    std::mutex clients_mutex_;
    std::vector<int> active_clients_;
    std::thread accept_thread_;
    std::vector<std::thread> client_threads_;
    mutable std::mutex reply_gate_mutex_;
    mutable std::condition_variable reply_gate_cv_;
    std::vector<std::size_t> reply_gate_ordinals_;
    std::size_t next_reply_gate_ = 0;
    std::size_t active_reply_gate_ = 0;
    int active_reply_gate_client_ = -1;
    bool release_current_reply_gate_ = false;
    bool release_all_reply_gates_ = false;
};

} // namespace kv_cache_manager::test
