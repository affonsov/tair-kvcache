#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kv_cache_manager/client/include/transfer_client.h"
#include "kv_cache_manager/client/test/valkey_loopback_test_server.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {
namespace {

using test::LoopbackRespServer;
using namespace std::chrono_literals;

constexpr char kStorageIdentity[] = "valkey-public";
constexpr int64_t kTestDatabase = 7;
// Separate Get and Put StaticClientPool instances, each with kDefaultPoolSize (4)
// connections, are opened during TransferClient::Create.
constexpr std::size_t kExpectedPoolConnections = 8;

std::string ClientConfig(int get_timeout_ms = 2000, int put_timeout_ms = 2000) {
    return R"({
        "instance_group": "valkey-public-group",
        "instance_id": "valkey-public-instance",
        "block_size": 16,
        "sdk_config": {
            "thread_num": 1,
            "queue_size": 8,
            "sdk_backend_configs": [{"type": "valkey"}],
            "timeout_config": {
                "get_timeout_ms": )" +
           std::to_string(get_timeout_ms) + R"(,
                "put_timeout_ms": )" +
           std::to_string(put_timeout_ms) + R"(
            }
        },
        "location_spec_infos": {"tp0": 1}
    })";
}

InitParams TestInitParams(int port) {
    InitParams params;
    params.role_type = RoleType::WORKER;
    params.self_location_spec_name = "tp0";
    params.storage_configs = R"([{
        "type": "valkey",
        "global_unique_name": ")" +
                             std::string(kStorageIdentity) + R"(",
        "storage_spec": {"endpoint": "valkey://127.0.0.1:)" +
                             std::to_string(port) + R"(/?db=7"}
    }])";
    return params;
}

std::string OpaqueId(std::size_t index) {
    std::ostringstream stream;
    stream << std::hex << std::setw(32) << std::setfill('0') << index;
    return stream.str();
}

std::string Location(std::size_t index, std::size_t size) {
    return "valkey://" + std::string(kStorageIdentity) + "/" + OpaqueId(index + 1) + "?size=" + std::to_string(size);
}

BlockBuffer CpuBuffer(void *base, std::size_t size) { return BlockBuffer{{Iov{MemoryType::CPU, base, size, false}}}; }

class InMemoryValkey {
public:
    LoopbackRespServer::Response Handle(const LoopbackRespServer::Command &command) {
        if (command.size() == 3 && command[0] == "SET") {
            std::lock_guard lock(mutex_);
            values_[command[1]] = command[2];
            return LoopbackRespServer::Response::Reply("+OK\r\n");
        }
        if (command.size() == 5 && command[0] == "EVAL_RO" && command[2] == "1") {
            std::lock_guard lock(mutex_);
            const auto value = values_.find(command[3]);
            if (value == values_.end()) {
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(false, {}));
            }
            return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, value->second));
        }
        valid_.store(false, std::memory_order_release);
        return LoopbackRespServer::Response::Reply("-ERR unsupported test command\r\n");
    }

    bool Valid() const { return valid_.load(std::memory_order_acquire); }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::string> values_;
    std::atomic<bool> valid_{true};
};

class CommandBarrier {
public:
    void EnterAndWait() {
        std::unique_lock lock(mutex_);
        entered_ = true;
        cv_.notify_all();
        (void)cv_.wait_for(lock, 5s, [this] { return released_; });
    }

    bool WaitUntilEntered() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, 5s, [this] { return entered_; });
    }

    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool released_ = false;
};

class BarrierReleaseGuard {
public:
    explicit BarrierReleaseGuard(CommandBarrier &barrier) : barrier_(barrier) {}
    ~BarrierReleaseGuard() { barrier_.Release(); }

private:
    CommandBarrier &barrier_;
};

class ReplyGateReleaseGuard {
public:
    explicit ReplyGateReleaseGuard(LoopbackRespServer &server) : server_(server) {}
    ~ReplyGateReleaseGuard() { server_.ReleaseAllReplyGates(); }

private:
    LoopbackRespServer &server_;
};

bool CompletedExpectedPoolHandshakes(LoopbackRespServer &server) {
    return server.WaitForCompletedHandshakes(kExpectedPoolConnections, 5s) &&
           server.AcceptedConnectionCount() == kExpectedPoolConnections &&
           server.CompletedHandshakeCount() == kExpectedPoolConnections;
}

bool ObserveSliceBoundary(LoopbackRespServer &server, std::size_t reply_gate, std::size_t next_command) {
    if (!server.WaitForReplyGate(reply_gate, 5s)) {
        ADD_FAILURE() << "reply gate " << reply_gate << " was not reached";
        server.ReleaseAllReplyGates();
        return false;
    }
    EXPECT_TRUE(server.NoPendingInputAtReplyGate(100ms))
        << "application command after " << reply_gate << " was submitted before the slice reply";
    server.ReleaseReplyGate();
    if (!server.WaitForCommands(next_command, 5s)) {
        ADD_FAILURE() << "application command " << next_command << " was not observed after the slice reply";
        server.ReleaseAllReplyGates();
        return false;
    }
    return true;
}

class MappedPage {
public:
    MappedPage() {
        size_ = static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
        data_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    ~MappedPage() {
        if (data_ != MAP_FAILED) {
            (void)mprotect(data_, size_, PROT_READ | PROT_WRITE);
            (void)munmap(data_, size_);
        }
    }

    bool Valid() const { return data_ != MAP_FAILED; }
    void *Data() const { return data_; }
    bool Protect() { return mprotect(data_, size_, PROT_NONE) == 0; }
    bool Unprotect() { return mprotect(data_, size_, PROT_READ | PROT_WRITE) == 0; }

private:
    void *data_ = MAP_FAILED;
    std::size_t size_ = 0;
};

TEST(ValkeyTransferClientTest, SegmentedBinaryAndEmptyCpuBlocksCrossBothSliceLimits) {
    constexpr std::size_t kMiB = 1024 * 1024;
    InMemoryValkey state;
    LoopbackRespServer server([&](const auto &command) { return state.Handle(command); }, kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    std::vector<std::string> sources(67);
    sources[64].assign(kMiB, 'b');
    sources[64][0] = '\0';
    sources[64][kMiB - 1] = static_cast<char>(0xff);
    sources[65] = "z";
    UriStrVec locations;
    BlockBuffers put_buffers;
    locations.reserve(sources.size());
    put_buffers.reserve(sources.size());
    for (std::size_t index = 0; index < sources.size(); ++index) {
        locations.push_back(Location(index, sources[index].size()));
        if (index == 64) {
            put_buffers.push_back(BlockBuffer{{
                Iov{MemoryType::CPU, sources[index].data(), 333333, false},
                Iov{MemoryType::CPU, sources[index].data() + 333333, kMiB - 333333, false},
            }});
        } else {
            put_buffers.push_back(
                CpuBuffer(sources[index].empty() ? nullptr : sources[index].data(), sources[index].size()));
        }
    }

    const std::size_t put_start = server.CommandCount();
    server.SetReplyGates({put_start + 64, put_start + 65});
    std::future<std::pair<ClientErrorCode, UriStrVec>> save_future;
    ReplyGateReleaseGuard put_gate_guard(server);
    save_future = std::async(std::launch::async, [&] { return client->SaveKvCaches(locations, put_buffers); });
    if (ObserveSliceBoundary(server, put_start + 64, put_start + 65)) {
        (void)ObserveSliceBoundary(server, put_start + 65, put_start + 66);
    }
    const auto [save_ec, actual_locations] = save_future.get();
    ASSERT_EQ(ER_OK, save_ec);
    EXPECT_EQ(locations, actual_locations);

    std::vector<std::string> destinations(sources.size());
    destinations[64].assign(kMiB, 'x');
    destinations[65] = "x";
    BlockBuffers get_buffers;
    get_buffers.reserve(destinations.size());
    for (std::size_t index = 0; index < destinations.size(); ++index) {
        if (index == 64) {
            get_buffers.push_back(BlockBuffer{{
                Iov{MemoryType::CPU, destinations[index].data(), 17, false},
                Iov{MemoryType::CPU, destinations[index].data() + 17, kMiB - 17, false},
            }});
        } else {
            get_buffers.push_back(CpuBuffer(destinations[index].empty() ? nullptr : destinations[index].data(),
                                            destinations[index].size()));
        }
    }

    const std::size_t get_start = server.CommandCount();
    server.SetReplyGates({get_start + 64, get_start + 65});
    std::future<ClientErrorCode> get_future;
    ReplyGateReleaseGuard get_gate_guard(server);
    get_future = std::async(std::launch::async, [&] { return client->LoadKvCaches(actual_locations, get_buffers); });
    if (ObserveSliceBoundary(server, get_start + 64, get_start + 65)) {
        (void)ObserveSliceBoundary(server, get_start + 65, get_start + 66);
    }
    EXPECT_EQ(ER_OK, get_future.get());
    EXPECT_EQ(sources, destinations);
    client.reset();
    EXPECT_TRUE(state.Valid());
    EXPECT_TRUE(server.Ok()) << server.Error();
}

TEST(ValkeyTransferClientTest, BoundedReadPreservesMissingAndOversizedDestinations) {
    std::atomic<std::size_t> read_index{0};
    std::atomic<bool> valid{true};
    LoopbackRespServer server(
        [&](const auto &command) {
            if (command.size() != 5 || command[0] != "EVAL_RO" || command[2] != "1" || command[4] != "4") {
                valid.store(false, std::memory_order_release);
                return LoopbackRespServer::Response::Reply("-ERR invalid bounded read\r\n");
            }
            switch (read_index.fetch_add(1, std::memory_order_acq_rel)) {
            case 0:
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, "good"));
            case 1:
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(false, {}));
            default:
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, "large"));
            }
        },
        kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    char valid_destination[4] = {'v', 'v', 'v', 'v'};
    char missing_destination[4] = {'m', 'm', 'm', 'm'};
    char oversized_destination[4] = {'o', 'o', 'o', 'o'};
    const UriStrVec locations{Location(0, 4), Location(1, 4), Location(2, 4)};
    const BlockBuffers buffers{
        CpuBuffer(valid_destination, sizeof(valid_destination)),
        CpuBuffer(missing_destination, sizeof(missing_destination)),
        CpuBuffer(oversized_destination, sizeof(oversized_destination)),
    };

    EXPECT_EQ(ER_SDKREAD_ERROR, client->LoadKvCaches(locations, buffers));
    EXPECT_EQ("good", std::string(valid_destination, sizeof(valid_destination)));
    EXPECT_EQ("mmmm", std::string(missing_destination, sizeof(missing_destination)));
    EXPECT_EQ("oooo", std::string(oversized_destination, sizeof(oversized_destination)));
    client.reset();
    EXPECT_TRUE(valid.load(std::memory_order_acquire));
    EXPECT_TRUE(server.Ok()) << server.Error();
}

enum TimeoutChildExitCode {
    kTimeoutChildSuccess = 0,
    kTimeoutChildServerStart = 10,
    kTimeoutChildClientCreate = 11,
    kTimeoutChildHandshake = 12,
    kTimeoutChildMapPage = 13,
    kTimeoutChildCommandNotObserved = 14,
    kTimeoutChildCallDidNotReturn = 15,
    kTimeoutChildWrongResult = 16,
    kTimeoutChildProtectPage = 17,
    kTimeoutChildReplyNotHandled = 18,
    kTimeoutChildUnprotectPage = 19,
    kTimeoutChildLateWrite = 20,
    kTimeoutChildServerError = 21,
};

const char *TimeoutChildPhase(int exit_code) {
    switch (exit_code) {
    case kTimeoutChildSuccess:
        return "success";
    case kTimeoutChildServerStart:
        return "loopback server start";
    case kTimeoutChildClientCreate:
        return "TransferClient creation";
    case kTimeoutChildHandshake:
        return "pool handshake";
    case kTimeoutChildMapPage:
        return "page allocation";
    case kTimeoutChildCommandNotObserved:
        return "blocked read observation";
    case kTimeoutChildCallDidNotReturn:
        return "bounded TransferClient return";
    case kTimeoutChildWrongResult:
        return "timeout result mapping";
    case kTimeoutChildProtectPage:
        return "page revocation";
    case kTimeoutChildReplyNotHandled:
        return "late reply handling";
    case kTimeoutChildUnprotectPage:
        return "page restoration";
    case kTimeoutChildLateWrite:
        return "post-return memory check";
    case kTimeoutChildServerError:
        return "loopback server cleanup";
    default:
        return "unknown child phase";
    }
}

int RunTimeoutMemoryIsolationChild() {
    CommandBarrier barrier;
    LoopbackRespServer server(
        [&](const auto &command) {
            if (command.size() == 5 && command[0] == "EVAL_RO") {
                barrier.EnterAndWait();
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, "z"), true);
            }
            return LoopbackRespServer::Response::Reply("-ERR unexpected command\r\n");
        },
        kTestDatabase);
    if (!server.Ok()) {
        return kTimeoutChildServerStart;
    }
    auto client = TransferClient::Create(ClientConfig(500, 500), TestInitParams(server.Port()));
    if (!client) {
        return kTimeoutChildClientCreate;
    }
    if (!CompletedExpectedPoolHandshakes(server)) {
        return kTimeoutChildHandshake;
    }
    MappedPage destination;
    if (!destination.Valid()) {
        return kTimeoutChildMapPage;
    }
    std::memset(destination.Data(), 'x', 1);

    std::future<ClientErrorCode> result;
    BarrierReleaseGuard release_guard(barrier);
    result = std::async(std::launch::async,
                        [&] { return client->LoadKvCaches({Location(0, 1)}, {CpuBuffer(destination.Data(), 1)}); });
    if (!barrier.WaitUntilEntered()) {
        return kTimeoutChildCommandNotObserved;
    }
    if (result.wait_for(5s) != std::future_status::ready) {
        return kTimeoutChildCallDidNotReturn;
    }
    if (result.get() != ER_SDK_TIMEOUT) {
        return kTimeoutChildWrongResult;
    }
    if (!destination.Protect()) {
        return kTimeoutChildProtectPage;
    }
    barrier.Release();
    if (!server.WaitForHandled(1, 5s)) {
        return kTimeoutChildReplyNotHandled;
    }
    if (!destination.Unprotect()) {
        return kTimeoutChildUnprotectPage;
    }
    if (*static_cast<char *>(destination.Data()) != 'x') {
        return kTimeoutChildLateWrite;
    }
    client.reset();
    return server.Ok() ? kTimeoutChildSuccess : kTimeoutChildServerError;
}

struct ChildWaitResult {
    bool wait_failed = false;
    bool timed_out = false;
    int status = 0;
};

ChildWaitResult WaitForChild(pid_t child, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    ChildWaitResult result;
    while (std::chrono::steady_clock::now() < deadline) {
        const pid_t wait_result = waitpid(child, &result.status, WNOHANG);
        if (wait_result == child) {
            return result;
        }
        if (wait_result < 0 && errno != EINTR) {
            result.wait_failed = true;
            return result;
        }
        std::this_thread::sleep_for(10ms);
    }

    result.timed_out = true;
    (void)kill(child, SIGKILL);
    while (waitpid(child, &result.status, 0) < 0 && errno == EINTR) {}
    return result;
}

// Proves the public call returns ER_SDK_TIMEOUT within its bound and that the
// caller's page stays untouched after it is revoked. The SDK-level cancellation
// tests cover the TryAcquireCallerMemory race between a worker and the wrapper.
TEST(ValkeyTransferClientTest, HardTimeoutReturnsBoundedAndLeavesRevokedBufferUntouched) {
    const pid_t child = fork();
    ASSERT_GE(child, 0) << "fork failed: " << std::strerror(errno);
    if (child == 0) {
        _exit(RunTimeoutMemoryIsolationChild());
    }

    const ChildWaitResult result = WaitForChild(child, 15s);
    ASSERT_FALSE(result.wait_failed) << "waitpid failed: " << std::strerror(errno);
    ASSERT_FALSE(result.timed_out) << "timeout-memory child exceeded its bounded wait and was killed";
    ASSERT_TRUE(WIFEXITED(result.status))
        << "timeout-memory child terminated by signal " << (WIFSIGNALED(result.status) ? WTERMSIG(result.status) : 0);
    const int exit_code = WEXITSTATUS(result.status);
    EXPECT_EQ(kTimeoutChildSuccess, exit_code) << "timeout-memory child failed during " << TimeoutChildPhase(exit_code);
}

TEST(ValkeyTransferClientTest, PutCertaintyMappingSuppressesReturnedLocations) {
    LoopbackRespServer server(
        [](const auto &command) {
            if (command.size() != 3 || command[0] != "SET") {
                return LoopbackRespServer::Response::Reply("-ERR unexpected command\r\n");
            }
            if (command[2] == "ambiguous") {
                return LoopbackRespServer::Response::Close();
            }
            return LoopbackRespServer::Response::Reply("-ERR definitive rejection\r\n");
        },
        kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    std::string ambiguous = "ambiguous";
    auto result =
        client->SaveKvCaches({Location(0, ambiguous.size())}, {CpuBuffer(ambiguous.data(), ambiguous.size())});
    EXPECT_EQ(ER_SDK_TIMEOUT, result.first);
    EXPECT_TRUE(result.second.empty());

    std::string rejected = "rejected";
    result = client->SaveKvCaches({Location(1, rejected.size())}, {CpuBuffer(rejected.data(), rejected.size())});
    EXPECT_EQ(ER_SDKWRITE_ERROR, result.first);
    EXPECT_TRUE(result.second.empty());
    client.reset();
    EXPECT_TRUE(server.Ok()) << server.Error();
}

TEST(ValkeyTransferClientTest, EarlierGetSliceRemainsVisibleWhenLaterSliceTimesOut) {
    CommandBarrier tail_barrier;
    std::atomic<std::size_t> read_index{0};
    LoopbackRespServer server(
        [&](const auto &command) {
            if (command.size() != 5 || command[0] != "EVAL_RO") {
                return LoopbackRespServer::Response::Reply("-ERR unexpected command\r\n");
            }
            const std::size_t index = read_index.fetch_add(1, std::memory_order_acq_rel);
            if (index < 64) {
                return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, "a"));
            }
            tail_barrier.EnterAndWait();
            return LoopbackRespServer::Response::Reply(LoopbackRespServer::BoundedGetReply(true, "b"), true);
        },
        kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(500, 500), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    std::vector<char> destinations(65, 'x');
    UriStrVec locations;
    BlockBuffers buffers;
    for (std::size_t index = 0; index < destinations.size(); ++index) {
        locations.push_back(Location(index, 1));
        buffers.push_back(CpuBuffer(&destinations[index], 1));
    }

    auto result = std::async(std::launch::async, [&] { return client->LoadKvCaches(locations, buffers); });
    BarrierReleaseGuard release_guard(tail_barrier);
    const bool entered = tail_barrier.WaitUntilEntered();
    EXPECT_TRUE(entered);
    if (!entered) {
        tail_barrier.Release();
    }
    const auto ready = result.wait_for(5s);
    EXPECT_EQ(std::future_status::ready, ready);
    if (ready != std::future_status::ready) {
        tail_barrier.Release();
    }
    EXPECT_EQ(ER_SDK_TIMEOUT, result.get());
    for (std::size_t index = 0; index < 64; ++index) {
        EXPECT_EQ('a', destinations[index]);
    }
    EXPECT_EQ('x', destinations.back());
    tail_barrier.Release();
    client.reset();
    EXPECT_TRUE(server.Ok()) << server.Error();
}

} // namespace
} // namespace kv_cache_manager
