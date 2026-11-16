#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(USING_CUDA) && defined(USING_MUSA)
#error "ValkeyTransferClientGpuTest cannot enable CUDA and MUSA simultaneously"
#endif

#if defined(USING_CUDA)
#include <cuda_runtime.h>
#elif defined(USING_MUSA)
#include <musa_runtime.h>
#else
#error "ValkeyTransferClientGpuTest requires CUDA or MUSA"
#endif

#include "kv_cache_manager/client/include/transfer_client.h"
#include "kv_cache_manager/client/test/valkey_loopback_test_server.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {
namespace {

using test::LoopbackRespServer;
using namespace std::chrono_literals;

#if defined(USING_CUDA)
using GpuError = cudaError_t;
constexpr GpuError kGpuSuccess = cudaSuccess;
GpuError GetDeviceCount(int *count) { return cudaGetDeviceCount(count); }
GpuError GetDevice(int *device) { return cudaGetDevice(device); }
GpuError SetDevice(int device) { return cudaSetDevice(device); }
GpuError Allocate(void **pointer, std::size_t size) { return cudaMalloc(pointer, size); }
GpuError Free(void *pointer) { return cudaFree(pointer); }
GpuError HostToDevice(void *destination, const void *source, std::size_t size) {
    return cudaMemcpy(destination, source, size, cudaMemcpyHostToDevice);
}
GpuError DeviceToHost(void *destination, const void *source, std::size_t size) {
    return cudaMemcpy(destination, source, size, cudaMemcpyDeviceToHost);
}
#elif defined(USING_MUSA)
using GpuError = musaError_t;
constexpr GpuError kGpuSuccess = musaSuccess;
GpuError GetDeviceCount(int *count) { return musaGetDeviceCount(count); }
GpuError GetDevice(int *device) { return musaGetDevice(device); }
GpuError SetDevice(int device) { return musaSetDevice(device); }
GpuError Allocate(void **pointer, std::size_t size) { return musaMalloc(pointer, size); }
GpuError Free(void *pointer) { return musaFree(pointer); }
GpuError HostToDevice(void *destination, const void *source, std::size_t size) {
    return musaMemcpy(destination, source, size, musaMemcpyHostToDevice);
}
GpuError DeviceToHost(void *destination, const void *source, std::size_t size) {
    return musaMemcpy(destination, source, size, musaMemcpyDeviceToHost);
}
#endif

constexpr char kStorageIdentity[] = "valkey-public-gpu";
constexpr int64_t kTestDatabase = 7;
// Separate Get and Put StaticClientPool instances, each with kDefaultPoolSize (4)
// connections, are opened during TransferClient::Create.
constexpr std::size_t kExpectedPoolConnections = 8;

class DeviceBuffer {
public:
    ~DeviceBuffer() {
        if (pointer_ == nullptr) {
            return;
        }
        int original = -1;
        if (GetDevice(&original) == kGpuSuccess && SetDevice(device_) == kGpuSuccess) {
            EXPECT_EQ(kGpuSuccess, Free(pointer_));
            EXPECT_EQ(kGpuSuccess, SetDevice(original));
        }
    }

    bool AllocateOn(int device, std::size_t size) {
        int original = -1;
        if (GetDevice(&original) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
            return false;
        }
        device_ = device;
        const GpuError allocation = Allocate(&pointer_, size);
        const GpuError restoration = SetDevice(original);
        return allocation == kGpuSuccess && restoration == kGpuSuccess;
    }

    void *At(std::size_t offset = 0) const {
        return reinterpret_cast<void *>(reinterpret_cast<std::uintptr_t>(pointer_) + offset);
    }

private:
    void *pointer_ = nullptr;
    int device_ = -1;
};

bool CopyToDevice(int device, void *destination, const void *source, std::size_t size) {
    int original = -1;
    if (GetDevice(&original) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
        return false;
    }
    const GpuError copy = HostToDevice(destination, source, size);
    const GpuError restoration = SetDevice(original);
    return copy == kGpuSuccess && restoration == kGpuSuccess;
}

bool CopyFromDevice(int device, void *destination, const void *source, std::size_t size) {
    int original = -1;
    if (GetDevice(&original) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
        return false;
    }
    const GpuError copy = DeviceToHost(destination, source, size);
    const GpuError restoration = SetDevice(original);
    return copy == kGpuSuccess && restoration == kGpuSuccess;
}

std::string ClientConfig() {
    return R"({
        "instance_group": "valkey-public-gpu-group",
        "instance_id": "valkey-public-gpu-instance",
        "block_size": 16,
        "sdk_config": {
            "thread_num": 1,
            "queue_size": 8,
            "sdk_backend_configs": [{"type": "valkey"}],
            "timeout_config": {"get_timeout_ms": 2000, "put_timeout_ms": 2000}
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

class InMemoryValkey {
public:
    LoopbackRespServer::Response Handle(const LoopbackRespServer::Command &command) {
        if (command.size() == 3 && command[0] == "SET") {
            std::lock_guard lock(mutex_);
            values_[command[1]] = command[2];
            return LoopbackRespServer::Response::Reply("+OK\r\n");
        }
        if (command.size() == 5 && command[0] == "EVAL_RO") {
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

class ValkeyTransferClientGpuTest : public TESTBASE {
public:
    void SetUp() override {
        if (GetDeviceCount(&device_count_) != kGpuSuccess || device_count_ == 0) {
            GTEST_SKIP() << "No accelerator device is available";
        }
        ASSERT_EQ(kGpuSuccess, GetDevice(&incoming_device_));
        ASSERT_EQ(kGpuSuccess, SetDevice(0));
        allocation_device_ = device_count_ > 1 ? 1 : 0;
        if (device_count_ > 1) {
            RecordProperty(
                "device_coverage",
                "public end-to-end data and slicing coverage with caller device 0 and allocations on device 1");
        } else {
            RecordProperty(
                "device_coverage",
                "single-device end-to-end data and slicing coverage; cross-device allocation is unavailable");
        }
        RecordProperty(
            "worker_device_restoration",
            "covered by the internal ValkeySdkGpuTest; this public target does not observe a worker thread's device");
    }

    void TearDown() override {
        if (incoming_device_ >= 0) {
            EXPECT_EQ(kGpuSuccess, SetDevice(incoming_device_));
        }
    }

protected:
    int device_count_ = 0;
    int incoming_device_ = -1;
    int allocation_device_ = -1;
};

TEST_F(ValkeyTransferClientGpuTest, GpuOnlyAndMixedBuffersAreVisibleOnReturn) {
    InMemoryValkey state;
    LoopbackRespServer server([&](const auto &command) { return state.Handle(command); }, kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    DeviceBuffer gpu_source;
    DeviceBuffer gpu_destination;
    ASSERT_TRUE(gpu_source.AllocateOn(allocation_device_, 6));
    ASSERT_TRUE(gpu_destination.AllocateOn(allocation_device_, 6));
    const char source_bytes[6] = {'A', 'B', 'C', 'D', 'Y', 'Z'};
    ASSERT_TRUE(CopyToDevice(allocation_device_, gpu_source.At(), source_bytes, sizeof(source_bytes)));
    char mixed_cpu_source = 'x';
    const UriStrVec locations{Location(0, 4), Location(1, 3)};
    const BlockBuffers sources{
        BlockBuffer{{Iov{MemoryType::GPU, gpu_source.At(), 4, false}}},
        BlockBuffer{
            {Iov{MemoryType::CPU, &mixed_cpu_source, 1, false}, Iov{MemoryType::GPU, gpu_source.At(4), 2, false}}},
    };

    const auto [save_ec, actual_locations] = client->SaveKvCaches(locations, sources);
    ASSERT_EQ(ER_OK, save_ec);
    EXPECT_EQ(locations, actual_locations);

    const char replacement[6] = {'q', 'q', 'q', 'q', 'q', 'q'};
    ASSERT_TRUE(CopyToDevice(allocation_device_, gpu_source.At(), replacement, sizeof(replacement)));
    char mixed_cpu_destination = '?';
    const BlockBuffers destinations{
        BlockBuffer{{Iov{MemoryType::GPU, gpu_destination.At(), 4, false}}},
        BlockBuffer{{Iov{MemoryType::CPU, &mixed_cpu_destination, 1, false},
                     Iov{MemoryType::GPU, gpu_destination.At(4), 2, false}}},
    };
    ASSERT_EQ(ER_OK, client->LoadKvCaches(actual_locations, destinations));

    char observed[6] = {};
    ASSERT_TRUE(CopyFromDevice(allocation_device_, observed, gpu_destination.At(), sizeof(observed)));
    EXPECT_EQ("ABCD", std::string(observed, 4));
    EXPECT_EQ('x', mixed_cpu_destination);
    EXPECT_EQ("YZ", std::string(observed + 4, 2));
    client.reset();
    EXPECT_TRUE(state.Valid());
    EXPECT_TRUE(server.Ok()) << server.Error();
}

TEST_F(ValkeyTransferClientGpuTest, MixedCpuGpuBuffersCrossSixtyFourBlockSlice) {
    constexpr std::size_t kBlockCount = 65;
    InMemoryValkey state;
    LoopbackRespServer server([&](const auto &command) { return state.Handle(command); }, kTestDatabase);
    ASSERT_TRUE(server.Ok()) << server.Error();
    auto client = TransferClient::Create(ClientConfig(), TestInitParams(server.Port()));
    ASSERT_NE(nullptr, client);
    ASSERT_TRUE(CompletedExpectedPoolHandshakes(server))
        << "accepted=" << server.AcceptedConnectionCount() << ", completed=" << server.CompletedHandshakeCount();

    std::vector<char> cpu_source(kBlockCount);
    std::vector<char> cpu_destination(kBlockCount, 'x');
    std::vector<char> device_source(kBlockCount);
    DeviceBuffer gpu_source;
    DeviceBuffer gpu_destination;
    ASSERT_TRUE(gpu_source.AllocateOn(allocation_device_, kBlockCount));
    ASSERT_TRUE(gpu_destination.AllocateOn(allocation_device_, kBlockCount));
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        cpu_source[index] = static_cast<char>('a' + index % 26);
        device_source[index] = cpu_source[index];
    }
    ASSERT_TRUE(CopyToDevice(allocation_device_, gpu_source.At(), device_source.data(), device_source.size()));

    UriStrVec locations;
    BlockBuffers sources;
    BlockBuffers destinations;
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        const bool gpu = index % 2 != 0;
        locations.push_back(Location(index, 1));
        sources.push_back(BlockBuffer{
            {Iov{gpu ? MemoryType::GPU : MemoryType::CPU, gpu ? gpu_source.At(index) : &cpu_source[index], 1, false}}});
        destinations.push_back(BlockBuffer{{Iov{gpu ? MemoryType::GPU : MemoryType::CPU,
                                                gpu ? gpu_destination.At(index) : &cpu_destination[index],
                                                1,
                                                false}}});
    }

    const std::size_t put_start = server.CommandCount();
    server.SetReplyGates({put_start + 64});
    std::future<std::pair<ClientErrorCode, UriStrVec>> save_future;
    ReplyGateReleaseGuard put_gate_guard(server);
    save_future = std::async(std::launch::async, [&] { return client->SaveKvCaches(locations, sources); });
    (void)ObserveSliceBoundary(server, put_start + 64, put_start + 65);
    const auto [save_ec, actual_locations] = save_future.get();
    ASSERT_EQ(ER_OK, save_ec);

    const std::size_t get_start = server.CommandCount();
    server.SetReplyGates({get_start + 64});
    std::future<ClientErrorCode> get_future;
    ReplyGateReleaseGuard get_gate_guard(server);
    get_future = std::async(std::launch::async, [&] { return client->LoadKvCaches(actual_locations, destinations); });
    (void)ObserveSliceBoundary(server, get_start + 64, get_start + 65);
    ASSERT_EQ(ER_OK, get_future.get());

    std::vector<char> observed_gpu(kBlockCount, 'x');
    ASSERT_TRUE(CopyFromDevice(allocation_device_, observed_gpu.data(), gpu_destination.At(), observed_gpu.size()));
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        EXPECT_EQ(cpu_source[index], index % 2 == 0 ? cpu_destination[index] : observed_gpu[index]);
    }
    client.reset();
    EXPECT_TRUE(state.Valid());
    EXPECT_TRUE(server.Ok()) << server.Error();
}

} // namespace
} // namespace kv_cache_manager
