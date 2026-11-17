#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(USING_CUDA) && defined(USING_MUSA)
#error "ValkeySdkGpuTest cannot enable CUDA and MUSA simultaneously"
#endif

#if defined(USING_CUDA)
#include <cuda_runtime.h>
#elif defined(USING_MUSA)
#include <musa_runtime.h>
#else
#error "ValkeySdkGpuTest requires CUDA or MUSA"
#endif

#include "kv_cache_manager/client/src/internal/sdk/sdk_wrapper.h"
#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"
#include "kv_cache_manager/common/lock_free_thread_pool.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {
namespace {

#if defined(USING_CUDA)
using GpuError = cudaError_t;
constexpr GpuError kGpuSuccess = cudaSuccess;

GpuError GetDeviceCount(int *count) { return cudaGetDeviceCount(count); }
GpuError GetDevice(int *device) { return cudaGetDevice(device); }
GpuError SetDevice(int device) { return cudaSetDevice(device); }
GpuError DeviceAllocate(void **pointer, std::size_t size) { return cudaMalloc(pointer, size); }
GpuError DeviceFree(void *pointer) { return cudaFree(pointer); }
GpuError CopyHostToDevice(void *destination, const void *source, std::size_t size) {
    return cudaMemcpy(destination, source, size, cudaMemcpyHostToDevice);
}
GpuError CopyDeviceToHost(void *destination, const void *source, std::size_t size) {
    return cudaMemcpy(destination, source, size, cudaMemcpyDeviceToHost);
}
#elif defined(USING_MUSA)
using GpuError = musaError_t;
constexpr GpuError kGpuSuccess = musaSuccess;

GpuError GetDeviceCount(int *count) { return musaGetDeviceCount(count); }
GpuError GetDevice(int *device) { return musaGetDevice(device); }
GpuError SetDevice(int device) { return musaSetDevice(device); }
GpuError DeviceAllocate(void **pointer, std::size_t size) { return musaMalloc(pointer, size); }
GpuError DeviceFree(void *pointer) { return musaFree(pointer); }
GpuError CopyHostToDevice(void *destination, const void *source, std::size_t size) {
    return musaMemcpy(destination, source, size, musaMemcpyHostToDevice);
}
GpuError CopyDeviceToHost(void *destination, const void *source, std::size_t size) {
    return musaMemcpy(destination, source, size, musaMemcpyDeviceToHost);
}
#endif

class DeviceBuffer {
public:
    DeviceBuffer() = default;
    ~DeviceBuffer() {
        if (!pointer_) {
            return;
        }
        int original_device = -1;
        const GpuError get_error = GetDevice(&original_device);
        EXPECT_EQ(kGpuSuccess, get_error);
        if (get_error != kGpuSuccess) {
            return;
        }
        const GpuError set_error = SetDevice(device_);
        EXPECT_EQ(kGpuSuccess, set_error);
        if (set_error != kGpuSuccess) {
            return;
        }
        EXPECT_EQ(kGpuSuccess, DeviceFree(pointer_));
        EXPECT_EQ(kGpuSuccess, SetDevice(original_device));
    }

    DeviceBuffer(const DeviceBuffer &) = delete;
    DeviceBuffer &operator=(const DeviceBuffer &) = delete;

    bool Allocate(int device, std::size_t size) {
        int original_device = -1;
        if (GetDevice(&original_device) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
            return false;
        }
        device_ = device;
        const GpuError allocate_error = DeviceAllocate(&pointer_, size);
        const GpuError restore_error = SetDevice(original_device);
        return allocate_error == kGpuSuccess && restore_error == kGpuSuccess;
    }

    void *get(std::size_t offset = 0) const {
        return reinterpret_cast<void *>(reinterpret_cast<std::uintptr_t>(pointer_) + offset);
    }

private:
    void *pointer_ = nullptr;
    int device_ = -1;
};

bool CopyToDevice(int device, void *destination, const void *source, std::size_t size) {
    int original_device = -1;
    if (GetDevice(&original_device) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
        return false;
    }
    const GpuError copy_error = CopyHostToDevice(destination, source, size);
    const GpuError restore_error = SetDevice(original_device);
    return copy_error == kGpuSuccess && restore_error == kGpuSuccess;
}

bool CopyFromDevice(int device, void *destination, const void *source, std::size_t size) {
    int original_device = -1;
    if (GetDevice(&original_device) != kGpuSuccess || SetDevice(device) != kGpuSuccess) {
        return false;
    }
    const GpuError copy_error = CopyDeviceToHost(destination, source, size);
    const GpuError restore_error = SetDevice(original_device);
    return copy_error == kGpuSuccess && restore_error == kGpuSuccess;
}

struct FakeGpuRedisState {
    std::size_t set_calls = 0;
    std::size_t get_calls = 0;
    std::size_t get_offset = 0;
    std::vector<std::vector<std::string>> set_batches;
    std::vector<std::string> get_values;
    std::vector<ValkeyGetStringsResult> scripted_get_results;
    std::mutex mutex;
    std::condition_variable cv;
    std::size_t blocked_get_call = std::numeric_limits<std::size_t>::max();
    bool blocked_get_entered = false;
    bool release_blocked_get = false;
    std::optional<SdkCallContext> active_get_context;
};

class FakeGpuRedisClient : public RedisClient {
public:
    explicit FakeGpuRedisClient(const StandardUri &endpoint) : RedisClient(endpoint) {}

protected:
    bool IsContextOk() const override { return true; }
    bool Reconnect() override { return true; }
};

class GpuTestValkeySdk : public ValkeySdk {
public:
    explicit GpuTestValkeySdk(std::shared_ptr<FakeGpuRedisState> state) : state_(std::move(state)) {}

    ClientErrorCode GetWithContext(const std::vector<DataStorageUri> &remote_uris,
                                   const BlockBuffers &local_buffers,
                                   const SdkCallContext &context) override {
        {
            std::lock_guard lock(state_->mutex);
            state_->active_get_context = context;
        }
        state_->cv.notify_all();
        return ValkeySdk::GetWithContext(remote_uris, local_buffers, context);
    }

protected:
    std::shared_ptr<RedisClient> CreateClient(const RedisConnectionOptions &options) override {
        return std::make_shared<FakeGpuRedisClient>(options.endpoint);
    }
    bool PrepareClient(RedisClient &) override { return true; }
    ValkeySetStringsResult SetStringsUntil(RedisClient &,
                                           const std::vector<std::string> &keys,
                                           const std::vector<std::string> &values,
                                           Deadline) override {
        ++state_->set_calls;
        state_->set_batches.push_back(values);
        ValkeySetStringsResult result;
        result.operation_error = EC_OK;
        result.errors.assign(keys.size(), EC_OK);
        result.certainties.assign(keys.size(), ValkeyWriteCertainty::CONFIRMED);
        return result;
    }
    ValkeyGetStringsResult GetStringsUntil(RedisClient &,
                                           const std::vector<std::string> &keys,
                                           const std::vector<std::size_t> &,
                                           Deadline) override {
        const std::size_t call_index = state_->get_calls++;
        if (call_index == state_->blocked_get_call) {
            std::unique_lock lock(state_->mutex);
            state_->blocked_get_entered = true;
            state_->cv.notify_all();
            state_->cv.wait(lock, [&] { return state_->release_blocked_get; });
        }
        if (call_index < state_->scripted_get_results.size()) {
            return state_->scripted_get_results[call_index];
        }
        ValkeyGetStringsResult result;
        result.operation_error = EC_OK;
        result.errors.assign(keys.size(), EC_OK);
        const auto begin = state_->get_values.begin() + state_->get_offset;
        result.values.assign(begin, begin + keys.size());
        state_->get_offset += keys.size();
        return result;
    }

private:
    std::shared_ptr<FakeGpuRedisState> state_;
};

std::shared_ptr<SdkBackendConfig> MakeSdkConfig() {
    auto config = std::make_shared<ValkeySdkConfig>();
    SdkTimeoutConfig timeout;
    timeout.set_get_timeout_ms(2000);
    timeout.set_put_timeout_ms(2000);
    config->set_timeout_config(timeout);
    return config;
}

std::shared_ptr<StorageConfig> MakeStorageConfig() {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint("valkey://cache.internal:6379/?db=11");
    return std::make_shared<StorageConfig>(DataStorageType::DATA_STORAGE_TYPE_VALKEY, "valkey-primary", spec);
}

std::string OpaqueId(std::size_t index) {
    std::ostringstream stream;
    stream << std::hex << std::setw(32) << std::setfill('0') << index;
    return stream.str();
}

DataStorageUri MakeLocation(std::size_t index, std::size_t size) {
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation("valkey-primary", OpaqueId(index + 1), size, uri));
    return uri;
}

ValkeyGetStringsResult GetResult(std::vector<std::string> values, ErrorCode operation_error = EC_OK) {
    ValkeyGetStringsResult result;
    result.operation_error = operation_error;
    result.errors.assign(values.size(), operation_error == EC_OK ? EC_OK : operation_error);
    result.values = std::move(values);
    return result;
}

class ValkeySdkGpuTest : public TESTBASE {
public:
    void SetUp() override {
        if (GetDeviceCount(&device_count_) != kGpuSuccess || device_count_ == 0) {
            GTEST_SKIP() << "No accelerator device is available";
        }
        const GpuError current_device_error = GetDevice(&caller_device_);
        if (current_device_error != kGpuSuccess) {
            GTEST_SKIP() << "Cannot read the caller accelerator device";
        }
        runtime_ready_ = true;
        worker_device_ = 0;
        allocation_device_ = device_count_ > 1 ? 1 : 0;
        if (device_count_ > 1) {
            RecordProperty("device_topology", "multi-device owning-device switch covered");
        } else {
            RecordProperty("device_topology", "single-device fallback; cross-device switch unavailable");
        }

        state_ = std::make_shared<FakeGpuRedisState>();
        sdk_ = std::make_shared<GpuTestValkeySdk>(state_);
        ASSERT_EQ(ER_OK, sdk_->Init(MakeSdkConfig(), MakeStorageConfig()));

        wrapper_.wrapper_config_ = std::make_shared<SdkWrapperConfig>();
        SetWrapperTimeout(2000);
        wrapper_.wait_task_thread_pool_ = std::make_unique<LockFreeThreadPool>(1, 16, "ValkeyGpuWorker");
        ASSERT_TRUE(wrapper_.wait_task_thread_pool_->start());
        wrapper_.sdk_map_["valkey-primary"] = sdk_;
        ASSERT_TRUE(SetWorkerDevice(worker_device_));
        EXPECT_EQ(worker_device_, WorkerDevice());
    }

    void TearDown() override {
        if (runtime_ready_) {
            EXPECT_EQ(kGpuSuccess, SetDevice(caller_device_));
        }
    }

protected:
    void SetWrapperTimeout(int timeout_ms) {
        SdkTimeoutConfig timeout;
        timeout.set_get_timeout_ms(timeout_ms);
        timeout.set_put_timeout_ms(timeout_ms);
        wrapper_.wrapper_config_->set_timeout_config(timeout);
    }

    bool RunOnWorker(const std::function<bool()> &action) {
        auto submission =
            wrapper_.wait_task_thread_pool_->TryAsync([action] { return action() ? ER_OK : ER_CUDAMEMCPY_ERROR; });
        if (!submission.accepted()) {
            return false;
        }
        return submission.future.get() == ER_OK;
    }

    bool SetWorkerDevice(int device) {
        return RunOnWorker([device] { return SetDevice(device) == kGpuSuccess; });
    }

    int WorkerDevice() {
        int observed = -1;
        EXPECT_TRUE(RunOnWorker([&] { return GetDevice(&observed) == kGpuSuccess; }));
        return observed;
    }

    bool runtime_ready_ = false;
    int device_count_ = 0;
    int caller_device_ = -1;
    int worker_device_ = 0;
    int allocation_device_ = 0;
    std::shared_ptr<FakeGpuRedisState> state_;
    std::shared_ptr<GpuTestValkeySdk> sdk_;
    SdkWrapper wrapper_;
};

TEST_F(ValkeySdkGpuTest, OwningDevicesInteriorPointersAndWorkerRestoration) {
    DeviceBuffer first_device;
    DeviceBuffer second_device;
    const int second_allocation_device = device_count_ > 1 ? worker_device_ : allocation_device_;
    ASSERT_TRUE(first_device.Allocate(allocation_device_, 4));
    ASSERT_TRUE(second_device.Allocate(second_allocation_device, 4));
    ASSERT_TRUE(CopyToDevice(allocation_device_, first_device.get(1), "ab", 2));
    ASSERT_TRUE(CopyToDevice(second_allocation_device, second_device.get(1), "CD", 2));

    char cpu_put = '!';
    BlockBuffer put_buffer;
    put_buffer.iovs = {
        Iov{MemoryType::GPU, first_device.get(1), 2, false},
        Iov{MemoryType::CPU, &cpu_put, 1, false},
        Iov{MemoryType::GPU, nullptr, 0, false},
        Iov{MemoryType::GPU, second_device.get(1), 2, false},
    };
    const std::vector<DataStorageUri> uris{MakeLocation(0, 5)};
    auto actual = std::make_shared<std::vector<DataStorageUri>>();

    EXPECT_EQ(worker_device_, WorkerDevice());
    ASSERT_EQ(ER_OK, wrapper_.Put(uris, {put_buffer}, actual));
    EXPECT_EQ(worker_device_, WorkerDevice());
    ASSERT_EQ(1u, state_->set_batches.size());
    EXPECT_EQ("ab!CD", state_->set_batches[0][0]);
    ASSERT_TRUE(CopyToDevice(allocation_device_, first_device.get(1), "xx", 2));
    EXPECT_EQ("ab!CD", state_->set_batches[0][0]);

    state_->get_values = {"12#34"};
    char cpu_get = 'x';
    BlockBuffer get_buffer;
    get_buffer.iovs = {
        Iov{MemoryType::GPU, first_device.get(1), 2, false},
        Iov{MemoryType::CPU, &cpu_get, 1, false},
        Iov{MemoryType::GPU, nullptr, 0, false},
        Iov{MemoryType::GPU, second_device.get(1), 2, false},
    };
    ASSERT_EQ(ER_OK, wrapper_.Get(uris, {get_buffer}));
    EXPECT_EQ(worker_device_, WorkerDevice());
    state_->get_values[0] = "zzzzz";

    char first_observed[2] = {};
    char second_observed[2] = {};
    ASSERT_TRUE(CopyFromDevice(allocation_device_, first_observed, first_device.get(1), 2));
    ASSERT_TRUE(CopyFromDevice(second_allocation_device, second_observed, second_device.get(1), 2));
    EXPECT_EQ("12", std::string(first_observed, 2));
    EXPECT_EQ('#', cpu_get);
    EXPECT_EQ("34", std::string(second_observed, 2));
}

TEST_F(ValkeySdkGpuTest, MixedCpuGpuOrderingCrossesSliceBoundary) {
    constexpr std::size_t kBlockCount = 65;
    std::vector<char> put_host(kBlockCount);
    std::vector<char> get_host(kBlockCount, 'x');
    std::vector<char> device_source(kBlockCount);
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        put_host[index] = static_cast<char>('a' + index % 26);
        device_source[index] = put_host[index];
    }
    DeviceBuffer put_device;
    DeviceBuffer get_device;
    ASSERT_TRUE(put_device.Allocate(allocation_device_, kBlockCount));
    ASSERT_TRUE(get_device.Allocate(allocation_device_, kBlockCount));
    ASSERT_TRUE(CopyToDevice(allocation_device_, put_device.get(), device_source.data(), device_source.size()));

    std::vector<DataStorageUri> uris;
    BlockBuffers put_buffers;
    BlockBuffers get_buffers;
    state_->get_values.reserve(kBlockCount);
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        uris.push_back(MakeLocation(index, 1));
        const bool gpu = index % 2 != 0;
        void *put_base = gpu ? put_device.get(index) : &put_host[index];
        void *get_base = gpu ? get_device.get(index) : &get_host[index];
        put_buffers.push_back({{Iov{gpu ? MemoryType::GPU : MemoryType::CPU, put_base, 1, false},
                                Iov{MemoryType::GPU, nullptr, 0, false}}});
        get_buffers.push_back({{Iov{gpu ? MemoryType::GPU : MemoryType::CPU, get_base, 1, false},
                                Iov{MemoryType::GPU, nullptr, 0, false}}});
        state_->get_values.emplace_back(1, static_cast<char>('A' + index % 26));
    }
    auto actual = std::make_shared<std::vector<DataStorageUri>>();

    ASSERT_EQ(ER_OK, wrapper_.Put(uris, put_buffers, actual));
    EXPECT_EQ(worker_device_, WorkerDevice());
    ASSERT_EQ(2u, state_->set_batches.size());
    EXPECT_EQ(64u, state_->set_batches[0].size());
    EXPECT_EQ(1u, state_->set_batches[1].size());
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        EXPECT_EQ(std::string(1, put_host[index]), state_->set_batches[index / 64][index % 64]);
    }

    ASSERT_EQ(ER_OK, wrapper_.Get(uris, get_buffers));
    EXPECT_EQ(worker_device_, WorkerDevice());
    std::vector<char> observed_gpu(kBlockCount, 'x');
    ASSERT_TRUE(CopyFromDevice(allocation_device_, observed_gpu.data(), get_device.get(), observed_gpu.size()));
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        const char observed = index % 2 == 0 ? get_host[index] : observed_gpu[index];
        EXPECT_EQ(static_cast<char>('A' + index % 26), observed);
    }
}

TEST_F(ValkeySdkGpuTest, IgnoredAndZeroSizeIovsPreserveSemanticsThroughWorker) {
    char cpu_first = 'x';
    DeviceBuffer ignored_device;
    DeviceBuffer gpu_last;
    ASSERT_TRUE(ignored_device.Allocate(allocation_device_, 2));
    ASSERT_TRUE(gpu_last.Allocate(allocation_device_, 1));
    const char ignored_seed[2] = {'q', 'q'};
    ASSERT_TRUE(CopyToDevice(allocation_device_, ignored_device.get(), ignored_seed, sizeof(ignored_seed)));
    BlockBuffer buffer;
    buffer.iovs = {
        Iov{MemoryType::CPU, &cpu_first, 1, false},
        Iov{MemoryType::GPU, ignored_device.get(), 2, true},
        Iov{MemoryType::GPU, nullptr, 0, true},
        Iov{MemoryType::GPU, gpu_last.get(), 1, false},
    };
    const std::vector<DataStorageUri> uris{MakeLocation(0, 4)};
    state_->get_values = {"aXXb"};

    ASSERT_EQ(ER_OK, wrapper_.Get(uris, {buffer}));
    EXPECT_EQ(worker_device_, WorkerDevice());
    EXPECT_EQ('a', cpu_first);
    char ignored_observed[2] = {};
    char last_observed = '\0';
    ASSERT_TRUE(CopyFromDevice(allocation_device_, ignored_observed, ignored_device.get(), sizeof(ignored_observed)));
    ASSERT_TRUE(CopyFromDevice(allocation_device_, &last_observed, gpu_last.get(), sizeof(last_observed)));
    EXPECT_EQ("qq", std::string(ignored_observed, 2));
    EXPECT_EQ('b', last_observed);

    auto actual = std::make_shared<std::vector<DataStorageUri>>();
    EXPECT_EQ(ER_INVALID_PARAMS, wrapper_.Put(uris, {buffer}, actual));
    EXPECT_EQ(0u, state_->set_calls);
    EXPECT_EQ(worker_device_, WorkerDevice());
}

TEST_F(ValkeySdkGpuTest, RuntimeFailuresMapAndDoNotChangeWorkerDevice) {
    BlockBuffer invalid_gpu;
    invalid_gpu.iovs = {Iov{MemoryType::GPU, reinterpret_cast<void *>(1), 1, false}};
    const std::vector<DataStorageUri> uris{MakeLocation(0, 1)};
    auto actual = std::make_shared<std::vector<DataStorageUri>>();

    EXPECT_EQ(ER_CUDAMEMCPY_ERROR, wrapper_.Put(uris, {invalid_gpu}, actual));
    EXPECT_EQ(0u, state_->set_calls);
    EXPECT_EQ(worker_device_, WorkerDevice());

    state_->get_values = {"x"};
    EXPECT_EQ(ER_CUDAMEMCPY_ERROR, wrapper_.Get(uris, {invalid_gpu}));
    EXPECT_EQ(1u, state_->get_calls);
    EXPECT_EQ(worker_device_, WorkerDevice());
}

TEST_F(ValkeySdkGpuTest, DeadlineCancellationWaitsForLaterSliceIoAndOutputGuard) {
    constexpr std::size_t kBlockCount = 129;
    DeviceBuffer destination;
    ASSERT_TRUE(destination.Allocate(allocation_device_, kBlockCount));
    std::vector<char> initial(kBlockCount, 'x');
    ASSERT_TRUE(CopyToDevice(allocation_device_, destination.get(), initial.data(), initial.size()));

    std::vector<DataStorageUri> uris;
    BlockBuffers buffers;
    for (std::size_t index = 0; index < kBlockCount; ++index) {
        uris.push_back(MakeLocation(index, 1));
        buffers.push_back({{Iov{MemoryType::GPU, destination.get(index), 1, false}}});
    }
    state_->scripted_get_results = {
        GetResult(std::vector<std::string>(64, "a")),
        GetResult(std::vector<std::string>(64, "b"), EC_TIMEOUT),
        GetResult({"c"}),
    };
    state_->blocked_get_call = 1;
    SetWrapperTimeout(500);

    auto result = std::async(std::launch::async, [&] { return wrapper_.Get(uris, buffers); });
    std::optional<SdkCallContext> task_context;
    bool barrier_ready = false;
    {
        std::unique_lock lock(state_->mutex);
        barrier_ready = state_->cv.wait_until(lock, std::chrono::steady_clock::now() + std::chrono::seconds(5), [&] {
            return state_->blocked_get_entered && state_->active_get_context.has_value();
        });
        task_context = state_->active_get_context;
    }
    EXPECT_TRUE(barrier_ready);
    EXPECT_TRUE(task_context.has_value());
    if (!barrier_ready || !task_context.has_value()) {
        {
            std::lock_guard lock(state_->mutex);
            state_->release_blocked_get = true;
        }
        state_->cv.notify_all();
        EXPECT_NE(std::future_status::timeout, result.wait_for(std::chrono::seconds(5)));
        return;
    }
    EXPECT_TRUE(task_context->HasAdmittedOutputCommit());

    const auto cancellation_watchdog = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!task_context->shared_state_->cancelled.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < cancellation_watchdog) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(task_context->shared_state_->cancelled.load(std::memory_order_acquire));
    EXPECT_EQ(std::future_status::timeout, result.wait_for(std::chrono::seconds(0)));

    {
        std::lock_guard lock(state_->mutex);
        state_->release_blocked_get = true;
    }
    state_->cv.notify_all();

    // The guarded Valkey task can no longer touch caller buffers after the drain.
    EXPECT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, result.get());
    EXPECT_EQ(2u, state_->get_calls);
    EXPECT_EQ(worker_device_, WorkerDevice());
    std::vector<char> observed(kBlockCount, '\0');
    ASSERT_TRUE(CopyFromDevice(allocation_device_, observed.data(), destination.get(), observed.size()));
    EXPECT_TRUE(std::all_of(observed.begin(), observed.begin() + 64, [](char value) { return value == 'a'; }));
    EXPECT_TRUE(std::all_of(observed.begin() + 64, observed.end(), [](char value) { return value == 'x'; }));
    EXPECT_EQ(std::optional<ClientErrorCode>(ER_SDK_TIMEOUT), task_context->CompletedOutputCommit());
}

} // namespace
} // namespace kv_cache_manager
