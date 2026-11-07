#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

#include "kv_cache_manager/client/include/common.h"
#include "kv_cache_manager/client/src/internal/config/sdk_config.h"
#include "kv_cache_manager/client/src/internal/sdk/sdk_type.h"
#include "kv_cache_manager/data_storage/data_storage_uri.h"
#include "kv_cache_manager/data_storage/storage_config.h"

namespace kv_cache_manager {

// Per-call state shared by SdkWrapper and contextual SDK implementations. Child
// contexts share the deadline, cancellation, and caller-memory synchronization,
// but each child publishes its own output-commit result.
class SdkCallContext {
private:
    struct SharedState;
    struct OutputCommitState;

public:
    using Deadline = std::chrono::steady_clock::time_point;

    class CallerMemoryGuard {
    public:
        CallerMemoryGuard(const CallerMemoryGuard &) = delete;
        CallerMemoryGuard &operator=(const CallerMemoryGuard &) = delete;
        CallerMemoryGuard(CallerMemoryGuard &&other) noexcept
            : shared_state_(std::move(other.shared_state_))
            , output_commit_state_(std::move(other.output_commit_state_)) {}
        CallerMemoryGuard &operator=(CallerMemoryGuard &&other) noexcept {
            if (this != &other) {
                Release();
                shared_state_ = std::move(other.shared_state_);
                output_commit_state_ = std::move(other.output_commit_state_);
            }
            return *this;
        }
        ~CallerMemoryGuard() { Release(); }

        // Publish the result only after all caller-visible writes covered by
        // this output guard are complete. Destruction without Complete() leaves
        // the commit admitted but incomplete.
        void Complete(ClientErrorCode result);

    private:
        friend class SdkCallContext;
        CallerMemoryGuard(std::shared_ptr<SharedState> shared_state,
                          std::shared_ptr<OutputCommitState> output_commit_state)
            : shared_state_(std::move(shared_state)), output_commit_state_(std::move(output_commit_state)) {}
        void Release();

        std::shared_ptr<SharedState> shared_state_;
        std::shared_ptr<OutputCommitState> output_commit_state_;
    };

    enum class AccessKind {
        SOURCE_READ,
        OUTPUT_COMMIT,
    };

    explicit SdkCallContext(Deadline deadline);

    Deadline deadline() const;
    bool IsLive() const;
    SdkCallContext MakeTaskContext() const;
    bool HasAdmittedOutputCommit() const;
    std::optional<ClientErrorCode> CompletedOutputCommit() const;
    std::optional<CallerMemoryGuard> TryAcquireCallerMemory(AccessKind kind = AccessKind::SOURCE_READ) const;

    // Cancellation is published separately so callers can stop new access
    // before bounded future draining, then synchronize admitted access.
    void Cancel() const;
    void WaitForCallerMemory() const;
    void CancelAndWait() const;

private:
    SdkCallContext(std::shared_ptr<SharedState> shared_state, std::shared_ptr<OutputCommitState> output_commit_state);

    struct SharedState {
        explicit SharedState(Deadline value) : deadline(value) {}

        const Deadline deadline;
        std::atomic<bool> cancelled{false};
        std::mutex mutex;
        std::condition_variable cv;
        std::size_t active_callers = 0;
    };

    enum class OutputCommitPhase : std::uint32_t {
        NOT_ADMITTED,
        ADMITTED,
        COMPLETED,
    };

    struct OutputCommitState {
        static constexpr std::uint64_t Encode(OutputCommitPhase phase, ClientErrorCode result = ER_TASK_CANCELLED) {
            return (static_cast<std::uint64_t>(static_cast<std::uint32_t>(static_cast<std::int32_t>(result))) << 32) |
                   static_cast<std::uint32_t>(phase);
        }

        static constexpr OutputCommitPhase Phase(std::uint64_t value) {
            return static_cast<OutputCommitPhase>(static_cast<std::uint32_t>(value));
        }

        static constexpr ClientErrorCode Result(std::uint64_t value) {
            return static_cast<ClientErrorCode>(static_cast<std::int32_t>(value >> 32));
        }

        std::atomic<std::uint64_t> value{Encode(OutputCommitPhase::NOT_ADMITTED)};
    };

    std::shared_ptr<SharedState> shared_state_;
    std::shared_ptr<OutputCommitState> output_commit_state_;
};

// Existing storage SDK ABI. Keep the virtual method order unchanged.
class SdkInterface {
public:
    SdkInterface() {}
    virtual ~SdkInterface() = default;
    virtual ClientErrorCode Init(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                 const std::shared_ptr<StorageConfig> &storage_config) = 0;

    virtual SdkType Type() = 0;

    // 一个remote_uri和一个Blockbuffer对应一个block
    //
    // 同序契约（Get/Put 共同遵守）：出参与入参按位置一一对应。
    //   - Get：local_buffers[i] 必须被 remote_uris[i] 指向的 block 填充。
    //   - Put：返回时 (*actual_remote_uris)[i] 必须是 remote_uris[i] /
    //     local_buffers[i] 这个 block 实际写入的远端位置，且
    //     actual_remote_uris->size() == remote_uris.size()。
    // 实现不得按内部分组（如按 path 聚合）的迭代顺序回填 actual_remote_uris；
    // 即使同一请求内多个 block 落在不同 path（交错出现），也必须保证上述同序性。
    // 上层 SdkWrapper 依赖该契约把各 SDK 的返回值回填到原始请求位置。
    // 超时路径必须输出可归因日志（超时契约见类前注释）。
    virtual ClientErrorCode Get(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers &local_buffers) = 0;
    // actual_remote_uris是实际存储的远端地址
    virtual ClientErrorCode Put(const std::vector<DataStorageUri> &remote_uris,
                                const BlockBuffers &local_buffers,
                                std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) = 0;

protected:
    virtual ClientErrorCode Alloc(const std::vector<DataStorageUri> &remote_uris,
                                  std::vector<DataStorageUri> &alloc_uris) = 0;

    using GroupMap = std::unordered_map<std::string, BlockGroup>;
    // 按 path 分组，并记录每个元素在原始入参中的下标到 BlockGroup::indices。
    GroupMap SplitByPath(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers &local_buffers);
};

// Optional contextual API. SdkWrapper discovers it with dynamic_cast and uses
// the unchanged SdkInterface methods for legacy implementations.
//
// Buffer-safety contract: implementations access caller buffers only while
// holding a guard from SdkCallContext::TryAcquireCallerMemory(), and leave no
// asynchronous access running once that guard is released. SdkWrapper relies
// on this to report ER_SDK_TIMEOUT_BUFFER_SAFE / ER_SDK_ERROR_BUFFER_SAFE for
// contextual tasks after cancellation and WaitForCallerMemory().
class ContextualSdkInterface {
public:
    virtual ~ContextualSdkInterface() = default;
    virtual ClientErrorCode GetWithContext(const std::vector<DataStorageUri> &remote_uris,
                                           const BlockBuffers &local_buffers,
                                           const SdkCallContext &context) = 0;
    virtual ClientErrorCode PutWithContext(const std::vector<DataStorageUri> &remote_uris,
                                           const BlockBuffers &local_buffers,
                                           std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris,
                                           const SdkCallContext &context) = 0;
};

} // namespace kv_cache_manager
