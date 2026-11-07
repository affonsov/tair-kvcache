#include "kv_cache_manager/client/src/internal/sdk/sdk_interface.h"

namespace kv_cache_manager {

SdkCallContext::SdkCallContext(Deadline deadline)
    : shared_state_(std::make_shared<SharedState>(deadline))
    , output_commit_state_(std::make_shared<OutputCommitState>()) {}

SdkCallContext::SdkCallContext(std::shared_ptr<SharedState> shared_state,
                               std::shared_ptr<OutputCommitState> output_commit_state)
    : shared_state_(std::move(shared_state)), output_commit_state_(std::move(output_commit_state)) {}

SdkCallContext::Deadline SdkCallContext::deadline() const { return shared_state_->deadline; }

bool SdkCallContext::IsLive() const {
    return !shared_state_->cancelled.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < shared_state_->deadline;
}

SdkCallContext SdkCallContext::MakeTaskContext() const {
    return SdkCallContext(shared_state_, std::make_shared<OutputCommitState>());
}

bool SdkCallContext::HasAdmittedOutputCommit() const {
    const auto value = output_commit_state_->value.load(std::memory_order_acquire);
    return OutputCommitState::Phase(value) != OutputCommitPhase::NOT_ADMITTED;
}

std::optional<ClientErrorCode> SdkCallContext::CompletedOutputCommit() const {
    const auto value = output_commit_state_->value.load(std::memory_order_acquire);
    if (OutputCommitState::Phase(value) != OutputCommitPhase::COMPLETED) {
        return std::nullopt;
    }
    return OutputCommitState::Result(value);
}

std::optional<SdkCallContext::CallerMemoryGuard> SdkCallContext::TryAcquireCallerMemory(AccessKind kind) const {
    std::unique_lock lock(shared_state_->mutex);
    if (shared_state_->cancelled.load(std::memory_order_acquire) ||
        std::chrono::steady_clock::now() >= shared_state_->deadline) {
        return std::nullopt;
    }

    std::shared_ptr<OutputCommitState> output_commit_state;
    if (kind == AccessKind::OUTPUT_COMMIT) {
        auto expected = OutputCommitState::Encode(OutputCommitPhase::NOT_ADMITTED);
        if (!output_commit_state_->value.compare_exchange_strong(expected,
                                                                 OutputCommitState::Encode(OutputCommitPhase::ADMITTED),
                                                                 std::memory_order_acq_rel,
                                                                 std::memory_order_acquire)) {
            return std::nullopt;
        }
        output_commit_state = output_commit_state_;
    }
    ++shared_state_->active_callers;
    return CallerMemoryGuard(shared_state_, std::move(output_commit_state));
}

void SdkCallContext::Cancel() const {
    std::unique_lock lock(shared_state_->mutex);
    shared_state_->cancelled.store(true, std::memory_order_release);
}

void SdkCallContext::WaitForCallerMemory() const {
    std::unique_lock lock(shared_state_->mutex);
    shared_state_->cv.wait(lock, [this] { return shared_state_->active_callers == 0; });
}

void SdkCallContext::CancelAndWait() const {
    Cancel();
    WaitForCallerMemory();
}

void SdkCallContext::CallerMemoryGuard::Complete(ClientErrorCode result) {
    if (!output_commit_state_) {
        return;
    }
    auto expected = OutputCommitState::Encode(OutputCommitPhase::ADMITTED);
    output_commit_state_->value.compare_exchange_strong(expected,
                                                        OutputCommitState::Encode(OutputCommitPhase::COMPLETED, result),
                                                        std::memory_order_acq_rel,
                                                        std::memory_order_acquire);
}

void SdkCallContext::CallerMemoryGuard::Release() {
    auto shared_state = std::move(shared_state_);
    output_commit_state_.reset();
    if (!shared_state) {
        return;
    }
    std::unique_lock lock(shared_state->mutex);
    --shared_state->active_callers;
    if (shared_state->active_callers == 0) {
        shared_state->cv.notify_all();
    }
}

SdkInterface::GroupMap SdkInterface::SplitByPath(const std::vector<DataStorageUri> &remote_uris,
                                                 const BlockBuffers &local_buffers) {
    GroupMap groups;
    for (size_t i = 0; i < remote_uris.size(); ++i) {
        const auto &uri = remote_uris[i];
        const BlockBuffer &buf = local_buffers[i];
        auto &group = groups[uri.GetPath()];
        group.remote_uris.push_back(uri);
        group.local_buffers.push_back(buf);
        group.indices.push_back(i);
    }
    return groups;
}
} // namespace kv_cache_manager
