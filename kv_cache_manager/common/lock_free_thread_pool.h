#pragma once

#include <autil/LockFreeThreadPool.h>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace kv_cache_manager {

class LockFreeThreadPool {
public:
    enum class SubmissionStatus {
        ACCEPTED,
        POOL_STOPPED,
        ITEM_IS_NULL,
        QUEUE_FULL,
    };

    template <typename Result>
    struct SubmissionResult {
        SubmissionStatus status{SubmissionStatus::POOL_STOPPED};
        std::future<Result> future;

        bool accepted() const { return status == SubmissionStatus::ACCEPTED; }
    };

    LockFreeThreadPool(size_t thread_num,
                       size_t queue_size,
                       const std::string &thread_name,
                       bool stop_if_has_exception = false);
    virtual ~LockFreeThreadPool();

    bool start(const std::string &name = "");
    void stop();
    void waitFinish();
    bool isFull() const;

    template <typename Function>
    auto async(Function &&function) {
        return pool_->async(std::forward<Function>(function));
    }

    // Submits without blocking and without running the task inline. A task
    // that is not accepted never runs, and its result carries no valid future.
    template <typename Function>
    auto TryAsync(Function &&function) -> SubmissionResult<std::invoke_result_t<std::decay_t<Function> &>> {
        using Result = std::invoke_result_t<std::decay_t<Function> &>;
        auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Function>(function));
        auto future = task->get_future();
        const SubmissionStatus status = TryPush([task = std::move(task)] { (*task)(); });
        if (status != SubmissionStatus::ACCEPTED) {
            return {status, {}};
        }
        return {status, std::move(future)};
    }

protected:
    // Single submission point for TryAsync, virtual so tests can inject outcomes.
    virtual SubmissionStatus TryPush(std::function<void()> &&task);

private:
    std::unique_ptr<autil::LockFreeThreadPool> pool_;
};

} // namespace kv_cache_manager
