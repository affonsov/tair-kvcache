#pragma once

#include <functional>
#include <future>
#include <memory>
#include <string>

#include "kv_cache_manager/client/include/common.h"

namespace autil {
class LockFreeThreadPool;
}

namespace kv_cache_manager {

class LockFreeThreadPool {
public:
    enum class SubmissionStatus {
        ACCEPTED,
        POOL_STOPPED,
        ITEM_IS_NULL,
        QUEUE_FULL,
    };

    struct SubmissionResult {
        SubmissionStatus status{SubmissionStatus::POOL_STOPPED};
        std::future<ClientErrorCode> future;

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
    std::future<ClientErrorCode> async(std::function<ClientErrorCode()> &&func);
    virtual SubmissionResult TryAsync(std::function<ClientErrorCode()> &&func);

private:
    std::unique_ptr<autil::LockFreeThreadPool> pool_;
};

} // namespace kv_cache_manager