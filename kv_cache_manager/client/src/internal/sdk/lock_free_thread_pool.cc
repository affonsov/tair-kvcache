#include "kv_cache_manager/client/src/internal/sdk/lock_free_thread_pool.h"

#include <autil/LockFreeThreadPool.h>

#include "kv_cache_manager/common/logger.h"

namespace kv_cache_manager {

LockFreeThreadPool::LockFreeThreadPool(size_t thread_num,
                                       size_t queue_size,
                                       const std::string &thread_name,
                                       bool stop_if_has_exception)
    : pool_(new autil::LockFreeThreadPool(thread_num, queue_size, nullptr, thread_name, stop_if_has_exception)) {}
LockFreeThreadPool::~LockFreeThreadPool() { pool_.reset(); }

bool LockFreeThreadPool::start(const std::string &name) {
    if (!pool_->start(name)) {
        KVCM_LOG_WARN("start lock free thread pool %s failed", name.c_str());
        return false;
    }
    return true;
}
void LockFreeThreadPool::stop() { pool_->stop(); }
void LockFreeThreadPool::waitFinish() { pool_->waitFinish(); }
bool LockFreeThreadPool::isFull() const { return pool_->isFull(); }
std::future<ClientErrorCode> LockFreeThreadPool::async(std::function<ClientErrorCode()> &&func) {
    return pool_->async(std::move(func));
}

LockFreeThreadPool::SubmissionResult LockFreeThreadPool::TryAsync(std::function<ClientErrorCode()> &&func) {
    auto task = std::make_shared<std::packaged_task<ClientErrorCode()>>(std::move(func));
    auto future = task->get_future();
    const auto result = pool_->pushTask([task = std::move(task)] { (*task)(); }, false, false);

    switch (result) {
    case autil::ThreadPoolBase::ERROR_NONE:
        return {SubmissionStatus::ACCEPTED, std::move(future)};
    case autil::ThreadPoolBase::ERROR_POOL_HAS_STOP:
        return {SubmissionStatus::POOL_STOPPED, {}};
    case autil::ThreadPoolBase::ERROR_POOL_ITEM_IS_NULL:
        return {SubmissionStatus::ITEM_IS_NULL, {}};
    case autil::ThreadPoolBase::ERROR_POOL_QUEUE_FULL:
        return {SubmissionStatus::QUEUE_FULL, {}};
    }
    return {SubmissionStatus::POOL_STOPPED, {}};
}

} // namespace kv_cache_manager
