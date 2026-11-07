#include "kv_cache_manager/common/lock_free_thread_pool.h"

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

LockFreeThreadPool::SubmissionStatus LockFreeThreadPool::TryPush(std::function<void()> &&task) {
    switch (pool_->pushTask(std::move(task), false, false)) {
    case autil::ThreadPoolBase::ERROR_NONE:
        return SubmissionStatus::ACCEPTED;
    case autil::ThreadPoolBase::ERROR_POOL_HAS_STOP:
        return SubmissionStatus::POOL_STOPPED;
    case autil::ThreadPoolBase::ERROR_POOL_ITEM_IS_NULL:
        return SubmissionStatus::ITEM_IS_NULL;
    case autil::ThreadPoolBase::ERROR_POOL_QUEUE_FULL:
        return SubmissionStatus::QUEUE_FULL;
    }
    return SubmissionStatus::POOL_STOPPED;
}

} // namespace kv_cache_manager
