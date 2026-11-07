#include <atomic>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <stdexcept>

#include "kv_cache_manager/client/src/internal/sdk/lock_free_thread_pool.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {
namespace {

TEST(LockFreeThreadPoolTest, TryAsyncReturnsAcceptedResult) {
    LockFreeThreadPool pool(1, 1, "TryAsyncAccepted");
    ASSERT_TRUE(pool.start());

    auto submission = pool.TryAsync([] { return ER_OK; });
    ASSERT_TRUE(submission.accepted());
    ASSERT_TRUE(submission.future.valid());
    EXPECT_EQ(ER_OK, submission.future.get());
    pool.stop();
}

TEST(LockFreeThreadPoolTest, TryAsyncPreservesTaskExceptionInAcceptedFuture) {
    LockFreeThreadPool pool(1, 1, "TryAsyncException");
    ASSERT_TRUE(pool.start());

    auto submission = pool.TryAsync([]() -> ClientErrorCode { throw std::runtime_error("task failure"); });
    ASSERT_TRUE(submission.accepted());
    EXPECT_THROW((void)submission.future.get(), std::runtime_error);
    pool.stop();
}

TEST(LockFreeThreadPoolTest, TryAsyncRejectsStoppedPoolWithoutValidFuture) {
    LockFreeThreadPool pool(1, 1, "TryAsyncStopped");
    ASSERT_TRUE(pool.start());
    pool.stop();

    auto submission = pool.TryAsync([] { return ER_OK; });
    EXPECT_EQ(LockFreeThreadPool::SubmissionStatus::POOL_STOPPED, submission.status);
    EXPECT_FALSE(submission.future.valid());
}

TEST(LockFreeThreadPoolTest, TryAsyncRejectsFullQueueWithoutInlineExecution) {
    LockFreeThreadPool pool(1, 1, "TryAsyncFull");
    ASSERT_TRUE(pool.start());

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    auto blocker = pool.TryAsync([&] {
        blocker_started.set_value();
        release_blocker_future.wait();
        return ER_OK;
    });
    ASSERT_TRUE(blocker.accepted());
    ASSERT_EQ(std::future_status::ready, blocker_started_future.wait_for(std::chrono::seconds(1)));

    std::atomic<int> queued_executions{0};
    auto first_queued = pool.TryAsync([&] {
        queued_executions.fetch_add(1);
        return ER_OK;
    });
    auto over_nominal_capacity = pool.TryAsync([&] {
        queued_executions.fetch_add(1);
        return ER_OK;
    });
    ASSERT_TRUE(first_queued.accepted());
    ASSERT_TRUE(over_nominal_capacity.accepted());

    std::atomic<int> rejected_executions{0};
    auto rejected = pool.TryAsync([&] {
        rejected_executions.fetch_add(1);
        return ER_OK;
    });
    EXPECT_EQ(LockFreeThreadPool::SubmissionStatus::QUEUE_FULL, rejected.status);
    EXPECT_FALSE(rejected.future.valid());
    EXPECT_EQ(0, rejected_executions.load());

    release_blocker.set_value();
    EXPECT_EQ(ER_OK, blocker.future.get());
    EXPECT_EQ(ER_OK, first_queued.future.get());
    EXPECT_EQ(ER_OK, over_nominal_capacity.future.get());
    EXPECT_EQ(2, queued_executions.load());
    EXPECT_EQ(0, rejected_executions.load());
    pool.stop();
}

} // namespace
} // namespace kv_cache_manager
