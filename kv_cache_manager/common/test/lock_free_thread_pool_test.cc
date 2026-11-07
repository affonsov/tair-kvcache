#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>

#include "kv_cache_manager/common/lock_free_thread_pool.h"
#include "kv_cache_manager/common/unittest.h"

using namespace kv_cache_manager;
using namespace std::chrono_literals;

class LockFreeThreadPoolTest : public TESTBASE {};

TEST_F(LockFreeThreadPoolTest, AsyncSupportsGenericResultsAndExceptions) {
    LockFreeThreadPool pool(2, 8, "generic");
    ASSERT_TRUE(pool.start());
    std::function<int()> function = []() { return 42; };
    EXPECT_EQ(42, pool.async(function).get());
    auto move_only = pool.async([value = std::make_unique<int>(7)]() mutable { return std::move(value); });
    EXPECT_EQ(7, *move_only.get());
    std::atomic<bool> invoked{false};
    pool.async([&]() { invoked = true; }).get();
    EXPECT_TRUE(invoked.load());
    auto failed = pool.async([]() -> int { throw std::runtime_error("task failure"); });
    EXPECT_THROW(failed.get(), std::runtime_error);
    EXPECT_EQ(9, pool.async([]() { return 9; }).get());
}

TEST_F(LockFreeThreadPoolTest, TwoWorkersExecuteConcurrently) {
    LockFreeThreadPool pool(2, 2, "concurrent");
    ASSERT_TRUE(pool.start());
    std::promise<void> first_started, second_started, release;
    auto first_running = first_started.get_future();
    auto second_running = second_started.get_future();
    auto gate = release.get_future().share();
    auto first = pool.async([&first_started, gate]() {
        first_started.set_value();
        gate.wait();
    });
    auto second = pool.async([&second_started, gate]() {
        second_started.set_value();
        gate.wait();
    });
    // Always release both workers before collecting results, even on failure.
    EXPECT_EQ(std::future_status::ready, first_running.wait_for(1s));
    EXPECT_EQ(std::future_status::ready, second_running.wait_for(1s));
    release.set_value();
    first.get();
    second.get();
}

TEST_F(LockFreeThreadPoolTest, TryAsyncReturnsAcceptedResult) {
    LockFreeThreadPool pool(1, 1, "TryAsyncAccepted");
    ASSERT_TRUE(pool.start());

    auto submission = pool.TryAsync([] { return 7; });
    ASSERT_TRUE(submission.accepted());
    ASSERT_TRUE(submission.future.valid());
    EXPECT_EQ(7, submission.future.get());
    pool.stop();
}

TEST_F(LockFreeThreadPoolTest, TryAsyncPreservesTaskExceptionInAcceptedFuture) {
    LockFreeThreadPool pool(1, 1, "TryAsyncException");
    ASSERT_TRUE(pool.start());

    auto submission = pool.TryAsync([]() -> int { throw std::runtime_error("task failure"); });
    ASSERT_TRUE(submission.accepted());
    EXPECT_THROW((void)submission.future.get(), std::runtime_error);
    pool.stop();
}

TEST_F(LockFreeThreadPoolTest, TryAsyncRejectsStoppedPoolWithoutValidFuture) {
    LockFreeThreadPool pool(1, 1, "TryAsyncStopped");
    ASSERT_TRUE(pool.start());
    pool.stop();

    auto submission = pool.TryAsync([] { return 7; });
    EXPECT_EQ(LockFreeThreadPool::SubmissionStatus::POOL_STOPPED, submission.status);
    EXPECT_FALSE(submission.future.valid());
}

TEST_F(LockFreeThreadPoolTest, TryAsyncRejectsFullQueueWithoutInlineExecution) {
    LockFreeThreadPool pool(1, 1, "TryAsyncFull");
    ASSERT_TRUE(pool.start());

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    auto blocker = pool.TryAsync([&] {
        blocker_started.set_value();
        release_blocker_future.wait();
        return 7;
    });
    ASSERT_TRUE(blocker.accepted());
    ASSERT_EQ(std::future_status::ready, blocker_started_future.wait_for(std::chrono::seconds(1)));

    std::atomic<int> queued_executions{0};
    auto first_queued = pool.TryAsync([&] {
        queued_executions.fetch_add(1);
        return 7;
    });
    auto over_nominal_capacity = pool.TryAsync([&] {
        queued_executions.fetch_add(1);
        return 7;
    });
    ASSERT_TRUE(first_queued.accepted());
    ASSERT_TRUE(over_nominal_capacity.accepted());

    std::atomic<int> rejected_executions{0};
    auto rejected = pool.TryAsync([&] {
        rejected_executions.fetch_add(1);
        return 7;
    });
    EXPECT_EQ(LockFreeThreadPool::SubmissionStatus::QUEUE_FULL, rejected.status);
    EXPECT_FALSE(rejected.future.valid());
    EXPECT_EQ(0, rejected_executions.load());

    release_blocker.set_value();
    EXPECT_EQ(7, blocker.future.get());
    EXPECT_EQ(7, first_queued.future.get());
    EXPECT_EQ(7, over_nominal_capacity.future.get());
    EXPECT_EQ(2, queued_executions.load());
    EXPECT_EQ(0, rejected_executions.load());
    pool.stop();
}
