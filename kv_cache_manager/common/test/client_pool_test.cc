#include <atomic>
#include <memory>
#include <thread>

#include "kv_cache_manager/common/client_pool.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {

class ClientPoolTest : public TESTBASE {
public:
};

class FakeClient {
public:
    explicit FakeClient(std::shared_ptr<std::atomic_int> count) { count->fetch_add(1, std::memory_order_relaxed); }
};

class PreDeadlineApiClientPool final : public ClientPool<FakeClient> {
    using Base = ClientPool<FakeClient>;

public:
    explicit PreDeadlineApiClientPool(CreateClientCallback cb) : ClientPool<FakeClient>(std::move(cb)) {}

    bool Initialize() override { return InitializePoolStateWithSize(1); }

    ClientHandle AcquireClient(int64_t timeout_ms = 1000) override {
        if (!pool_state_) {
            return ClientHandle(nullptr, nullptr);
        }
        auto client = pool_state_->AcquireClient(timeout_ms);
        return ClientHandle(pool_state_, std::move(client));
    }
};

TEST_F(ClientPoolTest, TestInheritedDeadlineAcquirePreservesSourceCompatibility) {
    auto count = std::make_shared<std::atomic_int>(0);
    PreDeadlineApiClientPool client_pool([count]() { return std::make_shared<FakeClient>(count); });
    ASSERT_TRUE(client_pool.Initialize());

    {
        auto held = client_pool.AcquireClientUntil(std::chrono::steady_clock::now() + std::chrono::seconds(1));
        ASSERT_TRUE(held);
        EXPECT_FALSE(client_pool.AcquireClientUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(30)));
        EXPECT_EQ(1, count->load(std::memory_order_relaxed));
    }

    EXPECT_TRUE(client_pool.AcquireClientUntil(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    EXPECT_EQ(1, count->load(std::memory_order_relaxed));
}

TEST_F(ClientPoolTest, TestStaticClientPoolAcquireAndRelease) {
    auto count = std::make_shared<std::atomic_int>(0);
    StaticClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 2);
    ASSERT_TRUE(client_pool.Initialize());
    ASSERT_EQ(2, count->load(std::memory_order_relaxed));
    {
        auto handle_1 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_1);
        auto handle_2 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_2);
        auto handle_3 = client_pool.AcquireClient(1);
        ASSERT_FALSE(handle_3);
    }
    ASSERT_EQ(2, count->load(std::memory_order_relaxed));
    {
        auto handle_1 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_1);
        auto handle_2 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_2);
        auto handle_3 = client_pool.AcquireClient(1);
        ASSERT_FALSE(handle_3);
    }
}

TEST_F(ClientPoolTest, TestStaticClientPoolAcquireAndReleaseMultithread) {
    size_t thread_num = 8;
    auto count = std::make_shared<std::atomic_int>(0);
    StaticClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 2);
    ASSERT_TRUE(client_pool.Initialize());
    ASSERT_EQ(2, count->load(std::memory_order_relaxed));
    std::atomic<bool> go = false;
    auto thread_fcn = [this, &client_pool, &go]() {
        while (!go.load(std::memory_order_relaxed)) {}
        {
            auto handle = client_pool.AcquireClient();
            ASSERT_TRUE(handle);
        }
        {
            auto handle = client_pool.AcquireClient();
            ASSERT_TRUE(handle);
        }
    };
    for (int i = 0; i < 20; ++i) {
        std::vector<std::thread> threads;
        for (int j = 0; j < thread_num; ++j) {
            threads.push_back(std::thread(thread_fcn));
        }
        go.store(true, std::memory_order_relaxed);
        for (auto &thread : threads) {
            thread.join();
        }
        go.store(false, std::memory_order_relaxed);
    }
}

TEST_F(ClientPoolTest, TestStaticClientPoolAbsoluteDeadline) {
    auto count = std::make_shared<std::atomic_int>(0);
    StaticClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 1);
    ASSERT_TRUE(client_pool.Initialize());

    std::thread waiter;
    std::atomic<bool> started{false};
    std::atomic<bool> acquired{false};
    {
        auto held = client_pool.AcquireClient(1);
        ASSERT_TRUE(held);
        EXPECT_FALSE(client_pool.AcquireClientUntil(std::chrono::steady_clock::now()));
        waiter = std::thread([&] {
            started.store(true, std::memory_order_release);
            auto handle = client_pool.AcquireClientUntil(std::chrono::steady_clock::now() + std::chrono::seconds(2));
            acquired.store(static_cast<bool>(handle), std::memory_order_release);
        });
        while (!started.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    waiter.join();
    EXPECT_TRUE(acquired.load(std::memory_order_acquire));
}

TEST_F(ClientPoolTest, TestDynamicClientPoolDeadlineDoesNotCreateOrWaitForCreatorLock) {
    auto count = std::make_shared<std::atomic_int>(0);
    DynamicClientPool<FakeClient> client_pool(
        [count]() {
            count->fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            return std::make_shared<FakeClient>(count);
        },
        0,
        1);
    ASSERT_TRUE(client_pool.Initialize());

    std::atomic<bool> finished{false};
    std::atomic<bool> acquired{false};
    std::unique_lock<std::mutex> creator_lock(client_pool.acq_mux_);
    const auto start = std::chrono::steady_clock::now();
    std::thread waiter([&] {
        auto handle = client_pool.AcquireClientUntil(start + std::chrono::milliseconds(50));
        acquired.store(static_cast<bool>(handle), std::memory_order_release);
        finished.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_TRUE(finished.load(std::memory_order_acquire));
    creator_lock.unlock();
    waiter.join();

    EXPECT_FALSE(acquired.load(std::memory_order_acquire));
    EXPECT_EQ(0, count->load(std::memory_order_relaxed));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(225));
}

TEST_F(ClientPoolTest, TestDynamicClientPoolDeadlineWaitsForExistingClientUnderContention) {
    auto count = std::make_shared<std::atomic_int>(0);
    DynamicClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 1, 2);
    ASSERT_TRUE(client_pool.Initialize());
    std::atomic<bool> acquired{false};
    std::thread waiter;
    {
        auto held = client_pool.AcquireClient(1);
        ASSERT_TRUE(held);
        waiter = std::thread([&] {
            auto handle = client_pool.AcquireClientUntil(std::chrono::steady_clock::now() + std::chrono::seconds(1));
            acquired.store(static_cast<bool>(handle), std::memory_order_release);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    waiter.join();

    EXPECT_TRUE(acquired.load(std::memory_order_acquire));
    EXPECT_EQ(1, count->load(std::memory_order_relaxed));
}

TEST_F(ClientPoolTest, TestDynamicClientPoolAcquireAndRelease) {
    auto count = std::make_shared<std::atomic_int>(0);
    DynamicClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 1, 2);
    ASSERT_TRUE(client_pool.Initialize());
    ASSERT_EQ(1, count->load(std::memory_order_relaxed));
    {
        auto handle_1 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_1);
        ASSERT_EQ(1, count->load(std::memory_order_relaxed));
        auto handle_2 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_2);
        ASSERT_EQ(2, count->load(std::memory_order_relaxed));
        auto handle_3 = client_pool.AcquireClient(1);
        ASSERT_FALSE(handle_3);
        ASSERT_EQ(2, count->load(std::memory_order_relaxed));
    }
    {
        auto handle_1 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_1);
        ASSERT_EQ(2, count->load(std::memory_order_relaxed));
        auto handle_2 = client_pool.AcquireClient(1);
        ASSERT_TRUE(handle_2);
        ASSERT_EQ(2, count->load(std::memory_order_relaxed));
        auto handle_3 = client_pool.AcquireClient(1);
        ASSERT_FALSE(handle_3);
        ASSERT_EQ(2, count->load(std::memory_order_relaxed));
    }
}

TEST_F(ClientPoolTest, TestDynamicClientPoolAcquireAndReleaseMultithread) {
    size_t thread_num = 8;
    auto count = std::make_shared<std::atomic_int>(0);
    DynamicClientPool<FakeClient> client_pool([count]() { return std::make_shared<FakeClient>(count); }, 0, 16);
    ASSERT_TRUE(client_pool.Initialize());
    ASSERT_EQ(0, count->load(std::memory_order_relaxed));
    std::atomic<bool> go = false;
    auto thread_fcn = [this, &client_pool, &go]() {
        while (!go.load(std::memory_order_relaxed)) {}
        {
            auto handle = client_pool.AcquireClient(5);
            ASSERT_TRUE(handle);
        }
        {
            auto handle = client_pool.AcquireClient(5);
            ASSERT_TRUE(handle);
        }
    };
    for (int i = 0; i < 20; ++i) {
        std::vector<std::thread> threads;
        for (int j = 0; j < thread_num; ++j) {
            threads.push_back(std::thread(thread_fcn));
        }
        go.store(true, std::memory_order_relaxed);
        for (auto &thread : threads) {
            thread.join();
        }
        go.store(false, std::memory_order_relaxed);
        ASSERT_LT(count->load(std::memory_order_relaxed), 9);
    }
}

} // namespace kv_cache_manager