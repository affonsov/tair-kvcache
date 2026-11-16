#include <atomic>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <typeinfo>
#include <unistd.h>
#include <vector>

#include "kv_cache_manager/client/src/internal/config/sdk_config.h"
#include "kv_cache_manager/client/src/internal/sdk/deadline_util.h"
#include "kv_cache_manager/client/src/internal/sdk/sdk_factory.h"
#include "kv_cache_manager/client/src/internal/sdk/sdk_interface.h"
#include "kv_cache_manager/client/src/internal/sdk/sdk_wrapper.h"
#include "kv_cache_manager/common/lock_free_thread_pool.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/data_storage_uri.h"

using namespace kv_cache_manager;

class SdkWrapperTest : public TESTBASE {
public:
    void SetUp() override {
        client_config_ = CreateTestClientConfig();
        init_params_.role_type = RoleType::WORKER;
        init_params_.regist_span = new RegistSpan();
        auto buffer = malloc(1024 * 1024);
        init_params_.regist_span->base = buffer;
        init_params_.regist_span->size = 1024 * 1024;
        init_params_.self_location_spec_name = "tp0";
        init_params_.storage_configs = CreateTestStorageConfigs();
        root_path_ = GetPrivateTestRuntimeDataPath();
    }

    void TearDown() override {
        free(init_params_.regist_span->base);
        delete init_params_.regist_span;
    }

private:
    std::unique_ptr<ClientConfig> CreateTestClientConfig() {
        auto client_config = std::make_unique<ClientConfig>();
        std::string client_config_str = R"({
            "instance_group": "group",
            "instance_id": "instance",
            "address": [
                "127.0.0.1:8080"
            ],
            "block_size": 128,
            "sdk_config": {
                "thread_num": 8,
                "queue_size": 2000,
                "sdk_backend_configs": [
                    {
                        "type": "file"
                    }
                ],
                "timeout_config": {
                    "put_timeout_ms": 2000,
                    "get_timeout_ms": 2000
                }
            },
            "model_deployment": {
                "model_name": "test_model",
                "dtype": "FP8",
                "use_mla": false,
                "tp_size": 1,
                "dp_size": 1,
                "pp_size": 1
            },
            "location_spec_infos": {
                "tp0": 1024
            }
        })";
        client_config->FromJsonString(client_config_str);
        return client_config;
    }

    std::string CreateTestStorageConfigs() {
        return "["
               // #ifdef ENABLE_HF3FS
               //                R"({
               //             "type": "hf3fs",
               //             "global_unique_name": "3fs_test",
               //             "storage_spec": {
               //                 "cluster_name": "3fs_cluster",
               //                 "mountpoint": "/3fs/stage/3fs",
               //                 "root_dir": "3fs_test/",
               //                 "key_count_per_file": 2
               //             }
               //         },)"
               // #endif
               R"({
            "type": "file",
            "global_unique_name": "nfs_test",
            "storage_spec": {
                "root_path": "/nfs/",
                "key_count_per_file": 2
            }
        }
    ])";
    }

private:
    std::unique_ptr<ClientConfig> client_config_;
    InitParams init_params_;
    std::string root_path_;
};

TEST_F(SdkWrapperTest, TestInit) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));
}

TEST_F(SdkWrapperTest, TestInitWithEmptyWrapperConfig) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_INVALID_CLIENT_CONFIG, sdk_wrapper.Init(nullptr, init_params_));
}

TEST_F(SdkWrapperTest, TestInitWithEmptyStorageConfigs) {
    SdkWrapper sdk_wrapper;
    InitParams init_params = init_params_;
    init_params.storage_configs = "[]";
    ASSERT_EQ(ER_INVALID_STORAGE_CONFIG, sdk_wrapper.Init(client_config_, init_params));
}

TEST_F(SdkWrapperTest, TestInitWithInvalidStorageConfigs) {
    SdkWrapper sdk_wrapper;
    InitParams init_params = init_params_;
    init_params.storage_configs = "[invalid json]";
    ASSERT_EQ(ER_INVALID_STORAGE_CONFIG, sdk_wrapper.Init(client_config_, init_params));
}

TEST_F(SdkWrapperTest, TestPrepareSharedMemoryRegistrationOwnsFd) {
    FILE *file = tmpfile();
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(ftruncate(fileno(file), static_cast<off_t>(init_params_.regist_span->size)), 0);

    SharedMemoryRegistration registration;
    registration.base = init_params_.regist_span->base;
    registration.size = init_params_.regist_span->size;
    registration.fd = fileno(file);

    SdkWrapper sdk_wrapper;
    SharedMemoryRegistration prepared_registration;
    ASSERT_EQ(ER_OK, sdk_wrapper.PrepareSharedMemoryRegistration(registration, prepared_registration));
    EXPECT_NE(prepared_registration.fd, registration.fd);
    const int fd_flags = fcntl(prepared_registration.fd, F_GETFD);
    ASSERT_GE(fd_flags, 0);
    EXPECT_NE(fd_flags & FD_CLOEXEC, 0);

    ASSERT_EQ(fclose(file), 0);
    struct stat file_stat {};
    EXPECT_EQ(fstat(prepared_registration.fd, &file_stat), 0);
}

TEST_F(SdkWrapperTest, TestDestructorKeepsSharedMemoryFdAliveForRunningTasks) {
    FILE *file = tmpfile();
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(ftruncate(fileno(file), static_cast<off_t>(init_params_.regist_span->size)), 0);

    SharedMemoryRegistration registration;
    registration.base = init_params_.regist_span->base;
    registration.size = init_params_.regist_span->size;
    registration.fd = fileno(file);

    auto sdk_wrapper = std::make_unique<SdkWrapper>();
    SharedMemoryRegistration prepared_registration;
    ASSERT_EQ(ER_OK, sdk_wrapper->PrepareSharedMemoryRegistration(registration, prepared_registration));
    const int owned_fd = prepared_registration.fd;

    sdk_wrapper->wait_task_thread_pool_ = std::make_unique<LockFreeThreadPool>(1, 1, "SdkWrapperTestPool");
    ASSERT_TRUE(sdk_wrapper->wait_task_thread_pool_->start());

    std::promise<void> task_started;
    auto task_started_future = task_started.get_future();
    std::promise<void> allow_task_finish;
    auto allow_task_finish_future = allow_task_finish.get_future().share();
    std::atomic<bool> fd_valid_in_task{false};
    auto task_result = sdk_wrapper->wait_task_thread_pool_->async([&]() {
        task_started.set_value();
        allow_task_finish_future.wait();
        struct stat file_stat {};
        fd_valid_in_task.store(fstat(owned_fd, &file_stat) == 0);
        return ER_OK;
    });
    const auto task_started_status = task_started_future.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(task_started_status, std::future_status::ready);

    std::promise<void> destructor_started;
    auto destructor_started_future = destructor_started.get_future();
    std::thread destroyer([&]() {
        destructor_started.set_value();
        sdk_wrapper.reset();
    });
    EXPECT_EQ(destructor_started_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    allow_task_finish.set_value();
    destroyer.join();

    EXPECT_EQ(task_result.get(), ER_OK);
    EXPECT_TRUE(fd_valid_in_task.load());
    EXPECT_EQ(fcntl(owned_fd, F_GETFD), -1);
    ASSERT_EQ(fclose(file), 0);
}

TEST_F(SdkWrapperTest, TestPrepareSharedMemoryRegistrationRejectsInvalidValues) {
    SdkWrapper sdk_wrapper;
    SharedMemoryRegistration prepared_registration;
    SharedMemoryRegistration registration;

    registration.fd = 0;
    EXPECT_EQ(ER_INVALID_PARAMS, sdk_wrapper.PrepareSharedMemoryRegistration(registration, prepared_registration));

    registration = SharedMemoryRegistration();
    registration.base = init_params_.regist_span->base;
    EXPECT_EQ(ER_INVALID_PARAMS, sdk_wrapper.PrepareSharedMemoryRegistration(registration, prepared_registration));

    FILE *file = tmpfile();
    ASSERT_NE(file, nullptr);
    registration = SharedMemoryRegistration();
    registration.base = init_params_.regist_span->base;
    registration.size = init_params_.regist_span->size;
    registration.fd = fileno(file);
    EXPECT_EQ(ER_INVALID_PARAMS, sdk_wrapper.PrepareSharedMemoryRegistration(registration, prepared_registration));
    ASSERT_EQ(fclose(file), 0);
}

TEST_F(SdkWrapperTest, TestUpdateTairMempoolSdkConfigWithSharedMemory) {
    FILE *file = tmpfile();
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(ftruncate(fileno(file), static_cast<off_t>(init_params_.regist_span->size)), 0);

    SharedMemoryRegistration registration;
    registration.base = init_params_.regist_span->base;
    registration.size = init_params_.regist_span->size;
    registration.fd = fileno(file);

    SdkWrapper sdk_wrapper;
    SharedMemoryRegistration prepared_registration;
    ASSERT_EQ(ER_OK, sdk_wrapper.PrepareSharedMemoryRegistration(registration, prepared_registration));
    ClientMemoryRegistrations memory_registrations;
    memory_registrations.host = prepared_registration;
    memory_registrations.gpu.push_back({reinterpret_cast<void *>(0x1000), 4096, 0});

    for (const auto type :
         {DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL, DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD}) {
        SCOPED_TRACE(static_cast<int>(type));
        auto config = std::make_shared<TairMempoolSdkConfig>(type);
        ASSERT_EQ(ER_OK, sdk_wrapper.UpdateTairMempoolSdkConfig(config, &memory_registrations));
        EXPECT_EQ(config->shm_fd(), prepared_registration.fd);
        EXPECT_EQ(config->shm_size(), registration.size);
        EXPECT_EQ(config->client_base(), registration.base);
        ASSERT_EQ(config->gpu_memory_spans().size(), 1u);
        EXPECT_EQ(config->gpu_memory_spans()[0].base, reinterpret_cast<void *>(0x1000));
        EXPECT_EQ(config->gpu_memory_spans()[0].size, 4096u);
        EXPECT_EQ(config->gpu_memory_spans()[0].device_id, 0);
    }

    ASSERT_EQ(fclose(file), 0);
}

// TODO: mock mooncake
//  TEST_F(SdkWrapperTest, TestInitWithMooncake) {
//  #ifdef ENABLE_MOONCAKE
//      auto wrapper_config = CreateTestWrapperConfig();
//      auto mooncake_config = std::make_shared<MooncakeSdkConfig>();
//      mooncake_config->set_type(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE);
//      mooncake_config->set_location("*");
//      mooncake_config->set_put_replica_num(2);
//      wrapper_config->sdk_config_map_[DataStorageType::DATA_STORAGE_TYPE_MOONCAKE] = mooncake_config;
//      InitParams init_params;
//      {
//          SdkWrapper sdk_wrapper;
//          ASSERT_FALSE(sdk_wrapper.Init(wrapper_config, init_params));
//      }
//      {
//          SdkWrapper sdk_wrapper;
//          ASSERT_TRUE(sdk_wrapper.Init(wrapper_config, init_params_));
//      }
//      ASSERT_TRUE(false);
//  #else
//      GTEST_SKIP() << "mooncake not enabled, skipping init sdk wrapper with mooncake config";
//  #endif
//  }

TEST_F(SdkWrapperTest, TestPutAndGet) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));
    std::vector<DataStorageUri> remote_uris;
    remote_uris.push_back(DataStorageUri("file://nfs_test/" + root_path_ + "/nfs/0/0/1?blkid=0&size=1024"));
    BlockBuffers local_buffers;
    BlockBuffer buffer;
    local_buffers.push_back(buffer);
    auto actual_remote_uris = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_OK, sdk_wrapper.Put(remote_uris, local_buffers, actual_remote_uris));
    ASSERT_EQ(actual_remote_uris->size(), 1);
    ASSERT_EQ(actual_remote_uris->at(0).ToUriString(), remote_uris[0].ToUriString());

    ASSERT_EQ(ER_OK, sdk_wrapper.Get(*actual_remote_uris, local_buffers));
}

TEST_F(SdkWrapperTest, TestValid) {
    SdkWrapper sdk_wrapper;
    std::vector<DataStorageUri> remote_uris;
    remote_uris.push_back(DataStorageUri("file://nfs_test/nfs/0/0/1?blkid=0"));
    BlockBuffers local_buffers;
    ASSERT_EQ(ER_INVALID_PARAMS, sdk_wrapper.Valid(remote_uris, local_buffers));
    BlockBuffer buffer;
    local_buffers.push_back(buffer);
    ASSERT_EQ(ER_OK, sdk_wrapper.Valid(remote_uris, local_buffers));
}

TEST_F(SdkWrapperTest, TestGetSdk) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));
    DataStorageUri remote_uri("file://nfs_test/nfs/0/0/1?blkid=0");
    ASSERT_TRUE(sdk_wrapper.GetSdk(remote_uri));
    remote_uri = DataStorageUri("file://invalid/nfs/1/0/1?blkid=0");
    ASSERT_FALSE(sdk_wrapper.GetSdk(remote_uri));
#if ENABLE_HF3FS && USING_CUDA
    remote_uri = DataStorageUri("3fs://3fs_test/3fs_test/0/1?blkid=0");
    // ASSERT_TRUE(sdk_wrapper.GetSdk(remote_uri));
#endif
    remote_uri = DataStorageUri("invalid:///mnt/nfs/0/0/1?blkid=0");
    ASSERT_FALSE(sdk_wrapper.GetSdk(remote_uri));
}

TEST_F(SdkWrapperTest, TestUpdateMooncakeSdkConfig) {
    SdkWrapper sdk_wrapper;
    RegistSpan span;
    auto sdk_backend_config =
        client_config_->sdk_wrapper_config()->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_NFS);
    ASSERT_TRUE(sdk_backend_config);
    ASSERT_EQ(ER_OK, sdk_wrapper.UpdateMooncakeSdkConfig(sdk_backend_config, nullptr, ""));
    ASSERT_EQ(ER_OK, sdk_wrapper.UpdateMooncakeSdkConfig(sdk_backend_config, &span, ""));
#ifdef ENABLE_MOONCAKE
    auto mooncake_config = std::make_shared<MooncakeSdkConfig>();
    mooncake_config->set_type(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE);
    mooncake_config->set_location("*");
    mooncake_config->set_put_replica_num(2);
    ASSERT_EQ(ER_INVALID_PARAMS, sdk_wrapper.UpdateMooncakeSdkConfig(mooncake_config, nullptr, ""));
    ASSERT_EQ(ER_OK, sdk_wrapper.UpdateMooncakeSdkConfig(mooncake_config, &span, ""));
#endif
}

// ============================================================================
// Multi-storage test fixture
// ============================================================================
class SdkWrapperMultiStorageTest : public TESTBASE {
public:
    void SetUp() override {
        root_path_ = GetPrivateTestRuntimeDataPath();
        client_config_ = CreateTestClientConfig();
        init_params_.role_type = RoleType::WORKER;
        init_params_.regist_span = new RegistSpan();
        auto buffer = malloc(1024 * 1024);
        init_params_.regist_span->base = buffer;
        init_params_.regist_span->size = 1024 * 1024;
        init_params_.self_location_spec_name = "tp0";
        init_params_.storage_configs = CreateMultiStorageConfigs();
    }

    void TearDown() override {
        free(init_params_.regist_span->base);
        delete init_params_.regist_span;
    }

protected:
    std::unique_ptr<ClientConfig> client_config_;
    InitParams init_params_;
    std::string root_path_;

private:
    std::unique_ptr<ClientConfig> CreateTestClientConfig() {
        auto client_config = std::make_unique<ClientConfig>();
        std::string client_config_str = R"({
            "instance_group": "group",
            "instance_id": "instance",
            "address": ["127.0.0.1:8080"],
            "block_size": 128,
            "sdk_config": {
                "thread_num": 8,
                "queue_size": 2000,
                "sdk_backend_configs": [{"type": "file"}],
                "timeout_config": {
                    "put_timeout_ms": 2000,
                    "get_timeout_ms": 2000
                }
            },
            "model_deployment": {
                "model_name": "test_model",
                "dtype": "FP8",
                "use_mla": false,
                "tp_size": 1,
                "dp_size": 1,
                "pp_size": 1
            },
            "location_spec_infos": {"tp0": 1024}
        })";
        client_config->FromJsonString(client_config_str);
        return client_config;
    }

    std::string CreateMultiStorageConfigs() {
        return "["
               R"({"type":"file","global_unique_name":"nfs_a","storage_spec":{"root_path":")" +
               root_path_ +
               R"(/nfs_a/","key_count_per_file":2}},)"
               R"({"type":"file","global_unique_name":"nfs_b","storage_spec":{"root_path":")" +
               root_path_ +
               R"(/nfs_b/","key_count_per_file":2}})"
               "]";
    }
};

TEST_F(SdkWrapperMultiStorageTest, TestMixedStoragePutAndGet) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));

    // 同 backend 使用同 path（不同 blkid），避免 SDK 内部 SplitByPath 导致重排
    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=0&size=1024"),
        DataStorageUri("file://nfs_b/" + root_path_ + "/nfs_b/0/0/1?blkid=1&size=1024"),
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=2&size=1024"),
    };
    BlockBuffers local_buffers = {BlockBuffer(), BlockBuffer(), BlockBuffer()};

    auto actual_remote_uris = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_OK, sdk_wrapper.Put(remote_uris, local_buffers, actual_remote_uris));

    ASSERT_EQ(actual_remote_uris->size(), 3);
    ASSERT_EQ(actual_remote_uris->at(0).ToUriString(), remote_uris[0].ToUriString());
    ASSERT_EQ(actual_remote_uris->at(1).ToUriString(), remote_uris[1].ToUriString());
    ASSERT_EQ(actual_remote_uris->at(2).ToUriString(), remote_uris[2].ToUriString());

    ASSERT_EQ(ER_OK, sdk_wrapper.Get(*actual_remote_uris, local_buffers));
}

TEST_F(SdkWrapperMultiStorageTest, TestSingleStorageBackwardCompat) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));

    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=0&size=1024"),
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=1&size=1024"),
    };
    BlockBuffers local_buffers = {BlockBuffer(), BlockBuffer()};

    auto actual_remote_uris = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_OK, sdk_wrapper.Put(remote_uris, local_buffers, actual_remote_uris));
    ASSERT_EQ(actual_remote_uris->size(), 2);
    ASSERT_EQ(actual_remote_uris->at(0).ToUriString(), remote_uris[0].ToUriString());
    ASSERT_EQ(actual_remote_uris->at(1).ToUriString(), remote_uris[1].ToUriString());

    ASSERT_EQ(ER_OK, sdk_wrapper.Get(*actual_remote_uris, local_buffers));
}

TEST_F(SdkWrapperMultiStorageTest, TestMixedStorageWithInvalidSdk) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));

    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=0&size=1024"),
        DataStorageUri("file://unknown_backend/" + root_path_ + "/unknown/0/0/1?blkid=1&size=1024"),
    };
    BlockBuffers local_buffers = {BlockBuffer(), BlockBuffer()};

    auto actual_remote_uris = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_GETSDK_ERROR, sdk_wrapper.Put(remote_uris, local_buffers, actual_remote_uris));
    ASSERT_EQ(ER_GETSDK_ERROR, sdk_wrapper.Get(remote_uris, local_buffers));
}

TEST_F(SdkWrapperMultiStorageTest, TestGroupBySdk) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));

    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/1?blkid=0&size=1024"),
        DataStorageUri("file://nfs_b/" + root_path_ + "/nfs_b/0/0/2?blkid=1&size=1024"),
        DataStorageUri("file://nfs_a/" + root_path_ + "/nfs_a/0/0/3?blkid=2&size=1024"),
    };
    BlockBuffers local_buffers = {BlockBuffer(), BlockBuffer(), BlockBuffer()};

    std::vector<SdkWrapper::SdkGroup> groups;
    ASSERT_EQ(ER_OK, sdk_wrapper.GroupBySdk(remote_uris, local_buffers, groups));
    ASSERT_EQ(groups.size(), 2);

    // 第一组 nfs_a: indices 0, 2
    ASSERT_EQ(groups[0].indices.size(), 2);
    ASSERT_EQ(groups[0].indices[0], 0);
    ASSERT_EQ(groups[0].indices[1], 2);
    ASSERT_EQ(groups[0].uris.size(), 2);
    ASSERT_EQ(groups[0].buffers.size(), 2);

    // 第二组 nfs_b: index 1
    ASSERT_EQ(groups[1].indices.size(), 1);
    ASSERT_EQ(groups[1].indices[0], 1);
    ASSERT_EQ(groups[1].uris.size(), 1);
    ASSERT_EQ(groups[1].buffers.size(), 1);
}

// ============================================================================
// ============================================================================

// 可控 fake SDK：记录 Get/Put 调用，可注入延迟；Init 观测 wrapper 注入的静态预算。
struct FakeSdkControl {
    std::atomic<int> get_call_count{0};
    std::atomic<int> put_call_count{0};
    std::atomic<int> get_delay_ms{0}; // Get 内的睡眠时长（模拟慢 I/O）
    std::atomic<int> get_result{static_cast<int>(ER_OK)};
    // Init 时观测到的注入预算（TestStaticBudgetInjection 用）：wrapper 应把自身
    // timeout_config 注入 SdkBackendConfig，后端据此自律。
    std::atomic<int> observed_get_budget_ms{0};
    std::atomic<int> observed_put_budget_ms{0};
    std::atomic<int> contextual_get_calls{0};
    std::atomic<int64_t> first_deadline_ticks{0};
    std::atomic<int64_t> second_deadline_ticks{0};
    std::atomic<int64_t> remaining_ms_at_backend_entry{0};
    std::atomic<bool> throw_from_get{false};
    std::atomic<bool> throw_from_put{false};
    std::atomic<bool> use_context_output_commit{false};
    std::atomic<bool> output_access_attempted{false};
    std::atomic<bool> output_access_succeeded{false};
    std::function<void()> before_output_access;
    std::function<void()> after_output_access_admitted;
    std::function<void()> after_output_commit;
};

class FakeSdk : public SdkInterface, public ContextualSdkInterface {
public:
    explicit FakeSdk(std::shared_ptr<FakeSdkControl> ctrl) : ctrl_(std::move(ctrl)) {}

    ClientErrorCode Init(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                         const std::shared_ptr<StorageConfig> &) override {
        if (sdk_backend_config) {
            ctrl_->observed_get_budget_ms.store(sdk_backend_config->timeout_config().get_timeout_ms());
            ctrl_->observed_put_budget_ms.store(sdk_backend_config->timeout_config().put_timeout_ms());
        }
        return ER_OK;
    }
    SdkType Type() override { return SdkType::LOCAL_FILE; }
    ClientErrorCode Get(const std::vector<DataStorageUri> &, const BlockBuffers &) override {
        ctrl_->get_call_count.fetch_add(1);
        int delay_ms = ctrl_->get_delay_ms.load();
        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }
        return static_cast<ClientErrorCode>(ctrl_->get_result.load());
    }
    ClientErrorCode GetWithContext(const std::vector<DataStorageUri> &remote_uris,
                                   const BlockBuffers &local_buffers,
                                   const SdkCallContext &context) override {
        const int call = ctrl_->contextual_get_calls.fetch_add(1);
        const int64_t deadline_ticks =
            std::chrono::duration_cast<std::chrono::nanoseconds>(context.deadline().time_since_epoch()).count();
        if (call == 0) {
            ctrl_->first_deadline_ticks.store(deadline_ticks);
        } else if (call == 1) {
            ctrl_->second_deadline_ticks.store(deadline_ticks);
        }
        ctrl_->remaining_ms_at_backend_entry.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(context.deadline() - std::chrono::steady_clock::now())
                .count());
        if (ctrl_->throw_from_get.load()) {
            throw std::runtime_error("fake SDK task failure");
        }
        if (ctrl_->use_context_output_commit.load()) {
            if (ctrl_->before_output_access) {
                ctrl_->before_output_access();
            }
            ClientErrorCode result = ER_OK;
            {
                auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
                ctrl_->output_access_succeeded.store(access.has_value());
                ctrl_->output_access_attempted.store(true);
                if (!access) {
                    return ER_SDK_TIMEOUT;
                }
                if (ctrl_->after_output_access_admitted) {
                    ctrl_->after_output_access_admitted();
                }
                result = Get(remote_uris, local_buffers);
                access->Complete(result);
            }
            if (ctrl_->after_output_commit) {
                ctrl_->after_output_commit();
            }
            return result;
        }
        return Get(remote_uris, local_buffers);
    }
    ClientErrorCode Put(const std::vector<DataStorageUri> &remote_uris,
                        const BlockBuffers &,
                        std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) override {
        ctrl_->put_call_count.fetch_add(1);
        *actual_remote_uris = remote_uris;
        return ER_OK;
    }
    ClientErrorCode PutWithContext(const std::vector<DataStorageUri> &remote_uris,
                                   const BlockBuffers &local_buffers,
                                   std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris,
                                   const SdkCallContext &context) override {
        if (ctrl_->throw_from_put.load()) {
            throw std::runtime_error("fake SDK Put task failure");
        }
        auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
        if (!access) {
            return ER_SDK_TIMEOUT;
        }
        const auto result = Put(remote_uris, local_buffers, std::move(actual_remote_uris));
        access->Complete(result);
        return result;
    }

protected:
    ClientErrorCode Alloc(const std::vector<DataStorageUri> &, std::vector<DataStorageUri> &) override { return ER_OK; }

private:
    std::shared_ptr<FakeSdkControl> ctrl_;
};

// Test-only factory returning FakeSdk for NFS; production code has no test hooks.
class FakeSdkFactory : public SdkFactory {
public:
    explicit FakeSdkFactory(std::shared_ptr<FakeSdkControl> ctrl) : ctrl_(std::move(ctrl)) {}

    void FailOnCreateCall(int call) { fail_on_create_call_ = call; }
    void AllowAllCreates() { fail_on_create_call_ = -1; }
    void SetSdkForStorage(const std::string &storage_name, std::shared_ptr<SdkInterface> sdk) {
        sdk_overrides_[storage_name] = std::move(sdk);
    }

    std::shared_ptr<SdkInterface> CreateSdk(const DataStorageType &type,
                                            const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                            const std::shared_ptr<StorageConfig> &storage_config) override {
        const int call = create_calls_++;
        if (call == fail_on_create_call_) {
            return nullptr;
        }
        const auto override_it = sdk_overrides_.find(storage_config->global_unique_name());
        if (override_it != sdk_overrides_.end()) {
            return override_it->second->Init(sdk_backend_config, storage_config) == ER_OK ? override_it->second
                                                                                          : nullptr;
        }
        if (type == DataStorageType::DATA_STORAGE_TYPE_NFS || type == DataStorageType::DATA_STORAGE_TYPE_VALKEY) {
            auto sdk = std::make_shared<FakeSdk>(ctrl_);
            return sdk->Init(sdk_backend_config, storage_config) == ER_OK ? sdk : nullptr;
        }
        return SdkFactory::CreateSdk(type, sdk_backend_config, storage_config);
    }

private:
    std::shared_ptr<FakeSdkControl> ctrl_;
    int create_calls_{0};
    int fail_on_create_call_{-1};
    std::map<std::string, std::shared_ptr<SdkInterface>> sdk_overrides_;
};

// Init wrapper with a fake factory injected (-fno-access-control).
// Caller keeps the returned factory alive for the wrapper's lifetime.
std::unique_ptr<FakeSdkFactory> InitWrapperWithFake(SdkWrapper &wrapper,
                                                    const std::shared_ptr<FakeSdkControl> &ctrl,
                                                    const std::unique_ptr<ClientConfig> &client_config,
                                                    InitParams &init_params,
                                                    ClientErrorCode &ec) {
    auto factory = std::make_unique<FakeSdkFactory>(ctrl);
    wrapper.sdk_factory_ = factory.get();
    ec = wrapper.Init(client_config, init_params);
    return factory;
}

// 覆盖 timeout_config 的 ClientConfig（其余字段与 fixture 一致）。
std::unique_ptr<ClientConfig>
MakeClientConfigWithTimeouts(int put_timeout_ms, int get_timeout_ms, int queue_size = 2000, int thread_num = 8) {
    auto client_config = std::make_unique<ClientConfig>();
    std::string client_config_str = R"({
        "instance_group": "group",
        "instance_id": "instance",
        "address": ["127.0.0.1:8080"],
        "block_size": 128,
        "sdk_config": {
            "thread_num": )" + std::to_string(thread_num) +
                                    R"(,
            "queue_size": )" + std::to_string(queue_size) +
                                    R"(,
            "sdk_backend_configs": [{"type": "file"}],
            "timeout_config": {
                "put_timeout_ms": )" +
                                    std::to_string(put_timeout_ms) + R"(,
                "get_timeout_ms": )" +
                                    std::to_string(get_timeout_ms) + R"(
            }
        },
        "model_deployment": {
            "model_name": "test_model", "dtype": "FP8", "use_mla": false,
            "tp_size": 1, "dp_size": 1, "pp_size": 1
        },
        "location_spec_infos": {"tp0": 1024}
    })";
    client_config->FromJsonString(client_config_str);
    return client_config;
}

// Wait for a condition to hold (deadline/admission events happen asynchronously).
bool WaitForTrue(const std::function<bool()> &cond, int max_retry = 300) {
    for (int i = 0; i < max_retry; ++i) {
        if (cond()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

// 验证准入修复：排队超过静态预算的任务不得再发起 I/O。
// 方法：get_timeout_ms 调小到 50ms + 占满线程池（8 线程各睡 500ms）→
// 分组任务必然在队列里等过 wrapper deadline → 启动时被准入检查拦下。
TEST_F(SdkWrapperTest, TestAdmissionRejectOnExpiredDeadline) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    auto client_config = MakeClientConfigWithTimeouts(/*put_timeout_ms=*/2000, /*get_timeout_ms=*/50);
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto fake_factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    // 占满线程池的全部 8 个线程，每个睡眠 500ms。
    std::vector<std::future<ClientErrorCode>> blockers;
    for (size_t i = 0; i < 8; ++i) {
        blockers.push_back(sdk_wrapper.wait_task_thread_pool_->async([]() -> ClientErrorCode {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return ER_OK;
        }));
    }

    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_test/" + root_path_ + "/nfs/0/0/1?blkid=0&size=1024")};
    BlockBuffers local_buffers = {BlockBuffer()};

    auto start = std::chrono::steady_clock::now();
    ClientErrorCode ec = sdk_wrapper.Get(remote_uris, local_buffers);
    int64_t elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    // 核心断言：fake SDK 的 Get 从未被调用（I/O 未发起），返回超时，且不等待在飞任务。
    ASSERT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, ec); // the expired task never started I/O
    ASSERT_EQ(0, ctrl->get_call_count.load());
    ASSERT_LT(elapsed_ms, 400); // 占位任务要 500ms 才结束；若等待它们则此处必超

    // 回收占位任务，避免线程池析构时等待未完成任务。
    for (auto &f : blockers) {
        f.get();
    }
}

// 验证超时有界返回：fake SDK 睡 1500ms、get_timeout_ms=200 → wrapper 必须在远小于
// fake 睡眠时长内返回（证明没有 drain / 等待 in-flight I/O 的阻塞）。
TEST_F(SdkWrapperTest, TestNoUnboundedWaitOnTimeout) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    ctrl->get_delay_ms.store(1500);
    auto client_config = MakeClientConfigWithTimeouts(/*put_timeout_ms=*/2000, /*get_timeout_ms=*/200);
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto fake_factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_test/" + root_path_ + "/nfs/0/0/1?blkid=0&size=1024")};
    BlockBuffers local_buffers = {BlockBuffer()};

    auto start = std::chrono::steady_clock::now();
    ClientErrorCode ec = sdk_wrapper.Get(remote_uris, local_buffers);
    int64_t elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, ec); // the contextual fake holds no caller-memory guard
    ASSERT_LT(elapsed_ms, 1000);               // 若有人改回"等 in-flight 完成"，此断言会立刻变红

    // 等 fake 的睡眠结束，避免线程池/进程退出时任务仍在跑。
    std::this_thread::sleep_for(std::chrono::milliseconds(1600));
}

// 验证静态预算注入：wrapper 在 Init 阶段把自身 timeout_config 注入
// SdkBackendConfig，后端（fake 观测面）读到的是同一份预算 —— 这是后端
// "从自身任务起点起算 deadline 并自律"的数据来源。
TEST_F(SdkWrapperTest, TestStaticBudgetInjection) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto fake_factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config_, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    // fixture 的 client_config_：put_timeout_ms=2000, get_timeout_ms=2000。
    ASSERT_EQ(2000, ctrl->observed_get_budget_ms.load());
    ASSERT_EQ(2000, ctrl->observed_put_budget_ms.load());
}

// 用于直接调用受保护方法 SplitByPath 的最小实现。
class TestSplitSdk : public SdkInterface {
public:
    ClientErrorCode Init(const std::shared_ptr<SdkBackendConfig> &, const std::shared_ptr<StorageConfig> &) override {
        return ER_OK;
    }
    SdkType Type() override { return SdkType::LOCAL_FILE; }
    ClientErrorCode Get(const std::vector<DataStorageUri> &, const BlockBuffers &) override { return ER_OK; }
    ClientErrorCode Put(const std::vector<DataStorageUri> &,
                        const BlockBuffers &,
                        std::shared_ptr<std::vector<DataStorageUri>>) override {
        return ER_OK;
    }

protected:
    ClientErrorCode Alloc(const std::vector<DataStorageUri> &, std::vector<DataStorageUri> &) override { return ER_OK; }
};

// 验证保序：交错多 path 输入时，每组 indices 记录原始下标。
TEST_F(SdkWrapperTest, TestSplitByPathRecordsIndices) {
    TestSplitSdk sdk;
    std::vector<DataStorageUri> remote_uris = {
        DataStorageUri("file://nfs_test/shared?blkid=0&size=1024"),
        DataStorageUri("file://nfs_test/other?blkid=0&size=1024"),
        DataStorageUri("file://nfs_test/shared?blkid=1&size=1024"),
        DataStorageUri("file://nfs_test/other?blkid=1&size=1024"),
    };
    BlockBuffers local_buffers = {BlockBuffer(), BlockBuffer(), BlockBuffer(), BlockBuffer()};

    auto groups = sdk.SplitByPath(remote_uris, local_buffers);
    ASSERT_EQ(groups.size(), 2);

    const auto &shared = groups.at("/shared");
    ASSERT_EQ(shared.indices.size(), 2);
    ASSERT_EQ(shared.indices[0], 0);
    ASSERT_EQ(shared.indices[1], 2);
    ASSERT_EQ(shared.remote_uris.size(), 2);
    ASSERT_EQ(shared.remote_uris[0].ToUriString(), remote_uris[0].ToUriString());
    ASSERT_EQ(shared.remote_uris[1].ToUriString(), remote_uris[2].ToUriString());
    ASSERT_EQ(shared.local_buffers.size(), 2);

    const auto &other = groups.at("/other");
    ASSERT_EQ(other.indices.size(), 2);
    ASSERT_EQ(other.indices[0], 1);
    ASSERT_EQ(other.indices[1], 3);
    ASSERT_EQ(other.remote_uris[0].ToUriString(), remote_uris[1].ToUriString());
    ASSERT_EQ(other.remote_uris[1].ToUriString(), remote_uris[3].ToUriString());
}

TEST(SdkInterfaceContextCompatibilityTest, LegacySubclassDoesNotChangeExistingVtable) {
    TestSplitSdk sdk;
    SdkInterface *legacy = &sdk;

    EXPECT_EQ(nullptr, dynamic_cast<ContextualSdkInterface *>(legacy));
    EXPECT_EQ(ER_OK, legacy->Get({}, {}));
    EXPECT_EQ(ER_OK, legacy->Put({}, {}, std::make_shared<std::vector<DataStorageUri>>()));
}

TEST_F(SdkWrapperTest, WrapperPassesOneDeadlineToEveryGroup) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    SdkWrapper sdk_wrapper;
    init_params_.storage_configs = R"([
        {"type":"file","global_unique_name":"nfs_a","storage_spec":{"root_path":"/nfs_a/","key_count_per_file":2}},
        {"type":"file","global_unique_name":"nfs_b","storage_spec":{"root_path":"/nfs_b/","key_count_per_file":2}}
    ])";
    auto client_config = MakeClientConfigWithTimeouts(2000, 2000);
    ClientErrorCode init_ec = ER_OK;
    auto fake_factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_a/a?blkid=0&size=0"),
        DataStorageUri("file://nfs_b/b?blkid=0&size=0"),
    };
    ASSERT_EQ(ER_OK, sdk_wrapper.Get(uris, {BlockBuffer(), BlockBuffer()}));
    ASSERT_EQ(2, ctrl->contextual_get_calls.load());
    EXPECT_EQ(ctrl->first_deadline_ticks.load(), ctrl->second_deadline_ticks.load());
}

TEST_F(SdkWrapperTest, QueueTimeConsumesWrapperEntryDeadline) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    SdkWrapper sdk_wrapper;
    auto client_config = MakeClientConfigWithTimeouts(2000, 1000, 1);
    ClientErrorCode init_ec = ER_OK;
    auto fake_factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::atomic<int> blockers_started{0};
    std::vector<std::future<ClientErrorCode>> blockers;
    for (int i = 0; i < 8; ++i) {
        blockers.push_back(sdk_wrapper.wait_task_thread_pool_->async([&]() {
            blockers_started.fetch_add(1);
            release_future.wait();
            return ER_OK;
        }));
    }
    const bool blockers_ready = WaitForTrue([&] { return blockers_started.load() == 8; });
    EXPECT_TRUE(blockers_ready);

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    auto result = std::async(std::launch::async, [&] { return sdk_wrapper.Get(uris, {BlockBuffer()}); });
    const bool queue_filled = WaitForTrue([&] { return sdk_wrapper.wait_task_thread_pool_->isFull(); });
    EXPECT_TRUE(queue_filled);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    release.set_value();
    const auto result_code = result.get();
    sdk_wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_OK, result_code);
    EXPECT_EQ(1, ctrl->contextual_get_calls.load());
    EXPECT_LT(ctrl->remaining_ms_at_backend_entry.load(), 900);
    for (auto &blocker : blockers) {
        EXPECT_EQ(ER_OK, blocker.get());
    }
}

TEST_F(SdkWrapperTest, GetAndPutBeforeSuccessfulInitFailClosed) {
    SdkWrapper sdk_wrapper;
    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    auto actual = std::make_shared<std::vector<DataStorageUri>>(uris);

    EXPECT_EQ(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Get(uris, {BlockBuffer()}));
    EXPECT_EQ(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Put(uris, {BlockBuffer()}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(uris[0].ToUriString(), (*actual)[0].ToUriString());
}

TEST_F(SdkWrapperTest, EarlyInitFailureLeavesWrapperFailClosed) {
    SdkWrapper sdk_wrapper;
    InitParams invalid_params = init_params_;
    invalid_params.storage_configs = "[]";
    EXPECT_EQ(ER_INVALID_STORAGE_CONFIG, sdk_wrapper.Init(client_config_, invalid_params));

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    EXPECT_EQ(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Get(uris, {BlockBuffer()}));
    EXPECT_EQ(nullptr, sdk_wrapper.wait_task_thread_pool_);
    EXPECT_TRUE(sdk_wrapper.sdk_map_.empty());
}

TEST_F(SdkWrapperTest, PartialSdkCreationFailureCanRetryWithoutPublishedState) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    SdkWrapper sdk_wrapper;
    init_params_.storage_configs = R"([
        {"type":"file","global_unique_name":"nfs_a","storage_spec":{"root_path":"/nfs_a/","key_count_per_file":2}},
        {"type":"file","global_unique_name":"nfs_b","storage_spec":{"root_path":"/nfs_b/","key_count_per_file":2}}
    ])";
    auto factory = std::make_unique<FakeSdkFactory>(ctrl);
    factory->FailOnCreateCall(1);
    sdk_wrapper.sdk_factory_ = factory.get();

    EXPECT_EQ(ER_CREATESDK_ERROR, sdk_wrapper.Init(client_config_, init_params_));
    EXPECT_EQ(nullptr, sdk_wrapper.wrapper_config_);
    EXPECT_TRUE(sdk_wrapper.storage_configs_.empty());
    EXPECT_EQ(nullptr, sdk_wrapper.wait_task_thread_pool_);
    EXPECT_TRUE(sdk_wrapper.sdk_map_.empty());

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_a/a?blkid=0&size=0"),
    };
    EXPECT_EQ(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Get(uris, {BlockBuffer()}));

    factory->AllowAllCreates();
    EXPECT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));
    EXPECT_NE(nullptr, sdk_wrapper.wait_task_thread_pool_);
    EXPECT_EQ(2u, sdk_wrapper.sdk_map_.size());
    EXPECT_EQ(ER_OK, sdk_wrapper.Get(uris, {BlockBuffer()}));
}

TEST_F(SdkWrapperTest, RepeatedSuccessfulInitIsRejectedWithoutMutatingLiveState) {
    SdkWrapper sdk_wrapper;
    ASSERT_EQ(ER_OK, sdk_wrapper.Init(client_config_, init_params_));
    auto original_pool = sdk_wrapper.wait_task_thread_pool_.get();
    auto original_sdk = sdk_wrapper.sdk_map_.at("nfs_test");

    EXPECT_EQ(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Init(client_config_, init_params_));
    EXPECT_EQ(original_pool, sdk_wrapper.wait_task_thread_pool_.get());
    EXPECT_EQ(original_sdk, sdk_wrapper.sdk_map_.at("nfs_test"));

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    EXPECT_NE(ER_INVALID_SDKWRAPPER_CONFIG, sdk_wrapper.Get(uris, {BlockBuffer()}));
}

TEST_F(SdkWrapperTest, ConcurrentInitSerializesAndPublishesOnlyOneState) {
    SdkWrapper sdk_wrapper;
    std::promise<void> start;
    auto start_future = start.get_future().share();
    auto first = std::async(std::launch::async, [&] {
        start_future.wait();
        return sdk_wrapper.Init(client_config_, init_params_);
    });
    auto second = std::async(std::launch::async, [&] {
        start_future.wait();
        return sdk_wrapper.Init(client_config_, init_params_);
    });
    start.set_value();

    const auto first_result = first.get();
    const auto second_result = second.get();
    EXPECT_TRUE((first_result == ER_OK && second_result == ER_INVALID_SDKWRAPPER_CONFIG) ||
                (second_result == ER_OK && first_result == ER_INVALID_SDKWRAPPER_CONFIG));
    EXPECT_NE(nullptr, sdk_wrapper.wait_task_thread_pool_);
    EXPECT_EQ(1u, sdk_wrapper.sdk_map_.size());
}

TEST_F(SdkWrapperTest, TaskExceptionsMapToOperationErrorsWithoutEscaping) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    ctrl->throw_from_get.store(true);
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config_, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    EXPECT_EQ(ER_SDK_ERROR_BUFFER_SAFE, sdk_wrapper.Get(uris, {BlockBuffer()}));

    ctrl->throw_from_get.store(false);
    ctrl->throw_from_put.store(true);
    auto actual = std::make_shared<std::vector<DataStorageUri>>(uris);
    EXPECT_EQ(ER_SDK_ERROR_BUFFER_SAFE, sdk_wrapper.Put(uris, {BlockBuffer()}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(uris[0].ToUriString(), (*actual)[0].ToUriString());
}

TEST_F(SdkWrapperTest, PartialSubmissionRejectsWithoutRunningAcceptedGroupInline) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    auto client_config = MakeClientConfigWithTimeouts(2000, 2000, 1, 1);
    init_params_.storage_configs = R"([
        {"type":"file","global_unique_name":"nfs_a","storage_spec":{"root_path":"/nfs_a/","key_count_per_file":2}},
        {"type":"file","global_unique_name":"nfs_b","storage_spec":{"root_path":"/nfs_b/","key_count_per_file":2}},
        {"type":"file","global_unique_name":"nfs_c","storage_spec":{"root_path":"/nfs_c/","key_count_per_file":2}}
    ])";
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    auto blocker = sdk_wrapper.wait_task_thread_pool_->TryAsync([&] {
        blocker_started.set_value();
        release_blocker_future.wait();
        return ER_OK;
    });
    EXPECT_TRUE(blocker.accepted());
    if (!blocker.accepted()) {
        release_blocker.set_value();
        sdk_wrapper.wait_task_thread_pool_->waitFinish();
        return;
    }
    const auto blocker_started_status = blocker_started_future.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(std::future_status::ready, blocker_started_status);

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_a/a?blkid=0&size=0"),
        DataStorageUri("file://nfs_b/b?blkid=0&size=0"),
        DataStorageUri("file://nfs_c/c?blkid=0&size=0"),
    };
    auto result = std::async(std::launch::async, [&] {
        return sdk_wrapper.Get(uris, {BlockBuffer(), BlockBuffer(), BlockBuffer()});
    });
    const bool queue_filled = WaitForTrue([&] { return sdk_wrapper.wait_task_thread_pool_->isFull(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(0, ctrl->get_call_count.load());
    release_blocker.set_value();
    const auto result_code = result.get();
    const auto blocker_code = blocker.future.get();
    sdk_wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_TRUE(queue_filled);
    EXPECT_EQ(ER_SDK_ERROR_BUFFER_SAFE, result_code); // the rejected group never ran
    EXPECT_EQ(ER_OK, blocker_code);
    EXPECT_EQ(0, ctrl->get_call_count.load());
}

TEST(SdkCallContextTest, SecondOutputAdmissionIsRejectedWithoutTrackingAnotherCaller) {
    const SdkCallContext context(std::chrono::steady_clock::now() + std::chrono::seconds(5));
    auto first = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    ASSERT_TRUE(first.has_value());

    EXPECT_FALSE(context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT).has_value());

    first.reset();
    context.CancelAndWait();
}

TEST(SdkCallContextTest, CompletedOutputCommitIsPublishedBeforeGuardRelease) {
    const SdkCallContext context(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    {
        auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
        ASSERT_TRUE(access.has_value());
        EXPECT_TRUE(context.HasAdmittedOutputCommit());
        EXPECT_FALSE(context.CompletedOutputCommit().has_value());
        access->Complete(ER_OK);
    }

    context.WaitForCallerMemory();
    ASSERT_TRUE(context.CompletedOutputCommit().has_value());
    EXPECT_EQ(ER_OK, *context.CompletedOutputCommit());
}

TEST(SdkCallContextTest, FirstCompletedOutputResultIsImmutable) {
    const SdkCallContext context(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    ASSERT_TRUE(access.has_value());

    access->Complete(ER_OK);
    access->Complete(ER_SDK_TIMEOUT);

    ASSERT_TRUE(context.CompletedOutputCommit().has_value());
    EXPECT_EQ(ER_OK, *context.CompletedOutputCommit());
}

TEST(SdkCallContextTest, DestroyedIncompleteOutputGuardBlocksReadmission) {
    const SdkCallContext context(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    {
        auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
        ASSERT_TRUE(access.has_value());
    }

    context.WaitForCallerMemory();
    EXPECT_TRUE(context.HasAdmittedOutputCommit());
    EXPECT_FALSE(context.CompletedOutputCommit().has_value());
    EXPECT_FALSE(context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT).has_value());
}

TEST_F(SdkWrapperTest, SingleGroupCommitCompletingDuringCancellationPreservesSuccess) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    ctrl->use_context_output_commit.store(true);
    auto client_config = MakeClientConfigWithTimeouts(2000, 2000);
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    std::promise<void> commit_completed;
    auto commit_completed_future = commit_completed.get_future();
    std::promise<void> allow_callable_return;
    auto allow_callable_return_future = allow_callable_return.get_future().share();
    ctrl->after_output_commit = [&] {
        commit_completed.set_value();
        allow_callable_return_future.wait();
    };

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    const auto deadline_watchdog = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    auto result = std::async(std::launch::async, [&] { return sdk_wrapper.Get(uris, {BlockBuffer()}); });
    const auto commit_completed_status = commit_completed_future.wait_until(deadline_watchdog);
    EXPECT_EQ(std::future_status::ready, commit_completed_status);
    const auto result_status = result.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, result_status);
    allow_callable_return.set_value();
    const auto result_code = result.get();
    sdk_wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_OK, result_code);
    EXPECT_TRUE(ctrl->output_access_succeeded.load());
    EXPECT_EQ(1, ctrl->get_call_count.load());
}

TEST_F(SdkWrapperTest, QueuedWorkerCannotAcquireCallerMemoryAfterWrapperReturns) {
    auto ctrl = std::make_shared<FakeSdkControl>();
    ctrl->use_context_output_commit.store(true);
    auto client_config = MakeClientConfigWithTimeouts(2000, 2000);
    SdkWrapper sdk_wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto factory = InitWrapperWithFake(sdk_wrapper, ctrl, client_config, init_params_, init_ec);
    ASSERT_EQ(ER_OK, init_ec);

    std::promise<void> worker_paused;
    auto worker_paused_future = worker_paused.get_future();
    std::promise<void> allow_access;
    auto allow_access_future = allow_access.get_future().share();
    ctrl->before_output_access = [&] {
        worker_paused.set_value();
        allow_access_future.wait();
    };

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://nfs_test/a?blkid=0&size=0"),
    };
    auto result = std::async(std::launch::async, [&] { return sdk_wrapper.Get(uris, {BlockBuffer()}); });
    const auto worker_paused_status = worker_paused_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, worker_paused_status);
    const auto result_status = result.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, result_status);

    allow_access.set_value();
    const auto result_code = result.get();
    sdk_wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, result_code); // the queued worker never acquired caller memory
    EXPECT_TRUE(ctrl->output_access_attempted.load());
    EXPECT_FALSE(ctrl->output_access_succeeded.load());
    EXPECT_EQ(0, ctrl->get_call_count.load());
}

namespace {

struct SdkWrapperLayoutBaseline {
    SdkFactory *sdk_factory;
    std::shared_ptr<SdkWrapperConfig> wrapper_config;
    std::vector<std::shared_ptr<StorageConfig>> storage_configs;
    std::unique_ptr<LockFreeThreadPool> wait_task_thread_pool;
    std::map<std::string, std::shared_ptr<SdkInterface>> sdk_map;
    int owned_shm_fd;
};

static_assert(sizeof(SdkWrapper) == sizeof(SdkWrapperLayoutBaseline));
static_assert(sizeof(SdkInterface) == sizeof(void *));

std::unique_ptr<LockFreeThreadPool> MakeStartedPool(std::size_t thread_num = 2, std::size_t queue_size = 8) {
    auto pool = std::make_unique<LockFreeThreadPool>(thread_num, queue_size, "SdkWrapperOutcomeTestPool");
    EXPECT_TRUE(pool->start());
    return pool;
}

TEST(SdkWrapperOutcomeTest, CompletedSingleGroupCommitWinsWithoutFutureReadinessRace) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::seconds(2));
    std::promise<void> commit_completed;
    auto commit_completed_future = commit_completed.get_future();
    std::promise<void> allow_callable_return;
    auto allow_callable_return_future = allow_callable_return.get_future().share();

    std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> tasks;
    tasks.push_back([&](const SdkCallContext &task_context) {
        {
            auto guard = task_context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
            if (!guard) {
                return ER_SDK_TIMEOUT;
            }
            guard->Complete(ER_OK);
        }
        commit_completed.set_value();
        allow_callable_return_future.wait();
        return ER_OK;
    });

    auto result = std::async(std::launch::async, [&] {
        return wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    });
    const auto commit_completed_status = commit_completed_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, commit_completed_status);
    const auto result_status = result.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, result_status);
    allow_callable_return.set_value();
    const auto result_code = result.get();
    wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_OK, result_code);
}

TEST(SdkWrapperOutcomeTest, OneCommittedGroupCannotPreserveLegacyPeerAtDeadline) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::seconds(2));
    std::promise<void> legacy_access_started;
    auto legacy_access_started_future = legacy_access_started.get_future();
    std::promise<void> release_legacy_access;
    auto release_legacy_access_future = release_legacy_access.get_future().share();

    std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> tasks;
    tasks.push_back([](const SdkCallContext &task_context) {
        auto guard = task_context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
        if (!guard) {
            return ER_SDK_TIMEOUT;
        }
        guard->Complete(ER_OK);
        return ER_OK;
    });
    tasks.push_back([&](const SdkCallContext &task_context) {
        auto guard = task_context.TryAcquireCallerMemory();
        if (!guard) {
            return ER_SDK_TIMEOUT;
        }
        legacy_access_started.set_value();
        release_legacy_access_future.wait();
        return ER_OK;
    });

    auto result = std::async(std::launch::async, [&] {
        return wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    });
    const auto legacy_access_status = legacy_access_started_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, legacy_access_status);
    const auto cancellation_watchdog = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (root.IsLive() && std::chrono::steady_clock::now() < cancellation_watchdog) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(root.IsLive());
    release_legacy_access.set_value();
    const auto result_status = result.wait_for(std::chrono::seconds(5));
    const auto result_code = result.get();
    wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(std::future_status::ready, result_status);
    // The timeout still wins; both tasks finished with ER_OK before the wrapper returned.
    EXPECT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, result_code);
}

TEST(SdkWrapperOutcomeTest, DestroyedCommitGuardCannotPreservePeerCommitAtDeadline) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::seconds(2));
    std::promise<void> both_guards_released;
    auto both_guards_released_future = both_guards_released.get_future();
    std::promise<void> allow_callable_return;
    auto allow_callable_return_future = allow_callable_return.get_future().share();
    std::atomic<int> released_count{0};

    std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> tasks;
    for (int index = 0; index < 2; ++index) {
        tasks.push_back([&, index](const SdkCallContext &task_context) {
            {
                auto guard = task_context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
                if (!guard) {
                    return ER_SDK_TIMEOUT;
                }
                if (index == 0) {
                    guard->Complete(ER_OK);
                }
            }
            if (released_count.fetch_add(1) + 1 == 2) {
                both_guards_released.set_value();
            }
            allow_callable_return_future.wait();
            return ER_OK;
        });
    }

    auto result = std::async(std::launch::async, [&] {
        return wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    });
    const auto guards_released_status = both_guards_released_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, guards_released_status);
    const auto result_status = result.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(std::future_status::ready, result_status);
    allow_callable_return.set_value();
    const auto result_code = result.get();
    wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_SDK_TIMEOUT, result_code);
}

class WaitForFirstTaskStartPool : public LockFreeThreadPool {
public:
    explicit WaitForFirstTaskStartPool(std::shared_future<void> first_started)
        : LockFreeThreadPool(1, 1, "WaitForFirstTaskStartPool"), first_started_(std::move(first_started)) {}

protected:
    SubmissionStatus TryPush(std::function<void()> &&task) override {
        const auto status = LockFreeThreadPool::TryPush(std::move(task));
        if (calls_++ == 0 && status == SubmissionStatus::ACCEPTED) {
            first_started_.wait();
        }
        return status;
    }

private:
    std::shared_future<void> first_started_;
    int calls_ = 0;
};

TEST(SdkWrapperOutcomeTest, SubmissionRejectionStillReturnsAcceptedPeerTimeout) {
    SdkWrapper wrapper;
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::seconds(2));
    std::promise<void> first_started;
    auto first_started_future = first_started.get_future().share();
    auto pool = std::make_unique<WaitForFirstTaskStartPool>(first_started_future);
    ASSERT_TRUE(pool->start());
    wrapper.wait_task_thread_pool_ = std::move(pool);
    std::promise<void> release_first;
    auto release_first_future = release_first.get_future().share();

    std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> tasks;
    tasks.push_back([&](const SdkCallContext &) {
        first_started.set_value();
        release_first_future.wait();
        return ER_SDK_TIMEOUT;
    });
    tasks.push_back([](const SdkCallContext &) { return ER_OK; });
    tasks.push_back([](const SdkCallContext &) { return ER_OK; });

    auto result = std::async(std::launch::async, [&] {
        return wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    });
    const auto first_started_status = first_started_future.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(std::future_status::ready, first_started_status);
    release_first.set_value();
    const auto result_code = result.get();
    wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_SDK_TIMEOUT, result_code);
}

enum class SubmissionThrowMode {
    STANDARD,
    UNKNOWN,
};

class ThrowOnSecondSubmissionPool : public LockFreeThreadPool {
public:
    ThrowOnSecondSubmissionPool(SubmissionThrowMode mode, std::shared_future<void> first_finished)
        : LockFreeThreadPool(1, 4, "ThrowOnSecondSubmissionPool")
        , mode_(mode)
        , first_finished_(std::move(first_finished)) {}

protected:
    SubmissionStatus TryPush(std::function<void()> &&task) override {
        if (calls_++ == 1) {
            first_finished_.wait();
            if (mode_ == SubmissionThrowMode::STANDARD) {
                throw std::runtime_error("injected submission failure");
            }
            throw 7;
        }
        return LockFreeThreadPool::TryPush(std::move(task));
    }

private:
    SubmissionThrowMode mode_;
    std::shared_future<void> first_finished_;
    int calls_ = 0;
};

class SubmissionExceptionPrecedenceTest : public ::testing::TestWithParam<SubmissionThrowMode> {};

TEST_P(SubmissionExceptionPrecedenceTest, AcceptedPeerTimeoutBeatsSubmissionException) {
    std::promise<void> first_finished;
    auto first_finished_future = first_finished.get_future().share();
    SdkWrapper wrapper;
    auto pool = std::make_unique<ThrowOnSecondSubmissionPool>(GetParam(), first_finished_future);
    ASSERT_TRUE(pool->start());
    wrapper.wait_task_thread_pool_ = std::move(pool);
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::seconds(2));

    std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> tasks;
    tasks.push_back([&](const SdkCallContext &) {
        first_finished.set_value();
        return ER_SDK_TIMEOUT;
    });
    tasks.push_back([](const SdkCallContext &) { return ER_OK; });

    EXPECT_EQ(ER_SDK_TIMEOUT, wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root));
}

INSTANTIATE_TEST_SUITE_P(StandardAndUnknown,
                         SubmissionExceptionPrecedenceTest,
                         ::testing::Values(SubmissionThrowMode::STANDARD, SubmissionThrowMode::UNKNOWN));

} // namespace

namespace {

ClientErrorCode
RunParallelForTest(SdkWrapper &wrapper, std::vector<SdkWrapper::ParallelTask> tasks, std::chrono::milliseconds budget) {
    const SdkCallContext root(std::chrono::steady_clock::now() + budget);
    const auto result = wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    wrapper.wait_task_thread_pool_->waitFinish();
    return result;
}

} // namespace

// Upstream buffer-safe contract: SAFE only when every task provably stopped
// touching caller buffers. Guarded tasks qualify after the drain; unguarded
// tasks only with OK, cancellation, or an explicit SAFE outcome.
TEST(SdkWrapperBufferSafetyTest, UnguardedPlainErrorIsNotReportedBufferSafe) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    std::vector<SdkWrapper::ParallelTask> tasks;
    tasks.push_back({[](const SdkCallContext &) { return ER_SDKREAD_ERROR; }, false});
    EXPECT_EQ(ER_SDKREAD_ERROR, RunParallelForTest(wrapper, std::move(tasks), std::chrono::seconds(2)));
}

TEST(SdkWrapperBufferSafetyTest, UnguardedExplicitSafeErrorIsReportedBufferSafe) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    std::vector<SdkWrapper::ParallelTask> tasks;
    tasks.push_back({[](const SdkCallContext &) { return ER_SDK_ERROR_BUFFER_SAFE; }, false});
    tasks.push_back({[](const SdkCallContext &) { return ER_OK; }, false});
    EXPECT_EQ(ER_SDK_ERROR_BUFFER_SAFE, RunParallelForTest(wrapper, std::move(tasks), std::chrono::seconds(2)));
}

TEST(SdkWrapperBufferSafetyTest, SafeFirstErrorCannotHideUnguardedPeerFailure) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool(1, 8);
    std::vector<SdkWrapper::ParallelTask> tasks;
    tasks.push_back({[](const SdkCallContext &) { return ER_SDK_ERROR_BUFFER_SAFE; }, false});
    tasks.push_back({[](const SdkCallContext &) { return ER_SDKREAD_ERROR; }, false});
    const auto result = RunParallelForTest(wrapper, std::move(tasks), std::chrono::seconds(2));
    EXPECT_NE(ER_SDK_ERROR_BUFFER_SAFE, result);
    EXPECT_NE(ER_SDK_TIMEOUT_BUFFER_SAFE, result);
}

TEST(SdkWrapperBufferSafetyTest, GuardedPlainErrorIsReportedBufferSafe) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    std::vector<SdkWrapper::ParallelTask> tasks;
    tasks.push_back({[](const SdkCallContext &) { return ER_SDKREAD_ERROR; }, true});
    EXPECT_EQ(ER_SDK_ERROR_BUFFER_SAFE, RunParallelForTest(wrapper, std::move(tasks), std::chrono::seconds(2)));
}

TEST(SdkWrapperBufferSafetyTest, UnfinishedUnguardedPeerKeepsPlainTimeout) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::vector<SdkWrapper::ParallelTask> tasks;
    tasks.push_back({[](const SdkCallContext &) { return ER_OK; }, true});
    tasks.push_back({[release_future](const SdkCallContext &) {
                         release_future.wait();
                         return ER_OK;
                     },
                     false});
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
    const auto result = wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    release.set_value();
    wrapper.wait_task_thread_pool_->waitFinish();
    EXPECT_EQ(ER_SDK_TIMEOUT, result);
}

TEST(SdkWrapperBufferSafetyTest, UnfinishedGuardedPeerIsBufferSafeAfterDrain) {
    SdkWrapper wrapper;
    wrapper.wait_task_thread_pool_ = MakeStartedPool();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::vector<SdkWrapper::ParallelTask> tasks;
    // Blocked in "I/O" without a caller-memory guard, like a Valkey request waiting on the network.
    tasks.push_back({[release_future](const SdkCallContext &) {
                         release_future.wait();
                         return ER_OK;
                     },
                     true});
    const SdkCallContext root(std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
    const auto result = wrapper.RunWithTimeoutParallel(SdkWrapper::OpType::GET, std::move(tasks), root);
    release.set_value();
    wrapper.wait_task_thread_pool_->waitFinish();
    EXPECT_EQ(ER_SDK_TIMEOUT_BUFFER_SAFE, result);
}

TEST_F(SdkWrapperTest, JsonConfigInitializesValkeyThroughFactoryWithoutNetwork) {
    auto client_config = std::make_unique<ClientConfig>();
    ASSERT_TRUE(client_config->FromJsonString(R"({
        "instance_group": "group",
        "instance_id": "instance",
        "address": ["127.0.0.1:8080"],
        "block_size": 128,
        "sdk_config": {
            "thread_num": 1,
            "queue_size": 4,
            "sdk_backend_configs": [{"type": "valkey"}],
            "timeout_config": {"put_timeout_ms": 2000, "get_timeout_ms": 2000}
        },
        "model_deployment": {
            "model_name": "test_model", "dtype": "FP8", "use_mla": false,
            "tp_size": 1, "dp_size": 1, "pp_size": 1
        },
        "location_spec_infos": {"tp0": 1024}
    })"));
    InitParams params = init_params_;
    params.storage_configs = R"([{
        "type": "valkey",
        "global_unique_name": "valkey-primary",
        "storage_spec": {"endpoint": "valkey://cache.internal:6379/?db=1"}
    }])";
    auto ctrl = std::make_shared<FakeSdkControl>();
    SdkWrapper wrapper;
    ClientErrorCode init_ec = ER_OK;
    auto factory = InitWrapperWithFake(wrapper, ctrl, client_config, params, init_ec);

    ASSERT_EQ(ER_OK, init_ec);
    ASSERT_EQ(1u, wrapper.sdk_map_.size());
    EXPECT_NE(nullptr, wrapper.sdk_map_.at("valkey-primary"));
    const auto cloned_config = wrapper.wrapper_config_->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    ASSERT_NE(nullptr, cloned_config);
    EXPECT_EQ(typeid(ValkeySdkConfig), typeid(*cloned_config));
}

namespace {

class PausingLegacySdk : public SdkInterface {
public:
    PausingLegacySdk(std::promise<void> &started, std::shared_future<void> release)
        : started_(started), release_(std::move(release)) {}

    ClientErrorCode Init(const std::shared_ptr<SdkBackendConfig> &, const std::shared_ptr<StorageConfig> &) override {
        return ER_OK;
    }
    SdkType Type() override { return SdkType::LOCAL_FILE; }
    ClientErrorCode Get(const std::vector<DataStorageUri> &, const BlockBuffers &) override {
        started_.set_value();
        release_.wait();
        return ER_OK;
    }
    ClientErrorCode Put(const std::vector<DataStorageUri> &,
                        const BlockBuffers &,
                        std::shared_ptr<std::vector<DataStorageUri>>) override {
        return ER_OK;
    }

protected:
    ClientErrorCode Alloc(const std::vector<DataStorageUri> &, std::vector<DataStorageUri> &) override { return ER_OK; }

private:
    std::promise<void> &started_;
    std::shared_future<void> release_;
};

TEST_F(SdkWrapperTest, ContextualCommitCannotPreserveLateLegacyGroup) {
    init_params_.storage_configs = R"([
        {"type":"file","global_unique_name":"contextual","storage_spec":{"root_path":"/a/","key_count_per_file":2}},
        {"type":"file","global_unique_name":"legacy","storage_spec":{"root_path":"/b/","key_count_per_file":2}}
    ])";
    auto client_config = MakeClientConfigWithTimeouts(2000, 80, 8, 2);
    auto contextual_control = std::make_shared<FakeSdkControl>();
    contextual_control->use_context_output_commit.store(true);
    std::promise<void> legacy_started;
    auto legacy_started_future = legacy_started.get_future();
    std::promise<void> release_legacy;
    auto release_legacy_future = release_legacy.get_future().share();
    auto factory = std::make_unique<FakeSdkFactory>(contextual_control);
    factory->SetSdkForStorage("legacy", std::make_shared<PausingLegacySdk>(legacy_started, release_legacy_future));
    SdkWrapper wrapper;
    wrapper.sdk_factory_ = factory.get();
    ASSERT_EQ(ER_OK, wrapper.Init(client_config, init_params_));

    const std::vector<DataStorageUri> uris{
        DataStorageUri("file://contextual/a?blkid=0&size=0"),
        DataStorageUri("file://legacy/b?blkid=0&size=0"),
    };
    auto result = std::async(std::launch::async, [&] { return wrapper.Get(uris, {BlockBuffer(), BlockBuffer()}); });
    const auto legacy_started_status = legacy_started_future.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(std::future_status::ready, legacy_started_status);
    const auto result_status = result.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(std::future_status::ready, result_status);
    release_legacy.set_value();
    const auto result_code = result.get();
    wrapper.wait_task_thread_pool_->waitFinish();

    EXPECT_EQ(ER_SDK_TIMEOUT, result_code);
    EXPECT_TRUE(contextual_control->output_access_succeeded.load());
}

} // namespace
