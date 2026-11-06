#include <fstream>
#include <gtest/gtest.h>
#include <typeinfo>

#include "kv_cache_manager/client/src/internal/config/sdk_config.h"
#include "kv_cache_manager/client/src/internal/config/test/config_test_base.h"
#include "kv_cache_manager/common/logger.h"

using namespace kv_cache_manager;

class SdkBackendConfigTest : public ConfigTestBase {};

TEST_F(SdkBackendConfigTest, TestHf3fsSdkConfigSuccess) {
    Hf3fsSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_hf3fs_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_backend_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestHf3fsSdkConfigInvalidType) {
    Hf3fsSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_hf3fs_invalid_type.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_FALSE(sdk_backend_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestMooncakeSdkConfigSuccess) {
    MooncakeSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_mooncake_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_backend_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestMooncakeSdkConfigEmptyLocation) {
    MooncakeSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_mooncake_empty_location.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_FALSE(sdk_backend_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestMooncakeSdkConfigInvalidReplicaNum) {
    MooncakeSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_mooncake_invalid_replica_num.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_FALSE(sdk_backend_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestTairMempoolSdkConfigSuccess) {
    TairMempoolSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_tair_mempool_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_backend_config.Validate());
    ASSERT_EQ("logs/pace_client.log", sdk_backend_config.sdk_log_file_path());
    ASSERT_EQ("DEBUG", sdk_backend_config.sdk_log_level());
}

TEST_F(SdkBackendConfigTest, TestNfsSdkConfigSuccess) {
    NfsSdkConfig sdk_backend_config;
    std::string file_content = getFileContent("sdk_config_nfs_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_backend_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_backend_config.Validate());
}

// SdkWrapperConfig tests
TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigSuccess) {
    SdkWrapperConfig sdk_wrapper_config;
    std::string file_content = getFileContent("sdk_wrapper_config_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_wrapper_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigInvalidTimeout) {
    SdkWrapperConfig sdk_wrapper_config;
    std::string file_content = getFileContent("sdk_wrapper_config_invalid_timeout.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(file_content));
    ASSERT_FALSE(sdk_wrapper_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigEmptySdkConfigs) {
    SdkWrapperConfig sdk_wrapper_config;
    std::string file_content = getFileContent("sdk_wrapper_config_empty_sdk_configs.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_wrapper_config.Validate());
}

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigGetSdkConfig) {
    SdkWrapperConfig sdk_wrapper_config;
    std::string file_content = getFileContent("sdk_wrapper_config_success.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(file_content));

    auto hf3fs_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_HF3FS);
    ASSERT_NE(hf3fs_config, nullptr);

    auto mooncake_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE);
    ASSERT_NE(mooncake_config, nullptr);

    auto tair_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL);
    ASSERT_NE(tair_config, nullptr);

    auto tair_ssd_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD);
    ASSERT_NE(tair_ssd_config, nullptr);
    EXPECT_EQ(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD, tair_ssd_config->type());

    auto nfs_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_NFS);
    ASSERT_NE(nfs_config, nullptr);

    auto unknown_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_UNKNOWN);
    ASSERT_EQ(unknown_config, nullptr);
}

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigParsesTairMempoolSsd) {
    SdkWrapperConfig sdk_wrapper_config;
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(R"({
        "thread_num": 8,
        "queue_size": 2000,
        "sdk_backend_configs": [{
            "type": "pace_ssd",
            "sdk_log_file_path": "logs/pace_ssd_client.log",
            "sdk_log_level": "INFO",
            "spec_byte_sizes_per_block": {}
        }],
        "timeout_config": {"put_timeout_ms": 15000, "get_timeout_ms": 15000}
    })"));

    const auto config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD);
    ASSERT_NE(nullptr, config);
    EXPECT_EQ(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD, config->type());
    EXPECT_EQ("logs/pace_ssd_client.log", config->sdk_log_file_path());
}

TEST_F(SdkBackendConfigTest, TestDuplicatedSdkConfig) {
    SdkWrapperConfig sdk_wrapper_config;
    std::string file_content = getFileContent("sdk_wrapper_config_duplicated_sdk_config.json");
    ASSERT_FALSE(file_content.empty());
    ASSERT_TRUE(sdk_wrapper_config.FromJsonString(file_content));
    ASSERT_TRUE(sdk_wrapper_config.Validate());

    auto mooncake_config = sdk_wrapper_config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE);
    ASSERT_NE(mooncake_config, nullptr);
    ASSERT_EQ(8, std::dynamic_pointer_cast<MooncakeSdkConfig>(mooncake_config)->put_replica_num());
}

TEST_F(SdkBackendConfigTest, TestSdkTimeoutConfigDefault) {
    SdkTimeoutConfig timeout_config;

    ASSERT_EQ(15000, timeout_config.get_timeout_ms());
    ASSERT_EQ(15000, timeout_config.put_timeout_ms());
    ASSERT_TRUE(timeout_config.Validate());
}

// Test cases for operator== functions
TEST_F(SdkBackendConfigTest, TestSdkTimeoutConfigEquality) {
    SdkTimeoutConfig config1;
    config1.set_put_timeout_ms(1000);
    config1.set_get_timeout_ms(2000);

    SdkTimeoutConfig config2;
    config2.set_put_timeout_ms(1000);
    config2.set_get_timeout_ms(2000);

    SdkTimeoutConfig config3;
    config3.set_put_timeout_ms(1500);
    config3.set_get_timeout_ms(2000);

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestSdkBackendConfigEquality) {
    SdkBackendConfig config1;
    config1.set_type(DataStorageType::DATA_STORAGE_TYPE_HF3FS);
    config1.set_sdk_log_file_path("log1.txt");
    config1.set_sdk_log_level("DEBUG");
    config1.set_spec_byte_sizes_per_block({{"default", 1024}});

    SdkBackendConfig config2;
    config2.set_type(DataStorageType::DATA_STORAGE_TYPE_HF3FS);
    config2.set_sdk_log_file_path("log1.txt");
    config2.set_sdk_log_level("DEBUG");
    config2.set_spec_byte_sizes_per_block({{"default", 1024}});

    SdkBackendConfig config3;
    config3.set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    config3.set_sdk_log_file_path("log1.txt");
    config3.set_sdk_log_level("DEBUG");
    config3.set_spec_byte_sizes_per_block({{"default", 1024}});

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestHf3fsSdkConfigEquality) {
    Hf3fsSdkConfig config1;
    config1.set_read_iov_block_size(1024);
    config1.set_read_iov_size(1ULL << 32);
    config1.set_write_iov_block_size(1ULL << 20);
    config1.set_write_iov_size(1ULL << 32);

    Hf3fsSdkConfig config2;
    config2.set_read_iov_block_size(1024);
    config2.set_read_iov_size(1ULL << 32);
    config2.set_write_iov_block_size(1ULL << 20);
    config2.set_write_iov_size(1ULL << 32);

    Hf3fsSdkConfig config3;
    config3.set_read_iov_block_size(2048); // Different value
    config3.set_read_iov_size(1ULL << 32);
    config3.set_write_iov_block_size(1ULL << 20);
    config3.set_write_iov_size(1ULL << 32);

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestMooncakeSdkConfigEquality) {
    MooncakeSdkConfig config1;
    config1.set_local_buffer_size(1024);
    config1.set_location("localhost");
    config1.set_put_replica_num(2);

    MooncakeSdkConfig config2;
    config2.set_local_buffer_size(1024);
    config2.set_location("localhost");
    config2.set_put_replica_num(2);

    MooncakeSdkConfig config3;
    config3.set_local_buffer_size(2048); // Different value
    config3.set_location("localhost");
    config3.set_put_replica_num(2);

    // Test equality (note: local_mem_ptr_ is not compared)
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestTairMempoolSdkConfigEquality) {
    TairMempoolSdkConfig config1;
    config1.set_type(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL);
    config1.set_sdk_log_file_path("tair.log");
    config1.set_sdk_log_level("INFO");
    config1.set_spec_byte_sizes_per_block({{"default", 2048}});

    TairMempoolSdkConfig config2;
    config2.set_type(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL);
    config2.set_sdk_log_file_path("tair.log");
    config2.set_sdk_log_level("INFO");
    config2.set_spec_byte_sizes_per_block({{"default", 2048}});

    TairMempoolSdkConfig config3;
    config3.set_type(DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL);
    config3.set_sdk_log_file_path("tair.log");
    config3.set_sdk_log_level("DEBUG"); // Different value
    config3.set_spec_byte_sizes_per_block({{"default", 2048}});

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestNfsSdkConfigEquality) {
    NfsSdkConfig config1;
    config1.set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    config1.set_sdk_log_file_path("nfs.log");
    config1.set_sdk_log_level("DEBUG");
    config1.set_spec_byte_sizes_per_block({{"default", 4096}});

    NfsSdkConfig config2;
    config2.set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    config2.set_sdk_log_file_path("nfs.log");
    config2.set_sdk_log_level("DEBUG");
    config2.set_spec_byte_sizes_per_block({{"default", 4096}});

    NfsSdkConfig config3;
    config3.set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    config3.set_sdk_log_file_path("nfs.log");
    config3.set_sdk_log_level("INFO"); // Different value
    config3.set_spec_byte_sizes_per_block({{"default", 4096}});

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigEquality) {
    // Create first config
    SdkWrapperConfig config1;
    config1.set_thread_num(8);
    config1.set_queue_size(2000);

    SdkTimeoutConfig timeout1;
    timeout1.set_put_timeout_ms(1000);
    timeout1.set_get_timeout_ms(2000);
    config1.set_timeout_config(timeout1);

    // Create second identical config
    SdkWrapperConfig config2;
    config2.set_thread_num(8);
    config2.set_queue_size(2000);

    SdkTimeoutConfig timeout2;
    timeout2.set_put_timeout_ms(1000);
    timeout2.set_get_timeout_ms(2000);
    config2.set_timeout_config(timeout2);

    // Create third different config
    SdkWrapperConfig config3;
    config3.set_thread_num(16); // Different value
    config3.set_queue_size(2000);

    SdkTimeoutConfig timeout3;
    timeout3.set_put_timeout_ms(1000);
    timeout3.set_get_timeout_ms(2000);
    config3.set_timeout_config(timeout3);

    // Test equality
    ASSERT_TRUE(config1 == config2);
    ASSERT_FALSE(config1 != config2);

    // Test inequality
    ASSERT_FALSE(config1 == config3);
    ASSERT_TRUE(config1 != config3);

    // Test self-equality
    ASSERT_TRUE(config1 == config1);
    ASSERT_FALSE(config1 != config1);
}

namespace {

void SetProgrammaticBaseFields(SdkBackendConfig &config) {
    config.set_sdk_log_file_path("programmatic.log");
    config.set_sdk_log_level("TRACE");
    config.set_spec_byte_sizes_per_block({{"tp0", 1234}});
    SdkTimeoutConfig timeout;
    timeout.set_get_timeout_ms(37);
    timeout.set_put_timeout_ms(41);
    config.set_timeout_config(timeout);
}

class FutureValkeySdkConfig : public SdkBackendConfig {
public:
    FutureValkeySdkConfig() { set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY); }
};

TEST_F(SdkBackendConfigTest, TestSdkWrapperConfigParsesValkeyBackend) {
    SdkWrapperConfig config;
    ASSERT_TRUE(config.FromJsonString(R"({
        "thread_num": 3,
        "queue_size": 7,
        "sdk_backend_configs": [{
            "type": "valkey",
            "sdk_log_file_path": "valkey.log",
            "sdk_log_level": "INFO",
            "spec_byte_sizes_per_block": {"tp0": 1024}
        }],
        "timeout_config": {"put_timeout_ms": 17, "get_timeout_ms": 13}
    })"));

    const auto backend = config.GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    ASSERT_NE(nullptr, backend);
    EXPECT_EQ(typeid(SdkBackendConfig), typeid(*backend));
    EXPECT_EQ(DataStorageType::DATA_STORAGE_TYPE_VALKEY, backend->type());
    EXPECT_EQ("valkey.log", backend->sdk_log_file_path());
    EXPECT_TRUE(config.Validate());
}

TEST_F(SdkBackendConfigTest, TestTypeSafeClonePreservesEverySupportedConcreteConfig) {
    auto hf3fs = std::make_shared<Hf3fsSdkConfig>();
    SetProgrammaticBaseFields(*hf3fs);
    hf3fs->set_mountpoint("/programmatic/hf3fs");
    hf3fs->set_root_dir("root");
    hf3fs->set_read_iov_block_size(11);
    hf3fs->set_read_iov_size(12);
    hf3fs->set_write_iov_block_size(13);
    hf3fs->set_write_iov_size(14);
    auto hf3fs_clone = CloneSdkBackendConfig(hf3fs);
    ASSERT_NE(nullptr, hf3fs_clone);
    ASSERT_EQ(typeid(Hf3fsSdkConfig), typeid(*hf3fs_clone));
    EXPECT_EQ(*hf3fs, static_cast<const Hf3fsSdkConfig &>(*hf3fs_clone));

    auto vcns = std::make_shared<Hf3fsSdkConfig>(*hf3fs);
    vcns->set_type(DataStorageType::DATA_STORAGE_TYPE_VCNS_HF3FS);
    auto vcns_clone = CloneSdkBackendConfig(vcns);
    ASSERT_NE(nullptr, vcns_clone);
    ASSERT_EQ(typeid(Hf3fsSdkConfig), typeid(*vcns_clone));
    EXPECT_EQ(*vcns, static_cast<const Hf3fsSdkConfig &>(*vcns_clone));

    auto mooncake = std::make_shared<MooncakeSdkConfig>();
    SetProgrammaticBaseFields(*mooncake);
    mooncake->set_local_mem_ptr(reinterpret_cast<void *>(0x1234));
    mooncake->set_local_buffer_size(5678);
    mooncake->set_location("programmatic-location");
    mooncake->set_put_replica_num(3);
    mooncake->set_self_location_spec_name("tp0");
    auto mooncake_clone = CloneSdkBackendConfig(mooncake);
    ASSERT_NE(nullptr, mooncake_clone);
    ASSERT_EQ(typeid(MooncakeSdkConfig), typeid(*mooncake_clone));
    const auto &typed_mooncake_clone = static_cast<const MooncakeSdkConfig &>(*mooncake_clone);
    EXPECT_EQ(*mooncake, typed_mooncake_clone);
    EXPECT_EQ(mooncake->local_mem_ptr(), typed_mooncake_clone.local_mem_ptr());

    for (const DataStorageType type :
         {DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL, DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD}) {
        auto tair = std::make_shared<TairMempoolSdkConfig>(type);
        SetProgrammaticBaseFields(*tair);
        tair->set_shm_fd(9);
        tair->set_shm_size(8192);
        tair->set_client_base(reinterpret_cast<void *>(0x2345));
        tair->set_gpu_memory_spans({{reinterpret_cast<void *>(0x3456), 4096, 2}});
        auto tair_clone = CloneSdkBackendConfig(tair);
        ASSERT_NE(nullptr, tair_clone);
        ASSERT_EQ(typeid(TairMempoolSdkConfig), typeid(*tair_clone));
        EXPECT_EQ(*tair, static_cast<const TairMempoolSdkConfig &>(*tair_clone));
    }

    auto nfs = std::make_shared<NfsSdkConfig>();
    SetProgrammaticBaseFields(*nfs);
    auto nfs_clone = CloneSdkBackendConfig(nfs);
    ASSERT_NE(nullptr, nfs_clone);
    ASSERT_EQ(typeid(NfsSdkConfig), typeid(*nfs_clone));
    EXPECT_EQ(*nfs, static_cast<const NfsSdkConfig &>(*nfs_clone));

    auto valkey = std::make_shared<SdkBackendConfig>();
    valkey->set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    SetProgrammaticBaseFields(*valkey);
    auto valkey_clone = CloneSdkBackendConfig(valkey);
    ASSERT_NE(nullptr, valkey_clone);
    ASSERT_EQ(typeid(SdkBackendConfig), typeid(*valkey_clone));
    EXPECT_EQ(*valkey, *valkey_clone);
}

TEST_F(SdkBackendConfigTest, TestTypeSafeCloneFailsClosedForUnknownAndMismatchedTypes) {
    auto unknown = std::make_shared<SdkBackendConfig>();
    unknown->set_type(DataStorageType::DATA_STORAGE_TYPE_UNKNOWN);
    EXPECT_EQ(nullptr, CloneSdkBackendConfig(unknown));

    auto wrong_dynamic_type = std::make_shared<Hf3fsSdkConfig>();
    wrong_dynamic_type->set_type(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE);
    EXPECT_EQ(nullptr, CloneSdkBackendConfig(wrong_dynamic_type));

    auto future_valkey = std::make_shared<FutureValkeySdkConfig>();
    EXPECT_EQ(nullptr, CloneSdkBackendConfig(future_valkey));

    auto wrapper = std::make_shared<SdkWrapperConfig>();
    wrapper->set_sdk_backend_configs_map({{DataStorageType::DATA_STORAGE_TYPE_NFS, future_valkey}});
    EXPECT_EQ(nullptr, CloneSdkWrapperConfig(wrapper));
}

TEST_F(SdkBackendConfigTest, TestWrapperClonePreservesDynamicTypesAndProgrammaticFields) {
    auto mooncake = std::make_shared<MooncakeSdkConfig>();
    mooncake->set_local_mem_ptr(reinterpret_cast<void *>(0x4567));
    mooncake->set_local_buffer_size(100);
    mooncake->set_self_location_spec_name("tp-special");
    auto valkey = std::make_shared<SdkBackendConfig>();
    valkey->set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    SetProgrammaticBaseFields(*valkey);

    auto source = std::make_shared<SdkWrapperConfig>();
    source->set_thread_num(5);
    source->set_queue_size(9);
    source->set_sdk_backend_configs_map({
        {DataStorageType::DATA_STORAGE_TYPE_MOONCAKE, mooncake},
        {DataStorageType::DATA_STORAGE_TYPE_VALKEY, valkey},
    });

    const auto clone = CloneSdkWrapperConfig(source);
    ASSERT_NE(nullptr, clone);
    EXPECT_EQ(*source, *clone);
    EXPECT_NE(source->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE),
              clone->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE));
    EXPECT_EQ(typeid(MooncakeSdkConfig),
              typeid(*clone->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE)));
    EXPECT_EQ(typeid(SdkBackendConfig), typeid(*clone->GetSdkBackendConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY)));
}

} // namespace
