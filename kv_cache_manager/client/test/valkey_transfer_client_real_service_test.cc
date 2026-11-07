#include <memory>
#include <string>
#include <vector>

#include "kv_cache_manager/client/include/transfer_client.h"
#include "kv_cache_manager/common/request_context.h"
#include "kv_cache_manager/common/test/secure_valkey_test_environment.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/data_storage_manager.h"
#include "kv_cache_manager/metrics/metrics_registry.h"
#include "kv_cache_manager/protocol/protobuf/admin_service.pb.h"
#include "kv_cache_manager/service/util/manager_message_proto_util.h"

namespace kv_cache_manager {
namespace {

constexpr char kStorageName[] = "vlk05-transfer-real";

class StorageCleanupGuard {
public:
    StorageCleanupGuard(DataStorageManager &manager,
                        RequestContext &request_context,
                        const std::vector<DataStorageUri> &uris)
        : manager_(manager), request_context_(request_context), uris_(uris) {}

    ~StorageCleanupGuard() noexcept {
        if (!active_) {
            return;
        }
        try {
            if (!uris_.empty()) {
                (void)manager_.Delete(&request_context_, kStorageName, uris_, []() {});
            }
            (void)manager_.DoCleanup();
        } catch (...) {}
    }

    void Release() noexcept { active_ = false; }

private:
    DataStorageManager &manager_;
    RequestContext &request_context_;
    const std::vector<DataStorageUri> &uris_;
    bool active_ = true;
};

std::string ClientConfigJson() {
    return R"({
        "instance_group": "vlk05-test-group",
        "instance_id": "vlk05-test-instance",
        "block_size": 16,
        "sdk_config": {
            "thread_num": 2,
            "queue_size": 16,
            "sdk_backend_configs": [{
                "type": "valkey",
                "sdk_log_file_path": "",
                "sdk_log_level": "",
                "spec_byte_sizes_per_block": {}
            }],
            "timeout_config": {
                "get_timeout_ms": 10000,
                "put_timeout_ms": 10000
            }
        },
        "location_spec_infos": {
            "tp0": 7
        }
    })";
}

std::string Flatten(const BlockBuffer &buffer) {
    std::string result;
    for (const auto &iov : buffer.iovs) {
        result.append(static_cast<const char *>(iov.base), iov.size);
    }
    return result;
}

class ValkeyTransferClientRealServiceTest : public TESTBASE {
public:
    ValkeyTransferClientRealServiceTest() : server_(kStorageName, 15) {}

protected:
    // Plaintext valkey:// endpoints cannot carry credentials, so this suite
    // starts its own unauthenticated loopback server instead of the shared one.
    test::SecureValkeyTestServer server_;
};

TEST_F(ValkeyTransferClientRealServiceTest, PublicTransferClientRoundTripsBinaryCpuBuffers) {
    ASSERT_TRUE(server_.StartPlaintextWithoutAuthentication());
    const std::string valkey_endpoint = server_.Endpoint("127.0.0.1", false);
    proto::admin::StorageConfig proto_config;
    proto_config.set_global_unique_name(kStorageName);
    proto_config.set_check_storage_available_when_open(true);
    proto_config.mutable_valkey()->set_endpoint(valkey_endpoint);

    StorageConfig storage_config;
    ProtoConvert::StorageFromProto(&proto_config, storage_config);
    ASSERT_EQ(DataStorageType::DATA_STORAGE_TYPE_VALKEY, storage_config.type());
    std::string invalid_fields;
    ASSERT_TRUE(storage_config.ValidateRequiredFields(invalid_fields)) << invalid_fields;

    auto metrics_registry = std::make_shared<MetricsRegistry>();
    DataStorageManager storage_manager(metrics_registry);
    RequestContext request_context("vlk05-transfer-real-service");
    std::vector<DataStorageUri> storage_uris;
    StorageCleanupGuard cleanup_guard(storage_manager, request_context, storage_uris);
    ASSERT_EQ(EC_OK, storage_manager.RegisterStorage(&request_context, kStorageName, storage_config));

    auto binary_locations = storage_manager.Create(&request_context, kStorageName, {"first", "second"}, 7, []() {});
    auto empty_locations = storage_manager.Create(&request_context, kStorageName, {"empty"}, 0, []() {});
    ASSERT_EQ(2u, binary_locations.size());
    ASSERT_EQ(1u, empty_locations.size());

    UriStrVec locations;
    for (const auto &item : binary_locations) {
        ASSERT_EQ(EC_OK, item.first);
        storage_uris.push_back(item.second);
        locations.push_back(item.second.ToUriString());
    }
    ASSERT_EQ(EC_OK, empty_locations.front().first);
    storage_uris.push_back(empty_locations.front().second);
    locations.push_back(empty_locations.front().second.ToUriString());
    ASSERT_EQ(3u, locations.size());
    EXPECT_EQ(0u, locations[0].find("valkey://vlk05-transfer-real/"));
    EXPECT_EQ(0u, locations[1].find("valkey://vlk05-transfer-real/"));
    EXPECT_EQ(0u, locations[2].find("valkey://vlk05-transfer-real/"));

    InitParams init_params;
    init_params.role_type = RoleType::WORKER;
    init_params.self_location_spec_name = "tp0";
    init_params.storage_configs = "[" + storage_config.ToJsonString() + "]";
    auto transfer_client = TransferClient::Create(ClientConfigJson(), init_params);
    ASSERT_NE(nullptr, transfer_client);

    std::string first_part_1({'a', 'b', '\0'});
    std::string first_part_2({'c', 'd', static_cast<char>(0xff), static_cast<char>(0x80)});
    std::string second_part_1({'\0', '1', '2', '3'});
    std::string second_part_2({static_cast<char>(0xfe), '4', '\0'});
    BlockBuffers source_buffers{
        BlockBuffer{{Iov{MemoryType::CPU, first_part_1.data(), first_part_1.size(), false},
                     Iov{MemoryType::CPU, first_part_2.data(), first_part_2.size(), false}}},
        BlockBuffer{{Iov{MemoryType::CPU, second_part_1.data(), second_part_1.size(), false},
                     Iov{MemoryType::CPU, second_part_2.data(), second_part_2.size(), false}}},
        BlockBuffer{},
    };

    const auto [save_ec, actual_locations] = transfer_client->SaveKvCaches(locations, source_buffers);
    ASSERT_EQ(ER_OK, save_ec);
    EXPECT_EQ(locations, actual_locations);

    std::vector<char> first_loaded_1(2);
    std::vector<char> first_loaded_2(5);
    std::vector<char> second_loaded_1(5);
    std::vector<char> second_loaded_2(2);
    BlockBuffers destination_buffers{
        BlockBuffer{{Iov{MemoryType::CPU, first_loaded_1.data(), first_loaded_1.size(), false},
                     Iov{MemoryType::CPU, first_loaded_2.data(), first_loaded_2.size(), false}}},
        BlockBuffer{{Iov{MemoryType::CPU, second_loaded_1.data(), second_loaded_1.size(), false},
                     Iov{MemoryType::CPU, second_loaded_2.data(), second_loaded_2.size(), false}}},
        BlockBuffer{},
    };
    ASSERT_EQ(ER_OK, transfer_client->LoadKvCaches(actual_locations, destination_buffers));
    EXPECT_EQ(Flatten(source_buffers[0]), Flatten(destination_buffers[0]));
    EXPECT_EQ(Flatten(source_buffers[1]), Flatten(destination_buffers[1]));
    EXPECT_TRUE(Flatten(destination_buffers[2]).empty());

    EXPECT_EQ(std::vector<ErrorCode>(storage_uris.size(), EC_OK),
              storage_manager.Delete(&request_context, kStorageName, storage_uris, []() {}));
    EXPECT_EQ(std::vector<bool>(storage_uris.size(), false), storage_manager.Exist(kStorageName, storage_uris, false));
    EXPECT_EQ(EC_OK, storage_manager.DoCleanup());
    cleanup_guard.Release();
}

constexpr char kSecureStorageName[] = "secure-transfer-real";

class ValkeyTransferClientSecureRealServiceTest : public TESTBASE {
public:
    ValkeyTransferClientSecureRealServiceTest() : server_(kSecureStorageName, 15) {}

protected:
    test::SecureValkeyTestServer server_;
};

TEST_F(ValkeyTransferClientSecureRealServiceTest, PublicTransferClientRoundTripsBinaryCpuBuffers) {
    ASSERT_TRUE(server_.StartTls());
    proto::admin::StorageConfig proto_config;
    proto_config.set_global_unique_name(kSecureStorageName);
    proto_config.set_check_storage_available_when_open(true);
    proto_config.mutable_valkey()->set_endpoint(server_.Endpoint());
    proto_config.mutable_valkey()->set_security_dir(server_.security_dir());

    StorageConfig storage_config;
    ProtoConvert::StorageFromProto(&proto_config, storage_config);
    ASSERT_EQ(DataStorageType::DATA_STORAGE_TYPE_VALKEY, storage_config.type());
    std::string invalid_fields;
    ASSERT_TRUE(storage_config.ValidateRequiredFields(invalid_fields)) << invalid_fields;
    const std::string serialized_config = storage_config.ToJsonString();
    EXPECT_NE(std::string::npos, serialized_config.find(server_.security_dir()));
    EXPECT_EQ(std::string::npos, serialized_config.find("username"));
    EXPECT_EQ(std::string::npos, serialized_config.find("password"));

    auto metrics_registry = std::make_shared<MetricsRegistry>();
    DataStorageManager storage_manager(metrics_registry);
    RequestContext request_context("secure-transfer-real-service");
    ASSERT_EQ(EC_OK, storage_manager.RegisterStorage(&request_context, kSecureStorageName, storage_config));

    auto binary_locations =
        storage_manager.Create(&request_context, kSecureStorageName, {"first", "second"}, 7, []() {});
    auto empty_locations = storage_manager.Create(&request_context, kSecureStorageName, {"empty"}, 0, []() {});
    ASSERT_EQ(2u, binary_locations.size());
    ASSERT_EQ(1u, empty_locations.size());

    std::vector<DataStorageUri> storage_uris;
    UriStrVec locations;
    for (const auto &item : binary_locations) {
        ASSERT_EQ(EC_OK, item.first);
        storage_uris.push_back(item.second);
        locations.push_back(item.second.ToUriString());
    }
    ASSERT_EQ(EC_OK, empty_locations.front().first);
    storage_uris.push_back(empty_locations.front().second);
    locations.push_back(empty_locations.front().second.ToUriString());
    ASSERT_EQ(3u, locations.size());
    for (const std::string &location : locations) {
        EXPECT_EQ(0u, location.find(std::string("valkey://") + kSecureStorageName + "/"));
        EXPECT_EQ(std::string::npos, location.find("valkeys://"));
        EXPECT_EQ(std::string::npos, location.find(server_.security_dir()));
    }

    InitParams init_params;
    init_params.role_type = RoleType::WORKER;
    init_params.self_location_spec_name = "tp0";
    init_params.storage_configs = "[" + serialized_config + "]";
    auto transfer_client = TransferClient::Create(ClientConfigJson(), init_params);
    ASSERT_NE(nullptr, transfer_client);

    std::string first_part_1({'a', 'b', '\0'});
    std::string first_part_2({'c', 'd', static_cast<char>(0xff), static_cast<char>(0x80)});
    std::string second_part_1({'\0', '1', '2', '3'});
    std::string second_part_2({static_cast<char>(0xfe), '4', '\0'});
    BlockBuffers source_buffers{
        BlockBuffer{{Iov{MemoryType::CPU, first_part_1.data(), first_part_1.size(), false},
                     Iov{MemoryType::CPU, first_part_2.data(), first_part_2.size(), false}}},
        BlockBuffer{{Iov{MemoryType::CPU, second_part_1.data(), second_part_1.size(), false},
                     Iov{MemoryType::CPU, second_part_2.data(), second_part_2.size(), false}}},
        BlockBuffer{},
    };

    const auto [save_ec, actual_locations] = transfer_client->SaveKvCaches(locations, source_buffers);
    ASSERT_EQ(ER_OK, save_ec);
    EXPECT_EQ(locations, actual_locations);

    std::vector<char> first_loaded_1(2);
    std::vector<char> first_loaded_2(5);
    std::vector<char> second_loaded_1(5);
    std::vector<char> second_loaded_2(2);
    BlockBuffers destination_buffers{
        BlockBuffer{{Iov{MemoryType::CPU, first_loaded_1.data(), first_loaded_1.size(), false},
                     Iov{MemoryType::CPU, first_loaded_2.data(), first_loaded_2.size(), false}}},
        BlockBuffer{{Iov{MemoryType::CPU, second_loaded_1.data(), second_loaded_1.size(), false},
                     Iov{MemoryType::CPU, second_loaded_2.data(), second_loaded_2.size(), false}}},
        BlockBuffer{},
    };
    ASSERT_EQ(ER_OK, transfer_client->LoadKvCaches(actual_locations, destination_buffers));
    EXPECT_EQ(Flatten(source_buffers[0]), Flatten(destination_buffers[0]));
    EXPECT_EQ(Flatten(source_buffers[1]), Flatten(destination_buffers[1]));
    EXPECT_TRUE(Flatten(destination_buffers[2]).empty());

    EXPECT_EQ(std::vector<ErrorCode>(storage_uris.size(), EC_OK),
              storage_manager.Delete(&request_context, kSecureStorageName, storage_uris, []() {}));
    EXPECT_EQ(std::vector<bool>(storage_uris.size(), false),
              storage_manager.Exist(kSecureStorageName, storage_uris, false));
    EXPECT_EQ(EC_OK, storage_manager.DoCleanup());
}

} // namespace
} // namespace kv_cache_manager
