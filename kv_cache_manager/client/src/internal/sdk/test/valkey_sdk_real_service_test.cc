#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"
#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/common/test/secure_valkey_test_environment.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {
namespace {

std::string ValkeyEndpoint() {
    const char *configured_host = std::getenv("KVCM_TEST_REDIS_HOST");
    const std::string host = configured_host == nullptr ? "127.0.0.1" : configured_host;
    return "valkey://" + host + ":6379/?db=13";
}

std::shared_ptr<SdkBackendConfig> RealSdkConfig() {
    auto config = std::make_shared<SdkBackendConfig>();
    config->set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    return config;
}

std::shared_ptr<StorageConfig> RealStorageConfig() {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint(ValkeyEndpoint());
    return std::make_shared<StorageConfig>(DataStorageType::DATA_STORAGE_TYPE_VALKEY, "vlk04-real", spec);
}

DataStorageUri RealLocation(const std::string &opaque_id, std::size_t size) {
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation("vlk04-real", opaque_id, size, uri));
    return uri;
}

class RedisKeyCleanupGuard {
public:
    RedisKeyCleanupGuard(RedisClient &client, const std::vector<std::string> &keys) : client_(client), keys_(keys) {}

    ~RedisKeyCleanupGuard() noexcept {
        try {
            (void)client_.Delete(keys_);
        } catch (...) {}
    }

private:
    RedisClient &client_;
    const std::vector<std::string> &keys_;
};

class ValkeySdkRealServiceTest : public TESTBASE {};

TEST_F(ValkeySdkRealServiceTest, MultiSegmentBinaryRoundTrip) {
    RedisClient cleanup_client(StandardUri::FromUri(ValkeyEndpoint()));
    ASSERT_TRUE(cleanup_client.Open());

    const std::vector<DataStorageUri> uris{
        RealLocation("40000000000000000000000000000001", 7),
        RealLocation("40000000000000000000000000000002", 0),
    };
    std::vector<std::string> redis_keys;
    for (const auto &uri : uris) {
        std::string key;
        ASSERT_TRUE(ValkeyLocationToRedisKey(uri, "vlk04-real", key));
        redis_keys.push_back(std::move(key));
    }
    RedisKeyCleanupGuard cleanup_guard(cleanup_client, redis_keys);
    (void)cleanup_client.Delete(redis_keys);

    ValkeySdk sdk;
    ASSERT_EQ(ER_OK, sdk.Init(RealSdkConfig(), RealStorageConfig()));
    std::string put_first("ab\0", 3);
    std::string put_second("cd\xff\x80", 4);
    BlockBuffer put_binary;
    put_binary.iovs = {
        Iov{MemoryType::CPU, put_first.data(), put_first.size(), false},
        Iov{MemoryType::CPU, put_second.data(), put_second.size(), false},
    };
    auto actual = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_OK, sdk.Put(uris, {put_binary, BlockBuffer{}}, actual));
    ASSERT_EQ(uris.size(), actual->size());

    char get_first[2] = {};
    char get_second[5] = {};
    BlockBuffer get_binary;
    get_binary.iovs = {
        Iov{MemoryType::CPU, get_first, sizeof(get_first), false},
        Iov{MemoryType::CPU, get_second, sizeof(get_second), false},
    };
    ASSERT_EQ(ER_OK, sdk.Get(uris, {get_binary, BlockBuffer{}}));
    EXPECT_EQ(std::string("ab", 2), std::string(get_first, sizeof(get_first)));
    EXPECT_EQ(std::string("\0cd\xff\x80", 5), std::string(get_second, sizeof(get_second)));
}

constexpr char kSecureStorageIdentity[] = "secure-valkey-sdk";

std::shared_ptr<StorageConfig> SecureStorageConfig(const test::SecureValkeyTestServer &server) {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint(server.Endpoint());
    spec->set_security_dir(server.security_dir());
    return std::make_shared<StorageConfig>(DataStorageType::DATA_STORAGE_TYPE_VALKEY, kSecureStorageIdentity, spec);
}

RedisConnectionOptions SecureConnectionOptions(const test::SecureValkeyTestServer &server) {
    RedisConnectionOptions options;
    options.endpoint = StandardUri::FromUri(server.Endpoint());
    options.security.transport = RedisTransportSecurity::TLS_NAMED_ACL;
    options.security.directory = server.security_dir();
    return options;
}

DataStorageUri SecureLocation(const std::string &opaque_id, std::size_t size) {
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation(kSecureStorageIdentity, opaque_id, size, uri));
    return uri;
}

class ValkeySdkSecureRealServiceTest : public TESTBASE {
public:
    ValkeySdkSecureRealServiceTest() : server_(kSecureStorageIdentity, 13) {}

protected:
    test::SecureValkeyTestServer server_;
};

TEST_F(ValkeySdkSecureRealServiceTest, MultiSegmentBinaryAndEmptyValueRoundTrip) {
    ASSERT_TRUE(server_.StartTls());
    RedisClient cleanup_client(SecureConnectionOptions(server_));
    ASSERT_TRUE(cleanup_client.Open());

    const std::vector<DataStorageUri> uris{
        SecureLocation("40000000000000000000000000000001", 7),
        SecureLocation("40000000000000000000000000000002", 0),
    };
    std::vector<std::string> redis_keys;
    for (const auto &uri : uris) {
        std::string key;
        ASSERT_TRUE(ValkeyLocationToRedisKey(uri, kSecureStorageIdentity, key));
        redis_keys.push_back(std::move(key));
    }
    RedisKeyCleanupGuard cleanup_guard(cleanup_client, redis_keys);
    (void)cleanup_client.Delete(redis_keys);

    ValkeySdk sdk;
    ASSERT_EQ(ER_OK, sdk.Init(RealSdkConfig(), SecureStorageConfig(server_)));
    std::string put_first("ab\0", 3);
    std::string put_second("cd\xff\x80", 4);
    BlockBuffer put_binary;
    put_binary.iovs = {
        Iov{MemoryType::CPU, put_first.data(), put_first.size(), false},
        Iov{MemoryType::CPU, put_second.data(), put_second.size(), false},
    };
    auto actual = std::make_shared<std::vector<DataStorageUri>>();
    ASSERT_EQ(ER_OK, sdk.Put(uris, {put_binary, BlockBuffer{}}, actual));
    ASSERT_EQ(uris.size(), actual->size());

    char get_first[2] = {};
    char get_second[5] = {};
    BlockBuffer get_binary;
    get_binary.iovs = {
        Iov{MemoryType::CPU, get_first, sizeof(get_first), false},
        Iov{MemoryType::CPU, get_second, sizeof(get_second), false},
    };
    ASSERT_EQ(ER_OK, sdk.Get(uris, {get_binary, BlockBuffer{}}));
    EXPECT_EQ(std::string("ab", 2), std::string(get_first, sizeof(get_first)));
    EXPECT_EQ(std::string("\0cd\xff\x80", 5), std::string(get_second, sizeof(get_second)));
}

} // namespace
} // namespace kv_cache_manager
