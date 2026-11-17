#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/common/test/secure_valkey_test_environment.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/valkey_backend.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

namespace kv_cache_manager {
namespace {

class ValkeyBackendRealServiceTest : public TESTBASE {
public:
    ValkeyBackendRealServiceTest() : server_("vlk03-primary") {}

protected:
    // Plaintext valkey:// endpoints cannot carry credentials, so this suite
    // starts its own unauthenticated loopback server instead of the shared one.
    std::string ValkeyEndpoint(int64_t db) const { return server_.Endpoint("127.0.0.1", false, db); }

    StorageConfig RealValkeyConfig(const std::string &identity, int64_t db) const {
        auto spec = std::make_shared<ValkeyStorageSpec>();
        spec->set_endpoint(ValkeyEndpoint(db));
        return StorageConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY, identity, spec);
    }

    std::unique_ptr<RedisClient> OpenFixtureClient(int64_t db) const {
        auto client = std::make_unique<RedisClient>(StandardUri::FromUri(ValkeyEndpoint(db)));
        if (!client->Open()) {
            return nullptr;
        }
        return client;
    }

    test::SecureValkeyTestServer server_;
};

TEST_F(ValkeyBackendRealServiceTest, ManagesLivenessDeletionAndDbStorageIsolation) {
    ASSERT_TRUE(server_.StartPlaintextWithoutAuthentication());
    auto metrics_registry = std::make_shared<MetricsRegistry>();
    ValkeyDataStorageBackend backend(metrics_registry);
    ASSERT_EQ(EC_OK, backend.Open(RealValkeyConfig("vlk03-primary", 11), "real-service"));

    const auto created = backend.Create({"duplicate", "duplicate"}, 256, "real-service", nullptr);
    ASSERT_EQ(2u, created.size());
    ASSERT_EQ(EC_OK, created[0].first);
    ASSERT_EQ(EC_OK, created[1].first);
    EXPECT_NE(created[0].second.ToUriString(), created[1].second.ToUriString());
    EXPECT_EQ((std::vector<bool>{false, false}), backend.Exist({created[0].second, created[1].second}));

    std::vector<std::string> created_keys;
    for (const auto &item : created) {
        std::string key;
        ASSERT_TRUE(ValkeyLocationToRedisKey(item.second, "vlk03-primary", key));
        created_keys.push_back(std::move(key));
    }
    auto db11_client = OpenFixtureClient(11);
    ASSERT_NE(nullptr, db11_client);
    std::string binary_value("binary\0value\xff", 13);
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              db11_client->SetStrings(created_keys, {binary_value, std::string()}));
    EXPECT_EQ((std::vector<bool>{true, true}), backend.Exist({created[0].second, created[1].second}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              backend.Delete({created[0].second, created[1].second}, "real-service", nullptr));
    EXPECT_EQ((std::vector<bool>{false, false}), backend.Exist({created[0].second, created[1].second}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              backend.Delete({created[0].second, created[1].second}, "real-service", nullptr));

    const std::string opaque_id = "abcdef0123456789abcdef0123456789";
    DataStorageUri primary_uri;
    DataStorageUri isolated_uri;
    ASSERT_TRUE(BuildValkeyLocation("vlk03-primary", opaque_id, 1, primary_uri));
    ASSERT_TRUE(BuildValkeyLocation("vlk03-isolated", opaque_id, 1, isolated_uri));
    std::string primary_key;
    ASSERT_TRUE(ValkeyLocationToRedisKey(primary_uri, "vlk03-primary", primary_key));
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK}), db11_client->SetStrings({primary_key}, {"identity"}));

    ValkeyDataStorageBackend isolated_backend(metrics_registry);
    ASSERT_EQ(EC_OK, isolated_backend.Open(RealValkeyConfig("vlk03-isolated", 11), "real-service"));
    EXPECT_EQ((std::vector<bool>{true}), backend.Exist({primary_uri}));
    EXPECT_EQ((std::vector<bool>{false}), isolated_backend.Exist({isolated_uri}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), backend.Delete({primary_uri}, "real-service", nullptr));

    auto db12_client = OpenFixtureClient(12);
    ASSERT_NE(nullptr, db12_client);
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK}), db12_client->SetStrings({primary_key}, {"other-db"}));
    EXPECT_EQ((std::vector<bool>{false}), backend.Exist({primary_uri}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK}), db12_client->Delete({primary_key}));

    db12_client->Close();
    db11_client->Close();
    EXPECT_EQ(EC_OK, isolated_backend.Close());
    EXPECT_EQ(EC_OK, backend.Close());
}

constexpr char kSecureStorageIdentity[] = "secure-valkey-backend";

StorageConfig SecureValkeyConfig(const test::SecureValkeyTestServer &server) {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint(server.Endpoint());
    spec->set_security_dir(server.security_dir());
    return StorageConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY, kSecureStorageIdentity, spec);
}

RedisConnectionOptions SecureConnectionOptions(const test::SecureValkeyTestServer &server) {
    RedisConnectionOptions options;
    options.endpoint = StandardUri::FromUri(server.Endpoint());
    options.security.transport = RedisTransportSecurity::TLS_NAMED_ACL;
    options.security.directory = server.security_dir();
    return options;
}

class ValkeyBackendSecureRealServiceTest : public TESTBASE {
public:
    ValkeyBackendSecureRealServiceTest() : server_(kSecureStorageIdentity) {}

protected:
    test::SecureValkeyTestServer server_;
};

TEST_F(ValkeyBackendSecureRealServiceTest, ManagesLivenessBinaryValuesAndDeletion) {
    ASSERT_TRUE(server_.StartTls());
    auto metrics_registry = std::make_shared<MetricsRegistry>();
    ValkeyDataStorageBackend backend(metrics_registry);
    ASSERT_EQ(EC_OK, backend.Open(SecureValkeyConfig(server_), "real-service"));

    const auto created = backend.Create({"duplicate", "duplicate"}, 256, "real-service", nullptr);
    ASSERT_EQ(2u, created.size());
    ASSERT_EQ(EC_OK, created[0].first);
    ASSERT_EQ(EC_OK, created[1].first);
    EXPECT_NE(created[0].second.ToUriString(), created[1].second.ToUriString());
    EXPECT_EQ(0u, created[0].second.ToUriString().find(std::string("valkey://") + kSecureStorageIdentity + "/"));
    EXPECT_EQ((std::vector<bool>{false, false}), backend.Exist({created[0].second, created[1].second}));

    std::vector<std::string> created_keys;
    for (const auto &item : created) {
        std::string key;
        ASSERT_TRUE(ValkeyLocationToRedisKey(item.second, kSecureStorageIdentity, key));
        created_keys.push_back(std::move(key));
    }
    RedisClient fixture_client(SecureConnectionOptions(server_));
    ASSERT_TRUE(fixture_client.Open());
    const std::string binary_value("binary\0value\xff", 13);
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              fixture_client.SetStrings(created_keys, {binary_value, std::string()}));
    EXPECT_EQ((std::vector<bool>{true, true}), backend.Exist({created[0].second, created[1].second}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              backend.Delete({created[0].second, created[1].second}, "real-service", nullptr));
    EXPECT_EQ((std::vector<bool>{false, false}), backend.Exist({created[0].second, created[1].second}));
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK}),
              backend.Delete({created[0].second, created[1].second}, "real-service", nullptr));

    fixture_client.Close();
    EXPECT_EQ(EC_OK, backend.Close());
}

} // namespace
} // namespace kv_cache_manager
