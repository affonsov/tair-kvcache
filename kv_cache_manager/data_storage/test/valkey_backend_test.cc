#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/valkey_backend.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

namespace kv_cache_manager {
namespace {

struct FakeRedisState {
    bool open_success = true;
    bool reply_count_mismatch = false;
    std::size_t io_calls = 0;
    std::size_t delete_calls = 0;
    std::size_t exists_calls = 0;
    std::vector<int64_t> delete_replies;
    std::vector<int64_t> exists_replies;
    std::vector<std::vector<std::string>> last_commands;
};

RedisClient::ReplyUPtr MakeIntegerReply(int64_t value) {
    auto *reply = static_cast<redisReply *>(std::calloc(1, sizeof(redisReply)));
    reply->type = REDIS_REPLY_INTEGER;
    reply->integer = value;
    return {reply, freeReplyObject};
}

RedisClient::ReplyUPtr MakeErrorReply() {
    auto *reply = static_cast<redisReply *>(std::calloc(1, sizeof(redisReply)));
    reply->type = REDIS_REPLY_ERROR;
    constexpr char kError[] = "ERR injected";
    reply->len = sizeof(kError) - 1;
    reply->str = static_cast<char *>(std::malloc(sizeof(kError)));
    std::memcpy(reply->str, kError, sizeof(kError));
    return {reply, freeReplyObject};
}

class FakeRedisClient : public RedisClient {
public:
    FakeRedisClient(const StandardUri &endpoint, std::shared_ptr<FakeRedisState> state)
        : RedisClient(endpoint), state_(std::move(state)) {}

protected:
    bool IsContextOk() const override { return true; }
    bool Reconnect() override { return state_->open_success; }

    std::vector<ReplyUPtr> TryExecPipeline(const std::vector<CmdArgs> &commands) override {
        ++state_->io_calls;
        state_->last_commands = commands;
        const bool is_delete = !commands.empty() && commands.front().front() == "DEL";
        const bool is_exists = !commands.empty() && commands.front().front() == "EXISTS";
        if (is_delete) {
            ++state_->delete_calls;
        }
        if (is_exists) {
            ++state_->exists_calls;
        }
        const std::vector<int64_t> configured = is_delete ? state_->delete_replies : state_->exists_replies;
        std::vector<ReplyUPtr> replies;
        replies.reserve(commands.size());
        for (std::size_t i = 0; i < commands.size(); ++i) {
            const int64_t value = i < configured.size() ? configured[i] : 1;
            replies.emplace_back(value < 0 ? MakeErrorReply() : MakeIntegerReply(value));
        }
        if (state_->reply_count_mismatch && !replies.empty()) {
            replies.pop_back();
        }
        return replies;
    }

private:
    std::shared_ptr<FakeRedisState> state_;
};

class TestValkeyBackend : public ValkeyDataStorageBackend {
public:
    TestValkeyBackend(std::shared_ptr<MetricsRegistry> metrics_registry, std::shared_ptr<FakeRedisState> state)
        : ValkeyDataStorageBackend(std::move(metrics_registry)), state_(std::move(state)) {}

    void set_fail_client_construction(bool fail) { fail_client_construction_ = fail; }
    void set_opaque_ids(std::vector<std::string> opaque_ids) { opaque_ids_ = std::move(opaque_ids); }
    std::size_t opaque_id_calls() const { return opaque_id_calls_; }
    const std::vector<std::string> &constructed_endpoints() const { return constructed_endpoints_; }

protected:
    std::shared_ptr<RedisClient> CreateClient(const StandardUri &endpoint) override {
        constructed_endpoints_.push_back(endpoint.ToUriString());
        if (fail_client_construction_) {
            return nullptr;
        }
        return std::make_shared<FakeRedisClient>(endpoint, state_);
    }

    std::string GenerateOpaqueId() override {
        ++opaque_id_calls_;
        if (next_opaque_id_ < opaque_ids_.size()) {
            return opaque_ids_[next_opaque_id_++];
        }
        return "ffffffffffffffffffffffffffffffff";
    }

private:
    std::shared_ptr<FakeRedisState> state_;
    bool fail_client_construction_ = false;
    std::size_t next_opaque_id_ = 0;
    std::size_t opaque_id_calls_ = 0;
    std::vector<std::string> opaque_ids_;
    std::vector<std::string> constructed_endpoints_;
};

class RealOpaqueIdValkeyBackend : public ValkeyDataStorageBackend {
public:
    using ValkeyDataStorageBackend::ValkeyDataStorageBackend;

    std::string CallGenerateOpaqueId() { return ValkeyDataStorageBackend::GenerateOpaqueId(); }
};

StorageConfig MakeValkeyConfig(const std::string &identity = "valkey-primary",
                               const std::string &endpoint = "valkey://cache.internal:6379/?db=11") {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint(endpoint);
    return StorageConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY, identity, spec);
}

DataStorageUri MakeLocation(const std::string &identity, const std::string &opaque_id, std::size_t size = 128) {
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation(identity, opaque_id, size, uri));
    return uri;
}

class ValkeyBackendTest : public TESTBASE {
public:
    void SetUp() override {
        metrics_registry_ = std::make_shared<MetricsRegistry>();
        state_ = std::make_shared<FakeRedisState>();
    }

protected:
    std::shared_ptr<MetricsRegistry> metrics_registry_;
    std::shared_ptr<FakeRedisState> state_;
};

TEST_F(ValkeyBackendTest, LocationUriAndRedisKeyAreStrictAndNamespaced) {
    const std::string opaque_id = "0123456789abcdef0123456789abcdef";
    DataStorageUri uri = MakeLocation("storage-a", opaque_id, 4096);
    EXPECT_EQ("valkey://storage-a/0123456789abcdef0123456789abcdef?size=4096", uri.ToUriString());
    EXPECT_TRUE(ValidateValkeyLocation(uri, "storage-a"));
    EXPECT_FALSE(ValidateValkeyLocation(uri, "storage-b"));

    ValkeyStorageSpec endpoint_spec;
    endpoint_spec.set_endpoint("valkey://storage-a:6379/");
    std::string invalid_fields;
    EXPECT_TRUE(endpoint_spec.ValidateRequiredFields(invalid_fields));
    EXPECT_FALSE(ValidateValkeyLocation(DataStorageUri(endpoint_spec.endpoint()), "storage-a"));

    ValkeyStorageSpec location_as_endpoint;
    location_as_endpoint.set_endpoint(uri.ToUriString());
    invalid_fields.clear();
    EXPECT_FALSE(location_as_endpoint.ValidateRequiredFields(invalid_fields));

    std::string redis_key;
    ASSERT_TRUE(ValkeyLocationToRedisKey(uri, "storage-a", redis_key));
    EXPECT_EQ("kvcm:valkey:9:storage-a:0123456789abcdef0123456789abcdef", redis_key);
    EXPECT_EQ(std::string::npos, uri.ToUriString().find("cache.internal"));
    EXPECT_EQ(std::string::npos, uri.ToUriString().find("db="));

    for (const std::string &invalid : {
             "redis://storage-a/0123456789abcdef0123456789abcdef?size=1",
             "valkey://storage-b/0123456789abcdef0123456789abcdef?size=1",
             "valkey:///0123456789abcdef0123456789abcdef?size=1",
             "valkey://user@storage-a/0123456789abcdef0123456789abcdef?size=1",
             "valkey://storage-a:6379/0123456789abcdef0123456789abcdef?size=1",
             "valkey://storage-a/?size=1",
             "valkey://storage-a/0123456789abcdef0123456789abcdef/extra?size=1",
             "valkey://storage-a/0123456789ABCDEF0123456789ABCDEF?size=1",
             "valkey://storage-a/0123456789abcdef0123456789abcdef",
             "valkey://storage-a/0123456789abcdef0123456789abcdef?size=-1",
             "valkey://storage-a/0123456789abcdef0123456789abcdef?size=+1",
             "valkey://storage-a/0123456789abcdef0123456789abcdef?size=0001",
             "valkey://storage-a/0123456789abcdef0123456789abcdef?size=bad",
             "valkey://storage-a/0123456789abcdef0123456789abcdef?extra=1&size=1",
         }) {
        DataStorageUri invalid_uri(invalid);
        EXPECT_FALSE(ValidateValkeyLocation(invalid_uri, "storage-a")) << invalid;
        EXPECT_FALSE(ValkeyLocationToRedisKey(invalid_uri, "storage-a", redis_key)) << invalid;
        EXPECT_TRUE(redis_key.empty());
    }
}

TEST_F(ValkeyBackendTest, GeneratedOpaqueIdIsAcceptedLowercaseHex) {
    RealOpaqueIdValkeyBackend backend(metrics_registry_);
    const std::string opaque_id = backend.CallGenerateOpaqueId();

    ASSERT_EQ(32u, opaque_id.size());
    EXPECT_TRUE(std::all_of(opaque_id.begin(), opaque_id.end(), [](const char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    }));
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation("valkey-primary", opaque_id, 128, uri));
    EXPECT_TRUE(ValidateValkeyLocation(uri, "valkey-primary"));
}

TEST_F(ValkeyBackendTest, InheritedOpenInitializesAndCloseClearsAvailability) {
    TestValkeyBackend backend(metrics_registry_, state_);
    EXPECT_EQ(DataStorageType::DATA_STORAGE_TYPE_VALKEY, backend.GetType());
    EXPECT_DOUBLE_EQ(0.0, backend.GetStorageUsageRatio("trace"));
    EXPECT_FALSE(backend.Available());

    DataStorageBackend *base = &backend;
    const StorageConfig config = MakeValkeyConfig();
    EXPECT_EQ(EC_OK, base->Open(config, "trace"));
    EXPECT_TRUE(backend.Available());
    EXPECT_EQ("valkey-primary", backend.GetStorageConfig().global_unique_name());
    EXPECT_NE(nullptr, backend.GetMetricsCollector());
    ASSERT_EQ(4u, backend.constructed_endpoints().size());
    for (const std::string &endpoint : backend.constructed_endpoints()) {
        EXPECT_EQ("valkey://cache.internal:6379/?db=11", endpoint);
    }
    EXPECT_EQ(EC_OK, backend.Close());
    EXPECT_FALSE(backend.Available());
}

TEST_F(ValkeyBackendTest, OpenRejectsWrongConfigAndPoolInitializationFailures) {
    auto nfs_spec = std::make_shared<NfsStorageSpec>();
    nfs_spec->set_root_path("/tmp");
    TestValkeyBackend backend(metrics_registry_, state_);
    EXPECT_EQ(
        EC_BADARGS,
        backend.Open(StorageConfig(DataStorageType::DATA_STORAGE_TYPE_VALKEY, "valkey-primary", nfs_spec), "trace"));
    EXPECT_FALSE(backend.Available());

    auto valkey_spec = std::make_shared<ValkeyStorageSpec>();
    valkey_spec->set_endpoint("redis://cache.internal:6379/");
    EXPECT_EQ(
        EC_BADARGS,
        backend.Open(StorageConfig(DataStorageType::DATA_STORAGE_TYPE_NFS, "valkey-primary", valkey_spec), "trace"));
    EXPECT_EQ(EC_BADARGS, backend.Open(MakeValkeyConfig("bad/identity"), "trace"));
    EXPECT_EQ(EC_BADARGS, backend.Open(MakeValkeyConfig("valkey-primary", "rediss://cache.internal:6379/"), "trace"));

    TestValkeyBackend compatibility_alias(metrics_registry_, state_);
    ASSERT_EQ(
        EC_OK,
        compatibility_alias.Open(MakeValkeyConfig("valkey-primary", "redis://cache.internal:6379/?db=11"), "trace"));
    ASSERT_EQ(4u, compatibility_alias.constructed_endpoints().size());
    for (const std::string &endpoint : compatibility_alias.constructed_endpoints()) {
        EXPECT_EQ("redis://cache.internal:6379/?db=11", endpoint);
    }
    EXPECT_EQ(EC_OK, compatibility_alias.Close());

    TestValkeyBackend construction_failure(metrics_registry_, state_);
    construction_failure.set_fail_client_construction(true);
    EXPECT_EQ(EC_ERROR, construction_failure.Open(MakeValkeyConfig(), "trace"));
    EXPECT_FALSE(construction_failure.Available());

    state_->open_success = false;
    TestValkeyBackend open_failure(metrics_registry_, state_);
    EXPECT_EQ(EC_ERROR, open_failure.Open(MakeValkeyConfig(), "trace"));
    EXPECT_FALSE(open_failure.Available());
}

TEST_F(ValkeyBackendTest, CreateReturnsUniqueSecretFreeLocationsWithoutRedisIo) {
    TestValkeyBackend backend(metrics_registry_, state_);
    backend.set_opaque_ids({"00000000000000000000000000000001",
                            "00000000000000000000000000000002",
                            "00000000000000000000000000000001",
                            "00000000000000000000000000000003"});
    ASSERT_EQ(EC_OK, backend.Open(MakeValkeyConfig(), "trace"));

    int callback_count = 0;
    const auto result = backend.Create({"duplicate", "duplicate", "other"}, 512, "trace", [&] { ++callback_count; });
    ASSERT_EQ(3u, result.size());
    EXPECT_EQ(1, callback_count);
    EXPECT_EQ(0u, state_->io_calls);
    EXPECT_EQ(EC_OK, result[0].first);
    EXPECT_EQ(EC_OK, result[1].first);
    EXPECT_EQ(EC_OK, result[2].first);
    EXPECT_NE(result[0].second.ToUriString(), result[1].second.ToUriString());
    EXPECT_NE(result[1].second.ToUriString(), result[2].second.ToUriString());
    std::unordered_set<std::string> unique_locations;
    for (const auto &item : result) {
        const DataStorageUri &uri = item.second;
        unique_locations.insert(uri.ToUriString());
        EXPECT_TRUE(ValidateValkeyLocation(uri, "valkey-primary"));
        EXPECT_EQ(std::string::npos, uri.ToUriString().find("cache.internal"));
        EXPECT_EQ(std::string::npos, uri.ToUriString().find("6379"));
        EXPECT_EQ(std::string::npos, uri.ToUriString().find("db="));
    }
    EXPECT_EQ(result.size(), unique_locations.size());

    backend.SetAvailable(false);
    const auto unavailable = backend.Create({"a"}, 1, "trace", [&] { ++callback_count; });
    ASSERT_EQ(1u, unavailable.size());
    EXPECT_EQ(EC_ERROR, unavailable[0].first);
    EXPECT_FALSE(unavailable[0].second.Valid());
    backend.SetAvailable(true);

    EXPECT_TRUE(backend.Create({}, 512, "trace", [&] { ++callback_count; }).empty());
    EXPECT_EQ(3, callback_count);
    ASSERT_EQ(EC_OK, backend.Close());
    const auto closed = backend.Create({"a", "b"}, 1, "trace", [&] { ++callback_count; });
    EXPECT_EQ(4, callback_count);
    ASSERT_EQ(2u, closed.size());
    EXPECT_EQ(EC_ERROR, closed[0].first);
    EXPECT_EQ(EC_ERROR, closed[1].first);
    EXPECT_FALSE(closed[0].second.Valid());
    EXPECT_FALSE(closed[1].second.Valid());
    EXPECT_EQ(0u, state_->io_calls);
}

TEST_F(ValkeyBackendTest, CreateExhaustsRepeatedOpaqueIdWithoutRedisIo) {
    TestValkeyBackend backend(metrics_registry_, state_);
    ASSERT_EQ(EC_OK, backend.Open(MakeValkeyConfig(), "trace"));

    int callback_count = 0;
    const auto result = backend.Create({"first", "second"}, 512, "trace", [&] { ++callback_count; });

    ASSERT_EQ(2u, result.size());
    EXPECT_EQ(EC_OK, result[0].first);
    EXPECT_EQ("valkey://valkey-primary/ffffffffffffffffffffffffffffffff?size=512", result[0].second.ToUriString());
    EXPECT_TRUE(result[0].second.Valid());
    EXPECT_EQ(EC_ERROR, result[1].first);
    EXPECT_FALSE(result[1].second.Valid());
    EXPECT_EQ(33u, backend.opaque_id_calls());
    EXPECT_EQ(1, callback_count);
    EXPECT_EQ(0u, state_->io_calls);
}

TEST_F(ValkeyBackendTest, DeletePreservesMixedAlignmentAndMapsMissingToSuccess) {
    TestValkeyBackend backend(metrics_registry_, state_);
    ASSERT_EQ(EC_OK, backend.Open(MakeValkeyConfig(), "trace"));
    const DataStorageUri first = MakeLocation("valkey-primary", "00000000000000000000000000000001");
    const DataStorageUri second = MakeLocation("valkey-primary", "00000000000000000000000000000002");
    const DataStorageUri third = MakeLocation("valkey-primary", "00000000000000000000000000000003");
    const DataStorageUri malformed("valkey://valkey-primary/not-hex?size=1");
    const DataStorageUri cross = MakeLocation("other-storage", "00000000000000000000000000000004");
    state_->delete_replies = {1, 0, -1};

    int callback_count = 0;
    EXPECT_EQ((std::vector<ErrorCode>{EC_OK, EC_BADARGS, EC_OK, EC_ERROR, EC_BADARGS}),
              backend.Delete({first, malformed, second, third, cross}, "trace", [&] { ++callback_count; }));
    EXPECT_EQ(1, callback_count);
    EXPECT_EQ(1u, state_->delete_calls);
    ASSERT_EQ(3u, state_->last_commands.size());
    EXPECT_EQ("kvcm:valkey:14:valkey-primary:00000000000000000000000000000001", state_->last_commands[0][1]);
    EXPECT_EQ("kvcm:valkey:14:valkey-primary:00000000000000000000000000000002", state_->last_commands[1][1]);
    EXPECT_EQ("kvcm:valkey:14:valkey-primary:00000000000000000000000000000003", state_->last_commands[2][1]);

    EXPECT_TRUE(backend.Delete({}, "trace", [&] { ++callback_count; }).empty());
    EXPECT_EQ(2, callback_count);
    EXPECT_EQ(1u, state_->delete_calls);

    ASSERT_EQ(EC_OK, backend.Close());
    EXPECT_EQ((std::vector<ErrorCode>{EC_ERROR, EC_BADARGS}),
              backend.Delete({first, malformed}, "trace", [&] { ++callback_count; }));
    EXPECT_EQ(3, callback_count);
    EXPECT_EQ(1u, state_->delete_calls);
}

TEST_F(ValkeyBackendTest, ExistUsesActualRepliesAndConservativeFallbacks) {
    TestValkeyBackend backend(metrics_registry_, state_);
    ASSERT_EQ(EC_OK, backend.Open(MakeValkeyConfig(), "trace"));
    const DataStorageUri first = MakeLocation("valkey-primary", "00000000000000000000000000000001");
    const DataStorageUri second = MakeLocation("valkey-primary", "00000000000000000000000000000002");
    const DataStorageUri third = MakeLocation("valkey-primary", "00000000000000000000000000000003");
    const DataStorageUri malformed("bad");
    const DataStorageUri cross = MakeLocation("other-storage", "00000000000000000000000000000004");
    state_->exists_replies = {1, 0, -1};

    EXPECT_EQ((std::vector<bool>{true, false, true, true, true}),
              backend.Exist({first, second, third, malformed, cross}));
    EXPECT_EQ(1u, state_->exists_calls);

    state_->reply_count_mismatch = true;
    EXPECT_EQ((std::vector<bool>{true, true}), backend.Exist({first, second}));
    EXPECT_EQ(2u, state_->exists_calls);
    state_->reply_count_mismatch = false;

    EXPECT_TRUE(backend.Exist({}).empty());
    EXPECT_EQ(2u, state_->exists_calls);
    ASSERT_EQ(EC_OK, backend.Close());
    EXPECT_EQ((std::vector<bool>{true, true}), backend.Exist({first, malformed}));
    EXPECT_EQ(2u, state_->exists_calls);
}

TEST_F(ValkeyBackendTest, MightExistLockUnlockAndCopyNeverUseRedis) {
    TestValkeyBackend backend(metrics_registry_, state_);
    const DataStorageUri malformed("bad");
    const std::vector<DataStorageUri> uris(3, malformed);
    EXPECT_EQ(std::vector<bool>(3, true), backend.MightExist(uris));
    EXPECT_EQ(std::vector<ErrorCode>(3, EC_OK), backend.Lock(uris));
    EXPECT_EQ(std::vector<ErrorCode>(3, EC_OK), backend.UnLock(uris));
    EXPECT_EQ(std::vector<ErrorCode>(3, EC_BADARGS), backend.Copy(uris, {malformed}, "trace"));
    EXPECT_EQ(std::vector<ErrorCode>(3, EC_UNIMPLEMENTED), backend.Copy(uris, uris, "trace"));
    EXPECT_TRUE(backend.MightExist({}).empty());
    EXPECT_TRUE(backend.Lock({}).empty());
    EXPECT_TRUE(backend.UnLock({}).empty());
    EXPECT_TRUE(backend.Copy({}, {}, "trace").empty());
    EXPECT_EQ(0u, state_->io_calls);
}

} // namespace
} // namespace kv_cache_manager
