#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"
#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {
namespace {

enum class ClientThrowMode {
    NONE,
    BAD_ALLOC,
    LENGTH_ERROR,
    RUNTIME_ERROR,
};

void ThrowIfConfigured(ClientThrowMode mode) {
    switch (mode) {
    case ClientThrowMode::NONE:
        return;
    case ClientThrowMode::BAD_ALLOC:
        throw std::bad_alloc();
    case ClientThrowMode::LENGTH_ERROR:
        throw std::length_error("fake Redis length error");
    case ClientThrowMode::RUNTIME_ERROR:
        throw std::runtime_error("fake Redis error");
    }
}

struct FakeRedisState {
    std::size_t set_calls = 0;
    std::size_t get_calls = 0;
    bool use_set_results = false;
    bool use_get_results = false;
    int set_delay_ms = 0;
    int get_delay_ms = 0;
    ClientThrowMode set_throw_mode = ClientThrowMode::NONE;
    ClientThrowMode get_throw_mode = ClientThrowMode::NONE;
    std::vector<ErrorCode> set_results;
    std::vector<ErrorCode> get_results;
    std::vector<std::string> get_values;
    std::vector<std::string> last_keys;
    std::vector<std::string> last_values;
};

class FakeRedisClient : public RedisClient {
public:
    explicit FakeRedisClient(const StandardUri &endpoint) : RedisClient(endpoint) {}

protected:
    bool IsContextOk() const override { return true; }
    bool Reconnect() override { return true; }
};

class TestValkeySdk : public ValkeySdk {
public:
    explicit TestValkeySdk(std::shared_ptr<FakeRedisState> state) : state_(std::move(state)) {}

    void set_fail_client_creation(bool value) { fail_client_creation_ = value; }
    const std::vector<std::string> &created_endpoints() const { return created_endpoints_; }

protected:
    std::shared_ptr<RedisClient> CreateClient(const StandardUri &endpoint) override {
        created_endpoints_.push_back(endpoint.ToUriString());
        if (fail_client_creation_) {
            return nullptr;
        }
        return std::make_shared<FakeRedisClient>(endpoint);
    }

    std::vector<ErrorCode>
    SetStrings(RedisClient &, const std::vector<std::string> &keys, const std::vector<std::string> &values) override {
        ++state_->set_calls;
        if (state_->set_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(state_->set_delay_ms));
        }
        ThrowIfConfigured(state_->set_throw_mode);
        state_->last_keys = keys;
        state_->last_values = values;
        return state_->use_set_results ? state_->set_results : std::vector<ErrorCode>(keys.size(), EC_OK);
    }

    std::vector<ErrorCode>
    GetStrings(RedisClient &, const std::vector<std::string> &keys, std::vector<std::string> &values) override {
        ++state_->get_calls;
        if (state_->get_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(state_->get_delay_ms));
        }
        ThrowIfConfigured(state_->get_throw_mode);
        state_->last_keys = keys;
        values = state_->get_values;
        return state_->use_get_results ? state_->get_results : std::vector<ErrorCode>(keys.size(), EC_OK);
    }

private:
    std::shared_ptr<FakeRedisState> state_;
    bool fail_client_creation_ = false;
    std::vector<std::string> created_endpoints_;
};

std::shared_ptr<SdkBackendConfig> MakeSdkConfig(int get_timeout_ms = 2000, int put_timeout_ms = 2000) {
    auto config = std::make_shared<SdkBackendConfig>();
    config->set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY);
    SdkTimeoutConfig timeout;
    timeout.set_get_timeout_ms(get_timeout_ms);
    timeout.set_put_timeout_ms(put_timeout_ms);
    config->set_timeout_config(timeout);
    return config;
}

std::shared_ptr<StorageConfig> MakeStorageConfig(const std::string &identity = "valkey-primary",
                                                 const std::string &endpoint = "valkey://cache.internal:6379/?db=11") {
    auto spec = std::make_shared<ValkeyStorageSpec>();
    spec->set_endpoint(endpoint);
    return std::make_shared<StorageConfig>(DataStorageType::DATA_STORAGE_TYPE_VALKEY, identity, spec);
}

DataStorageUri MakeLocation(const std::string &identity, const std::string &opaque_id, std::size_t size) {
    DataStorageUri uri;
    EXPECT_TRUE(BuildValkeyLocation(identity, opaque_id, size, uri));
    return uri;
}

BlockBuffer CpuBuffer(void *base, std::size_t size) {
    BlockBuffer buffer;
    buffer.iovs.push_back(Iov{MemoryType::CPU, base, size, false});
    return buffer;
}

class ValkeySdkTest : public TESTBASE {
public:
    void SetUp() override {
        state_ = std::make_shared<FakeRedisState>();
        sdk_ = std::make_unique<TestValkeySdk>(state_);
        ASSERT_EQ(ER_OK, sdk_->Init(MakeSdkConfig(), MakeStorageConfig()));
    }

protected:
    std::shared_ptr<FakeRedisState> state_;
    std::unique_ptr<TestValkeySdk> sdk_;
};

TEST(ValkeySdkInitTest, ValidatesConfigsInitializesSeparateTimedPoolsAndReportsType) {
    auto state = std::make_shared<FakeRedisState>();
    TestValkeySdk sdk(state);
    EXPECT_EQ(SdkType::VALKEY, sdk.Type());
    EXPECT_EQ(ER_INVALID_SDKBACKEND_CONFIG, sdk.Init(nullptr, MakeStorageConfig()));

    auto wrong_sdk = MakeSdkConfig();
    wrong_sdk->set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    EXPECT_EQ(ER_INVALID_SDKBACKEND_CONFIG, sdk.Init(wrong_sdk, MakeStorageConfig()));

    auto bad_timeout = MakeSdkConfig(0, 11);
    EXPECT_EQ(ER_INVALID_SDKBACKEND_CONFIG, sdk.Init(bad_timeout, MakeStorageConfig()));
    EXPECT_EQ(ER_INVALID_STORAGE_CONFIG, sdk.Init(MakeSdkConfig(), nullptr));

    auto wrong_storage = MakeStorageConfig();
    wrong_storage->set_type(DataStorageType::DATA_STORAGE_TYPE_NFS);
    EXPECT_EQ(ER_INVALID_STORAGE_CONFIG, sdk.Init(MakeSdkConfig(), wrong_storage));
    EXPECT_EQ(ER_INVALID_STORAGE_CONFIG, sdk.Init(MakeSdkConfig(), MakeStorageConfig("bad/identity")));
    for (const std::string &endpoint : {"rediss://cache.internal:6379/",
                                        "valkeys://cache.internal:6379/",
                                        "redis://user@cache.internal:6379/",
                                        "valkey://user@cache.internal:6379/",
                                        "valkey://cache.internal:6379/?db=1&extra=2"}) {
        EXPECT_EQ(ER_INVALID_STORAGE_CONFIG, sdk.Init(MakeSdkConfig(), MakeStorageConfig("valkey-primary", endpoint)));
    }

    TestValkeySdk failed_pool(state);
    failed_pool.set_fail_client_creation(true);
    EXPECT_EQ(ER_SDKINIT_ERROR, failed_pool.Init(MakeSdkConfig(), MakeStorageConfig()));

    TestValkeySdk valid(state);
    ASSERT_EQ(ER_OK, valid.Init(MakeSdkConfig(13, 17), MakeStorageConfig()));
    ASSERT_EQ(8u, valid.created_endpoints().size());
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ("valkey://cache.internal:6379/?db=11&timeout_ms=13", valid.created_endpoints()[i]);
    }
    for (std::size_t i = 4; i < 8; ++i) {
        EXPECT_EQ("valkey://cache.internal:6379/?db=11&timeout_ms=17", valid.created_endpoints()[i]);
    }

    TestValkeySdk redis_alias(state);
    ASSERT_EQ(ER_OK,
              redis_alias.Init(MakeSdkConfig(13, 17),
                               MakeStorageConfig("valkey-primary", "redis://cache.internal:6379/?db=11")));
    ASSERT_EQ(8u, redis_alias.created_endpoints().size());
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ("redis://cache.internal:6379/?db=11&timeout_ms=13", redis_alias.created_endpoints()[i]);
    }
    for (std::size_t i = 4; i < 8; ++i) {
        EXPECT_EQ("redis://cache.internal:6379/?db=11&timeout_ms=17", redis_alias.created_endpoints()[i]);
    }
}

TEST_F(ValkeySdkTest, PutGathersBinarySegmentsAndEmptyBlockInOneBatch) {
    const std::string first("ab\0", 3);
    const std::string second("\xffz", 2);
    BlockBuffer binary;
    binary.iovs = {
        Iov{MemoryType::CPU, const_cast<char *>(first.data()), first.size(), false},
        Iov{MemoryType::CPU, nullptr, 0, true},
        Iov{MemoryType::GPU, nullptr, 0, false},
        Iov{MemoryType::CPU, const_cast<char *>(second.data()), second.size(), false},
    };
    const std::vector<DataStorageUri> uris{
        MakeLocation("valkey-primary", "00000000000000000000000000000001", 5),
        MakeLocation("valkey-primary", "00000000000000000000000000000002", 0),
    };
    auto actual = std::make_shared<std::vector<DataStorageUri>>(
        1, MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1));

    ASSERT_EQ(ER_OK, sdk_->Put(uris, {binary, BlockBuffer{}}, actual));
    EXPECT_EQ(1u, state_->set_calls);
    ASSERT_EQ(2u, state_->last_values.size());
    EXPECT_EQ(std::string("ab\0\xffz", 5), state_->last_values[0]);
    EXPECT_TRUE(state_->last_values[1].empty());
    ASSERT_EQ(uris.size(), state_->last_keys.size());
    EXPECT_EQ("kvcm:valkey:14:valkey-primary:00000000000000000000000000000001", state_->last_keys[0]);
    EXPECT_EQ(uris[0].ToUriString(), (*actual)[0].ToUriString());
    EXPECT_EQ(uris[1].ToUriString(), (*actual)[1].ToUriString());
}

TEST_F(ValkeySdkTest, InvalidRequestsAreRejectedBeforeIoOrDestinationWrites) {
    char source[4] = {'d', 'a', 't', 'a'};
    const DataStorageUri valid = MakeLocation("valkey-primary", "00000000000000000000000000000003", 4);
    const DataStorageUri cross = MakeLocation("other-storage", "00000000000000000000000000000003", 4);
    const DataStorageUri wrong_size = MakeLocation("valkey-primary", "00000000000000000000000000000003", 3);
    const DataStorageUri malformed("valkey://valkey-primary/not-an-id?size=4");
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);
    const std::size_t huge_size = std::numeric_limits<std::size_t>::max() / 2 + 1;
    BlockBuffer accumulated_size_overflow;
    accumulated_size_overflow.iovs = {
        Iov{MemoryType::CPU, reinterpret_cast<void *>(1), huge_size, false},
        Iov{MemoryType::CPU, reinterpret_cast<void *>(1), huge_size, false},
    };

    struct InvalidCase {
        std::vector<DataStorageUri> uris;
        BlockBuffers buffers;
        ClientErrorCode expected;
    };
    std::vector<InvalidCase> cases{
        {{malformed}, {CpuBuffer(source, sizeof(source))}, ER_INVALID_PARAMS},
        {{cross}, {CpuBuffer(source, sizeof(source))}, ER_INVALID_PARAMS},
        {{wrong_size}, {CpuBuffer(source, sizeof(source))}, ER_INVALID_PARAMS},
        {{valid, valid}, {CpuBuffer(source, sizeof(source))}, ER_INVALID_PARAMS},
        {{valid}, {CpuBuffer(nullptr, sizeof(source))}, ER_INVALID_LOCAL_BUFFERS},
        {{valid},
         {CpuBuffer(reinterpret_cast<void *>(std::numeric_limits<std::uintptr_t>::max() - 1), 4)},
         ER_INVALID_ADDRESS},
        {{valid}, {accumulated_size_overflow}, ER_INVALID_LOCAL_BUFFERS},
        {{valid},
         {{std::vector<Iov>{Iov{MemoryType::GPU, source, sizeof(source), false}}}},
         ER_UNSUPPORTED_MEMORY_TYPE},
    };

    for (const auto &test_case : cases) {
        auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);
        char destination[4] = {'x', 'x', 'x', 'x'};
        BlockBuffers get_buffers = test_case.buffers;
        if (get_buffers.size() == 1 && get_buffers[0].iovs.size() == 1 && get_buffers[0].iovs[0].base == source) {
            get_buffers[0].iovs[0].base = destination;
        }
        EXPECT_EQ(test_case.expected, sdk_->Put(test_case.uris, test_case.buffers, actual));
        EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
        EXPECT_EQ(test_case.expected, sdk_->Get(test_case.uris, get_buffers));
        EXPECT_EQ(std::string("xxxx", 4), std::string(destination, sizeof(destination)));
    }
    EXPECT_EQ(0u, state_->set_calls);
    EXPECT_EQ(0u, state_->get_calls);

    auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);
    EXPECT_EQ(ER_INVALID_PARAMS, sdk_->Put({valid}, {CpuBuffer(source, sizeof(source))}, nullptr));
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
}

TEST_F(ValkeySdkTest, ZeroLengthBuffersRejectMissingOrMalformedDeclaredSizesBeforeIo) {
    const std::string opaque_id = "00000000000000000000000000000003";
    const std::vector<DataStorageUri> invalid_uris{
        DataStorageUri("valkey://valkey-primary/" + opaque_id),
        DataStorageUri("valkey://valkey-primary/" + opaque_id + "?size=abc"),
        DataStorageUri("valkey://valkey-primary/" + opaque_id + "?size=-1"),
    };
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);

    for (const auto &uri : invalid_uris) {
        auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);
        EXPECT_EQ(ER_INVALID_PARAMS, sdk_->Get({uri}, {BlockBuffer{}}));
        EXPECT_EQ(ER_INVALID_PARAMS, sdk_->Put({uri}, {BlockBuffer{}}, actual));
        ASSERT_EQ(1u, actual->size());
        EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
    }
    EXPECT_EQ(0u, state_->get_calls);
    EXPECT_EQ(0u, state_->set_calls);
}

TEST_F(ValkeySdkTest, GetSkipsIgnoredIovsButCountsThemInDeclaredSizeAndPutRejectsThem) {
    char first[2] = {'x', 'x'};
    char ignored[3] = {'i', 'i', 'i'};
    char following[2] = {'y', 'y'};
    BlockBuffer buffer;
    buffer.iovs = {
        Iov{MemoryType::CPU, first, sizeof(first), false},
        Iov{MemoryType::GPU, ignored, sizeof(ignored), true},
        Iov{MemoryType::GPU, nullptr, 1, true},
        Iov{MemoryType::CPU, following, sizeof(following), false},
    };
    const DataStorageUri wrong_size =
        MakeLocation("valkey-primary", "00000000000000000000000000000010", sizeof(first) + sizeof(following));
    const DataStorageUri uri = MakeLocation(
        "valkey-primary", "00000000000000000000000000000010", sizeof(first) + sizeof(ignored) + 1 + sizeof(following));
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);
    auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);

    EXPECT_EQ(ER_INVALID_PARAMS, sdk_->Get({wrong_size}, {buffer}));
    EXPECT_EQ(0u, state_->get_calls);

    state_->get_values = {"abXYZQde"};
    ASSERT_EQ(ER_OK, sdk_->Get({uri}, {buffer}));
    EXPECT_EQ("ab", std::string(first, sizeof(first)));
    EXPECT_EQ("iii", std::string(ignored, sizeof(ignored)));
    EXPECT_EQ("de", std::string(following, sizeof(following)));

    EXPECT_EQ(ER_INVALID_PARAMS, sdk_->Put({uri}, {buffer}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
    EXPECT_EQ(1u, state_->get_calls);
    EXPECT_EQ(0u, state_->set_calls);
}

TEST_F(ValkeySdkTest, PutPreservesOutputForBatchErrorsTimeoutsAndCardinalityMismatch) {
    char source = 'v';
    const std::vector<DataStorageUri> uris{MakeLocation("valkey-primary", "00000000000000000000000000000004", 1)};
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);
    auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);
    state_->use_set_results = true;

    state_->set_results = {EC_ERROR};
    EXPECT_EQ(ER_SDKWRITE_ERROR, sdk_->Put(uris, {CpuBuffer(&source, 1)}, actual));
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());

    state_->set_results = {EC_TIMEOUT};
    EXPECT_EQ(ER_SDK_TIMEOUT, sdk_->Put(uris, {CpuBuffer(&source, 1)}, actual));
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());

    state_->set_results.clear();
    EXPECT_EQ(ER_SDKWRITE_ERROR, sdk_->Put(uris, {CpuBuffer(&source, 1)}, actual));
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
    EXPECT_EQ(3u, state_->set_calls);

    ASSERT_EQ(ER_OK, sdk_->Put({}, {}, actual));
    EXPECT_TRUE(actual->empty());
    EXPECT_EQ(3u, state_->set_calls);
}

TEST_F(ValkeySdkTest, GetScattersBinarySegmentsAndEmptyBlockInOneBatch) {
    state_->get_values = {std::string("a\0b\xff", 4), std::string()};
    char first[2] = {};
    char second[2] = {};
    BlockBuffer binary;
    binary.iovs = {
        Iov{MemoryType::CPU, first, sizeof(first), false},
        Iov{MemoryType::CPU, nullptr, 0, false},
        Iov{MemoryType::CPU, second, sizeof(second), false},
    };
    const std::vector<DataStorageUri> uris{
        MakeLocation("valkey-primary", "00000000000000000000000000000005", 4),
        MakeLocation("valkey-primary", "00000000000000000000000000000006", 0),
    };

    ASSERT_EQ(ER_OK, sdk_->Get(uris, {binary, BlockBuffer{}}));
    EXPECT_EQ(1u, state_->get_calls);
    EXPECT_EQ(std::string("a\0", 2), std::string(first, sizeof(first)));
    EXPECT_EQ(std::string("b\xff", 2), std::string(second, sizeof(second)));
}

TEST_F(ValkeySdkTest, GetPopulatesValidPeersButPreservesFailedAndWrongSizedBlocks) {
    char missing[3] = {'m', 'm', 'm'};
    char error[3] = {'e', 'e', 'e'};
    char wrong[4] = {'w', 'w', 'w', 'w'};
    char valid_first[2] = {'v', 'v'};
    char valid_second[2] = {'v', 'v'};
    const std::vector<DataStorageUri> uris{
        MakeLocation("valkey-primary", "00000000000000000000000000000007", 3),
        MakeLocation("valkey-primary", "00000000000000000000000000000008", 3),
        MakeLocation("valkey-primary", "00000000000000000000000000000009", 4),
        MakeLocation("valkey-primary", "0000000000000000000000000000000a", 4),
    };
    BlockBuffer valid_buffer;
    valid_buffer.iovs = {Iov{MemoryType::CPU, valid_first, 2, false}, Iov{MemoryType::CPU, valid_second, 2, false}};
    state_->use_get_results = true;
    state_->get_results = {EC_NOENT, EC_ERROR, EC_OK, EC_OK};
    state_->get_values = {"", "ignored", "bad", std::string("o\0k\xff", 4)};

    EXPECT_EQ(ER_SDKREAD_ERROR,
              sdk_->Get(uris,
                        {CpuBuffer(missing, sizeof(missing)),
                         CpuBuffer(error, sizeof(error)),
                         CpuBuffer(wrong, sizeof(wrong)),
                         valid_buffer}));
    EXPECT_EQ(1u, state_->get_calls);
    EXPECT_EQ("mmm", std::string(missing, sizeof(missing)));
    EXPECT_EQ("eee", std::string(error, sizeof(error)));
    EXPECT_EQ("wwww", std::string(wrong, sizeof(wrong)));
    EXPECT_EQ(std::string("o\0", 2), std::string(valid_first, sizeof(valid_first)));
    EXPECT_EQ(std::string("k\xff", 2), std::string(valid_second, sizeof(valid_second)));
}

TEST_F(ValkeySdkTest, GetDefendsCardinalityAndPropagatesTimeoutAfterValidPeers) {
    char first[2] = {'x', 'x'};
    char second[2] = {'y', 'y'};
    const std::vector<DataStorageUri> uris{
        MakeLocation("valkey-primary", "0000000000000000000000000000000b", 2),
        MakeLocation("valkey-primary", "0000000000000000000000000000000c", 2),
    };
    const BlockBuffers buffers{CpuBuffer(first, 2), CpuBuffer(second, 2)};
    state_->use_get_results = true;

    state_->get_results = {EC_OK};
    state_->get_values = {"aa", "bb"};
    EXPECT_EQ(ER_SDKREAD_ERROR, sdk_->Get(uris, buffers));
    EXPECT_EQ("xx", std::string(first, 2));
    EXPECT_EQ("yy", std::string(second, 2));

    state_->get_results = {EC_OK, EC_OK};
    state_->get_values = {"aa"};
    EXPECT_EQ(ER_SDKREAD_ERROR, sdk_->Get(uris, buffers));
    EXPECT_EQ("xx", std::string(first, 2));
    EXPECT_EQ("yy", std::string(second, 2));

    state_->get_results = {EC_TIMEOUT, EC_OK};
    state_->get_values = {"ignored", "ok"};
    EXPECT_EQ(ER_SDK_TIMEOUT, sdk_->Get(uris, buffers));
    EXPECT_EQ("xx", std::string(first, 2));
    EXPECT_EQ("ok", std::string(second, 2));
    EXPECT_EQ(3u, state_->get_calls);
}

TEST_F(ValkeySdkTest, ClientExceptionsMapToCurrentErrorsWithoutPublishingOutputs) {
    char destination = 'x';
    char source = 'v';
    const std::vector<DataStorageUri> uris{MakeLocation("valkey-primary", "00000000000000000000000000000011", 1)};
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);

    const std::vector<std::pair<ClientThrowMode, ClientErrorCode>> get_cases{
        {ClientThrowMode::BAD_ALLOC, ER_INVALID_LOCAL_BUFFERS},
        {ClientThrowMode::LENGTH_ERROR, ER_INVALID_LOCAL_BUFFERS},
        {ClientThrowMode::RUNTIME_ERROR, ER_SDKREAD_ERROR},
    };
    for (const auto &[throw_mode, expected] : get_cases) {
        state_->get_throw_mode = throw_mode;
        destination = 'x';
        EXPECT_EQ(expected, sdk_->Get(uris, {CpuBuffer(&destination, 1)}));
        EXPECT_EQ('x', destination);
    }
    state_->get_throw_mode = ClientThrowMode::NONE;

    auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);
    state_->set_throw_mode = ClientThrowMode::RUNTIME_ERROR;
    EXPECT_EQ(ER_SDKWRITE_ERROR, sdk_->Put(uris, {CpuBuffer(&source, 1)}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
    EXPECT_EQ(get_cases.size(), state_->get_calls);
    EXPECT_EQ(1u, state_->set_calls);
}

TEST_F(ValkeySdkTest, SoftTimeoutKeepsLateGetIsolatedAndPublishesConfirmedLatePut) {
    constexpr int kShortTimeoutMs = 20;
    constexpr int kFakeDelayMs = 100;
    auto timeout_state = std::make_shared<FakeRedisState>();
    TestValkeySdk timeout_sdk(timeout_state);
    ASSERT_EQ(ER_OK, timeout_sdk.Init(MakeSdkConfig(kShortTimeoutMs, kShortTimeoutMs), MakeStorageConfig()));

    char source = 'v';
    char destination = 'x';
    const std::vector<DataStorageUri> uris{MakeLocation("valkey-primary", "0000000000000000000000000000000d", 1)};
    const DataStorageUri sentinel = MakeLocation("valkey-primary", "ffffffffffffffffffffffffffffffff", 1);
    auto actual = std::make_shared<std::vector<DataStorageUri>>(1, sentinel);

    timeout_state->set_delay_ms = kFakeDelayMs;
    EXPECT_EQ(ER_OK, timeout_sdk.Put(uris, {CpuBuffer(&source, 1)}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(uris[0].ToUriString(), actual->at(0).ToUriString());

    *actual = {sentinel};
    timeout_state->use_set_results = true;
    timeout_state->set_results = {EC_ERROR};
    EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Put(uris, {CpuBuffer(&source, 1)}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());

    timeout_state->set_throw_mode = ClientThrowMode::RUNTIME_ERROR;
    EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Put(uris, {CpuBuffer(&source, 1)}, actual));
    ASSERT_EQ(1u, actual->size());
    EXPECT_EQ(sentinel.ToUriString(), actual->at(0).ToUriString());
    timeout_state->set_throw_mode = ClientThrowMode::NONE;

    timeout_state->get_delay_ms = kFakeDelayMs;
    timeout_state->get_values = {"z"};
    EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Get(uris, {CpuBuffer(&destination, 1)}));
    EXPECT_EQ('x', destination);

    const std::vector<ClientThrowMode> get_throw_modes{
        ClientThrowMode::BAD_ALLOC,
        ClientThrowMode::LENGTH_ERROR,
        ClientThrowMode::RUNTIME_ERROR,
    };
    for (const ClientThrowMode throw_mode : get_throw_modes) {
        timeout_state->get_throw_mode = throw_mode;
        destination = 'x';
        EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Get(uris, {CpuBuffer(&destination, 1)}));
        EXPECT_EQ('x', destination);
        timeout_state->get_throw_mode = ClientThrowMode::NONE;
    }

    EXPECT_EQ(3u, timeout_state->set_calls);
    EXPECT_EQ(1u + get_throw_modes.size(), timeout_state->get_calls);
}

TEST_F(ValkeySdkTest, PoolExhaustionFailsWithoutBatchIo) {
    constexpr int kShortPoolBudgetMs = 20;
    auto timeout_state = std::make_shared<FakeRedisState>();
    TestValkeySdk timeout_sdk(timeout_state);
    ASSERT_EQ(ER_OK, timeout_sdk.Init(MakeSdkConfig(kShortPoolBudgetMs, kShortPoolBudgetMs), MakeStorageConfig()));

    char value = 'v';
    const std::vector<DataStorageUri> uris{MakeLocation("valkey-primary", "0000000000000000000000000000000e", 1)};
    const BlockBuffers buffers{CpuBuffer(&value, 1)};
    auto actual = std::make_shared<std::vector<DataStorageUri>>();

    auto put_one = timeout_sdk.put_client_pool_->AcquireClient(0);
    auto put_two = timeout_sdk.put_client_pool_->AcquireClient(0);
    auto put_three = timeout_sdk.put_client_pool_->AcquireClient(0);
    auto put_four = timeout_sdk.put_client_pool_->AcquireClient(0);
    ASSERT_TRUE(put_one && put_two && put_three && put_four);
    EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Put(uris, buffers, actual));
    EXPECT_TRUE(actual->empty());

    auto get_one = timeout_sdk.get_client_pool_->AcquireClient(0);
    auto get_two = timeout_sdk.get_client_pool_->AcquireClient(0);
    auto get_three = timeout_sdk.get_client_pool_->AcquireClient(0);
    auto get_four = timeout_sdk.get_client_pool_->AcquireClient(0);
    ASSERT_TRUE(get_one && get_two && get_three && get_four);
    EXPECT_EQ(ER_SDK_TIMEOUT, timeout_sdk.Get(uris, buffers));
    EXPECT_EQ(0u, timeout_state->set_calls);
    EXPECT_EQ(0u, timeout_state->get_calls);
}

} // namespace
} // namespace kv_cache_manager
