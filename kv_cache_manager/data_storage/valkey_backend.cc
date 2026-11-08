#include "kv_cache_manager/data_storage/valkey_backend.h"

#include <cstdint>
#include <iomanip>
#include <random>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {

ValkeyDataStorageBackend::ValkeyDataStorageBackend(std::shared_ptr<MetricsRegistry> metrics_registry)
    : DataStorageBackend(std::move(metrics_registry)) {}

ValkeyDataStorageBackend::~ValkeyDataStorageBackend() { Close(); }

DataStorageType ValkeyDataStorageBackend::GetType() { return DataStorageType::DATA_STORAGE_TYPE_VALKEY; }

bool ValkeyDataStorageBackend::Available() { return IsOpen() && IsAvailable(); }

double ValkeyDataStorageBackend::GetStorageUsageRatio(const std::string &trace_id) const { return 0.0; }

ErrorCode ValkeyDataStorageBackend::DoOpen(const StorageConfig &storage_config, const std::string &trace_id) {
    SetAvailable(false);

    std::string invalid_fields;
    const auto spec = std::dynamic_pointer_cast<ValkeyStorageSpec>(storage_config.storage_spec());
    DataStorageUri identity_check;
    if (storage_config.type() != DataStorageType::DATA_STORAGE_TYPE_VALKEY || spec == nullptr ||
        !storage_config.ValidateRequiredFields(invalid_fields) ||
        !BuildValkeyLocation(
            storage_config.global_unique_name(), "00000000000000000000000000000000", 0, identity_check)) {
        return EC_BADARGS;
    }

    const StandardUri endpoint = StandardUri::FromUri(spec->endpoint());
    auto pool = std::make_unique<StaticClientPool<RedisClient>>([this, endpoint]() {
        auto client = CreateClient(endpoint);
        if (client == nullptr || !client->Open()) {
            return std::shared_ptr<RedisClient>();
        }
        return client;
    });
    if (!pool->Initialize()) {
        return EC_ERROR;
    }

    storage_identity_ = storage_config.global_unique_name();
    client_pool_ = std::move(pool);
    SetOpen(true);
    SetAvailable(true);
    return EC_OK;
}

ErrorCode ValkeyDataStorageBackend::Close() {
    SetAvailable(false);
    SetOpen(false);
    client_pool_.reset();
    return EC_OK;
}

std::vector<std::pair<ErrorCode, DataStorageUri>> ValkeyDataStorageBackend::Create(const std::vector<std::string> &keys,
                                                                                   std::size_t size_per_key,
                                                                                   const std::string &trace_id,
                                                                                   std::function<void()> cb) {
    std::vector<std::pair<ErrorCode, DataStorageUri>> result(keys.size(), {EC_ERROR, DataStorageUri()});
    if (IsOpen() && IsAvailable()) {
        try {
            std::unordered_set<std::string> batch_ids;
            for (auto &item : result) {
                for (std::size_t attempt = 0; attempt < 32; ++attempt) {
                    std::string opaque_id = GenerateOpaqueId();
                    if (!batch_ids.insert(opaque_id).second) {
                        continue;
                    }
                    if (BuildValkeyLocation(storage_identity_, opaque_id, size_per_key, item.second)) {
                        item.first = EC_OK;
                        break;
                    }
                    batch_ids.erase(opaque_id);
                }
            }
        } catch (...) {
            // ID generation failures leave unfinished items in their ordinary error state.
        }
    }
    if (cb) {
        cb();
    }
    return result;
}

std::vector<ErrorCode> ValkeyDataStorageBackend::Delete(const std::vector<DataStorageUri> &storage_uris,
                                                        const std::string &trace_id,
                                                        std::function<void()> cb) {
    std::vector<ErrorCode> result(storage_uris.size(), EC_BADARGS);
    std::vector<std::size_t> valid_indexes;
    std::vector<std::string> redis_keys;
    valid_indexes.reserve(storage_uris.size());
    redis_keys.reserve(storage_uris.size());
    for (std::size_t i = 0; i < storage_uris.size(); ++i) {
        std::string redis_key;
        if (ValkeyLocationToRedisKey(storage_uris[i], storage_identity_, redis_key)) {
            valid_indexes.push_back(i);
            redis_keys.push_back(std::move(redis_key));
        }
    }

    if (!valid_indexes.empty()) {
        if (!IsOpen() || !IsAvailable() || client_pool_ == nullptr) {
            for (const std::size_t index : valid_indexes) {
                result[index] = EC_ERROR;
            }
        } else {
            auto client = client_pool_->AcquireClient();
            if (!client) {
                for (const std::size_t index : valid_indexes) {
                    result[index] = EC_TIMEOUT;
                }
            } else {
                const std::vector<ErrorCode> delete_result = client->Delete(redis_keys);
                for (std::size_t i = 0; i < valid_indexes.size(); ++i) {
                    if (i >= delete_result.size()) {
                        result[valid_indexes[i]] = EC_ERROR;
                    } else {
                        result[valid_indexes[i]] = delete_result[i] == EC_NOENT ? EC_OK : delete_result[i];
                    }
                }
            }
        }
    }

    if (cb) {
        cb();
    }
    return result;
}

std::vector<bool> ValkeyDataStorageBackend::Exist(const std::vector<DataStorageUri> &storage_uris) {
    std::vector<bool> result(storage_uris.size(), true);
    std::vector<std::size_t> valid_indexes;
    std::vector<std::string> redis_keys;
    valid_indexes.reserve(storage_uris.size());
    redis_keys.reserve(storage_uris.size());
    for (std::size_t i = 0; i < storage_uris.size(); ++i) {
        std::string redis_key;
        if (ValkeyLocationToRedisKey(storage_uris[i], storage_identity_, redis_key)) {
            valid_indexes.push_back(i);
            redis_keys.push_back(std::move(redis_key));
        }
    }
    if (valid_indexes.empty() || !IsOpen() || !IsAvailable() || client_pool_ == nullptr) {
        return result;
    }

    auto client = client_pool_->AcquireClient();
    if (!client) {
        return result;
    }

    std::vector<bool> exists;
    const std::vector<ErrorCode> exists_result = client->Exists(redis_keys, exists);
    if (exists_result.size() != valid_indexes.size() || exists.size() != valid_indexes.size()) {
        return result;
    }
    for (std::size_t i = 0; i < valid_indexes.size(); ++i) {
        if (exists_result[i] == EC_OK) {
            result[valid_indexes[i]] = exists[i];
        }
    }
    return result;
}

std::vector<bool> ValkeyDataStorageBackend::MightExist(const std::vector<DataStorageUri> &storage_uris) {
    return std::vector<bool>(storage_uris.size(), true);
}

std::vector<ErrorCode> ValkeyDataStorageBackend::Lock(const std::vector<DataStorageUri> &storage_uris) {
    return std::vector<ErrorCode>(storage_uris.size(), EC_OK);
}

std::vector<ErrorCode> ValkeyDataStorageBackend::UnLock(const std::vector<DataStorageUri> &storage_uris) {
    return std::vector<ErrorCode>(storage_uris.size(), EC_OK);
}

std::vector<ErrorCode> ValkeyDataStorageBackend::Copy(const std::vector<DataStorageUri> &src_uris,
                                                      const std::vector<DataStorageUri> &dst_uris,
                                                      const std::string &trace_id) {
    if (src_uris.size() != dst_uris.size()) {
        return std::vector<ErrorCode>(src_uris.size(), EC_BADARGS);
    }
    return std::vector<ErrorCode>(src_uris.size(), EC_UNIMPLEMENTED);
}

std::shared_ptr<RedisClient> ValkeyDataStorageBackend::CreateClient(const StandardUri &endpoint) {
    return std::make_shared<RedisClient>(endpoint);
}

std::string ValkeyDataStorageBackend::GenerateOpaqueId() {
    thread_local std::mt19937_64 generator = [] {
        std::random_device entropy;
        std::seed_seq seed{entropy(), entropy(), entropy(), entropy(), entropy(), entropy(), entropy(), entropy()};
        return std::mt19937_64(seed);
    }();
    const uint64_t high = generator();
    const uint64_t low = generator();
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << high << std::setw(16) << low;
    return stream.str();
}

} // namespace kv_cache_manager
