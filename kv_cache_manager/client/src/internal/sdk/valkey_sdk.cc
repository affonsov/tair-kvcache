#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

#include "kv_cache_manager/client/src/internal/sdk/deadline_util.h"
#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {
namespace {

void LogSoftTimeout(const char *operation,
                    const char *stage,
                    std::size_t block_count,
                    const std::string &storage_identity,
                    int configured_budget_ms,
                    int64_t start_ms) {
    KVCM_LOG_WARN("valkey sdk soft timeout: operation=%s stage=%s block_count=%zu storage_identity=%s "
                  "configured_budget_ms=%d elapsed_ms=%lld",
                  operation,
                  stage,
                  block_count,
                  storage_identity.c_str(),
                  configured_budget_ms,
                  static_cast<long long>(SteadyClockMs() - start_ms));
}

void LogLatePutSuccess(std::size_t block_count,
                       const std::string &storage_identity,
                       int configured_budget_ms,
                       int64_t start_ms) {
    KVCM_LOG_WARN("valkey sdk late completion: operation=put stage=redis_io block_count=%zu storage_identity=%s "
                  "configured_budget_ms=%d elapsed_ms=%lld result=success",
                  block_count,
                  storage_identity.c_str(),
                  configured_budget_ms,
                  static_cast<long long>(SteadyClockMs() - start_ms));
}

} // namespace

ClientErrorCode ValkeySdk::Init(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                const std::shared_ptr<StorageConfig> &storage_config) {
    if (!sdk_backend_config || !sdk_backend_config->Validate() || !sdk_backend_config->timeout_config().Validate() ||
        sdk_backend_config->type() != DataStorageType::DATA_STORAGE_TYPE_VALKEY) {
        return ER_INVALID_SDKBACKEND_CONFIG;
    }
    if (!storage_config || storage_config->type() != DataStorageType::DATA_STORAGE_TYPE_VALKEY) {
        return ER_INVALID_STORAGE_CONFIG;
    }

    std::string invalid_fields;
    const auto storage_spec = std::dynamic_pointer_cast<ValkeyStorageSpec>(storage_config->storage_spec());
    DataStorageUri identity_check;
    if (!storage_spec || !storage_config->ValidateRequiredFields(invalid_fields) ||
        !BuildValkeyLocation(
            storage_config->global_unique_name(), "00000000000000000000000000000000", 0, identity_check)) {
        return ER_INVALID_STORAGE_CONFIG;
    }

    StandardUri get_endpoint = StandardUri::FromUri(storage_spec->endpoint());
    StandardUri put_endpoint = get_endpoint;
    get_endpoint.SetParam("timeout_ms", std::to_string(sdk_backend_config->timeout_config().get_timeout_ms()));
    put_endpoint.SetParam("timeout_ms", std::to_string(sdk_backend_config->timeout_config().put_timeout_ms()));
    RedisSecurityOptions security_options;
    if (storage_spec->secure()) {
        security_options.transport = RedisTransportSecurity::TLS_NAMED_ACL;
        security_options.directory = storage_spec->security_dir();
    }

    auto make_pool = [this, security_options](const StandardUri &endpoint) {
        RedisConnectionOptions connection_options{endpoint, security_options};
        return std::make_unique<RedisClientPool>([this, connection_options]() {
            auto client = CreateClient(connection_options);
            if (!client || !client->Open()) {
                return std::shared_ptr<RedisClient>();
            }
            return client;
        });
    };

    auto get_pool = make_pool(get_endpoint);
    if (!get_pool->Initialize()) {
        return ER_SDKINIT_ERROR;
    }
    auto put_pool = make_pool(put_endpoint);
    if (!put_pool->Initialize()) {
        return ER_SDKINIT_ERROR;
    }

    storage_identity_ = storage_config->global_unique_name();
    timeout_config_ = sdk_backend_config->timeout_config();
    get_client_pool_ = std::move(get_pool);
    put_client_pool_ = std::move(put_pool);
    return ER_OK;
}

ClientErrorCode ValkeySdk::Get(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers &local_buffers) {
    if (remote_uris.size() != local_buffers.size()) {
        return ER_INVALID_PARAMS;
    }
    if (remote_uris.empty()) {
        return ER_OK;
    }
    if (!get_client_pool_) {
        return ER_SDKINIT_ERROR;
    }
    const int configured_budget_ms = timeout_config_.get_timeout_ms();
    const int64_t start_ms = SteadyClockMs();
    const int64_t deadline_ms = start_ms + configured_budget_ms;

    std::vector<std::string> redis_keys;
    std::vector<std::size_t> expected_sizes;
    ClientErrorCode ec = Preflight(remote_uris, local_buffers, storage_identity_, true, redis_keys, expected_sizes);
    if (ec != ER_OK) {
        return ec;
    }
    const auto remaining_ms = RemainingMs(deadline_ms);
    if (!remaining_ms) {
        LogSoftTimeout("get", "pre_acquire", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }

    auto client = get_client_pool_->AcquireClient(*remaining_ms);
    if (!client) {
        LogSoftTimeout("get", "pool_acquire", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }

    std::vector<std::string> values;
    std::vector<ErrorCode> results;
    try {
        results = GetStrings(*client, redis_keys, values);
    } catch (const std::bad_alloc &) {
        if (DeadlineExpired(deadline_ms)) {
            LogSoftTimeout("get", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
            return ER_SDK_TIMEOUT;
        }
        return ER_INVALID_LOCAL_BUFFERS;
    } catch (const std::length_error &) {
        if (DeadlineExpired(deadline_ms)) {
            LogSoftTimeout("get", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
            return ER_SDK_TIMEOUT;
        }
        return ER_INVALID_LOCAL_BUFFERS;
    } catch (...) {
        if (DeadlineExpired(deadline_ms)) {
            LogSoftTimeout("get", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
            return ER_SDK_TIMEOUT;
        }
        return ER_SDKREAD_ERROR;
    }
    if (DeadlineExpired(deadline_ms)) {
        LogSoftTimeout("get", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }
    if (results.size() != remote_uris.size() || values.size() != remote_uris.size()) {
        return ER_SDKREAD_ERROR;
    }

    bool has_error = false;
    bool has_timeout = false;
    for (std::size_t block_index = 0; block_index < results.size(); ++block_index) {
        if (results[block_index] == EC_TIMEOUT) {
            has_timeout = true;
            continue;
        }
        if (results[block_index] != EC_OK || values[block_index].size() != expected_sizes[block_index]) {
            has_error = true;
            continue;
        }

        std::size_t offset = 0;
        for (const auto &iov : local_buffers[block_index].iovs) {
            if (iov.size == 0) {
                continue;
            }
            if (!iov.ignore) {
                std::memcpy(iov.base, values[block_index].data() + offset, iov.size);
            }
            offset += iov.size;
        }
    }

    if (has_timeout) {
        LogSoftTimeout("get", "redis_result", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }
    return has_error ? ER_SDKREAD_ERROR : ER_OK;
}

ClientErrorCode ValkeySdk::Put(const std::vector<DataStorageUri> &remote_uris,
                               const BlockBuffers &local_buffers,
                               std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) {
    if (!actual_remote_uris || remote_uris.size() != local_buffers.size()) {
        return ER_INVALID_PARAMS;
    }
    if (remote_uris.empty()) {
        std::vector<DataStorageUri> empty;
        actual_remote_uris->swap(empty);
        return ER_OK;
    }
    if (!put_client_pool_) {
        return ER_SDKINIT_ERROR;
    }
    const int configured_budget_ms = timeout_config_.put_timeout_ms();
    const int64_t start_ms = SteadyClockMs();
    const int64_t deadline_ms = start_ms + configured_budget_ms;

    std::vector<std::string> redis_keys;
    std::vector<std::size_t> expected_sizes;
    ClientErrorCode ec = Preflight(remote_uris, local_buffers, storage_identity_, false, redis_keys, expected_sizes);
    if (ec != ER_OK) {
        return ec;
    }

    std::vector<std::string> values;
    ec = Gather(local_buffers, expected_sizes, values);
    if (ec != ER_OK) {
        return ec;
    }

    std::vector<DataStorageUri> successful_remote_uris;
    try {
        successful_remote_uris = remote_uris;
    } catch (const std::bad_alloc &) {
        return ER_INVALID_LOCAL_BUFFERS;
    } catch (const std::length_error &) {
        return ER_INVALID_LOCAL_BUFFERS;
    }

    const auto remaining_ms = RemainingMs(deadline_ms);
    if (!remaining_ms) {
        LogSoftTimeout("put", "pre_acquire", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }
    auto client = put_client_pool_->AcquireClient(*remaining_ms);
    if (!client) {
        LogSoftTimeout("put", "pool_acquire", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        return ER_SDK_TIMEOUT;
    }

    std::vector<ErrorCode> results;
    try {
        results = SetStrings(*client, redis_keys, values);
    } catch (...) {
        if (DeadlineExpired(deadline_ms)) {
            LogSoftTimeout("put", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
            return ER_SDK_TIMEOUT;
        }
        return ER_SDKWRITE_ERROR;
    }

    if (results.size() != remote_uris.size()) {
        if (DeadlineExpired(deadline_ms)) {
            LogSoftTimeout("put", "redis_io", remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
            return ER_SDK_TIMEOUT;
        }
        return ER_SDKWRITE_ERROR;
    }

    bool all_succeeded = true;
    bool has_timeout = false;
    for (const ErrorCode result : results) {
        all_succeeded = all_succeeded && result == EC_OK;
        has_timeout = has_timeout || result == EC_TIMEOUT;
    }

    const bool completed_late = DeadlineExpired(deadline_ms);
    if (all_succeeded) {
        actual_remote_uris->swap(successful_remote_uris);
        if (completed_late) {
            LogLatePutSuccess(remote_uris.size(), storage_identity_, configured_budget_ms, start_ms);
        }
        return ER_OK;
    }
    if (has_timeout || completed_late) {
        LogSoftTimeout("put",
                       has_timeout ? "redis_result" : "redis_io",
                       remote_uris.size(),
                       storage_identity_,
                       configured_budget_ms,
                       start_ms);
        return ER_SDK_TIMEOUT;
    }
    return ER_SDKWRITE_ERROR;
}

ClientErrorCode ValkeySdk::Alloc(const std::vector<DataStorageUri> &remote_uris,
                                 std::vector<DataStorageUri> &alloc_uris) {
    alloc_uris = remote_uris;
    return ER_OK;
}

std::shared_ptr<RedisClient> ValkeySdk::CreateClient(const RedisConnectionOptions &options) {
    return std::make_shared<RedisClient>(options);
}

std::vector<ErrorCode> ValkeySdk::SetStrings(RedisClient &client,
                                             const std::vector<std::string> &keys,
                                             const std::vector<std::string> &values) {
    return client.SetStrings(keys, values);
}

std::vector<ErrorCode>
ValkeySdk::GetStrings(RedisClient &client, const std::vector<std::string> &keys, std::vector<std::string> &values) {
    return client.GetStrings(keys, values);
}

ClientErrorCode ValkeySdk::Preflight(const std::vector<DataStorageUri> &remote_uris,
                                     const BlockBuffers &local_buffers,
                                     const std::string &storage_identity,
                                     bool allow_ignored_for_read,
                                     std::vector<std::string> &redis_keys,
                                     std::vector<std::size_t> &expected_sizes) {
    redis_keys.clear();
    expected_sizes.clear();
    if (remote_uris.size() != local_buffers.size()) {
        return ER_INVALID_PARAMS;
    }

    try {
        redis_keys.reserve(remote_uris.size());
        expected_sizes.reserve(remote_uris.size());
        const std::size_t maximum_value_size = std::string().max_size();
        for (std::size_t block_index = 0; block_index < remote_uris.size(); ++block_index) {
            std::size_t expected_size = 0;
            for (const auto &iov : local_buffers[block_index].iovs) {
                if (iov.size == 0) {
                    continue;
                }
                if (iov.ignore) {
                    if (!allow_ignored_for_read) {
                        return ER_INVALID_PARAMS;
                    }
                } else {
                    if (iov.type != MemoryType::CPU) {
                        return ER_UNSUPPORTED_MEMORY_TYPE;
                    }
                    if (!iov.base) {
                        return ER_INVALID_LOCAL_BUFFERS;
                    }
                    const std::uintptr_t base = reinterpret_cast<std::uintptr_t>(iov.base);
                    if (iov.size > std::numeric_limits<std::uintptr_t>::max() - base) {
                        return ER_INVALID_ADDRESS;
                    }
                }
                if (iov.size > maximum_value_size - expected_size) {
                    return ER_INVALID_LOCAL_BUFFERS;
                }
                expected_size += iov.size;
            }

            std::string redis_key;
            if (!ValkeyLocationToRedisKey(remote_uris[block_index], storage_identity, redis_key)) {
                return ER_INVALID_PARAMS;
            }
            std::uint64_t declared_size = 0;
            remote_uris[block_index].GetParamAs("size", declared_size);
            if (declared_size > std::numeric_limits<std::size_t>::max() ||
                static_cast<std::size_t>(declared_size) != expected_size) {
                return ER_INVALID_PARAMS;
            }
            redis_keys.push_back(std::move(redis_key));
            expected_sizes.push_back(expected_size);
        }
    } catch (const std::bad_alloc &) {
        return ER_INVALID_LOCAL_BUFFERS;
    } catch (const std::length_error &) {
        return ER_INVALID_LOCAL_BUFFERS;
    }
    return ER_OK;
}

ClientErrorCode ValkeySdk::Gather(const BlockBuffers &local_buffers,
                                  const std::vector<std::size_t> &expected_sizes,
                                  std::vector<std::string> &values) {
    values.clear();
    try {
        values.reserve(local_buffers.size());
        for (std::size_t block_index = 0; block_index < local_buffers.size(); ++block_index) {
            std::string value;
            value.reserve(expected_sizes[block_index]);
            for (const auto &iov : local_buffers[block_index].iovs) {
                if (iov.size != 0) {
                    value.append(static_cast<const char *>(iov.base), iov.size);
                }
            }
            values.push_back(std::move(value));
        }
    } catch (const std::bad_alloc &) {
        return ER_INVALID_LOCAL_BUFFERS;
    } catch (const std::length_error &) {
        return ER_INVALID_LOCAL_BUFFERS;
    }
    return ER_OK;
}

} // namespace kv_cache_manager
