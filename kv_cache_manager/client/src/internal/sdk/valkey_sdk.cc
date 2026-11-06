#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {

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

    auto make_pool = [this](const StandardUri &endpoint) {
        return std::make_unique<RedisClientPool>([this, endpoint]() {
            auto client = CreateClient(endpoint);
            if (!client || !PrepareClient(*client) || !client->Open()) {
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
    const SdkCallContext context(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(timeout_config_.get_timeout_ms()));
    return GetWithContext(remote_uris, local_buffers, context);
}

ClientErrorCode ValkeySdk::Put(const std::vector<DataStorageUri> &remote_uris,
                               const BlockBuffers &local_buffers,
                               std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) {
    const SdkCallContext context(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(timeout_config_.put_timeout_ms()));
    return PutWithContext(remote_uris, local_buffers, std::move(actual_remote_uris), context);
}

ClientErrorCode ValkeySdk::GetWithContext(const std::vector<DataStorageUri> &remote_uris,
                                          const BlockBuffers &local_buffers,
                                          const SdkCallContext &context) {
    if (remote_uris.size() != local_buffers.size()) {
        return ER_INVALID_PARAMS;
    }
    if (remote_uris.empty()) {
        return ER_OK;
    }
    if (!get_client_pool_) {
        return ER_SDKINIT_ERROR;
    }

    std::vector<std::string> redis_keys;
    std::vector<std::size_t> expected_sizes;
    ClientErrorCode ec = Preflight(remote_uris, local_buffers, storage_identity_, true, redis_keys, expected_sizes);
    if (ec != ER_OK) {
        return ec;
    }
    if (!context.IsLive()) {
        return ER_SDK_TIMEOUT;
    }

    auto client = AcquireGetClientUntil(context.deadline());
    if (!client) {
        return ER_SDK_TIMEOUT;
    }

    ValkeyGetStringsResult result;
    try {
        result = GetStringsUntil(*client, redis_keys, expected_sizes, context.deadline());
    } catch (const std::bad_alloc &) {
        return context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT;
    } catch (const std::length_error &) {
        return context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT;
    } catch (...) {
        return context.IsLive() ? ER_SDKREAD_ERROR : ER_SDK_TIMEOUT;
    }

    if (!context.IsLive() || result.operation_error == EC_TIMEOUT ||
        std::find(result.errors.begin(), result.errors.end(), EC_TIMEOUT) != result.errors.end()) {
        return ER_SDK_TIMEOUT;
    }
    if (result.operation_error != EC_OK || result.errors.size() != remote_uris.size() ||
        result.values.size() != remote_uris.size()) {
        return ER_SDKREAD_ERROR;
    }

    bool has_error = false;
    for (std::size_t block_index = 0; block_index < result.errors.size(); ++block_index) {
        if (result.errors[block_index] != EC_OK || result.values[block_index].size() != expected_sizes[block_index]) {
            has_error = true;
        }
    }

    auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    if (!access) {
        return ER_SDK_TIMEOUT;
    }
    OnOutputCommitAdmitted();
    for (std::size_t block_index = 0; block_index < result.errors.size(); ++block_index) {
        if (result.errors[block_index] != EC_OK || result.values[block_index].size() != expected_sizes[block_index]) {
            continue;
        }
        std::size_t offset = 0;
        for (const auto &iov : local_buffers[block_index].iovs) {
            if (iov.size == 0) {
                continue;
            }
            if (!iov.ignore) {
                std::memcpy(iov.base, result.values[block_index].data() + offset, iov.size);
            }
            offset += iov.size;
        }
    }
    const ClientErrorCode result_code = has_error ? ER_SDKREAD_ERROR : ER_OK;
    access->Complete(result_code);
    return result_code;
}

ClientErrorCode ValkeySdk::PutWithContext(const std::vector<DataStorageUri> &remote_uris,
                                          const BlockBuffers &local_buffers,
                                          std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris,
                                          const SdkCallContext &context) {
    if (!actual_remote_uris || remote_uris.size() != local_buffers.size()) {
        return ER_INVALID_PARAMS;
    }
    if (remote_uris.empty()) {
        auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
        if (!access) {
            return ER_SDK_TIMEOUT;
        }
        OnOutputCommitAdmitted();
        std::vector<DataStorageUri> empty;
        actual_remote_uris->swap(empty);
        access->Complete(ER_OK);
        return ER_OK;
    }
    if (!put_client_pool_) {
        return ER_SDKINIT_ERROR;
    }

    std::vector<std::string> redis_keys;
    std::vector<std::size_t> expected_sizes;
    ClientErrorCode ec = Preflight(remote_uris, local_buffers, storage_identity_, false, redis_keys, expected_sizes);
    if (ec != ER_OK) {
        return ec;
    }

    std::vector<std::string> values;
    ec = Gather(local_buffers, expected_sizes, context, values);
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

    if (!context.IsLive()) {
        return ER_SDK_TIMEOUT;
    }
    auto client = AcquirePutClientUntil(context.deadline());
    if (!client) {
        return ER_SDK_TIMEOUT;
    }

    ValkeySetStringsResult result;
    try {
        result = SetStringsUntil(*client, redis_keys, values, context.deadline());
    } catch (...) {
        return context.IsLive() ? ER_SDKWRITE_ERROR : ER_SDK_TIMEOUT;
    }

    const bool has_timeout = result.operation_error == EC_TIMEOUT ||
                             std::find(result.errors.begin(), result.errors.end(), EC_TIMEOUT) != result.errors.end();
    const bool has_ambiguous =
        std::find(result.certainties.begin(), result.certainties.end(), ValkeyWriteCertainty::AMBIGUOUS) !=
        result.certainties.end();
    if (!context.IsLive() || has_timeout || has_ambiguous) {
        return ER_SDK_TIMEOUT;
    }
    if (result.operation_error != EC_OK || result.errors.size() != remote_uris.size() ||
        result.certainties.size() != remote_uris.size()) {
        return ER_SDKWRITE_ERROR;
    }
    for (std::size_t i = 0; i < remote_uris.size(); ++i) {
        if (result.errors[i] != EC_OK || result.certainties[i] != ValkeyWriteCertainty::CONFIRMED) {
            return ER_SDKWRITE_ERROR;
        }
    }

    auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    if (!access) {
        return ER_SDK_TIMEOUT;
    }
    OnOutputCommitAdmitted();
    actual_remote_uris->swap(successful_remote_uris);
    access->Complete(ER_OK);
    return ER_OK;
}

ClientErrorCode ValkeySdk::Alloc(const std::vector<DataStorageUri> &remote_uris,
                                 std::vector<DataStorageUri> &alloc_uris) {
    alloc_uris = remote_uris;
    return ER_OK;
}

std::shared_ptr<RedisClient> ValkeySdk::CreateClient(const StandardUri &endpoint) {
    return std::make_shared<RedisClient>(endpoint);
}

bool ValkeySdk::PrepareClient(RedisClient &client) { return client.Prepare(); }

ValkeySdk::RedisClientHandle ValkeySdk::AcquireGetClientUntil(Deadline deadline) {
    return get_client_pool_->AcquireClientUntil(deadline);
}

ValkeySdk::RedisClientHandle ValkeySdk::AcquirePutClientUntil(Deadline deadline) {
    return put_client_pool_->AcquireClientUntil(deadline);
}

ValkeySetStringsResult ValkeySdk::SetStringsUntil(RedisClient &client,
                                                  const std::vector<std::string> &keys,
                                                  const std::vector<std::string> &values,
                                                  Deadline deadline) {
    return client.SetStringsUntil(keys, values, deadline);
}

ValkeyGetStringsResult ValkeySdk::GetStringsUntil(RedisClient &client,
                                                  const std::vector<std::string> &keys,
                                                  const std::vector<std::size_t> &expected_sizes,
                                                  Deadline deadline) {
    return client.GetStringsUntil(keys, expected_sizes, deadline);
}

void ValkeySdk::OnSourceChunkRead(std::size_t, std::size_t, std::size_t) {}

void ValkeySdk::OnOutputCommitAdmitted() {}

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
                                  const SdkCallContext &context,
                                  std::vector<std::string> &values) {
    constexpr std::size_t kGatherChunkSize = 64 * 1024;

    // Allocate and initialize owned destinations before admitting access to
    // caller memory. Once admitted, each cancellation interval contains at
    // most one 64 KiB copy plus test/instrumentation hook overhead.
    values.clear();
    try {
        values.reserve(local_buffers.size());
        for (const std::size_t expected_size : expected_sizes) {
            values.emplace_back(expected_size, '\0');
        }
    } catch (const std::bad_alloc &) {
        return context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT;
    } catch (const std::length_error &) {
        return context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT;
    }

    auto access = context.TryAcquireCallerMemory();
    if (!access) {
        return ER_SDK_TIMEOUT;
    }

    for (std::size_t block_index = 0; block_index < local_buffers.size(); ++block_index) {
        std::size_t destination_offset = 0;
        for (std::size_t iov_index = 0; iov_index < local_buffers[block_index].iovs.size(); ++iov_index) {
            const auto &iov = local_buffers[block_index].iovs[iov_index];
            std::size_t source_offset = 0;
            while (source_offset < iov.size) {
                if (!context.IsLive()) {
                    return ER_SDK_TIMEOUT;
                }
                const std::size_t chunk_size = std::min(kGatherChunkSize, iov.size - source_offset);
                std::memcpy(values[block_index].data() + destination_offset,
                            static_cast<const char *>(iov.base) + source_offset,
                            chunk_size);
                source_offset += chunk_size;
                destination_offset += chunk_size;
                OnSourceChunkRead(block_index, iov_index, chunk_size);
            }
        }
    }
    return ER_OK;
}

} // namespace kv_cache_manager
