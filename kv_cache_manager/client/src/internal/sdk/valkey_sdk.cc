#include "kv_cache_manager/client/src/internal/sdk/valkey_sdk.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

#if defined(USING_CUDA) && defined(USING_MUSA)
#error "ValkeySdk cannot enable CUDA and MUSA simultaneously"
#endif

#if defined(USING_CUDA)
#include "kv_cache_manager/client/src/internal/sdk/cuda_util.h"
#elif defined(USING_MUSA)
#include "kv_cache_manager/client/src/internal/sdk/musa_util.h"
#endif
#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/data_storage/valkey_uri_util.h"

namespace kv_cache_manager {
namespace {

constexpr std::size_t kMaximumSliceBlocks = 64;
constexpr std::size_t kMaximumSlicePayloadBytes = 1024 * 1024;

std::size_t SliceEnd(const std::vector<std::size_t> &expected_sizes, std::size_t begin) {
    std::size_t end = begin;
    std::size_t payload_bytes = 0;
    while (end < expected_sizes.size() && end - begin < kMaximumSliceBlocks) {
        const std::size_t block_bytes = expected_sizes[end];
        if (end != begin && block_bytes > kMaximumSlicePayloadBytes - payload_bytes) {
            break;
        }
        ++end;
        if (block_bytes > kMaximumSlicePayloadBytes) {
            break;
        }
        payload_bytes += block_bytes;
    }
    return end;
}

bool IsSupportedMemoryType(MemoryType type) {
    if (type == MemoryType::CPU) {
        return true;
    }
#if defined(USING_CUDA) || defined(USING_MUSA)
    return type == MemoryType::GPU;
#else
    return false;
#endif
}

#if defined(USING_CUDA)
using GpuError = cudaError_t;
using GpuMemcpyKind = cudaMemcpyKind;
using GpuPointerAttributes = cudaPointerAttributes;
constexpr GpuError kGpuSuccess = cudaSuccess;
constexpr GpuMemcpyKind kDeviceToHost = cudaMemcpyDeviceToHost;
constexpr GpuMemcpyKind kHostToDevice = cudaMemcpyHostToDevice;

const char *GpuErrorString(GpuError error) { return cudaGetErrorString(error); }
GpuError GpuPointerGetAttributes(GpuPointerAttributes *attributes, const void *pointer) {
    return cudaPointerGetAttributes(attributes, pointer);
}
GpuError GpuGetDevice(int *device) { return cudaGetDevice(device); }
GpuError GpuSetDevice(int device) { return cudaSetDevice(device); }
GpuError GpuMemcpy(void *destination, const void *source, std::size_t size, GpuMemcpyKind kind) {
    return cudaMemcpy(destination, source, size, kind);
}
#elif defined(USING_MUSA)
using GpuError = musaError_t;
using GpuMemcpyKind = musaMemcpyKind;
using GpuPointerAttributes = musaPointerAttributes;
constexpr GpuError kGpuSuccess = musaSuccess;
constexpr GpuMemcpyKind kDeviceToHost = musaMemcpyDeviceToHost;
constexpr GpuMemcpyKind kHostToDevice = musaMemcpyHostToDevice;

const char *GpuErrorString(GpuError error) { return musaGetErrorString(error); }
GpuError GpuPointerGetAttributes(GpuPointerAttributes *attributes, const void *pointer) {
    return musaPointerGetAttributes(attributes, pointer);
}
GpuError GpuGetDevice(int *device) { return musaGetDevice(device); }
GpuError GpuSetDevice(int device) { return musaSetDevice(device); }
GpuError GpuMemcpy(void *destination, const void *source, std::size_t size, GpuMemcpyKind kind) {
    return musaMemcpy(destination, source, size, kind);
}
#endif

#if defined(USING_CUDA) || defined(USING_MUSA)
void LogGpuError(const char *operation, const char *api, GpuError error) {
    KVCM_LOG_WARN("Valkey %s %s failed: error [%d] [%s]", operation, api, error, GpuErrorString(error));
}

ClientErrorCode CopyGpuOnOwningDevice(void *destination,
                                      const void *source,
                                      const void *device_pointer,
                                      std::size_t size,
                                      GpuMemcpyKind kind,
                                      const char *operation) {
    GpuPointerAttributes attributes{};
    GpuError operation_error = GpuPointerGetAttributes(&attributes, device_pointer);
    if (operation_error != kGpuSuccess) {
        LogGpuError(operation, "pointer attribute query", operation_error);
        return ER_CUDAMEMCPY_ERROR;
    }
    if (attributes.device < 0) {
        KVCM_LOG_WARN("Valkey %s pointer attribute query returned invalid device metadata", operation);
        return ER_CUDAMEMCPY_ERROR;
    }

    int original_device = -1;
    operation_error = GpuGetDevice(&original_device);
    if (operation_error != kGpuSuccess) {
        LogGpuError(operation, "get current device", operation_error);
        return ER_CUDAMEMCPY_ERROR;
    }

    const bool device_switch_required = original_device != attributes.device;
    if (device_switch_required) {
        operation_error = GpuSetDevice(attributes.device);
        if (operation_error != kGpuSuccess) {
            LogGpuError(operation, "set owning device", operation_error);
        }
    }
    if (operation_error == kGpuSuccess) {
        operation_error = GpuMemcpy(destination, source, size, kind);
        if (operation_error != kGpuSuccess) {
            LogGpuError(operation, "synchronous copy", operation_error);
        }
    }

    GpuError restore_error = kGpuSuccess;
    if (device_switch_required) {
        restore_error = GpuSetDevice(original_device);
        if (restore_error != kGpuSuccess) {
            LogGpuError(operation, "restore current device", restore_error);
        }
    }
    return operation_error == kGpuSuccess && restore_error == kGpuSuccess ? ER_OK : ER_CUDAMEMCPY_ERROR;
}
#endif

ClientErrorCode CopyFromCaller(char *destination, const Iov &iov, std::size_t source_offset, std::size_t size) {
    if (iov.type == MemoryType::CPU) {
        std::memcpy(destination, static_cast<const char *>(iov.base) + source_offset, size);
        return ER_OK;
    }
#if defined(USING_CUDA) || defined(USING_MUSA)
    const auto source_address = reinterpret_cast<std::uintptr_t>(iov.base) + source_offset;
    const void *source = reinterpret_cast<const void *>(source_address);
    return CopyGpuOnOwningDevice(destination, source, source, size, kDeviceToHost, "Put device-to-host copy");
#else
    return ER_UNSUPPORTED_MEMORY_TYPE;
#endif
}

ClientErrorCode CopyToCaller(const Iov &iov, const char *source) {
    if (iov.type == MemoryType::CPU) {
        std::memcpy(iov.base, source, iov.size);
        return ER_OK;
    }
#if defined(USING_CUDA) || defined(USING_MUSA)
    return CopyGpuOnOwningDevice(iov.base, source, iov.base, iov.size, kHostToDevice, "Get host-to-device copy");
#else
    return ER_UNSUPPORTED_MEMORY_TYPE;
#endif
}

} // namespace

ClientErrorCode ValkeySdk::Init(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                const std::shared_ptr<StorageConfig> &storage_config) {
    std::lock_guard init_lock(init_mutex_);
    if (get_client_pool_ || put_client_pool_) {
        KVCM_LOG_WARN("valkey sdk is already initialized; reinitialization is not supported");
        return ER_SDKINIT_ERROR;
    }
    if (!sdk_backend_config || typeid(*sdk_backend_config) != typeid(ValkeySdkConfig) ||
        !sdk_backend_config->Validate() || !sdk_backend_config->timeout_config().Validate() ||
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

    std::optional<SdkCallContext::CallerMemoryGuard> output_access;
    bool has_error = false;
    auto finish = [&output_access](ClientErrorCode result) {
        if (output_access) {
            output_access->Complete(result);
        }
        return result;
    };

    for (std::size_t begin = 0; begin < remote_uris.size();) {
        if (!context.IsLive()) {
            return finish(ER_SDK_TIMEOUT);
        }
        const std::size_t end = SliceEnd(expected_sizes, begin);
        ValkeyGetStringsResult result;
        try {
            const std::vector<std::string> slice_keys(redis_keys.begin() + begin, redis_keys.begin() + end);
            const std::vector<std::size_t> slice_sizes(expected_sizes.begin() + begin, expected_sizes.begin() + end);
            result = GetStringsUntil(*client, slice_keys, slice_sizes, context.deadline());
        } catch (const std::bad_alloc &) {
            return finish(context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT);
        } catch (const std::length_error &) {
            return finish(context.IsLive() ? ER_INVALID_LOCAL_BUFFERS : ER_SDK_TIMEOUT);
        } catch (...) {
            return finish(context.IsLive() ? ER_SDKREAD_ERROR : ER_SDK_TIMEOUT);
        }

        const std::size_t slice_blocks = end - begin;
        if (!context.IsLive() || result.operation_error == EC_TIMEOUT ||
            std::find(result.errors.begin(), result.errors.end(), EC_TIMEOUT) != result.errors.end()) {
            return finish(ER_SDK_TIMEOUT);
        }
        if (result.operation_error != EC_OK || result.errors.size() != slice_blocks ||
            result.values.size() != slice_blocks) {
            return finish(ER_SDKREAD_ERROR);
        }

        bool slice_has_scatter = false;
        for (std::size_t index = 0; index < slice_blocks; ++index) {
            if (result.errors[index] != EC_OK || result.values[index].size() != expected_sizes[begin + index]) {
                has_error = true;
                continue;
            }
            for (const auto &iov : local_buffers[begin + index].iovs) {
                if (!iov.ignore && iov.size != 0) {
                    slice_has_scatter = true;
                    break;
                }
            }
        }

        if (slice_has_scatter && !output_access) {
            output_access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
            if (!output_access) {
                return ER_SDK_TIMEOUT;
            }
        }

        if (slice_has_scatter) {
            for (std::size_t index = 0; index < slice_blocks; ++index) {
                if (result.errors[index] != EC_OK || result.values[index].size() != expected_sizes[begin + index]) {
                    continue;
                }
                std::size_t source_offset = 0;
                for (const auto &iov : local_buffers[begin + index].iovs) {
                    if (!iov.ignore && iov.size != 0) {
                        ec = CopyToCaller(iov, result.values[index].data() + source_offset);
                        if (ec != ER_OK) {
                            return finish(ec);
                        }
                    }
                    source_offset += iov.size;
                }
            }
        }
        begin = end;
    }

    return finish(has_error ? ER_SDKREAD_ERROR : ER_OK);
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

    for (std::size_t begin = 0; begin < remote_uris.size();) {
        if (!context.IsLive()) {
            return ER_SDK_TIMEOUT;
        }
        const std::size_t end = SliceEnd(expected_sizes, begin);
        {
            std::vector<std::string> values;
            ec = Gather(local_buffers, expected_sizes, begin, end, context, values);
            if (ec != ER_OK) {
                return ec;
            }
            if (!context.IsLive()) {
                return ER_SDK_TIMEOUT;
            }

            ValkeySetStringsResult result;
            try {
                const std::vector<std::string> slice_keys(redis_keys.begin() + begin, redis_keys.begin() + end);
                result = SetStringsUntil(*client, slice_keys, values, context.deadline());
            } catch (...) {
                return context.IsLive() ? ER_SDKWRITE_ERROR : ER_SDK_TIMEOUT;
            }

            const bool has_timeout =
                result.operation_error == EC_TIMEOUT ||
                std::find(result.errors.begin(), result.errors.end(), EC_TIMEOUT) != result.errors.end();
            const bool has_ambiguous =
                std::find(result.certainties.begin(), result.certainties.end(), ValkeyWriteCertainty::AMBIGUOUS) !=
                result.certainties.end();
            if (!context.IsLive() || has_timeout || has_ambiguous) {
                return ER_SDK_TIMEOUT;
            }
            const std::size_t slice_blocks = end - begin;
            if (result.operation_error != EC_OK || result.errors.size() != slice_blocks ||
                result.certainties.size() != slice_blocks) {
                return ER_SDKWRITE_ERROR;
            }
            for (std::size_t index = 0; index < slice_blocks; ++index) {
                if (result.errors[index] != EC_OK || result.certainties[index] != ValkeyWriteCertainty::CONFIRMED) {
                    return ER_SDKWRITE_ERROR;
                }
            }
        }
        begin = end;
    }

    auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    if (!access) {
        return ER_SDK_TIMEOUT;
    }
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
                    if (!IsSupportedMemoryType(iov.type)) {
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
                                  std::size_t begin,
                                  std::size_t end,
                                  const SdkCallContext &context,
                                  std::vector<std::string> &values) {
    // Allocate owned host destinations before admitting caller-memory access.
    // Each cancellation interval contains at most one synchronous 64 KiB copy.
    values.clear();
    try {
        values.reserve(end - begin);
        for (std::size_t index = begin; index < end; ++index) {
            values.emplace_back(expected_sizes[index], '\0');
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

    for (std::size_t block_index = begin; block_index < end; ++block_index) {
        std::size_t destination_offset = 0;
        for (const auto &iov : local_buffers[block_index].iovs) {
            const ClientErrorCode iov_ec = valkey_sdk_internal::CopyInBoundedChunks(
                iov.size, context, [&](std::size_t source_offset, std::size_t chunk_size) {
                    const ClientErrorCode copy_ec = CopyFromCaller(
                        values[block_index - begin].data() + destination_offset, iov, source_offset, chunk_size);
                    if (copy_ec == ER_OK) {
                        destination_offset += chunk_size;
                    }
                    return copy_ec;
                });
            if (iov_ec != ER_OK) {
                return iov_ec;
            }
        }
    }
    return ER_OK;
}

} // namespace kv_cache_manager
