#pragma once

#if defined(USING_CUDA) && defined(USING_MUSA)
#error "ValkeySdk cannot enable CUDA and MUSA simultaneously"
#endif

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "kv_cache_manager/client/src/internal/sdk/sdk_interface.h"
#include "kv_cache_manager/common/client_pool.h"
#include "kv_cache_manager/common/redis_client.h"

namespace kv_cache_manager {
namespace valkey_sdk_internal {

inline constexpr std::size_t kCopyChunkBytes = 64 * 1024;

template <typename CopyChunk>
ClientErrorCode CopyInBoundedChunks(std::size_t size, const SdkCallContext &context, CopyChunk copy_chunk) {
    std::size_t offset = 0;
    while (offset < size) {
        if (!context.IsLive()) {
            return ER_SDK_TIMEOUT;
        }
        const std::size_t chunk_size = std::min(kCopyChunkBytes, size - offset);
        const ClientErrorCode ec = copy_chunk(offset, chunk_size);
        if (ec != ER_OK) {
            return ec;
        }
        offset += chunk_size;
    }
    return ER_OK;
}

} // namespace valkey_sdk_internal

class ValkeySdk : public SdkInterface, public ContextualSdkInterface {
public:
    ValkeySdk() = default;
    ~ValkeySdk() override = default;

    ClientErrorCode Init(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                         const std::shared_ptr<StorageConfig> &storage_config) override;
    SdkType Type() override { return SdkType::VALKEY; }
    ClientErrorCode Get(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers &local_buffers) override;
    ClientErrorCode Put(const std::vector<DataStorageUri> &remote_uris,
                        const BlockBuffers &local_buffers,
                        std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) override;
    ClientErrorCode GetWithContext(const std::vector<DataStorageUri> &remote_uris,
                                   const BlockBuffers &local_buffers,
                                   const SdkCallContext &context) override;
    ClientErrorCode PutWithContext(const std::vector<DataStorageUri> &remote_uris,
                                   const BlockBuffers &local_buffers,
                                   std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris,
                                   const SdkCallContext &context) override;

protected:
    using RedisClientPool = StaticClientPool<RedisClient>;
    using RedisClientHandle = ClientPool<RedisClient>::ClientHandle;
    using Deadline = std::chrono::steady_clock::time_point;

    ClientErrorCode Alloc(const std::vector<DataStorageUri> &remote_uris,
                          std::vector<DataStorageUri> &alloc_uris) override;
    virtual std::shared_ptr<RedisClient> CreateClient(const RedisConnectionOptions &options);
    virtual bool PrepareClient(RedisClient &client);
    virtual RedisClientHandle AcquireGetClientUntil(Deadline deadline);
    virtual RedisClientHandle AcquirePutClientUntil(Deadline deadline);
    virtual ValkeySetStringsResult SetStringsUntil(RedisClient &client,
                                                   const std::vector<std::string> &keys,
                                                   const std::vector<std::string> &values,
                                                   Deadline deadline);
    virtual ValkeyGetStringsResult GetStringsUntil(RedisClient &client,
                                                   const std::vector<std::string> &keys,
                                                   const std::vector<std::size_t> &expected_sizes,
                                                   Deadline deadline);

private:
    static ClientErrorCode Preflight(const std::vector<DataStorageUri> &remote_uris,
                                     const BlockBuffers &local_buffers,
                                     const std::string &storage_identity,
                                     bool allow_ignored_for_read,
                                     std::vector<std::string> &redis_keys,
                                     std::vector<std::size_t> &expected_sizes);
    ClientErrorCode Gather(const BlockBuffers &local_buffers,
                           const std::vector<std::size_t> &expected_sizes,
                           std::size_t begin,
                           std::size_t end,
                           const SdkCallContext &context,
                           std::vector<std::string> &values);

    std::string storage_identity_;
    SdkTimeoutConfig timeout_config_;
    std::unique_ptr<RedisClientPool> get_client_pool_;
    std::unique_ptr<RedisClientPool> put_client_pool_;
};

} // namespace kv_cache_manager
