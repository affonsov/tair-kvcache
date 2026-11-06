#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "kv_cache_manager/client/src/internal/sdk/sdk_interface.h"
#include "kv_cache_manager/common/client_pool.h"
#include "kv_cache_manager/common/redis_client.h"

namespace kv_cache_manager {

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
    virtual std::shared_ptr<RedisClient> CreateClient(const StandardUri &endpoint);
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
    virtual void OnSourceChunkRead(std::size_t block_index, std::size_t iov_index, std::size_t chunk_size);
    virtual void OnOutputCommitAdmitted();

private:
    static ClientErrorCode Preflight(const std::vector<DataStorageUri> &remote_uris,
                                     const BlockBuffers &local_buffers,
                                     const std::string &storage_identity,
                                     bool allow_ignored_for_read,
                                     std::vector<std::string> &redis_keys,
                                     std::vector<std::size_t> &expected_sizes);
    ClientErrorCode Gather(const BlockBuffers &local_buffers,
                           const std::vector<std::size_t> &expected_sizes,
                           const SdkCallContext &context,
                           std::vector<std::string> &values);

    std::string storage_identity_;
    SdkTimeoutConfig timeout_config_;
    std::unique_ptr<RedisClientPool> get_client_pool_;
    std::unique_ptr<RedisClientPool> put_client_pool_;
};

} // namespace kv_cache_manager
