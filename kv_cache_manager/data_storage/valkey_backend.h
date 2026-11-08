#pragma once

#include <memory>
#include <string>

#include "kv_cache_manager/common/client_pool.h"
#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/data_storage/data_storage_backend.h"

namespace kv_cache_manager {

class ValkeyDataStorageBackend : public DataStorageBackend {
public:
    ValkeyDataStorageBackend() = delete;
    explicit ValkeyDataStorageBackend(std::shared_ptr<MetricsRegistry> metrics_registry);
    ~ValkeyDataStorageBackend() override;

    DataStorageType GetType() override;
    bool Available() override;
    double GetStorageUsageRatio(const std::string &trace_id) const override;

    ErrorCode DoOpen(const StorageConfig &storage_config, const std::string &trace_id) override;
    ErrorCode Close() override;

    std::vector<std::pair<ErrorCode, DataStorageUri>> Create(const std::vector<std::string> &keys,
                                                             std::size_t size_per_key,
                                                             const std::string &trace_id,
                                                             std::function<void()> cb) override;
    std::vector<ErrorCode> Delete(const std::vector<DataStorageUri> &storage_uris,
                                  const std::string &trace_id,
                                  std::function<void()> cb) override;
    std::vector<bool> Exist(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<bool> MightExist(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<ErrorCode> Lock(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<ErrorCode> UnLock(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<ErrorCode> Copy(const std::vector<DataStorageUri> &src_uris,
                                const std::vector<DataStorageUri> &dst_uris,
                                const std::string &trace_id) override;

protected:
    virtual std::shared_ptr<RedisClient> CreateClient(const RedisConnectionOptions &options);
    virtual std::string GenerateOpaqueId();

private:
    std::string storage_identity_;
    std::unique_ptr<StaticClientPool<RedisClient>> client_pool_;
};

} // namespace kv_cache_manager
