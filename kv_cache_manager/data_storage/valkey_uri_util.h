#pragma once

#include <cstddef>
#include <string>

#include "kv_cache_manager/data_storage/data_storage_uri.h"

namespace kv_cache_manager {

bool BuildValkeyLocation(const std::string &storage_identity,
                         const std::string &opaque_id,
                         std::size_t size,
                         DataStorageUri &out_uri);

bool ValidateValkeyLocation(const DataStorageUri &uri, const std::string &expected_storage_identity);

bool ValkeyLocationToRedisKey(const DataStorageUri &uri,
                              const std::string &expected_storage_identity,
                              std::string &out_key);

} // namespace kv_cache_manager
