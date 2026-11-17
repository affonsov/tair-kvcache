#include "kv_cache_manager/data_storage/valkey_uri_util.h"

#include <cctype>
#include <charconv>
#include <cstdint>
#include <string_view>

namespace kv_cache_manager {
namespace {

constexpr std::size_t kOpaqueIdLength = 32;

bool IsValidStorageIdentity(const std::string &identity) {
    if (identity.empty()) {
        return false;
    }
    for (const unsigned char ch : identity) {
        if (!std::isalnum(ch) && ch != '.' && ch != '-' && ch != '_') {
            return false;
        }
    }
    return true;
}

bool IsValidOpaqueId(std::string_view opaque_id) {
    if (opaque_id.size() != kOpaqueIdLength) {
        return false;
    }
    for (const char ch : opaque_id) {
        if (!std::isdigit(static_cast<unsigned char>(ch)) && (ch < 'a' || ch > 'f')) {
            return false;
        }
    }
    return true;
}

bool InspectValkeyLocation(const DataStorageUri &uri,
                           const std::string &expected_storage_identity,
                           std::string_view &out_opaque_id) {
    out_opaque_id = {};
    if (!IsValidStorageIdentity(expected_storage_identity) || !uri.Valid() || uri.GetProtocol() != "valkey" ||
        !uri.GetUserInfo().empty() || uri.GetHostName() != expected_storage_identity || uri.GetPort() != 0 ||
        uri.GetPath().size() != kOpaqueIdLength + 1 || uri.GetPath().front() != '/' || !uri.HasParam("size")) {
        return false;
    }

    out_opaque_id = std::string_view(uri.GetPath()).substr(1);
    if (!IsValidOpaqueId(out_opaque_id)) {
        return false;
    }

    const std::string size_string = uri.GetParam("size");
    if (size_string.empty() || size_string.front() == '-') {
        return false;
    }
    uint64_t size = 0;
    const auto [size_end, size_ec] = std::from_chars(size_string.data(), size_string.data() + size_string.size(), size);
    if (size_ec != std::errc{} || size_end != size_string.data() + size_string.size() ||
        std::to_string(size) != size_string) {
        return false;
    }

    const std::string expected_uri =
        "valkey://" + expected_storage_identity + "/" + std::string(out_opaque_id) + "?size=" + size_string;
    return uri.ToUriString() == expected_uri;
}

} // namespace

bool BuildValkeyLocation(const std::string &storage_identity,
                         const std::string &opaque_id,
                         std::size_t size,
                         DataStorageUri &out_uri) {
    out_uri = {};
    if (!IsValidStorageIdentity(storage_identity) || !IsValidOpaqueId(opaque_id)) {
        return false;
    }

    DataStorageUri uri;
    uri.SetProtocol("valkey");
    uri.SetHostName(storage_identity);
    uri.SetPath("/" + opaque_id);
    uri.SetParam("size", std::to_string(size));
    out_uri = std::move(uri);
    return true;
}

bool ValidateValkeyLocation(const DataStorageUri &uri, const std::string &expected_storage_identity) {
    std::string_view opaque_id;
    return InspectValkeyLocation(uri, expected_storage_identity, opaque_id);
}

bool ValkeyLocationToRedisKey(const DataStorageUri &uri,
                              const std::string &expected_storage_identity,
                              std::string &out_key) {
    out_key.clear();
    std::string_view opaque_id;
    if (!InspectValkeyLocation(uri, expected_storage_identity, opaque_id)) {
        return false;
    }
    out_key = "kvcm:valkey:" + std::to_string(expected_storage_identity.size()) + ":" + expected_storage_identity +
              ":" + std::string(opaque_id);
    return true;
}

} // namespace kv_cache_manager
