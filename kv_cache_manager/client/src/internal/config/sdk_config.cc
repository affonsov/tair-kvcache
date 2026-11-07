#include "kv_cache_manager/client/src/internal/config/sdk_config.h"

#include <algorithm>
#include <sstream>
#include <typeinfo>

#include "kv_cache_manager/common/logger.h"

namespace kv_cache_manager {

bool SdkTimeoutConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_MACRO(rapid_value, "put_timeout_ms", put_timeout_ms_);
    KVCM_JSON_GET_MACRO(rapid_value, "get_timeout_ms", get_timeout_ms_);
    return true;
}

void SdkTimeoutConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "put_timeout_ms", put_timeout_ms_);
    Put(writer, "get_timeout_ms", get_timeout_ms_);
}

bool SdkTimeoutConfig::Validate() const { return put_timeout_ms_ > 0 && get_timeout_ms_ > 0; }

bool SdkBackendConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    auto type_itr = rapid_value.FindMember("type");
    if (type_itr != rapid_value.MemberEnd()) {
        if (type_itr->value.IsString()) {
            type_ = ToDataStorageType(type_itr->value.GetString());
        }
    }
    KVCM_JSON_GET_MACRO(rapid_value, "sdk_log_file_path", sdk_log_file_path_);
    KVCM_JSON_GET_MACRO(rapid_value, "sdk_log_level", sdk_log_level_);
    KVCM_JSON_GET_MACRO(rapid_value, "spec_byte_sizes_per_block", spec_byte_sizes_per_block_);
    return true;
}

void SdkBackendConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "type", ToString(type_));
    Put(writer, "sdk_log_file_path", sdk_log_file_path_);
    Put(writer, "sdk_log_level", sdk_log_level_);
    Put(writer, "spec_byte_sizes_per_block", spec_byte_sizes_per_block_);
}

bool SdkBackendConfig::Validate() const { return DataStorageType::DATA_STORAGE_TYPE_UNKNOWN != type_; }

Hf3fsSdkConfig::Hf3fsSdkConfig() { set_type(DataStorageType::DATA_STORAGE_TYPE_HF3FS); }

bool Hf3fsSdkConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_MACRO(rapid_value, "mountpoint", mountpoint_);
    KVCM_JSON_GET_MACRO(rapid_value, "root_dir", root_dir_);
    KVCM_JSON_GET_MACRO(rapid_value, "read_iov_block_size", read_iov_block_size_);
    KVCM_JSON_GET_MACRO(rapid_value, "read_iov_size", read_iov_size_);
    KVCM_JSON_GET_MACRO(rapid_value, "write_iov_block_size", write_iov_block_size_);
    KVCM_JSON_GET_MACRO(rapid_value, "write_iov_size", write_iov_size_);
    return SdkBackendConfig::FromRapidValue(rapid_value);
}

void Hf3fsSdkConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "mountpoint", mountpoint_);
    Put(writer, "root_dir", root_dir_);
    Put(writer, "read_iov_block_size", read_iov_block_size_);
    Put(writer, "read_iov_size", read_iov_size_);
    Put(writer, "write_iov_block_size", write_iov_block_size_);
    Put(writer, "write_iov_size", write_iov_size_);
    SdkBackendConfig::ToRapidWriter(writer);
}

bool Hf3fsSdkConfig::Validate() const {
    // TODO: add more validate
    return SdkBackendConfig::Validate();
}

std::string Hf3fsSdkConfig::ToString() const {
    std::ostringstream oss;
    oss << "mountpoint: " << mountpoint_ << ", root_dir: " << root_dir_
        << ", read_iov_block_size: " << read_iov_block_size_ << ", read_iov_size: " << read_iov_size_
        << ", write_iov_block_size: " << write_iov_block_size_ << ", write_iov_size: " << write_iov_size_;
    return oss.str();
}

MooncakeSdkConfig::MooncakeSdkConfig() { set_type(DataStorageType::DATA_STORAGE_TYPE_MOONCAKE); }

bool MooncakeSdkConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_MACRO(rapid_value, "location", location_);
    KVCM_JSON_GET_MACRO(rapid_value, "put_replica_num", put_replica_num_);
    return SdkBackendConfig::FromRapidValue(rapid_value);
}

void MooncakeSdkConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "location", location_);
    Put(writer, "put_replica_num", put_replica_num_);
    SdkBackendConfig::ToRapidWriter(writer);
}

bool MooncakeSdkConfig::Validate() const {
    return !location_.empty() && put_replica_num_ > 0 && SdkBackendConfig::Validate();
}

std::string MooncakeSdkConfig::ToString() const {
    std::ostringstream oss;
    oss << "local_mem_ptr: " << local_mem_ptr_ << ", local_buffer_size:" << local_buffer_size_
        << ", location: " << location_ << ", put_replica_num:" << put_replica_num_
        << ", set_self_location_spec_name:" << self_location_spec_name_;
    return oss.str();
}

TairMempoolSdkConfig::TairMempoolSdkConfig(DataStorageType type) { set_type(type); }

bool TairMempoolSdkConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    return SdkBackendConfig::FromRapidValue(rapid_value);
};

void TairMempoolSdkConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    SdkBackendConfig::ToRapidWriter(writer);
}

NfsSdkConfig::NfsSdkConfig() { set_type(DataStorageType::DATA_STORAGE_TYPE_NFS); }

bool NfsSdkConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    return SdkBackendConfig::FromRapidValue(rapid_value);
};

void NfsSdkConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    SdkBackendConfig::ToRapidWriter(writer);
}

ValkeySdkConfig::ValkeySdkConfig() { set_type(DataStorageType::DATA_STORAGE_TYPE_VALKEY); }

bool SdkWrapperConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_MACRO(rapid_value, "thread_num", thread_num_);
    KVCM_JSON_GET_MACRO(rapid_value, "queue_size", queue_size_);
    auto sdk_backend_configs_iter = rapid_value.FindMember("sdk_backend_configs");
    if (sdk_backend_configs_iter != rapid_value.MemberEnd() && sdk_backend_configs_iter->value.IsArray()) {
        for (const auto &sdk_val : sdk_backend_configs_iter->value.GetArray()) {
            auto type_it = sdk_val.FindMember("type");
            if (type_it == sdk_val.MemberEnd() || !type_it->value.IsString()) {
                continue;
            }
            DataStorageType type = ToDataStorageType(type_it->value.GetString());
            std::shared_ptr<SdkBackendConfig> sdk_backend_config;
            switch (type) {
            case DataStorageType::DATA_STORAGE_TYPE_HF3FS:
                sdk_backend_config = std::make_shared<Hf3fsSdkConfig>();
                break;
            case DataStorageType::DATA_STORAGE_TYPE_VCNS_HF3FS:
                sdk_backend_config = std::make_shared<Hf3fsSdkConfig>();
                break;
            case DataStorageType::DATA_STORAGE_TYPE_MOONCAKE:
                sdk_backend_config = std::make_shared<MooncakeSdkConfig>();
                break;
            case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL:
            case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD:
                sdk_backend_config = std::make_shared<TairMempoolSdkConfig>(type);
                break;
            case DataStorageType::DATA_STORAGE_TYPE_NFS:
                sdk_backend_config = std::make_shared<NfsSdkConfig>();
                break;
            case DataStorageType::DATA_STORAGE_TYPE_VALKEY:
                sdk_backend_config = std::make_shared<ValkeySdkConfig>();
                break;
            default:
                break;
            }
            if (!sdk_backend_config)
                continue;

            if (!sdk_backend_config->FromRapidValue(sdk_val))
                continue;
            sdk_backend_configs_map_.insert_or_assign(type, sdk_backend_config);
        }
    }
    KVCM_JSON_GET_MACRO(rapid_value, "timeout_config", timeout_config_);
    return true;
}

void SdkWrapperConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "thread_num", thread_num_);
    Put(writer, "queue_size", queue_size_);
    std::vector<const SdkBackendConfig *> sdk_backend_configs;
    sdk_backend_configs.reserve(sdk_backend_configs_map_.size());
    for (const auto &pair : sdk_backend_configs_map_) {
        sdk_backend_configs.push_back(pair.second.get());
    }
    Put(writer, "sdk_backend_configs", sdk_backend_configs);
    Put(writer, "timeout_config", timeout_config_);
}

bool SdkWrapperConfig::Validate() const {
    return std::all_of(sdk_backend_configs_map_.begin(),
                       sdk_backend_configs_map_.end(),
                       [](const auto &pair) {
                           auto sdk_backend_config = pair.second;
                           return sdk_backend_config->Validate();
                       }) &&
           timeout_config_.Validate();
}

std::shared_ptr<SdkBackendConfig> SdkWrapperConfig::GetSdkBackendConfig(DataStorageType type) const {
    if (sdk_backend_configs_map_.find(type) != sdk_backend_configs_map_.end()) {
        return sdk_backend_configs_map_.at(type);
    }
    return nullptr;
}

bool SdkWrapperConfig::operator==(const SdkWrapperConfig &other) const {
    if (sdk_backend_configs_map_.size() != other.sdk_backend_configs_map_.size()) {
        return false;
    }

    for (const auto &[type, config] : sdk_backend_configs_map_) {
        const auto other_it = other.sdk_backend_configs_map_.find(type);
        if (other_it == other.sdk_backend_configs_map_.end()) {
            return false;
        }
        const auto &other_config = other_it->second;
        if (!config || !other_config) {
            if (config != other_config) {
                return false;
            }
            continue;
        }
        if (typeid(*config) != typeid(*other_config) || config->type() != type || other_config->type() != type) {
            return false;
        }

        bool equal = false;
        switch (type) {
        case DataStorageType::DATA_STORAGE_TYPE_HF3FS:
        case DataStorageType::DATA_STORAGE_TYPE_VCNS_HF3FS:
            if (typeid(*config) != typeid(Hf3fsSdkConfig)) {
                return false;
            }
            equal = static_cast<const Hf3fsSdkConfig &>(*config) == static_cast<const Hf3fsSdkConfig &>(*other_config);
            break;
        case DataStorageType::DATA_STORAGE_TYPE_MOONCAKE:
            if (typeid(*config) != typeid(MooncakeSdkConfig)) {
                return false;
            }
            equal = static_cast<const MooncakeSdkConfig &>(*config) ==
                    static_cast<const MooncakeSdkConfig &>(*other_config);
            break;
        case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL:
        case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD:
            if (typeid(*config) != typeid(TairMempoolSdkConfig)) {
                return false;
            }
            equal = static_cast<const TairMempoolSdkConfig &>(*config) ==
                    static_cast<const TairMempoolSdkConfig &>(*other_config);
            break;
        case DataStorageType::DATA_STORAGE_TYPE_NFS:
            if (typeid(*config) != typeid(NfsSdkConfig)) {
                return false;
            }
            equal = static_cast<const NfsSdkConfig &>(*config) == static_cast<const NfsSdkConfig &>(*other_config);
            break;
        case DataStorageType::DATA_STORAGE_TYPE_VALKEY:
            if (typeid(*config) != typeid(ValkeySdkConfig)) {
                return false;
            }
            equal =
                static_cast<const ValkeySdkConfig &>(*config) == static_cast<const ValkeySdkConfig &>(*other_config);
            break;
        default:
            return false;
        }
        if (!equal) {
            return false;
        }
    }

    return thread_num_ == other.thread_num_ && queue_size_ == other.queue_size_ &&
           timeout_config_ == other.timeout_config_;
}

bool SdkWrapperConfig::operator!=(const SdkWrapperConfig &other) const { return !(*this == other); }

std::shared_ptr<SdkBackendConfig> CloneSdkBackendConfig(const std::shared_ptr<SdkBackendConfig> &source) {
    if (!source) {
        return nullptr;
    }

    switch (source->type()) {
    case DataStorageType::DATA_STORAGE_TYPE_HF3FS:
    case DataStorageType::DATA_STORAGE_TYPE_VCNS_HF3FS:
        if (typeid(*source) != typeid(Hf3fsSdkConfig)) {
            return nullptr;
        }
        return std::make_shared<Hf3fsSdkConfig>(static_cast<const Hf3fsSdkConfig &>(*source));
    case DataStorageType::DATA_STORAGE_TYPE_MOONCAKE:
        if (typeid(*source) != typeid(MooncakeSdkConfig)) {
            return nullptr;
        }
        return std::make_shared<MooncakeSdkConfig>(static_cast<const MooncakeSdkConfig &>(*source));
    case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL:
    case DataStorageType::DATA_STORAGE_TYPE_TAIR_MEMPOOL_SSD:
        if (typeid(*source) != typeid(TairMempoolSdkConfig)) {
            return nullptr;
        }
        return std::make_shared<TairMempoolSdkConfig>(static_cast<const TairMempoolSdkConfig &>(*source));
    case DataStorageType::DATA_STORAGE_TYPE_NFS:
        if (typeid(*source) != typeid(NfsSdkConfig)) {
            return nullptr;
        }
        return std::make_shared<NfsSdkConfig>(static_cast<const NfsSdkConfig &>(*source));
    case DataStorageType::DATA_STORAGE_TYPE_VALKEY:
        // Reject future subclasses until their additional state has an
        // explicit copy path.
        if (typeid(*source) != typeid(ValkeySdkConfig)) {
            return nullptr;
        }
        return std::make_shared<ValkeySdkConfig>(static_cast<const ValkeySdkConfig &>(*source));
    default:
        return nullptr;
    }
}

std::shared_ptr<SdkWrapperConfig> CloneSdkWrapperConfig(const std::shared_ptr<SdkWrapperConfig> &source) {
    if (!source) {
        return nullptr;
    }

    auto clone = std::make_shared<SdkWrapperConfig>();
    clone->set_thread_num(source->thread_num());
    clone->set_queue_size(source->queue_size());
    clone->set_timeout_config(source->timeout_config());
    std::unordered_map<DataStorageType, std::shared_ptr<SdkBackendConfig>> backend_configs;
    backend_configs.reserve(source->sdk_backend_configs_map().size());
    for (const auto &[type, backend_config] : source->sdk_backend_configs_map()) {
        if (!backend_config || backend_config->type() != type) {
            return nullptr;
        }
        auto backend_clone = CloneSdkBackendConfig(backend_config);
        if (!backend_clone) {
            return nullptr;
        }
        backend_configs.emplace(type, std::move(backend_clone));
    }
    clone->set_sdk_backend_configs_map(backend_configs);
    return clone;
}

} // namespace kv_cache_manager
