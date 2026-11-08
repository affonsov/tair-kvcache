#include "kv_cache_manager/client/src/internal/sdk/sdk_wrapper.h"

#include <fcntl.h>
#include <limits>
#include <shared_mutex>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>

#include "kv_cache_manager/client/src/internal/sdk/sdk_factory.h"
#include "kv_cache_manager/client/src/internal/sdk/sdk_interface.h"
#include "kv_cache_manager/common/lock_free_thread_pool.h"
#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/data_storage/storage_config.h"

namespace kv_cache_manager {
namespace {

struct PendingInitResources {
    ~PendingInitResources() {
        if (thread_pool) {
            thread_pool->stop();
        }
        if (owned_fd >= 0) {
            close(owned_fd);
        }
    }

    std::unique_ptr<LockFreeThreadPool> thread_pool;
    int owned_fd{-1};
};

std::shared_mutex &SdkWrapperLifecycleMutex() {
    static std::shared_mutex mutex;
    return mutex;
}

class ContextCleanup {
public:
    explicit ContextCleanup(const SdkCallContext &context) : context_(context) {}
    ~ContextCleanup() {
        context_.Cancel();
        context_.WaitForCallerMemory();
    }

private:
    const SdkCallContext &context_;
};

} // namespace

SdkWrapper::SdkWrapper() : sdk_factory_(SdkFactory::GetInstance()) {}

SdkWrapper::~SdkWrapper() {
    std::unique_lock lifecycle_lock(SdkWrapperLifecycleMutex());
    if (wait_task_thread_pool_) {
        wait_task_thread_pool_->stop();
        wait_task_thread_pool_.reset();
    }
    sdk_map_.clear();
    if (owned_shm_fd_ >= 0) {
        close(owned_shm_fd_);
        owned_shm_fd_ = -1;
    }
}

ClientErrorCode SdkWrapper::Init(const std::unique_ptr<ClientConfig> &client_config,
                                 const InitParams &init_params,
                                 const ClientMemoryRegistrations *memory_registrations) {
    std::unique_lock lifecycle_lock(SdkWrapperLifecycleMutex());
    if (wait_task_thread_pool_) {
        KVCM_LOG_WARN("sdk wrapper is already initialized");
        return ER_INVALID_SDKWRAPPER_CONFIG;
    }

    PendingInitResources pending;
    try {
        if (!client_config) {
            KVCM_LOG_WARN("client config is null");
            return ER_INVALID_CLIENT_CONFIG;
        }
        auto wrapper_config = CloneSdkWrapperConfig(client_config->sdk_wrapper_config());
        if (!wrapper_config) {
            KVCM_LOG_WARN("sdk wrapper config is null or cannot be copied");
            return ER_INVALID_SDKWRAPPER_CONFIG;
        }

        std::vector<std::shared_ptr<StorageConfig>> storage_configs;
        if (!Jsonizable::FromJsonString(init_params.storage_configs, storage_configs)) {
            KVCM_LOG_WARN("parse storage config failed, storage_config_bytes: %zu", init_params.storage_configs.size());
            return ER_INVALID_STORAGE_CONFIG;
        }
        if (storage_configs.empty()) {
            KVCM_LOG_WARN("storage config is empty");
            return ER_INVALID_STORAGE_CONFIG;
        }

        SharedMemoryRegistration prepared_registration;
        const SharedMemoryRegistration *active_registration = nullptr;
        if (memory_registrations != nullptr) {
            auto ec = PrepareSharedMemoryRegistrationCandidate(
                memory_registrations->host, prepared_registration, pending.owned_fd);
            if (ec != ER_OK) {
                return ec;
            }
            if (prepared_registration.fd >= 0) {
                active_registration = &prepared_registration;
            }
        }
        ClientMemoryRegistrations prepared_memory_registrations;
        if (active_registration != nullptr) {
            prepared_memory_registrations.host = *active_registration;
        }
        if (memory_registrations != nullptr) {
            prepared_memory_registrations.gpu = memory_registrations->gpu;
        }

        auto regist_span = init_params.regist_span;
        const auto &location_spec_infos = client_config->location_spec_infos();
        if (location_spec_infos.empty()) {
            KVCM_LOG_WARN("location_spec_infos is empty");
            return ER_INVALID_CLIENT_CONFIG;
        }
        if (location_spec_infos.find(init_params.self_location_spec_name) == location_spec_infos.end()) {
            KVCM_LOG_WARN("location_spec_infos does not contain self_location_spec_name [%s]",
                          init_params.self_location_spec_name.c_str());
            return ER_INVALID_CLIENT_CONFIG;
        }

        pending.thread_pool = std::make_unique<LockFreeThreadPool>(
            wrapper_config->thread_num(), wrapper_config->queue_size(), "SdkWaitTaskPool");
        if (!pending.thread_pool->start()) {
            KVCM_LOG_WARN("start wait task thread pool failed, thread num: %zu, queue size: %zu",
                          wrapper_config->thread_num(),
                          wrapper_config->queue_size());
            return ER_THREADPOOL_ERROR;
        }

        std::map<std::string, std::shared_ptr<SdkInterface>> sdk_map;
        for (const auto &storage_config : storage_configs) {
            DataStorageType type = storage_config->type();
            const auto &sdk_backend_config = wrapper_config->GetSdkBackendConfig(type);
            if (!sdk_backend_config) {
                KVCM_LOG_WARN("sdk backend config is null, storage config: %s", storage_config->ToString().c_str());
                return ER_INVALID_SDKBACKEND_CONFIG;
            }
            auto ec = UpdateMooncakeSdkConfig(sdk_backend_config, regist_span, init_params.self_location_spec_name);
            if (ec != ER_OK) {
                KVCM_LOG_WARN("fill span failed, storage config: %s", storage_config->ToString().c_str());
                return ec;
            }
            ec = UpdateTairMempoolSdkConfig(sdk_backend_config,
                                            memory_registrations == nullptr ? nullptr : &prepared_memory_registrations);
            if (ec != ER_OK) {
                KVCM_LOG_WARN("fill tair mempool span failed, storage config: %s", storage_config->ToString().c_str());
                return ec;
            }

            sdk_backend_config->set_spec_byte_sizes_per_block(location_spec_infos);
            sdk_backend_config->set_timeout_config(wrapper_config->timeout_config());

            auto sdk = sdk_factory_->CreateSdk(type, sdk_backend_config, storage_config);
            if (!sdk) {
                KVCM_LOG_WARN("create sdk failed, storage config: %s", storage_config->ToString().c_str());
                return ER_CREATESDK_ERROR;
            }
            sdk_map.insert({storage_config->global_unique_name(), std::move(sdk)});
        }

        wrapper_config_ = std::move(wrapper_config);
        storage_configs_ = std::move(storage_configs);
        wait_task_thread_pool_ = std::move(pending.thread_pool);
        sdk_map_ = std::move(sdk_map);
        if (owned_shm_fd_ >= 0) {
            close(owned_shm_fd_);
        }
        owned_shm_fd_ = pending.owned_fd;
        pending.owned_fd = -1;
        return ER_OK;
    } catch (const std::exception &e) {
        KVCM_LOG_WARN("sdk wrapper initialization threw an exception: %s", e.what());
    } catch (...) {
        KVCM_LOG_WARN("sdk wrapper initialization threw an unknown exception");
    }
    return ER_SDKINIT_ERROR;
}

ClientErrorCode SdkWrapper::GroupBySdk(const std::vector<DataStorageUri> &remote_uris,
                                       const BlockBuffers &local_buffers,
                                       std::vector<SdkGroup> &groups) {
    std::unordered_map<std::string, size_t> hostname_to_group;
    for (size_t i = 0; i < remote_uris.size(); ++i) {
        std::string hostname = remote_uris[i].GetHostName();
        if (hostname.empty()) {
            KVCM_LOG_WARN("GroupBySdk: remote_uri %s has empty hostname", remote_uris[i].ToUriString().c_str());
            return ER_GETSDK_ERROR;
        }
        auto it = hostname_to_group.find(hostname);
        if (it == hostname_to_group.end()) {
            auto sdk = GetSdk(remote_uris[i]);
            if (!sdk) {
                KVCM_LOG_WARN("GroupBySdk: no sdk found for hostname: %s", hostname.c_str());
                return ER_GETSDK_ERROR;
            }
            hostname_to_group[hostname] = groups.size();
            groups.push_back({sdk, {}, {}, {}});
        }
        auto &group = groups[hostname_to_group[hostname]];
        group.indices.push_back(i);
        group.uris.push_back(remote_uris[i]);
        group.buffers.push_back(local_buffers[i]);
    }
    return ER_OK;
}

ClientErrorCode SdkWrapper::Get(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers &local_buffers) {
    std::shared_lock lifecycle_lock(SdkWrapperLifecycleMutex());
    if (!wait_task_thread_pool_) {
        return ER_INVALID_SDKWRAPPER_CONFIG;
    }
    const SdkCallContext context(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(wrapper_config_->timeout_config().get_timeout_ms()));
    const ContextCleanup context_cleanup(context);
    auto ec = Valid(remote_uris, local_buffers);
    if (ec != ER_OK) {
        return ec;
    }
    std::vector<SdkGroup> groups;
    ec = GroupBySdk(remote_uris, local_buffers, groups);
    if (ec != ER_OK) {
        return ec;
    }

    std::vector<ParallelTask> tasks;
    tasks.reserve(groups.size());
    for (const auto &group : groups) {
        auto *contextual_sdk = dynamic_cast<ContextualSdkInterface *>(group.sdk.get());
        tasks.push_back({[group, contextual_sdk](const SdkCallContext &task_context) {
                             if (contextual_sdk != nullptr) {
                                 return contextual_sdk->GetWithContext(group.uris, group.buffers, task_context);
                             }
                             return group.sdk->Get(group.uris, group.buffers);
                         },
                         contextual_sdk != nullptr});
    }

    return RunWithTimeoutParallel(OpType::GET, std::move(tasks), context);
}

ClientErrorCode SdkWrapper::Put(const std::vector<DataStorageUri> &remote_uris,
                                const BlockBuffers &local_buffers,
                                std::shared_ptr<std::vector<DataStorageUri>> actual_remote_uris) {
    std::shared_lock lifecycle_lock(SdkWrapperLifecycleMutex());
    if (!wait_task_thread_pool_) {
        return ER_INVALID_SDKWRAPPER_CONFIG;
    }
    const SdkCallContext context(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(wrapper_config_->timeout_config().put_timeout_ms()));
    const ContextCleanup context_cleanup(context);
    auto ec = Valid(remote_uris, local_buffers);
    if (ec != ER_OK) {
        KVCM_LOG_WARN("put failed, remote_uris or local_buffers invalid.");
        return ec;
    }
    if (!actual_remote_uris) {
        return ER_INVALID_PARAMS;
    }
    std::vector<SdkGroup> groups;
    ec = GroupBySdk(remote_uris, local_buffers, groups);
    if (ec != ER_OK) {
        return ec;
    }

    std::vector<ParallelTask> tasks;
    std::vector<std::shared_ptr<std::vector<DataStorageUri>>> group_results;
    tasks.reserve(groups.size());
    group_results.reserve(groups.size());

    for (const auto &group : groups) {
        auto group_actual_uris = std::make_shared<std::vector<DataStorageUri>>();
        group_results.push_back(group_actual_uris);
        auto *contextual_sdk = dynamic_cast<ContextualSdkInterface *>(group.sdk.get());
        tasks.push_back({[group, group_actual_uris, contextual_sdk](const SdkCallContext &task_context) {
                             if (contextual_sdk != nullptr) {
                                 return contextual_sdk->PutWithContext(
                                     group.uris, group.buffers, group_actual_uris, task_context);
                             }
                             return group.sdk->Put(group.uris, group.buffers, group_actual_uris);
                         },
                         contextual_sdk != nullptr});
    }

    ec = RunWithTimeoutParallel(OpType::PUT, std::move(tasks), context);
    if (ec != ER_OK) {
        KVCM_LOG_WARN("put failed, sdk error: %d", static_cast<int>(ec));
        return ec;
    }

    std::vector<DataStorageUri> combined(remote_uris.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        const auto &group = groups[i];
        const auto &group_actual_uris = group_results[i];

        if (group_actual_uris->size() != group.indices.size()) {
            KVCM_LOG_WARN("sdk returned mismatched actual_uris size: %zu vs %zu",
                          group_actual_uris->size(),
                          group.indices.size());
            return ER_SDKWRITE_ERROR;
        }
        for (size_t j = 0; j < group.indices.size(); ++j) {
            combined[group.indices[j]] = (*group_actual_uris)[j];
        }
    }

    auto access = context.TryAcquireCallerMemory(SdkCallContext::AccessKind::OUTPUT_COMMIT);
    if (!access) {
        // Every task already finished successfully; only the output commit missed the deadline.
        return ER_SDK_TIMEOUT_BUFFER_SAFE;
    }
    actual_remote_uris->swap(combined);
    access->Complete(ER_OK);
    return ER_OK;
}

ClientErrorCode SdkWrapper::Valid(const std::vector<DataStorageUri> &remote_uris, const BlockBuffers local_buffers) {
    if (remote_uris.empty() || local_buffers.empty() || remote_uris.size() != local_buffers.size()) {
        KVCM_LOG_WARN(
            "Check failed, remote_uris or local_buffers invalid, remote_uris size: %zu, local_buffers size: %zu",
            remote_uris.size(),
            local_buffers.size());
        return ER_INVALID_PARAMS;
    }

    const auto it = std::find_if(remote_uris.begin(), remote_uris.end(), [](const auto &uri) { return !uri.Valid(); });
    if (it != remote_uris.end()) {
        KVCM_LOG_WARN("Check failed, remote_uri %s invalid", it->ToUriString().c_str());
        return ER_INVALID_PARAMS;
    }
    return ER_OK;
}

std::shared_ptr<SdkInterface> SdkWrapper::GetSdk(const DataStorageUri &remote_uri) {
    std::string host_name = remote_uri.GetHostName();
    if (host_name.empty()) {
        KVCM_LOG_WARN("get sdk for remote_uri %s failed, remote_uri's host name is empty",
                      remote_uri.ToUriString().c_str());
        return nullptr;
    }
    auto it = sdk_map_.find(host_name);
    if (it != sdk_map_.end()) {
        return it->second;
    }
    return nullptr;
}

std::string SdkWrapper::getOpTypeString(OpType op_type) const {
    switch (op_type) {
    case OpType::GET: {
        return "get";
    }
    case OpType::PUT: {
        return "put";
    }
    }
    return "unknown";
}

ClientErrorCode
SdkWrapper::RunWithTimeoutParallel(OpType op_type,
                                   std::vector<std::function<ClientErrorCode(const SdkCallContext &)>> &&tasks,
                                   const SdkCallContext &context) const {
    std::vector<ParallelTask> unguarded_tasks;
    unguarded_tasks.reserve(tasks.size());
    for (auto &task : tasks) {
        unguarded_tasks.push_back({std::move(task), false});
    }
    return RunWithTimeoutParallel(op_type, std::move(unguarded_tasks), context);
}

namespace {

bool IsTimeoutResult(const ClientErrorCode ec) { return ec == ER_SDK_TIMEOUT || ec == ER_SDK_TIMEOUT_BUFFER_SAFE; }

// A finished task proves it no longer touches caller buffers only with success,
// cancellation before I/O, or an explicit *_BUFFER_SAFE result. A plain error
// may leave SDK-owned background transfers running.
bool IsBufferSafeOutcome(const ClientErrorCode ec) {
    return ec == ER_OK || ec == ER_TASK_CANCELLED || ec == ER_SDK_TIMEOUT_BUFFER_SAFE || ec == ER_SDK_ERROR_BUFFER_SAFE;
}

} // namespace

ClientErrorCode SdkWrapper::RunWithTimeoutParallel(OpType op_type,
                                                   std::vector<ParallelTask> &&tasks,
                                                   const SdkCallContext &context) const {
    if (tasks.empty()) {
        return ER_OK;
    }

    struct AcceptedTask {
        std::future<ClientErrorCode> future;
        SdkCallContext context;
        std::optional<ClientErrorCode> outcome;
        bool caller_memory_guarded = false;
    };

    const auto deadline = context.deadline();
    const ClientErrorCode task_exception_error = op_type == OpType::GET ? ER_SDKREAD_ERROR : ER_SDKWRITE_ERROR;
    auto stop = std::make_shared<std::atomic<bool>>(false);
    std::vector<AcceptedTask> accepted_tasks;

    // This is the only precedence decision used after cancellation. A known
    // timeout always wins. At the deadline, a task is unknown unless either its
    // future or its completed per-task Get commit published a result.
    auto resolve_after_drain = [&](ClientErrorCode fallback) {
        const bool at_deadline = std::chrono::steady_clock::now() >= deadline;
        bool all_get_commits_completed = op_type == OpType::GET && !accepted_tasks.empty();
        std::optional<ClientErrorCode> committed_get_error;

        for (const auto &accepted : accepted_tasks) {
            const auto commit_result = accepted.context.CompletedOutputCommit();
            if (!commit_result.has_value()) {
                all_get_commits_completed = false;
            } else if (IsTimeoutResult(*commit_result)) {
                return ER_SDK_TIMEOUT;
            } else if (*commit_result == ER_TASK_CANCELLED || *commit_result == ER_SDK_ERROR_BUFFER_SAFE) {
                if (!committed_get_error.has_value()) {
                    committed_get_error = task_exception_error;
                }
            } else if (*commit_result != ER_OK && !committed_get_error.has_value()) {
                committed_get_error = *commit_result;
            }

            if (accepted.outcome.has_value()) {
                if (IsTimeoutResult(*accepted.outcome)) {
                    return ER_SDK_TIMEOUT;
                }
            } else if (at_deadline && !(op_type == OpType::GET && commit_result.has_value())) {
                return ER_SDK_TIMEOUT;
            }
        }

        // A SAFE or cancelled first result cannot conceal an unknown peer
        // failure; buffer safety is decided separately by finalize().
        if (fallback == ER_TASK_CANCELLED || fallback == ER_SDK_ERROR_BUFFER_SAFE) {
            fallback = task_exception_error;
        }
        if (IsTimeoutResult(fallback)) {
            if (all_get_commits_completed) {
                return committed_get_error.value_or(ER_OK);
            }
            return ER_SDK_TIMEOUT;
        }
        return fallback;
    };

    // Called only after cancellation and WaitForCallerMemory(). Reports the
    // *_BUFFER_SAFE form when every accepted task provably no longer touches
    // caller buffers: guarded tasks always qualify, unguarded tasks only with a
    // buffer-safe outcome. Tasks that were never accepted never ran.
    auto finalize = [&](const ClientErrorCode result) {
        if (result == ER_OK) {
            return result;
        }
        for (const auto &accepted : accepted_tasks) {
            if (!accepted.caller_memory_guarded &&
                !(accepted.outcome.has_value() && IsBufferSafeOutcome(*accepted.outcome))) {
                return result;
            }
        }
        return IsTimeoutResult(result) ? ER_SDK_TIMEOUT_BUFFER_SAFE : ER_SDK_ERROR_BUFFER_SAFE;
    };

    try {
        accepted_tasks.reserve(tasks.size());
    } catch (...) {
        context.Cancel();
        context.WaitForCallerMemory();
        return finalize(resolve_after_drain(ER_THREADPOOL_ERROR));
    }

    auto consume_ready = [&](std::size_t index) {
        try {
            accepted_tasks[index].outcome = accepted_tasks[index].future.get();
        } catch (const std::exception &e) {
            KVCM_LOG_WARN("run %s parallel task threw an exception: %s", getOpTypeString(op_type).c_str(), e.what());
            accepted_tasks[index].outcome = task_exception_error;
        } catch (...) {
            KVCM_LOG_WARN("run %s parallel task threw an unknown exception", getOpTypeString(op_type).c_str());
            accepted_tasks[index].outcome = task_exception_error;
        }
    };

    auto collect_accepted = [&](bool wait_until_deadline) {
        for (std::size_t i = 0; i < accepted_tasks.size(); ++i) {
            auto &accepted = accepted_tasks[i];
            if (accepted.outcome.has_value()) {
                continue;
            }
            const auto status = wait_until_deadline ? accepted.future.wait_until(deadline)
                                                    : accepted.future.wait_for(std::chrono::milliseconds(0));
            if (status == std::future_status::ready) {
                consume_ready(i);
            }
        }
    };

    auto cancel_and_drain = [&] {
        stop->store(true, std::memory_order_release);
        context.Cancel();
        collect_accepted(true);
        context.WaitForCallerMemory();
        collect_accepted(false);
    };

    for (auto &task : tasks) {
        try {
            SdkCallContext task_context = context.MakeTaskContext();
            const bool caller_memory_guarded = task.caller_memory_guarded;
            auto wrapped = [stop, task_context, run = std::move(task.run)]() mutable -> ClientErrorCode {
                if (stop->load(std::memory_order_acquire)) {
                    return ER_TASK_CANCELLED;
                }
                if (!task_context.IsLive()) {
                    KVCM_LOG_WARN("deadline passed, skip SDK I/O");
                    return ER_SDK_TIMEOUT_BUFFER_SAFE;
                }
                return run(task_context);
            };

            auto submission = wait_task_thread_pool_->TryAsync(std::move(wrapped));
            if (!submission.accepted()) {
                KVCM_LOG_WARN("run %s parallel submission rejected, status: %d",
                              getOpTypeString(op_type).c_str(),
                              static_cast<int>(submission.status));
                cancel_and_drain();
                return finalize(resolve_after_drain(ER_THREADPOOL_ERROR));
            }
            accepted_tasks.push_back(
                {std::move(submission.future), std::move(task_context), std::nullopt, caller_memory_guarded});
        } catch (const std::exception &e) {
            KVCM_LOG_WARN(
                "run %s parallel submission threw an exception: %s", getOpTypeString(op_type).c_str(), e.what());
            cancel_and_drain();
            return finalize(resolve_after_drain(ER_THREADPOOL_ERROR));
        } catch (...) {
            KVCM_LOG_WARN("run %s parallel submission threw an unknown exception", getOpTypeString(op_type).c_str());
            cancel_and_drain();
            return finalize(resolve_after_drain(ER_THREADPOOL_ERROR));
        }
    }

    for (std::size_t i = 0; i < accepted_tasks.size(); ++i) {
        if (accepted_tasks[i].future.wait_until(deadline) != std::future_status::ready) {
            KVCM_LOG_WARN("run %s parallel timed out at group %zu/%zu",
                          getOpTypeString(op_type).c_str(),
                          i + 1,
                          accepted_tasks.size());
            cancel_and_drain();
            return finalize(resolve_after_drain(ER_SDK_TIMEOUT));
        }

        consume_ready(i);
        const auto ec = *accepted_tasks[i].outcome;
        if (ec != ER_OK) {
            KVCM_LOG_WARN("run %s parallel failed, error: %d, drain in-flight peers until deadline",
                          getOpTypeString(op_type).c_str(),
                          static_cast<int>(ec));
            cancel_and_drain();
            return finalize(resolve_after_drain(ec));
        }
    }

    if (!context.IsLive()) {
        cancel_and_drain();
        return finalize(resolve_after_drain(ER_SDK_TIMEOUT));
    }
    return ER_OK;
}

ClientErrorCode SdkWrapper::UpdateMooncakeSdkConfig(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                                    RegistSpan *span,
                                                    const std::string &self_location_spec_name) {
    if (DataStorageType::DATA_STORAGE_TYPE_MOONCAKE != sdk_backend_config->type()) {
        return ER_OK;
    }
    auto config = std::dynamic_pointer_cast<MooncakeSdkConfig>(sdk_backend_config);
    if (!config) {
        KVCM_LOG_WARN("convert to mooncake config failed");
        return ER_INVALID_SDKBACKEND_CONFIG;
    }
    if (span == nullptr) {
        KVCM_LOG_WARN("regist span is null but mooncake config is not null");
        return ER_INVALID_PARAMS;
    }
    config->set_local_mem_ptr(span->base);
    config->set_local_buffer_size(span->size);
    config->set_self_location_spec_name(self_location_spec_name);
    return ER_OK;
}

ClientErrorCode SdkWrapper::PrepareSharedMemoryRegistration(const SharedMemoryRegistration &shared_memory_registration,
                                                            SharedMemoryRegistration &prepared_registration) {
    std::unique_lock lifecycle_lock(SdkWrapperLifecycleMutex());
    if (wait_task_thread_pool_) {
        return ER_INVALID_SDKWRAPPER_CONFIG;
    }
    int owned_fd_candidate = -1;
    const auto ec =
        PrepareSharedMemoryRegistrationCandidate(shared_memory_registration, prepared_registration, owned_fd_candidate);
    if (ec != ER_OK) {
        return ec;
    }
    if (owned_shm_fd_ >= 0) {
        close(owned_shm_fd_);
    }
    owned_shm_fd_ = owned_fd_candidate;
    return ER_OK;
}

ClientErrorCode
SdkWrapper::PrepareSharedMemoryRegistrationCandidate(const SharedMemoryRegistration &shared_memory_registration,
                                                     SharedMemoryRegistration &prepared_registration,
                                                     int &owned_fd_candidate) {
    const bool disabled = shared_memory_registration.fd == -1 && shared_memory_registration.base == nullptr &&
                          shared_memory_registration.size == 0;
    if (disabled) {
        prepared_registration = shared_memory_registration;
        return ER_OK;
    }

    if (shared_memory_registration.fd < 0 || shared_memory_registration.base == nullptr ||
        shared_memory_registration.size == 0) {
        KVCM_LOG_WARN("shared memory registration must provide fd, base and size together");
        return ER_INVALID_PARAMS;
    }

    const uintptr_t base = reinterpret_cast<uintptr_t>(shared_memory_registration.base);
    if (shared_memory_registration.size > std::numeric_limits<uintptr_t>::max() - base) {
        KVCM_LOG_WARN("shared memory registration address range overflows");
        return ER_INVALID_PARAMS;
    }

    struct stat file_stat {};
    if (fstat(shared_memory_registration.fd, &file_stat) != 0 || file_stat.st_size < 0 ||
        static_cast<uintmax_t>(file_stat.st_size) < shared_memory_registration.size) {
        KVCM_LOG_WARN("shared memory fd is invalid or smaller than the registered range");
        return ER_INVALID_PARAMS;
    }

    int duplicated_fd = fcntl(shared_memory_registration.fd, F_DUPFD_CLOEXEC, 0);
    if (duplicated_fd < 0) {
        KVCM_LOG_WARN("duplicate shared memory fd failed");
        return ER_INVALID_PARAMS;
    }
    if (owned_fd_candidate >= 0) {
        close(owned_fd_candidate);
    }
    owned_fd_candidate = duplicated_fd;
    prepared_registration = shared_memory_registration;
    prepared_registration.fd = owned_fd_candidate;
    return ER_OK;
}

ClientErrorCode SdkWrapper::UpdateTairMempoolSdkConfig(const std::shared_ptr<SdkBackendConfig> &sdk_backend_config,
                                                       const ClientMemoryRegistrations *memory_registrations) {
    if (!IsTairMempoolStorageType(sdk_backend_config->type())) {
        return ER_OK;
    }
    auto config = std::dynamic_pointer_cast<TairMempoolSdkConfig>(sdk_backend_config);
    if (!config) {
        KVCM_LOG_WARN("convert to tair mempool config failed");
        return ER_INVALID_SDKBACKEND_CONFIG;
    }
    if (memory_registrations == nullptr) {
        return ER_OK;
    }
    config->set_shm_fd(memory_registrations->host.fd);
    config->set_shm_size(memory_registrations->host.size);
    config->set_client_base(memory_registrations->host.base);
    config->set_gpu_memory_spans(memory_registrations->gpu);
    return ER_OK;
}

} // namespace kv_cache_manager
