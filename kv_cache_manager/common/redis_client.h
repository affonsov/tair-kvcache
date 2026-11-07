#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <hiredis.h>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "kv_cache_manager/common/error_code.h"
#include "kv_cache_manager/common/standard_uri.h"

namespace kv_cache_manager {

enum class RedisTransportSecurity {
    PLAINTEXT,
    TLS_NAMED_ACL,
};

struct RedisSecurityOptions {
    RedisTransportSecurity transport = RedisTransportSecurity::PLAINTEXT;
    std::string directory;
};

struct RedisConnectionOptions {
    StandardUri endpoint;
    RedisSecurityOptions security;
};

namespace redis_client_internal {

constexpr std::size_t kMaxCommandArguments = 5;

struct CommandView {
    int argc = 0;
    std::array<const char *, kMaxCommandArguments> argv{};
    std::array<std::size_t, kMaxCommandArguments> lengths{};
};

template <typename... Arguments>
CommandView BuildCommandView(const Arguments &...arguments) noexcept {
    static_assert(sizeof...(arguments) <= kMaxCommandArguments, "too many Redis command arguments");
    const std::array<std::string_view, sizeof...(arguments)> views{std::string_view(arguments)...};
    CommandView command;
    command.argc = static_cast<int>(views.size());
    for (std::size_t i = 0; i < views.size(); ++i) {
        command.argv[i] = views[i].data();
        command.lengths[i] = views[i].size();
    }
    return command;
}

} // namespace redis_client_internal

// Per-command outcome for deadline-bounded SET requests.
enum class ValkeyWriteCertainty : uint8_t {
    NOT_SENT = 0,  // The request was never sent.
    CONFIRMED = 1, // The server returned exact +OK.
    AMBIGUOUS = 2, // The request was sent without a definitive reply.
    REJECTED = 3,  // The server returned a complete Redis error reply.
};

struct ValkeySetStringsResult {
    ErrorCode operation_error = EC_OK;
    std::vector<ErrorCode> errors;
    std::vector<ValkeyWriteCertainty> certainties;
    std::size_t confirmed_count = 0;
    std::size_t ambiguous_count = 0;
    std::size_t not_sent_count = 0;

    std::size_t RejectedCount() const {
        std::size_t count = 0;
        for (const ValkeyWriteCertainty certainty : certainties) {
            if (certainty == ValkeyWriteCertainty::REJECTED) {
                ++count;
            }
        }
        return count;
    }
};

struct ValkeyGetStringsResult {
    ErrorCode operation_error = EC_OK;
    std::vector<ErrorCode> errors;
    std::vector<std::string> values;
};

class RedisClient {
public:
    RedisClient(const StandardUri &storage_uri);
    RedisClient(const RedisConnectionOptions &options);
    virtual ~RedisClient(); // virtual for test

    RedisClient(const RedisClient &) = delete;
    RedisClient &operator=(const RedisClient &) = delete;
    RedisClient(RedisClient &&) = delete;
    RedisClient &operator=(RedisClient &&) = delete;

    using Deadline = std::chrono::steady_clock::time_point;

    bool Open();
    void Close();

    // Resolves and caches numeric IPv4/IPv6 addresses for deadline operations.
    // The original hostname remains available for later identity verification.
    // DNS refresh requires constructing and preparing a new SDK/client instance.
    bool Prepare();
    ValkeySetStringsResult
    SetStringsUntil(const std::vector<std::string> &keys, const std::vector<std::string> &values, Deadline deadline);
    // Uses one EVAL_RO per key to atomically distinguish missing from empty and
    // fetch at most expected_size + 1 bytes from a compliant Valkey server.
    // Trust boundary: stock hiredis cannot defend against a malicious
    // authenticated server that lies in a RESP bulk length. No parser patch is
    // applied here.
    ValkeyGetStringsResult GetStringsUntil(const std::vector<std::string> &keys,
                                           const std::vector<std::size_t> &expected_sizes,
                                           Deadline deadline);

    std::vector<ErrorCode> SetStrings(const std::vector<std::string> &keys, const std::vector<std::string> &values);
    std::vector<ErrorCode> GetStrings(const std::vector<std::string> &keys, std::vector<std::string> &out_values);
    std::vector<ErrorCode> Set(const std::vector<std::string> &keys,
                               const std::vector<std::map<std::string, std::string>> &field_maps);
    std::vector<ErrorCode> Update(const std::vector<std::string> &keys,
                                  const std::vector<std::map<std::string, std::string>> &field_maps);
    std::vector<ErrorCode> Upsert(const std::vector<std::string> &keys,
                                  const std::vector<std::map<std::string, std::string>> &field_maps);
    std::vector<ErrorCode> Delete(const std::vector<std::string> &keys);
    std::vector<ErrorCode> DeleteFields(const std::vector<std::string> &keys,
                                        const std::vector<std::vector<std::string>> &field_names_vec);
    std::vector<ErrorCode> Get(const std::vector<std::string> &keys,
                               const std::vector<std::string> &field_names,
                               std::vector<std::map<std::string, std::string>> &out_field_maps);
    std::vector<ErrorCode> Get(const std::vector<std::string> &keys,
                               const std::vector<std::vector<std::string>> &field_names_vec,
                               std::vector<std::map<std::string, std::string>> &out_field_maps);
    std::vector<ErrorCode> GetAllFields(const std::vector<std::string> &keys,
                                        std::vector<std::map<std::string, std::string>> &out_field_maps);
    std::vector<ErrorCode> Exists(const std::vector<std::string> &keys, std::vector<bool> &out_is_exist_vec);
    std::vector<ErrorCode> ExistsFieldWithPrefix(const std::vector<std::string> &keys,
                                                 const std::string &field_prefix,
                                                 std::vector<bool> &out_exists_vec);
    std::vector<ErrorCode> GetFieldNamesWithPrefix(const std::vector<std::string> &keys,
                                                   const std::string &field_prefix,
                                                   std::vector<std::vector<std::string>> &out_field_names_vec);
    ErrorCode Scan(const std::string &matching_prefix,
                   const std::string &cursor,
                   const int64_t limit,
                   std::string &out_next_cursor,
                   std::vector<std::string> &out_keys);
    ErrorCode Rand(const std::string &matching_prefix, const int64_t count, std::vector<std::string> &out_keys);

    using ReplyUPtr = std::unique_ptr<redisReply, void (*)(void *)>;
    using CmdArgs = std::vector<std::string>;

    // Batch-execute write commands (DEL/HSET/HDEL) via pipeline.
    // All commands are expected to return REDIS_REPLY_INTEGER.
    // Returns per-command ErrorCode; empty vector on total connection failure.
    // Sets all_ok to true if every command succeeded.
    std::vector<ErrorCode> BatchWrite(const std::vector<CmdArgs> &cmds, bool &out_all_ok);

    // --- Static command builders ---
    // Pure utility functions that construct Redis CmdArgs without any connection or state.
    // Callers use these to build command sequences, then execute via BatchExecute/CommandPipeline.

    // DEL + HSET per key (overwrite semantics). Skips keys with empty field_maps.
    static void BuildSetCmds(const std::vector<std::string> &keys,
                             const std::vector<std::map<std::string, std::string>> &field_maps,
                             std::vector<CmdArgs> &out_cmds);

    // HSET per key (merge/upsert semantics). Skips keys with empty field_maps.
    static void BuildHashSetCmds(const std::vector<std::string> &keys,
                                 const std::vector<std::map<std::string, std::string>> &field_maps,
                                 std::vector<CmdArgs> &out_cmds);

    // DEL per key.
    static void BuildDeleteCmds(const std::vector<std::string> &keys, std::vector<CmdArgs> &out_cmds);

    // HDEL per key. Skips keys with empty field_names.
    static void BuildHashDeleteCmds(const std::vector<std::string> &keys,
                                    const std::vector<std::vector<std::string>> &field_names_vec,
                                    std::vector<CmdArgs> &out_cmds);

protected:
    bool IsReplyOk(const redisReply *reply) const;
    bool CheckReplyInteger(const redisReply *reply) const;
    bool CheckReplyArray(const redisReply *reply) const;
    bool GetReplyStrOrNil(const redisReply *reply, std::string &out_str) const;
    bool Connect();
    void Disconnect();
    std::vector<ReplyUPtr> CommandPipeline(const std::vector<CmdArgs> &cmds);
    ErrorCode CommandPipeline(const std::vector<CmdArgs> &cmds, std::vector<ReplyUPtr> &out_replies);

    // virtual for test
    virtual bool IsContextOk() const;
    virtual bool Reconnect();
    virtual std::vector<ReplyUPtr> TryExecPipeline(const std::vector<CmdArgs> &cmds);
    virtual bool AppendDeadlineCommand(const CmdArgs &cmd);
    // Takes ownership of socket_fd exactly when a non-null context is returned.
    virtual redisContext *AdoptConnectedSocket(int socket_fd);

private:
    bool ConnectPlaintext();
    // Validates security_directory_ material, reads the ACL credentials, and
    // completes a verified TLS handshake on context_. Never falls back to plaintext.
    bool InitiateSecureSession(std::string &username, std::string &password);
    bool ConnectSecure();
    bool SetSocketTimeout();
    ReplyUPtr RunCommand(const CmdArgs &command);
    enum class StringCommand : uint8_t {
        SET,
        GET,
    };

    class DeadlineWatchdog;

    bool AppendCommand(const redis_client_internal::CommandView &command);
    ReplyUPtr ExecuteCommand(const redis_client_internal::CommandView &command);
    ErrorCode ExecuteStringPipeline(StringCommand command,
                                    const std::vector<std::string> &keys,
                                    const std::vector<std::string> *values,
                                    std::size_t begin,
                                    std::size_t end,
                                    std::vector<ReplyUPtr> &out_replies);

    struct NumericAddress {
        int family = 0;
        std::string address;
    };

    bool ConnectUntil(Deadline deadline, DeadlineWatchdog &watchdog);
    bool ReconnectUntil(Deadline deadline, DeadlineWatchdog &watchdog);
    bool
    ConnectNumericAddressUntil(const NumericAddress &numeric_address, Deadline deadline, DeadlineWatchdog &watchdog);
    bool ApplyRemainingSocketTimeout(Deadline deadline);
    void RestoreConfiguredSocketTimeoutOrDisconnect();

    redisContext *context_ = nullptr;
    std::string user_info_;
    std::string protocol_;
    std::string host_;
    std::string security_directory_;
    RedisTransportSecurity transport_security_ = RedisTransportSecurity::PLAINTEXT;
    std::vector<NumericAddress> numeric_addresses_;
    bool prepared_ = false;
    int64_t port_ = 0;
    int64_t db_ = 0;
    int64_t timeout_ms_ = 2000;
    int64_t retry_count_ = 2;
    int64_t randomkey_batch_num_ = 20;
};
} // namespace kv_cache_manager
