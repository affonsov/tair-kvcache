#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "kv_cache_manager/common/redis_client.h"
#include "kv_cache_manager/common/test/mock_redis_client.h"
#include "kv_cache_manager/common/test/redis_test_base.h"
#include "kv_cache_manager/common/unittest.h"

namespace kv_cache_manager {

class RedisClientTest : public RedisTestBase, public TESTBASE {
public:
    void SetUp() override;

    void TearDown() override {}

public:
    void PrepareMockRedisClient(const std::string &user_info, const std::string &host, int32_t port);
    void CheckReplyType(const redisReply *r, const int expected_type) const;
    std::vector<ReplyUPtr> MakeStatusReplies(std::size_t count);
    std::vector<ReplyUPtr> MakeStringReplies(const std::vector<std::string> &values);

private:
    std::unique_ptr<MockRedisClient> redis_client_;
};

void RedisClientTest::SetUp() { PrepareMockRedisClient("test_user:test_password", "test_host", /*port*/ 0); }

void RedisClientTest::PrepareMockRedisClient(const std::string &user_info, const std::string &host, int32_t port) {
    StandardUri storage_uri;
    storage_uri.user_info_ = user_info;
    storage_uri.hostname_ = host;
    storage_uri.port_ = port;
    storage_uri.params_["timeout_ms"] = "2000";
    storage_uri.params_["retry_count"] = "2";
    storage_uri.params_["randomkey_batch_num"] = "20";

    redis_client_ = std::make_unique<MockRedisClient>(storage_uri);
    ON_CALL(*redis_client_, IsContextOk()).WillByDefault(Invoke(redis_client_.get(), &RedisClient::IsContextOk));
    ON_CALL(*redis_client_, Reconnect()).WillByDefault(Invoke(redis_client_.get(), &RedisClient::Reconnect));
    ON_CALL(*redis_client_, TryExecPipeline(_))
        .WillByDefault(Invoke(redis_client_.get(), &RedisClient::TryExecPipeline));
}

void RedisClientTest::CheckReplyType(const redisReply *r, const int expected_type) const {
    ASSERT_TRUE(r);
    ASSERT_EQ(expected_type, r->type);
}

std::vector<RedisClient::ReplyUPtr> RedisClientTest::MakeStatusReplies(const std::size_t count) {
    std::vector<ReplyUPtr> replies;
    replies.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    }
    return replies;
}

std::vector<RedisClient::ReplyUPtr> RedisClientTest::MakeStringReplies(const std::vector<std::string> &values) {
    std::vector<ReplyUPtr> replies;
    replies.reserve(values.size());
    for (const std::string &value : values) {
        replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, value));
    }
    return replies;
}

TEST_F(RedisClientTest, TestCommandPipelineSimple) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    std::vector<ReplyUPtr> prepared_replies;
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::move(prepared_replies))));

    std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
    std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
    ASSERT_EQ(2, replies.size());
    CheckReplyType(replies[0].get(), REDIS_REPLY_STATUS);
    ASSERT_EQ("OK", std::string(replies[0]->str));
    CheckReplyType(replies[1].get(), REDIS_REPLY_STATUS);
    ASSERT_EQ("OK", std::string(replies[1]->str));
}

TEST_F(RedisClientTest, TestCommandPipelineError) {
    {
        // connection error always
        EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(false));
        EXPECT_CALL(*redis_client_, Reconnect()).WillRepeatedly(Return(false));

        std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(0, replies.size());
    }
    {
        // connection error and reconnect
        EXPECT_CALL(*redis_client_, IsContextOk()).WillOnce(Return(false)).WillRepeatedly(Return(true));
        EXPECT_CALL(*redis_client_, Reconnect()).WillRepeatedly(Return(true));

        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
        prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(2, replies.size());
        CheckReplyType(replies[0].get(), REDIS_REPLY_STATUS);
        ASSERT_EQ("OK", std::string(replies[0]->str));
        CheckReplyType(replies[1].get(), REDIS_REPLY_STATUS);
        ASSERT_EQ("OK", std::string(replies[1]->str));
    }
    {
        // connection error and reconnect error
        EXPECT_CALL(*redis_client_, IsContextOk()).WillOnce(Return(false)).WillRepeatedly(Return(true));
        EXPECT_CALL(*redis_client_, Reconnect()).WillOnce(Return(false)).WillRepeatedly(Return(true));

        std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(0, replies.size());
    }
    {
        // connection error in first pipeline, ok in second
        bool is_connection_ok = true;
        EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(ReturnPointee(&is_connection_ok));
        EXPECT_CALL(*redis_client_, Reconnect()).WillRepeatedly(DoAll(Assign(&is_connection_ok, true), Return(true)));

        std::vector<ReplyUPtr> prepared_replies_0;
        std::vector<ReplyUPtr> prepared_replies_1;
        prepared_replies_1.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
        prepared_replies_1.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_))
            .WillOnce(DoAll(Assign(&is_connection_ok, false), Return(ByMove(std::move(prepared_replies_0)))))
            .WillOnce(Return(ByMove(std::move(prepared_replies_1))));

        std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(2, replies.size());
        CheckReplyType(replies[0].get(), REDIS_REPLY_STATUS);
        ASSERT_EQ("OK", std::string(replies[0]->str));
        CheckReplyType(replies[1].get(), REDIS_REPLY_STATUS);
        ASSERT_EQ("OK", std::string(replies[1]->str));
    }
    {
        // connection ok but cmds is illegal
        EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
        EXPECT_CALL(*redis_client_, Reconnect()).WillRepeatedly(Return(true));

        std::vector<ReplyUPtr> prepared_replies;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<CmdArgs> cmds{{"fake_valid_cmd1"}, {"fake_valid_cmd2"}};
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(0, replies.size());
    }
    {
        // cmds is empty
        std::vector<CmdArgs> cmds;
        std::vector<ReplyUPtr> replies = redis_client_->CommandPipeline(cmds);
        ASSERT_EQ(0, replies.size());
    }
}

TEST_F(RedisClientTest, TestSet) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(2));
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(2));
        EXPECT_CALL(
            *redis_client_,
            TryExecPipeline(ElementsAre(
                ElementsAre(StrEq("DEL"), StrEq("key1")),
                ElementsAre(StrEq("HSET"), StrEq("key1"), StrEq("f1"), StrEq("v1-1-0"), StrEq("f2"), StrEq("v1-2-0")),
                ElementsAre(StrEq("DEL"), StrEq("key2")),
                ElementsAre(StrEq("HSET"), StrEq("key2"), StrEq("f1"), StrEq("v2-1-0"), StrEq("f2"), StrEq("v2-2-0")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Set(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(-1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(2));
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(-2));
        EXPECT_CALL(
            *redis_client_,
            TryExecPipeline(ElementsAre(
                ElementsAre(StrEq("DEL"), StrEq("key1")),
                ElementsAre(StrEq("HSET"), StrEq("key1"), StrEq("f1"), StrEq("v1-1-0"), StrEq("f2"), StrEq("v1-2-0")),
                ElementsAre(StrEq("DEL"), StrEq("key2")),
                ElementsAre(StrEq("HSET"), StrEq("key2"), StrEq("f1"), StrEq("v2-1-0"), StrEq("f2"), StrEq("v2-2-0")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Set(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        EXPECT_CALL(
            *redis_client_,
            TryExecPipeline(ElementsAre(
                ElementsAre(StrEq("DEL"), StrEq("key1")),
                ElementsAre(StrEq("HSET"), StrEq("key1"), StrEq("f1"), StrEq("v1-1-0"), StrEq("f2"), StrEq("v1-2-0")),
                ElementsAre(StrEq("DEL"), StrEq("key2")),
                ElementsAre(StrEq("HSET"), StrEq("key2"), StrEq("f1"), StrEq("v2-1-0"), StrEq("f2"), StrEq("v2-2-0")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Set(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), ec_per_key);
    }
}

TEST_F(RedisClientTest, TestSetStringsPipelineAndReplyValidation) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    const std::string binary_key("key\0\x80", 5);
    const std::string binary_value("value\0\xff", 7);
    const std::vector<std::string> keys{binary_key, "key2", "key3", "key4", "key5"};
    const std::vector<std::string> values{binary_value, "", "value3", "value4", "value5"};

    std::vector<ReplyUPtr> prepared_replies;
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, std::string("OK\0", 3)));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_ERROR, "ERR"));
    prepared_replies.emplace_back(MakeFakeReplyInteger(1));
    prepared_replies.emplace_back(nullptr, freeReplyObject);
    EXPECT_CALL(*redis_client_,
                TryExecPipeline(ElementsAre(ElementsAre(StrEq("SET"), StrEq(binary_key), StrEq(binary_value)),
                                            ElementsAre(StrEq("SET"), StrEq("key2"), StrEq("")),
                                            ElementsAre(StrEq("SET"), StrEq("key3"), StrEq("value3")),
                                            ElementsAre(StrEq("SET"), StrEq("key4"), StrEq("value4")),
                                            ElementsAre(StrEq("SET"), StrEq("key5"), StrEq("value5")))))
        .WillOnce(Return(ByMove(std::move(prepared_replies))));

    ASSERT_EQ((std::vector<ErrorCode>{EC_OK, EC_ERROR, EC_ERROR, EC_ERROR, EC_ERROR}),
              redis_client_->SetStrings(keys, values));
}

TEST_F(RedisClientTest, TestSetStringsReplyCountMismatch) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    std::vector<ReplyUPtr> prepared_replies;
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STATUS, "OK"));
    EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::move(prepared_replies))));

    const std::vector<std::string> keys{"key1", "key2"};
    ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), redis_client_->SetStrings(keys, {"value1", "value2"}));
}

TEST_F(RedisClientTest, TestStringCommandsBadArgumentsAndEmptyInputDoNoIo) {
    EXPECT_CALL(*redis_client_, TryExecPipeline(_)).Times(0);

    ASSERT_EQ(std::vector<ErrorCode>(2, EC_BADARGS), redis_client_->SetStrings({"key1", "key2"}, {"value1"}));
    ASSERT_TRUE(redis_client_->SetStrings({}, {}).empty());

    std::vector<std::string> out_values{"stale"};
    ASSERT_TRUE(redis_client_->GetStrings({}, out_values).empty());
    ASSERT_TRUE(out_values.empty());
}

TEST_F(RedisClientTest, TestGetStringsPipelineAndReplyAlignment) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    const std::string binary_key("key\0\xfe", 5);
    const std::string binary_value("value\0\x80\xff", 8);
    const std::vector<std::string> keys{binary_key, "key2", "key3", "key4", "key5", "key6"};

    std::vector<ReplyUPtr> prepared_replies;
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, binary_value));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, ""));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_NIL, ""));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_ERROR, "ERR"));
    prepared_replies.emplace_back(MakeFakeReplyInteger(1));
    prepared_replies.emplace_back(nullptr, freeReplyObject);
    EXPECT_CALL(*redis_client_,
                TryExecPipeline(ElementsAre(ElementsAre(StrEq("GET"), StrEq(binary_key)),
                                            ElementsAre(StrEq("GET"), StrEq("key2")),
                                            ElementsAre(StrEq("GET"), StrEq("key3")),
                                            ElementsAre(StrEq("GET"), StrEq("key4")),
                                            ElementsAre(StrEq("GET"), StrEq("key5")),
                                            ElementsAre(StrEq("GET"), StrEq("key6")))))
        .WillOnce(Return(ByMove(std::move(prepared_replies))));

    std::vector<std::string> out_values{"stale"};
    ASSERT_EQ((std::vector<ErrorCode>{EC_OK, EC_OK, EC_NOENT, EC_ERROR, EC_ERROR, EC_ERROR}),
              redis_client_->GetStrings(keys, out_values));
    ASSERT_EQ((std::vector<std::string>{binary_value, "", "", "", "", ""}), out_values);
}

TEST_F(RedisClientTest, TestGetStringsShortReplyFailsClosed) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    std::vector<ReplyUPtr> prepared_replies;
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "must-not-shift"));
    prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_NIL, ""));
    EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::move(prepared_replies))));

    const std::vector<std::string> keys{"key1", "key2", "key3"};
    std::vector<std::string> out_values{"stale"};
    ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), redis_client_->GetStrings(keys, out_values));
    ASSERT_EQ(std::vector<std::string>(keys.size()), out_values);
}

TEST_F(RedisClientTest, TestStringPipelinesSplitAtCommandLimitAndPreserveOrder) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    constexpr std::size_t kExpectedMaxCommandCount = 64;
    constexpr std::size_t kItemCount = kExpectedMaxCommandCount + 1;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kItemCount);
    values.reserve(kItemCount);
    for (std::size_t i = 0; i < kItemCount; ++i) {
        keys.emplace_back("chunk-key-" + std::to_string(i));
        std::string value = "chunk-value-" + std::to_string(i);
        value.push_back('\0');
        value.push_back(static_cast<char>(0x80 + i % 0x40));
        values.emplace_back(std::move(value));
    }

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([&](const std::vector<CmdArgs> &commands) {
            EXPECT_EQ(kExpectedMaxCommandCount, commands.size());
            for (std::size_t i = 0; i < commands.size(); ++i) {
                EXPECT_EQ((CmdArgs{"SET", keys[i], values[i]}), commands[i]);
            }
            return MakeStatusReplies(commands.size());
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([&](const std::vector<CmdArgs> &commands) {
            EXPECT_EQ(1u, commands.size());
            if (!commands.empty()) {
                EXPECT_EQ((CmdArgs{"SET", keys.back(), values.back()}), commands.front());
            }
            return MakeStatusReplies(commands.size());
        }));
    }
    EXPECT_EQ(std::vector<ErrorCode>(kItemCount, EC_OK), redis_client_->SetStrings(keys, values));

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([&](const std::vector<CmdArgs> &commands) {
            EXPECT_EQ(kExpectedMaxCommandCount, commands.size());
            for (std::size_t i = 0; i < commands.size(); ++i) {
                EXPECT_EQ((CmdArgs{"GET", keys[i]}), commands[i]);
            }
            return MakeStringReplies(
                std::vector<std::string>(values.begin(), values.begin() + kExpectedMaxCommandCount));
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([&](const std::vector<CmdArgs> &commands) {
            EXPECT_EQ(1u, commands.size());
            if (!commands.empty()) {
                EXPECT_EQ((CmdArgs{"GET", keys.back()}), commands.front());
            }
            return MakeStringReplies({values.back()});
        }));
    }
    std::vector<std::string> out_values;
    EXPECT_EQ(std::vector<ErrorCode>(kItemCount, EC_OK), redis_client_->GetStrings(keys, out_values));
    EXPECT_EQ(values, out_values);
}

TEST_F(RedisClientTest, TestStringPipelinesSplitAtByteLimitAndIsolateOversizedItems) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    constexpr std::size_t kExpectedMaxRequestPayloadBytes = 1024 * 1024;
    const std::string exact_key = "set-exact";
    const std::string exact_value(kExpectedMaxRequestPayloadBytes - 3 - exact_key.size(), 'x');
    const std::string oversized_value(kExpectedMaxRequestPayloadBytes + 1, 'y');
    const std::vector<std::string> set_keys{exact_key, "set-oversized", "set-tail"};
    const std::vector<std::string> set_values{exact_value, oversized_value, "tail"};
    {
        InSequence sequence;
        for (std::size_t i = 0; i < set_keys.size(); ++i) {
            EXPECT_CALL(
                *redis_client_,
                TryExecPipeline(ElementsAre(ElementsAre(StrEq("SET"), StrEq(set_keys[i]), StrEq(set_values[i])))))
                .WillOnce(Invoke(
                    [this](const std::vector<CmdArgs> &commands) { return MakeStatusReplies(commands.size()); }));
        }
    }
    EXPECT_EQ(std::vector<ErrorCode>(set_keys.size(), EC_OK), redis_client_->SetStrings(set_keys, set_values));

    const std::string exact_get_key(kExpectedMaxRequestPayloadBytes - 3, 'a');
    const std::string oversized_get_key(kExpectedMaxRequestPayloadBytes - 2, 'b');
    const std::vector<std::string> get_keys{exact_get_key, oversized_get_key, "get-tail"};
    const std::vector<std::string> get_values{"exact", "oversized", "tail"};
    {
        InSequence sequence;
        for (std::size_t i = 0; i < get_keys.size(); ++i) {
            EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAre(ElementsAre(StrEq("GET"), StrEq(get_keys[i])))))
                .WillOnce(Invoke([this, &get_values, i](const std::vector<CmdArgs> &) {
                    return MakeStringReplies({get_values[i]});
                }));
        }
    }
    std::vector<std::string> out_values;
    EXPECT_EQ(std::vector<ErrorCode>(get_keys.size(), EC_OK), redis_client_->GetStrings(get_keys, out_values));
    EXPECT_EQ(get_values, out_values);
}

TEST_F(RedisClientTest, TestStringPipelineMiddleFailureStopsLaterChunks) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    constexpr std::size_t kChunkSize = 64;
    constexpr std::size_t kItemCount = kChunkSize * 2 + 1;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kItemCount);
    values.reserve(kItemCount);
    for (std::size_t i = 0; i < kItemCount; ++i) {
        keys.emplace_back("failure-key-" + std::to_string(i));
        values.emplace_back("failure-value-" + std::to_string(i));
    }

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this](const std::vector<CmdArgs> &commands) {
            return MakeStatusReplies(commands.size());
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Return(ByMove(std::vector<ReplyUPtr>{})));
    }
    std::vector<ErrorCode> expected_set(kItemCount, EC_ERROR);
    std::fill(expected_set.begin(), expected_set.begin() + kChunkSize, EC_OK);
    EXPECT_EQ(expected_set, redis_client_->SetStrings(keys, values));

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this, &values](const std::vector<CmdArgs> &) {
            return MakeStringReplies(std::vector<std::string>(values.begin(), values.begin() + kChunkSize));
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this, &values](const std::vector<CmdArgs> &) {
            return MakeStringReplies(
                std::vector<std::string>(values.begin() + kChunkSize, values.begin() + kChunkSize * 2 - 1));
        }));
    }
    std::vector<ErrorCode> expected_get(kItemCount, EC_ERROR);
    std::fill(expected_get.begin(), expected_get.begin() + kChunkSize, EC_OK);
    std::vector<std::string> expected_values(kItemCount);
    std::copy(values.begin(), values.begin() + kChunkSize, expected_values.begin());
    std::vector<std::string> out_values;
    EXPECT_EQ(expected_get, redis_client_->GetStrings(keys, out_values));
    EXPECT_EQ(expected_values, out_values);
}

TEST_F(RedisClientTest, TestStringPipelineReplyErrorsContinueAcrossChunks) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    constexpr std::size_t kChunkSize = 64;
    constexpr std::size_t kItemCount = kChunkSize + 1;
    constexpr std::size_t kErrorIndex = 7;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kItemCount);
    values.reserve(kItemCount);
    for (std::size_t i = 0; i < kItemCount; ++i) {
        keys.emplace_back("reply-key-" + std::to_string(i));
        values.emplace_back("reply-value-" + std::to_string(i));
    }

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this](const std::vector<CmdArgs> &commands) {
            auto replies = MakeStatusReplies(commands.size());
            replies[kErrorIndex] = MakeFakeReply(REDIS_REPLY_ERROR, "ERR injected");
            return replies;
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this](const std::vector<CmdArgs> &commands) {
            return MakeStatusReplies(commands.size());
        }));
    }
    std::vector<ErrorCode> expected(kItemCount, EC_OK);
    expected[kErrorIndex] = EC_ERROR;
    EXPECT_EQ(expected, redis_client_->SetStrings(keys, values));

    {
        InSequence sequence;
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this, &values](const std::vector<CmdArgs> &) {
            auto replies = MakeStringReplies(std::vector<std::string>(values.begin(), values.begin() + kChunkSize));
            replies[kErrorIndex] = MakeFakeReply(REDIS_REPLY_ERROR, "ERR injected");
            return replies;
        }));
        EXPECT_CALL(*redis_client_, TryExecPipeline(_)).WillOnce(Invoke([this, &values](const std::vector<CmdArgs> &) {
            return MakeStringReplies({values.back()});
        }));
    }
    std::vector<std::string> expected_values = values;
    expected_values[kErrorIndex].clear();
    std::vector<std::string> out_values;
    EXPECT_EQ(expected, redis_client_->GetStrings(keys, out_values));
    EXPECT_EQ(expected_values, out_values);
}

TEST_F(RedisClientTest, TestUpdate) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_exists_replies;
        prepared_exists_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_exists_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_exists_replies))));

        std::vector<ReplyUPtr> prepared_hset_replies;
        prepared_hset_replies.emplace_back(MakeFakeReplyInteger(2));
        prepared_hset_replies.emplace_back(MakeFakeReplyInteger(2));
        EXPECT_CALL(
            *redis_client_,
            TryExecPipeline(ElementsAre(
                ElementsAre(StrEq("HSET"), StrEq("key1"), StrEq("f1"), StrEq("v1-1-0"), StrEq("f2"), StrEq("v1-2-0")),
                ElementsAre(StrEq("HSET"), StrEq("key2"), StrEq("f1"), StrEq("v2-1-0"), StrEq("f2"), StrEq("v2-2-0")))))
            .WillOnce(Return(ByMove(std::move(prepared_hset_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Update(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_exists_replies;
        prepared_exists_replies.emplace_back(MakeFakeReplyInteger(0));
        prepared_exists_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_exists_replies))));

        std::vector<ReplyUPtr> prepared_hset_replies;
        prepared_hset_replies.emplace_back(MakeFakeReplyInteger(0));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("HSET"), StrEq("key2"), StrEq("f1"), StrEq("v2-1-0"), StrEq("f2"), StrEq("v2-2-0")))))
            .WillOnce(Return(ByMove(std::move(prepared_hset_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Update(keys, field_maps);
        std::vector<ErrorCode> expected_ec_per_key{EC_NOENT, EC_OK};
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_exists_replies;
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_exists_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                   {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Update(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), ec_per_key);
    }
}

TEST_F(RedisClientTest, TestDelete) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("DEL"), StrEq("key1")),
                                                ElementsAre(StrEq("DEL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        auto ec_per_key = redis_client_->Delete(keys);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(-1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(0));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("DEL"), StrEq("key1")),
                                                ElementsAre(StrEq("DEL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        auto ec_per_key = redis_client_->Delete(keys);
        std::vector<ErrorCode> expected_ec_per_key{EC_ERROR, EC_NOENT};
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("DEL"), StrEq("key1")),
                                                ElementsAre(StrEq("DEL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        auto ec_per_key = redis_client_->Delete(keys);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_ERROR), ec_per_key);
    }
}

TEST_F(RedisClientTest, TestGet) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_hmget_replies;
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({"v1-1-0", "v1-2-0"}));
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({"v2-1-0", "v2-2-0"}));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HMGET"), StrEq("key1"), StrEq("f1"), StrEq("f2")),
                                                ElementsAre(StrEq("HMGET"), StrEq("key2"), StrEq("f1"), StrEq("f2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hmget_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                            {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->Get(keys, /*field_names*/ std::vector<std::string>{"f1", "f2"}, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
    {
        std::vector<ReplyUPtr> prepared_hmget_replies;
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({std::nullopt, std::nullopt})); // key not exist
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({std::nullopt, "v2-2-0"}));     // f1 not exist
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HMGET"), StrEq("key1"), StrEq("f1"), StrEq("f2")),
                                                ElementsAre(StrEq("HMGET"), StrEq("key2"), StrEq("f1"), StrEq("f2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hmget_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{}, {{"f2", "v2-2-0"}}};
        std::vector<ErrorCode> expected_ec_per_key{EC_NOENT, EC_OK};
        auto ec_per_key = redis_client_->Get(keys, /*field_names*/ std::vector<std::string>{"f1", "f2"}, field_maps);
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
    {
        // all key not exist
        std::vector<ReplyUPtr> prepared_hmget_replies;
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({std::nullopt, std::nullopt}));
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({std::nullopt, std::nullopt}));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HMGET"), StrEq("key1"), StrEq("f1"), StrEq("f2")),
                                                ElementsAre(StrEq("HMGET"), StrEq("key2"), StrEq("f1"), StrEq("f2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hmget_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{}, {}};
        std::vector<ErrorCode> expected_ec_per_key{EC_NOENT, EC_NOENT};
        auto ec_per_key = redis_client_->Get(keys, /*field_names*/ std::vector<std::string>{"f1", "f2"}, field_maps);
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
    {
        std::vector<ReplyUPtr> prepared_hmget_replies;
        prepared_hmget_replies.emplace_back(MakeFakeReplyInteger(1)); // reply is not array
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({std::nullopt, "v2-2-0"}));
        prepared_hmget_replies[1]->element[1]->type = REDIS_REPLY_ERROR; // f2 reply error
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HMGET"), StrEq("key1"), StrEq("f1"), StrEq("f2")),
                                                ElementsAre(StrEq("HMGET"), StrEq("key2"), StrEq("f1"), StrEq("f2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hmget_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps(2);
        std::vector<ErrorCode> expected_ec_per_key{EC_ERROR, EC_ERROR};
        auto ec_per_key = redis_client_->Get(keys, /*field_names*/ std::vector<std::string>{"f1", "f2"}, field_maps);
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
}

TEST_F(RedisClientTest, TestGetAllFields) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_hgetall_replies;
        prepared_hgetall_replies.emplace_back(MakeFakeReplyArrayString({"f1", "v1-1-0", "f2", "v1-2-0"}));
        prepared_hgetall_replies.emplace_back(MakeFakeReplyArrayString({"f1", "v2-1-0", "f2", "v2-2-0"}));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HGETALL"), StrEq("key1")),
                                                ElementsAre(StrEq("HGETALL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hgetall_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{{"f1", "v1-1-0"}, {"f2", "v1-2-0"}},
                                                                            {{"f1", "v2-1-0"}, {"f2", "v2-2-0"}}};
        auto ec_per_key = redis_client_->GetAllFields(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
    {
        std::vector<ReplyUPtr> prepared_hgetall_replies;
        prepared_hgetall_replies.emplace_back(MakeFakeReplyArrayString({})); // not exist
        prepared_hgetall_replies.emplace_back(MakeFakeReplyInteger(1));      // error
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HGETALL"), StrEq("key1")),
                                                ElementsAre(StrEq("HGETALL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hgetall_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{}, {}};
        std::vector<ErrorCode> expected_ec_per_key{EC_NOENT, EC_ERROR};
        auto ec_per_key = redis_client_->GetAllFields(keys, field_maps);
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
    {
        std::vector<ReplyUPtr> prepared_hgetall_replies;
        prepared_hgetall_replies.emplace_back(MakeFakeReplyArrayString({"f1", "v1-1-0"}));
        prepared_hgetall_replies.emplace_back(MakeFakeReplyArrayString({"f1", "v2-1-0", "f2"})); // invalid element 3
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HGETALL"), StrEq("key1")),
                                                ElementsAre(StrEq("HGETALL"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_hgetall_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{{{"f1", "v1-1-0"}}, {}};
        std::vector<ErrorCode> expected_ec_per_key{EC_OK, EC_ERROR};
        auto ec_per_key = redis_client_->GetAllFields(keys, field_maps);
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }
}

TEST_F(RedisClientTest, TestExists) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<bool> is_exist_vec;
        auto ec_per_key = redis_client_->Exists(keys, is_exist_vec);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(std::vector<bool>(keys.size(), true), is_exist_vec);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_ERROR, "ERROR")); // error
        prepared_replies.emplace_back(MakeFakeReplyInteger(0));                   // not exist
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<bool> is_exist_vec;
        auto ec_per_key = redis_client_->Exists(keys, is_exist_vec);
        std::vector<ErrorCode> expected_ec_per_key{EC_ERROR, EC_OK};
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        std::vector<bool> expected_is_exist_vec{false, false};
        ASSERT_EQ(expected_is_exist_vec, is_exist_vec);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_ERROR, "ERROR")); // all error
        prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_ERROR, "ERROR")); // all error
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key1")),
                                                ElementsAre(StrEq("EXISTS"), StrEq("key2")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key1", "key2"};
        std::vector<bool> is_exist_vec;
        auto ec_per_key = redis_client_->Exists(keys, is_exist_vec);
        std::vector<ErrorCode> expected_ec_per_key{EC_ERROR, EC_ERROR};
        ASSERT_EQ(expected_ec_per_key, ec_per_key);
        std::vector<bool> expected_is_exist_vec{false, false};
        ASSERT_EQ(expected_is_exist_vec, is_exist_vec);
    }
}

TEST_F(RedisClientTest, TestScan) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_replies;
        std::vector<std::optional<std::string>> expected_keys_opt{"instance:key1", "instance:key2", "instance:key3"};
        prepared_replies.emplace_back(MakeFakeReplyScan(/*next_cursor*/ "5", expected_keys_opt));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("SCAN"), StrEq("0"), StrEq("MATCH"), StrEq("instance:*"), StrEq("COUNT"), StrEq("5")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::string next_cursor;
        std::vector<std::string> keys;
        std::vector<std::string> expected_keys{"instance:key1", "instance:key2", "instance:key3"};
        auto ec = redis_client_->Scan(/*matching_prefix*/ "instance:", /*cursor*/ "0", /*limit*/ 5, next_cursor, keys);
        ASSERT_EQ(EC_OK, ec);
        ASSERT_EQ("5", next_cursor);
        ASSERT_EQ(expected_keys, keys);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        std::vector<std::optional<std::string>> expected_keys_opt{};
        prepared_replies.emplace_back(MakeFakeReplyScan(/*next_cursor*/ "5", expected_keys_opt));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("SCAN"), StrEq("0"), StrEq("MATCH"), StrEq("instance:*"), StrEq("COUNT"), StrEq("5")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::string next_cursor;
        std::vector<std::string> keys;
        std::vector<std::string> expected_keys{};
        auto ec = redis_client_->Scan(/*matching_prefix*/ "instance:", /*cursor*/ "0", /*limit*/ 5, next_cursor, keys);
        ASSERT_EQ(EC_OK, ec);
        ASSERT_EQ("5", next_cursor);
        ASSERT_EQ(expected_keys, keys);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        std::vector<std::optional<std::string>> expected_keys_opt{std::nullopt}; // invalid nil key
        prepared_replies.emplace_back(MakeFakeReplyScan(/*next_cursor*/ "5", expected_keys_opt));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("SCAN"), StrEq("0"), StrEq("MATCH"), StrEq("instance:*"), StrEq("COUNT"), StrEq("5")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::string next_cursor;
        std::vector<std::string> keys;
        std::vector<std::string> expected_keys{};
        auto ec = redis_client_->Scan(/*matching_prefix*/ "instance:", /*cursor*/ "0", /*limit*/ 5, next_cursor, keys);
        ASSERT_EQ(EC_ERROR, ec);
        ASSERT_EQ("", next_cursor);
        ASSERT_EQ(expected_keys, keys);
    }
    {
        std::vector<ReplyUPtr> prepared_replies;
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("SCAN"), StrEq("0"), StrEq("MATCH"), StrEq("instance:*"), StrEq("COUNT"), StrEq("5")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::string next_cursor;
        std::vector<std::string> keys;
        std::vector<std::string> expected_keys{};
        auto ec = redis_client_->Scan(/*matching_prefix*/ "instance:", /*cursor*/ "0", /*limit*/ 5, next_cursor, keys);
        ASSERT_EQ(EC_ERROR, ec);
        ASSERT_EQ("", next_cursor);
        ASSERT_EQ(expected_keys, keys);
    }
}

TEST_F(RedisClientTest, TestRand) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));
    {
        std::vector<ReplyUPtr> prepared_replies;
        for (int i = 0; i < 20; ++i) {
            if (i & 1) {
                prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
            } else {
                prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_NIL, ""));
            }
        }
        prepared_replies[0] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key1");
        prepared_replies[9] = MakeFakeReply(REDIS_REPLY_STRING, "instance2:key2");
        prepared_replies[19] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key3");
        std::vector<CmdArgs> expected_cmds(20, CmdArgs{"RANDOMKEY"});
        EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAreArray(expected_cmds)))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys;
        auto ec = redis_client_->Rand(/*matching_prefix*/ "instance1:", /*count*/ 2, keys);
        ASSERT_EQ(EC_OK, ec);
        std::vector<std::string> expected_keys{"instance1:key1", "instance1:key3"};
        ASSERT_EQ(expected_keys, keys);
    }
    {
        std::vector<ReplyUPtr> prepared_replies_0;
        for (int i = 0; i < 20; ++i) {
            prepared_replies_0.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
        }
        std::vector<ReplyUPtr> prepared_replies_1;
        for (int i = 0; i < 20; ++i) {
            prepared_replies_1.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
        }
        std::vector<ReplyUPtr> prepared_replies_2;
        for (int i = 0; i < 20; ++i) {
            prepared_replies_2.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
        }
        std::vector<ReplyUPtr> prepared_replies_3;
        for (int i = 0; i < 20; ++i) {
            prepared_replies_3.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
        }
        prepared_replies_0[0] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key1");
        prepared_replies_0[9] = MakeFakeReply(REDIS_REPLY_STRING, "instance2:key2");
        prepared_replies_0[19] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key3");
        std::vector<CmdArgs> expected_cmds(20, CmdArgs{"RANDOMKEY"});
        EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAreArray(expected_cmds)))
            .WillOnce(Return(ByMove(std::move(prepared_replies_0))))
            .WillOnce(Return(ByMove(std::move(prepared_replies_1))))
            .WillOnce(Return(ByMove(std::move(prepared_replies_2))))
            .WillOnce(Return(ByMove(std::move(prepared_replies_3))));

        std::vector<std::string> keys;
        auto ec = redis_client_->Rand(/*matching_prefix*/ "instance1:", /*count*/ 3, keys);
        ASSERT_EQ(EC_OK, ec);
        std::vector<std::string> expected_keys{"instance1:key1", "instance1:key3"};
        ASSERT_EQ(expected_keys, keys);
    }
    {
        std::vector<ReplyUPtr> prepared_replies_0;
        for (int i = 0; i < 20; ++i) {
            prepared_replies_0.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some_other_key"));
        }
        prepared_replies_0[0] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key1");
        prepared_replies_0[9] = MakeFakeReply(REDIS_REPLY_STRING, "instance2:key2");
        prepared_replies_0[19] = MakeFakeReply(REDIS_REPLY_STRING, "instance1:key3");
        std::vector<ReplyUPtr> prepared_replies_1; // empty, error happened
        std::vector<CmdArgs> expected_cmds_0(20, CmdArgs{"RANDOMKEY"});
        std::vector<CmdArgs> expected_cmds_1(20, CmdArgs{"RANDOMKEY"});
        EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAreArray(expected_cmds_0)))
            .WillOnce(Return(ByMove(std::move(prepared_replies_0))))
            .WillOnce(Return(ByMove(std::move(prepared_replies_1))));

        std::vector<std::string> keys;
        auto ec = redis_client_->Rand(/*matching_prefix*/ "instance1:", /*count*/ 3, keys);
        ASSERT_EQ(EC_ERROR, ec);
        std::vector<std::string> expected_keys{};
        ASSERT_EQ(expected_keys, keys);
    }
}

TEST_F(RedisClientTest, TestKeysAndFieldsWithSpaces) {
    EXPECT_CALL(*redis_client_, IsContextOk()).WillRepeatedly(Return(true));

    // Test key with spaces for Set method
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1)); // DEL reply
        prepared_replies.emplace_back(MakeFakeReplyInteger(2)); // HSET reply
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("DEL"), StrEq("key with spaces")),
                                                ElementsAre(StrEq("HSET"),
                                                            StrEq("key with spaces"),
                                                            StrEq("another field"),
                                                            StrEq("another value"),
                                                            StrEq("field with spaces"),
                                                            StrEq("value with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key with spaces"};
        std::vector<std::map<std::string, std::string>> field_maps{
            {{"field with spaces", "value with spaces"}, {"another field", "another value"}}};
        auto ec_per_key = redis_client_->Set(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }

    // Test key with spaces for Update method
    {
        std::vector<ReplyUPtr> prepared_exists_replies;
        prepared_exists_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_exists_replies))));

        std::vector<ReplyUPtr> prepared_hset_replies;
        prepared_hset_replies.emplace_back(MakeFakeReplyInteger(2));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HSET"),
                                                            StrEq("key with spaces"),
                                                            StrEq("another field"),
                                                            StrEq("another value"),
                                                            StrEq("field with spaces"),
                                                            StrEq("value with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_hset_replies))));

        std::vector<std::string> keys{"key with spaces"};
        std::vector<std::map<std::string, std::string>> field_maps{
            {{"field with spaces", "value with spaces"}, {"another field", "another value"}}};
        auto ec_per_key = redis_client_->Update(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }

    // Test key with spaces for Delete method
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAre(ElementsAre(StrEq("DEL"), StrEq("key with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key with spaces"};
        auto ec_per_key = redis_client_->Delete(keys);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
    }

    // Test key with spaces for Get method
    {
        std::vector<ReplyUPtr> prepared_hmget_replies;
        prepared_hmget_replies.emplace_back(MakeFakeReplyArrayString({"value with spaces", "another value"}));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("HMGET"), StrEq("key with spaces"), StrEq("field with spaces"), StrEq("another field")))))
            .WillOnce(Return(ByMove(std::move(prepared_hmget_replies))));

        std::vector<std::string> keys{"key with spaces"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{
            {{"field with spaces", "value with spaces"}, {"another field", "another value"}}};
        auto ec_per_key = redis_client_->Get(keys, /*field_names*/ std::vector<std::string>{"field with spaces", "another field"}, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }

    // Test key with spaces for GetAllFields method
    {
        std::vector<ReplyUPtr> prepared_hgetall_replies;
        prepared_hgetall_replies.emplace_back(
            MakeFakeReplyArrayString({"another field", "another value", "field with spaces", "value with spaces"}));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("HGETALL"), StrEq("key with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_hgetall_replies))));

        std::vector<std::string> keys{"key with spaces"};
        std::vector<std::map<std::string, std::string>> field_maps;
        std::vector<std::map<std::string, std::string>> expected_field_maps{
            {{"field with spaces", "value with spaces"}, {"another field", "another value"}}};
        auto ec_per_key = redis_client_->GetAllFields(keys, field_maps);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(expected_field_maps, field_maps);
    }

    // Test key with spaces for Exists method
    {
        std::vector<ReplyUPtr> prepared_replies;
        prepared_replies.emplace_back(MakeFakeReplyInteger(1));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(StrEq("EXISTS"), StrEq("key with spaces")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys{"key with spaces"};
        std::vector<bool> is_exist_vec;
        auto ec_per_key = redis_client_->Exists(keys, is_exist_vec);
        ASSERT_EQ(std::vector<ErrorCode>(keys.size(), EC_OK), ec_per_key);
        ASSERT_EQ(std::vector<bool>(keys.size(), true), is_exist_vec);
    }

    // Test key with spaces for Scan method
    {
        std::vector<ReplyUPtr> prepared_replies;
        std::vector<std::optional<std::string>> expected_keys_opt{"key with spaces", "another key with spaces"};
        prepared_replies.emplace_back(MakeFakeReplyScan(/*next_cursor*/ "0", expected_keys_opt));
        EXPECT_CALL(*redis_client_,
                    TryExecPipeline(ElementsAre(ElementsAre(
                        StrEq("SCAN"), StrEq("0"), StrEq("MATCH"), StrEq("key with*"), StrEq("COUNT"), StrEq("10")))))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::string next_cursor;
        std::vector<std::string> keys;
        std::vector<std::string> expected_keys{"key with spaces", "another key with spaces"};
        auto ec = redis_client_->Scan(/*matching_prefix*/ "key with", /*cursor*/ "0", /*limit*/ 10, next_cursor, keys);
        ASSERT_EQ(EC_OK, ec);
        ASSERT_EQ("0", next_cursor);
        ASSERT_EQ(expected_keys, keys);
    }

    // Test key with spaces for Rand method
    {
        std::vector<ReplyUPtr> prepared_replies;
        for (int i = 0; i < 20; ++i) {
            if (i & 1) {
                prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_STRING, "some other key"));
            } else {
                prepared_replies.emplace_back(MakeFakeReply(REDIS_REPLY_NIL, ""));
            }
        }
        prepared_replies[0] = MakeFakeReply(REDIS_REPLY_STRING, "key with spaces");
        prepared_replies[9] = MakeFakeReply(REDIS_REPLY_STRING, "key with spaces another");
        prepared_replies[19] = MakeFakeReply(REDIS_REPLY_STRING, "key with spaces yet another");
        std::vector<CmdArgs> expected_cmds(20, CmdArgs{"RANDOMKEY"});
        EXPECT_CALL(*redis_client_, TryExecPipeline(ElementsAreArray(expected_cmds)))
            .WillOnce(Return(ByMove(std::move(prepared_replies))));

        std::vector<std::string> keys;
        auto ec = redis_client_->Rand(/*matching_prefix*/ "key with", /*count*/ 2, keys);
        ASSERT_EQ(EC_OK, ec);
        std::vector<std::string> expected_keys{"key with spaces", "key with spaces another"};
        ASSERT_EQ(expected_keys, keys);
    }
}

// ==================== Static Command Builder Tests ====================

TEST_F(RedisClientTest, TestBuildSetCmds) {
    std::vector<std::string> keys = {"key1", "key2"};
    std::vector<std::map<std::string, std::string>> field_maps = {
        {{"f1", "v1"}, {"f2", "v2"}},
        {{"f3", "v3"}},
    };
    std::vector<CmdArgs> cmds;
    RedisClient::BuildSetCmds(keys, field_maps, cmds);

    // key1: DEL + HSET, key2: DEL + HSET = 4 cmds
    ASSERT_EQ(4, cmds.size());
    ASSERT_EQ(CmdArgs({"DEL", "key1"}), cmds[0]);
    ASSERT_EQ("HSET", cmds[1][0]);
    ASSERT_EQ("key1", cmds[1][1]);
    ASSERT_EQ(6, cmds[1].size()); // HSET key f1 v1 f2 v2
    ASSERT_EQ(CmdArgs({"DEL", "key2"}), cmds[2]);
    ASSERT_EQ("HSET", cmds[3][0]);
    ASSERT_EQ("key2", cmds[3][1]);
    ASSERT_EQ(4, cmds[3].size()); // HSET key f3 v3
}

TEST_F(RedisClientTest, TestBuildSetCmdsEmptyFieldMap) {
    std::vector<std::string> keys = {"key1"};
    std::vector<std::map<std::string, std::string>> field_maps = {{}};
    std::vector<CmdArgs> cmds;
    RedisClient::BuildSetCmds(keys, field_maps, cmds);

    // Empty field_map: DEL only, no HSET
    ASSERT_EQ(1, cmds.size());
    ASSERT_EQ(CmdArgs({"DEL", "key1"}), cmds[0]);
}

TEST_F(RedisClientTest, TestBuildSetCmdsEmpty) {
    std::vector<CmdArgs> cmds;
    RedisClient::BuildSetCmds({}, {}, cmds);
    ASSERT_TRUE(cmds.empty());
}

TEST_F(RedisClientTest, TestBuildHashSetCmds) {
    std::vector<std::string> keys = {"k1", "k2", "k3"};
    std::vector<std::map<std::string, std::string>> field_maps = {
        {{"a", "1"}},
        {},           // empty - should be skipped
        {{"b", "2"}, {"c", "3"}},
    };
    std::vector<CmdArgs> cmds;
    RedisClient::BuildHashSetCmds(keys, field_maps, cmds);

    // k1: HSET, k2: skipped, k3: HSET = 2 cmds
    ASSERT_EQ(2, cmds.size());
    ASSERT_EQ("HSET", cmds[0][0]);
    ASSERT_EQ("k1", cmds[0][1]);
    ASSERT_EQ(4, cmds[0].size()); // HSET k1 a 1
    ASSERT_EQ("HSET", cmds[1][0]);
    ASSERT_EQ("k3", cmds[1][1]);
    ASSERT_EQ(6, cmds[1].size()); // HSET k3 b 2 c 3
}

TEST_F(RedisClientTest, TestBuildHashSetCmdsEmpty) {
    std::vector<CmdArgs> cmds;
    RedisClient::BuildHashSetCmds({}, {}, cmds);
    ASSERT_TRUE(cmds.empty());
}

TEST_F(RedisClientTest, TestBuildDeleteCmds) {
    std::vector<std::string> keys = {"key_a", "key_b", "key_c"};
    std::vector<CmdArgs> cmds;
    RedisClient::BuildDeleteCmds(keys, cmds);

    ASSERT_EQ(3, cmds.size());
    ASSERT_EQ(CmdArgs({"DEL", "key_a"}), cmds[0]);
    ASSERT_EQ(CmdArgs({"DEL", "key_b"}), cmds[1]);
    ASSERT_EQ(CmdArgs({"DEL", "key_c"}), cmds[2]);
}

TEST_F(RedisClientTest, TestBuildDeleteCmdsEmpty) {
    std::vector<CmdArgs> cmds;
    RedisClient::BuildDeleteCmds({}, cmds);
    ASSERT_TRUE(cmds.empty());
}

TEST_F(RedisClientTest, TestBuildHashDeleteCmds) {
    std::vector<std::string> keys = {"k1", "k2", "k3"};
    std::vector<std::vector<std::string>> field_names_vec = {
        {"f1", "f2"},
        {},         // empty - should be skipped
        {"f3"},
    };
    std::vector<CmdArgs> cmds;
    RedisClient::BuildHashDeleteCmds(keys, field_names_vec, cmds);

    // k1: HDEL, k2: skipped, k3: HDEL = 2 cmds
    ASSERT_EQ(2, cmds.size());
    ASSERT_EQ(CmdArgs({"HDEL", "k1", "f1", "f2"}), cmds[0]);
    ASSERT_EQ(CmdArgs({"HDEL", "k3", "f3"}), cmds[1]);
}

TEST_F(RedisClientTest, TestBuildHashDeleteCmdsEmpty) {
    std::vector<CmdArgs> cmds;
    RedisClient::BuildHashDeleteCmds({}, {}, cmds);
    ASSERT_TRUE(cmds.empty());
}

TEST_F(RedisClientTest, TestBuildCmdsAppendToExisting) {
    std::vector<CmdArgs> cmds;
    cmds.push_back({"EXISTING_CMD"});

    RedisClient::BuildDeleteCmds({"k1"}, cmds);
    RedisClient::BuildHashSetCmds({"k2"}, {{{"f", "v"}}}, cmds);

    ASSERT_EQ(3, cmds.size());
    ASSERT_EQ("EXISTING_CMD", cmds[0][0]);
    ASSERT_EQ("DEL", cmds[1][0]);
    ASSERT_EQ("HSET", cmds[2][0]);
}

} // namespace kv_cache_manager
