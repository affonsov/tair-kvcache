#!/usr/bin/env python3

import re
import unittest
from pathlib import Path
from textwrap import dedent


_REPOSITORY_ROOT = Path(__file__).parents[3]

_JSONIZABLE_PARSE_BODY = dedent(
    """\
    doc.Parse(str.c_str());
    if (doc.HasParseError()) {
        KVCM_LOG_WARN("invalid json parse error code [%d], offset [%zu], input bytes [%zu]",
                      static_cast<int>(doc.GetParseError()),
                      static_cast<std::size_t>(doc.GetErrorOffset()),
                      str.size());
        return false;
    }
    return true;
    """
).strip()


def read_source(relative_path: str) -> str:
    return (_REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")


def _mask_range(masked: list[str], start: int, end: int) -> None:
    for index in range(start, end):
        if masked[index] not in {"\r", "\n"}:
            masked[index] = " "


def mask_cpp_non_code(source: str) -> str:
    """Mask C++ comments and literals without changing source offsets."""
    masked = list(source)
    raw_prefixes = ('u8R"', 'uR"', 'UR"', 'LR"', 'R"')
    invalid_raw_delimiter_characters = {
        " ",
        "(",
        ")",
        "\\",
        "\t",
        "\v",
        "\f",
        "\r",
        "\n",
    }
    index = 0

    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            if end == -1:
                end = len(source)
            _mask_range(masked, index, end)
            index = end
            continue

        if source.startswith("/*", index):
            closing = source.find("*/", index + 2)
            if closing == -1:
                raise AssertionError("Unterminated block comment")
            end = closing + 2
            _mask_range(masked, index, end)
            index = end
            continue

        raw_prefix = next(
            (prefix for prefix in raw_prefixes if source.startswith(prefix, index)),
            None,
        )
        if raw_prefix is not None and (
            index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_")
        ):
            delimiter_start = index + len(raw_prefix)
            opening_parenthesis = delimiter_start
            while (
                opening_parenthesis < len(source) and source[opening_parenthesis] != "("
            ):
                delimiter_character = source[opening_parenthesis]
                if (
                    delimiter_character in invalid_raw_delimiter_characters
                    or opening_parenthesis - delimiter_start == 16
                ):
                    raise AssertionError("Invalid raw string delimiter")
                opening_parenthesis += 1
            if opening_parenthesis == len(source):
                raise AssertionError("Unterminated raw string literal")

            delimiter = source[delimiter_start:opening_parenthesis]
            terminator = f'){delimiter}"'
            closing = source.find(terminator, opening_parenthesis + 1)
            if closing == -1:
                raise AssertionError("Unterminated raw string literal")
            end = closing + len(terminator)
            _mask_range(masked, index, end)
            index = end
            continue

        if source[index] in {'"', "'"}:
            quote = source[index]
            end = index + 1
            while end < len(source):
                if source[end] in {"\r", "\n"}:
                    raise AssertionError("Unterminated ordinary literal")
                if source[end] == "\\":
                    end += 1
                    if end == len(source):
                        raise AssertionError("Unterminated ordinary literal")
                    if source.startswith("\r\n", end):
                        end += 2
                    else:
                        end += 1
                    continue
                if source[end] == quote:
                    end += 1
                    break
                end += 1
            else:
                raise AssertionError("Unterminated ordinary literal")
            _mask_range(masked, index, end)
            index = end
            continue

        index += 1

    return "".join(masked)


def function_body(source: str, signature: str) -> str:
    masked = mask_cpp_non_code(source)
    matches = list(re.finditer(re.escape(signature), masked))
    if len(matches) != 1:
        raise AssertionError(
            f"Expected exactly one executable match for {signature}, found {len(matches)}"
        )

    body_start = masked.find("{", matches[0].end())
    if body_start == -1:
        raise AssertionError(f"Missing function body for {signature}")

    depth = 0
    for index in range(body_start, len(masked)):
        if masked[index] == "{":
            depth += 1
        elif masked[index] == "}":
            depth -= 1
            if depth == 0:
                return source[body_start + 1 : index]
    raise AssertionError(f"Unterminated function body for {signature}")


def log_calls(function: str) -> list[str]:
    masked = mask_cpp_non_code(function)
    calls = []
    for match in re.finditer(r"\bKVCM_(?:REDIS_)?LOG_[A-Z]+\s*\(", masked):
        open_parenthesis = masked.index("(", match.start())
        depth = 0
        for index in range(open_parenthesis, len(masked)):
            if masked[index] == "(":
                depth += 1
            elif masked[index] == ")":
                depth -= 1
                if depth == 0:
                    calls.append(function[match.start() : index + 1])
                    break
        else:
            raise AssertionError("Unterminated log call")
    return calls


def log_call_receives(function: str, expression: str) -> bool:
    return any(
        re.search(expression, call, flags=re.DOTALL) for call in log_calls(function)
    )


class SecureValkeySourceTest(unittest.TestCase):
    def test_cpp_lexical_mask_preserves_offsets_and_supported_literals(self) -> None:
        delimiter = "!#$%&*+,-./:;<"
        payload = 'KVCM_LOG_WARN("fake ) } // /*")'
        raw_literals = [
            f'const char *raw_{index} = {prefix}"{delimiter}({payload}){delimiter}";'
            for index, prefix in enumerate(("R", "u8R", "uR", "UR", "LR"))
        ]
        source = "\n".join(
            [
                r'const char *ordinary = "escaped \" quote and }";',
                r"char quote = '\'';",
                "// KVCM_LOG_INFO(commented)",
                "/* KVCM_LOG_ERROR(block_comment); { } */",
                *raw_literals,
                "return executable_code;",
            ]
        )

        masked = mask_cpp_non_code(source)

        self.assertEqual(len(source), len(masked))
        self.assertEqual(
            [index for index, character in enumerate(source) if character == "\n"],
            [index for index, character in enumerate(masked) if character == "\n"],
        )
        self.assertNotIn("KVCM_LOG_", masked)
        self.assertNotIn("escaped", masked)
        self.assertIn("return executable_code;", masked)

    def test_cpp_lexical_mask_rejects_unterminated_non_code(self) -> None:
        cases = (
            "/* unterminated",
            '"unterminated',
            "'unterminated",
            'R"tag(unterminated',
        )

        for source in cases:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "Unterminated"):
                    mask_cpp_non_code(source)

    def test_block_commented_approved_body_does_not_hide_unsafe_definition(
        self,
    ) -> None:
        source = dedent(
            f"""\
            /*
            bool Jsonizable::Parse(const std::string &str) {{
            {_JSONIZABLE_PARSE_BODY}
            }}
            */
            bool Jsonizable::Parse(const std::string &str) {{
                return false;
            }}
            """
        )

        parse_body = dedent(function_body(source, "bool Jsonizable::Parse")).strip()

        self.assertEqual("return false;", parse_body)
        self.assertNotEqual(_JSONIZABLE_PARSE_BODY, parse_body)

    def test_raw_string_approved_body_does_not_hide_unsafe_definition(self) -> None:
        source = dedent(
            f"""\
            const char *decoy = u8R"approved(
            bool Jsonizable::Parse(const std::string &str) {{
            {_JSONIZABLE_PARSE_BODY}
            }}
            )approved";
            bool Jsonizable::Parse(const std::string &str) {{
                return false;
            }}
            """
        )

        parse_body = dedent(function_body(source, "bool Jsonizable::Parse")).strip()

        self.assertEqual("return false;", parse_body)
        self.assertNotEqual(_JSONIZABLE_PARSE_BODY, parse_body)

    def test_function_body_ignores_comment_and_literal_braces(self) -> None:
        source = dedent(
            r"""
            bool Example() {
                const char *ordinary = "}";
                const char *escaped = "escaped quote: \" and brace {";
                char opening = '{';
                const char *raw = LR"braces({{ " }} })braces";
                /* }}} */
                // }
                if (true) {
                    return marker;
                }
            }
            bool Other() { return false; }
            """
        )

        body = function_body(source, "bool Example")

        self.assertIn("return marker;", body)
        self.assertNotIn("bool Other", body)

    def test_function_body_requires_one_executable_signature(self) -> None:
        source = "bool Example() { return true; } bool Example(int) { return false; }"

        with self.assertRaisesRegex(AssertionError, "found 2"):
            function_body(source, "bool Example")

    def test_log_calls_ignore_comments_and_literals(self) -> None:
        function = dedent(
            r"""
            // KVCM_LOG_WARN("commented %s", commented_secret.c_str());
            /* KVCM_REDIS_LOG_ERROR("blocked %s", blocked_secret.c_str()); */
            const char *quoted = "KVCM_LOG_INFO(quoted_secret)";
            const char *raw = R"logs(KVCM_LOG_DEBUG(raw_secret))logs";
            KVCM_LOG_INFO(
                "safe value (%s), parentheses ())",
                safe_value.c_str());
            """
        )

        calls = log_calls(function)

        self.assertEqual(1, len(calls))
        self.assertEqual(
            dedent(
                """\
                KVCM_LOG_INFO(
                    "safe value (%s), parentheses ())",
                    safe_value.c_str())
                """
            ).strip(),
            calls[0],
        )
        self.assertTrue(log_call_receives(function, r"safe_value\.c_str\(\)"))
        self.assertFalse(log_call_receives(function, r"(?:commented|blocked)_secret"))
        self.assertFalse(log_call_receives(function, r"(?:quoted|raw)_secret"))

    def test_named_acl_auth_uses_fixed_binary_safe_arguments(self) -> None:
        source = read_source("kv_cache_manager/common/redis_client.cc")
        auth = function_body(source, "RedisClient::ReplyUPtr RunAuthCommand")
        normalized = " ".join(auth.split())

        self.assertIn("std::array<const char *, 3> argv", normalized)
        self.assertIn("std::array<std::size_t, 3> argvlen", normalized)
        self.assertIn("credentials.username.data()", auth)
        self.assertIn("credentials.username.size()", auth)
        self.assertIn("credentials.password.data()", auth)
        self.assertIn("credentials.password.size()", auth)
        self.assertIn("redisCommandArgv", auth)
        self.assertNotIn("CmdArgs", auth)
        self.assertNotIn("std::vector", auth)
        self.assertNotRegex(auth, r"KVCM_(?:REDIS_)?LOG_")

    def test_credentials_are_raii_owned_and_cleansed_without_generic_error_logging(
        self,
    ) -> None:
        source = read_source("kv_cache_manager/common/redis_client.cc")
        cleanse = function_body(source, "void CleanseString")
        connect = function_body(source, "bool RedisClient::ConnectSecure")

        self.assertIn("#include <openssl/mem.h>", source)
        self.assertIn("if (!value.empty())", cleanse)
        self.assertIn("OPENSSL_cleanse(value.data(), value.size())", cleanse)
        self.assertLess(
            connect.index("Credentials credentials;"),
            connect.index("std::string directory_path"),
        )
        self.assertIn("RunAuthCommand(context_, credentials)", connect)
        self.assertIn("credentials.Cleanse()", connect)
        self.assertIn('IsExactStatus(auth_reply.get(), "OK")', connect)
        self.assertNotRegex(connect, r"IsReplyOk\s*\(\s*auth_reply")
        self.assertNotIn("RunCommand(auth", connect)
        self.assertFalse(
            log_call_receives(connect, r"credentials\.(?:username|password)")
        )
        self.assertFalse(log_call_receives(source, r"user_info_"))

    def test_raw_storage_config_is_not_passed_to_affected_logs(self) -> None:
        cases = (
            (
                "kv_cache_manager/client/src/transfer_client_impl.cc",
                "ClientErrorCode TransferClientImpl::InitInternal",
                r"init_params_\.storage_configs\.c_str\(\)",
                "init_params_.storage_configs.size()",
            ),
            (
                "kv_cache_manager/client/src/internal/stub/grpc_stub.cc",
                "std::pair<ClientErrorCode, std::string> GrpcStub::RegisterInstance",
                r"response\.storage_configs\(\)\.c_str\(\)",
                "response.storage_configs().size()",
            ),
            (
                "kv_cache_manager/client/src/internal/sdk/sdk_wrapper.cc",
                "ClientErrorCode SdkWrapper::Init",
                r"storage_configs\.c_str\(\)",
                "storage_configs.size()",
            ),
            (
                "kv_cache_manager/manager/cache_manager.cc",
                "ErrorCode CacheManager::DoRecoverOnce",
                r"config_str\.c_str\(\)",
                "config_str.size()",
            ),
            (
                "kv_cache_manager/manager/startup_config_loader.cc",
                "bool StartupConfigLoader::Load",
                r"config_str\.c_str\(\)",
                "config_str.size()",
            ),
        )

        for path, signature, raw_expression, byte_count_expression in cases:
            with self.subTest(path=path):
                body = function_body(read_source(path), signature)
                self.assertFalse(log_call_receives(body, raw_expression))
                self.assertTrue(
                    log_call_receives(body, re.escape(byte_count_expression))
                )

    def test_shared_json_parser_matches_strict_implementation(self) -> None:
        source = read_source("kv_cache_manager/common/jsonizable.cc")
        parse_body = dedent(function_body(source, "bool Jsonizable::Parse")).strip()

        self.assertEqual(_JSONIZABLE_PARSE_BODY, parse_body)


if __name__ == "__main__":
    unittest.main()
