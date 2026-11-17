load("@rules_python//python:py_test.bzl", "py_test")

package(default_visibility = ["//visibility:private"])

config_setting(
    name = "using_cuda",
    values = {"define": "using_cuda=true"},
)

config_setting(
    name = "using_musa",
    values = {"define": "using_musa=true"},
)

cc_test(
    name = "MetaClientTest",
    srcs = [
        "meta_client_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/client",
        "//kv_cache_manager/common:unittest",
    ],
)

cc_test(
    name = "TransferClientTest",
    srcs = [
        "transfer_client_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/client",
        "//kv_cache_manager/common:unittest",
    ],
)

cc_test(
    name = "ValkeyTransferClientTest",
    srcs = [
        "valkey_loopback_test_server.h",
        "valkey_transfer_client_test.cc",
    ],
    deps = [
        "//kv_cache_manager/client",
        "//kv_cache_manager/common:unittest",
    ],
)

cc_test(
    name = "ValkeyTransferClientGpuTest",
    srcs = [
        "valkey_loopback_test_server.h",
        "valkey_transfer_client_gpu_test.cc",
    ],
    target_compatible_with = select({
        ":using_cuda": [],
        ":using_musa": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    deps = [
        "//kv_cache_manager/client",
        "//kv_cache_manager/common:unittest",
    ] + select({
        ":using_cuda": ["@local_config_cuda//cuda"],
        ":using_musa": ["@local_config_musa//:musa"],
        "//conditions:default": [],
    }),
)

cc_test(
    name = "valkey_transfer_client_real_service_test",
    srcs = ["valkey_transfer_client_real_service_test.cc"],
    # CI selects manual live tests tagged "redis". Plaintext cases start their own
    # unauthenticated loopback valkey-server and secure cases start a TLS/ACL one,
    # both from PATH (provided by the CI image).
    tags = [
        "manual",
        "redis",
    ],
    deps = [
        "//kv_cache_manager/client",
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/common/test:secure_valkey_test_environment",
        "//kv_cache_manager/data_storage",
        "//kv_cache_manager/metrics:metrics_registry",
        "//kv_cache_manager/protocol/protobuf:service_cc_proto",
        "//kv_cache_manager/service/util:manager_message_proto_util",
    ],
)

py_binary(
    name = "asan_runner",
    srcs = ["asan_runner.py"],
    visibility = ["//visibility:private"],
)

py_test(
    name = "transfer_client_py_test",
    size = "small",
    srcs = [
        "asan_runner.py",
        "transfer_client_py_test.py",
    ],
    args = [
        "$(location lsan_suppressions.txt)",  # → argv[1]
        "$(location transfer_client_py_test.py)",  # → argv[2]
        # 后续可加 --test_arg="--verbose" 等，会传给真实测试
    ],
    data = ["lsan_suppressions.txt"],
    # 主入口改为 runner
    main = "asan_runner.py",
    deps = [
        "//kv_cache_manager/client/pybind:kvcm_py_client_lib",
    ],
)
