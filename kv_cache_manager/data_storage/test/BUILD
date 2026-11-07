package(default_visibility = ["//visibility:private"])

cc_test(
    name = "DataStorageManagerTest",
    srcs = [
        "data_storage_manager_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/common/test:log_capture",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "NfsBackendTest",
    srcs = [
        "nfs_backend_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "Hf3fsBackendTest",
    srcs = [
        "hf3fs_backend_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "DummyBackendTest",
    srcs = [
        "dummy_backend_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "StorageConfigTest",
    srcs = [
        "storage_config_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "EventReportBackendTest",
    srcs = [
        "event_report_backend_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage",
    ],
)

cc_test(
    name = "SnapshotUriUtilsTest",
    srcs = [
        "snapshot_uri_utils_test.cc",
    ],
    deps = [
        "//kv_cache_manager/data_storage:snapshot_uri_utils",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "ValkeyBackendTest",
    srcs = [
        "valkey_backend_test.cc",
    ],
    deps = [
        "//kv_cache_manager/common:redis_client",
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/data_storage:valkey_backend",
        "//kv_cache_manager/data_storage:valkey_uri_util",
        "//kv_cache_manager/metrics:metrics_registry",
        "@hiredis",
    ],
)

cc_test(
    name = "valkey_backend_real_service_test",
    srcs = [
        "valkey_backend_real_service_test.cc",
    ],
    tags = [
        "manual",
        "valkey",
    ],
    deps = [
        "//kv_cache_manager/common:redis_client",
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/common/test:secure_valkey_test_environment",
        "//kv_cache_manager/data_storage:valkey_backend",
        "//kv_cache_manager/data_storage:valkey_uri_util",
        "//kv_cache_manager/metrics:metrics_registry",
    ],
)
