genrule(
    name = "hiredis_pkg_include_hiredis_h",
    srcs = ["hiredis.h"],
    outs = ["pkg_include/hiredis/hiredis.h"],
    cmd = "mkdir -p $$(dirname $@) && cp $< $@",
)

cc_library(
    name = "hiredis",
    srcs = glob(
        ["*.c"],
        exclude = ["test.c"],
    ),
    hdrs = glob(["*.h"]) + [":hiredis_pkg_include_hiredis_h"],
    copts = ["-Wno-unused-function"],
    # "." keeps #include <hiredis.h> working; pkg_include matches distro layout for
    # #include <hiredis/hiredis.h> (e.g. mooncake-store redis HA code).
    includes = [
        ".",
        "pkg_include",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boringssl//:crypto",
        "@boringssl//:ssl",
    ],
)
