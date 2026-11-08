#pragma once

#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <sys/stat.h>

namespace kv_cache_manager::test {

// Captures KVCM log output produced while the object is alive without
// reconfiguring the process-wide logger. The test main logs to the console in
// debug builds and to logs/kv_cache_manager.log otherwise, so both stdout and
// the bytes appended to that file are collected. The default appender flushes
// synchronously.
class ScopedLogCapture {
public:
    ScopedLogCapture() {
        struct stat file_stat {};
        if (stat(kLogFile, &file_stat) == 0) {
            file_offset_ = static_cast<std::streamoff>(file_stat.st_size);
        }
        testing::internal::CaptureStdout();
        capturing_ = true;
    }

    ~ScopedLogCapture() { (void)Stop(); }

    ScopedLogCapture(const ScopedLogCapture &) = delete;
    ScopedLogCapture &operator=(const ScopedLogCapture &) = delete;

    // Returns everything logged since construction; later calls return the same text.
    const std::string &Stop() {
        if (!capturing_) {
            return logs_;
        }
        logs_ = testing::internal::GetCapturedStdout();
        capturing_ = false;
        std::ifstream file(kLogFile, std::ios::in | std::ios::binary);
        if (file) {
            file.seekg(file_offset_);
            logs_.append(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        }
        return logs_;
    }

private:
    static constexpr char kLogFile[] = "logs/kv_cache_manager.log";

    std::streamoff file_offset_ = 0;
    bool capturing_ = false;
    std::string logs_;
};

} // namespace kv_cache_manager::test
