#include "log.h"

#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <string_view>

namespace raft::logging {
namespace {

class LoggerState {
public:
    LoggerState() { ApplyOptions(Options{}); }

    void ApplyOptions(const Options& options) {
        std::lock_guard<std::mutex> lock(mutex_);
        options_ = options;
        file_stream_.close();
        if (options_.log_to_file) {
            file_stream_.open(options_.file_path, std::ios::out | std::ios::app);
        }
    }

    void SetMinLevel(Level level) {
        std::lock_guard<std::mutex> lock(mutex_);
        options_.min_level = level;
    }

    bool ShouldLogUnlocked(Level level) const {
        return static_cast<int>(level) >= static_cast<int>(options_.min_level);
    }

    bool ShouldLog(Level level) {
        std::lock_guard<std::mutex> lock(mutex_);
        return ShouldLogUnlocked(level);
    }

    void Write(Level level,
               const char* file,
               int         line,
               const char* function,
               std::string message) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ShouldLogUnlocked(level)) return;

        const std::string formatted = FormatLine(level, file, line, function, message);

        if (options_.log_to_stderr) {
            ::fwrite(formatted.data(), 1, formatted.size(), stderr);
            ::fflush(stderr);
        }
        if (options_.log_to_file && file_stream_.is_open()) {
            file_stream_ << formatted;
            file_stream_.flush();
        }
    }

private:
    static const char* LevelName(Level level) {
        switch (level) {
        case Level::Trace: return "TRACE";
        case Level::Debug: return "DEBUG";
        case Level::Info:  return "INFO";
        case Level::Warn:  return "WARN";
        case Level::Error: return "ERROR";
        case Level::Fatal: return "FATAL";
        }
        return "UNKNOWN";
    }

    static std::string Basename(const char* file) {
        std::string_view path(file == nullptr ? "" : file);
        const size_t slash = path.find_last_of("/\\");
        if (slash == std::string_view::npos) return std::string(path);
        return std::string(path.substr(slash + 1));
    }

    static std::string FormatTimestamp() {
        using namespace std::chrono;
        const auto now       = system_clock::now();
        const auto secs      = system_clock::to_time_t(now);
        const auto millisecs = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

        std::tm tm{};
        localtime_r(&secs, &tm);

        std::ostringstream out;
        out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
            << '.'
            << std::setw(3)
            << std::setfill('0')
            << millisecs.count();
        return out.str();
    }

    static std::string FormatLine(Level            level,
                                  const char*      file,
                                  int              line,
                                  const char*      function,
                                  const std::string& message) {
        std::ostringstream out;
        out << FormatTimestamp()
            << ' '
            << LevelName(level)
            << " pid="
            << ::getpid()
            << " tid="
            << GetCurrentThreadId()
            << " tname="
            << GetCurrentThreadName()
            << ' '
            << Basename(file)
            << ':'
            << line
            << ' '
            << (function == nullptr ? "" : function)
            << " | "
            << message
            << '\n';
        return out.str();
    }

    std::mutex    mutex_;
    Options       options_;
    std::ofstream file_stream_;
};

LoggerState& GetLoggerState() {
    static LoggerState state;
    return state;
}

thread_local std::string g_thread_name;
thread_local int g_thread_id = 0;

} // namespace

void Init(const Options& options) {
    GetLoggerState().ApplyOptions(options);
}

void Shutdown() {
    Options options;
    options.log_to_file = false;
    GetLoggerState().ApplyOptions(options);
}

void SetMinLevel(Level level) {
    GetLoggerState().SetMinLevel(level);
}

bool ShouldLog(Level level) {
    return GetLoggerState().ShouldLog(level);
}

void SetCurrentThreadName(const std::string& name) {
    g_thread_name = name;

    std::array<char, 16> buffer{};
    if (name.empty()) return;
    std::strncpy(buffer.data(), name.c_str(), buffer.size() - 1);
    buffer.back() = '\0';
    ::pthread_setname_np(::pthread_self(), buffer.data());
}

std::string GetCurrentThreadName() {
    if (!g_thread_name.empty()) return g_thread_name;

    std::array<char, 16> buffer{};
    if (::pthread_getname_np(::pthread_self(), buffer.data(), buffer.size()) == 0 &&
        buffer[0] != '\0') {
        g_thread_name = buffer.data();
        return g_thread_name;
    }

    return "unnamed";
}

int GetCurrentThreadId() {
    if (g_thread_id == 0) {
        g_thread_id = static_cast<int>(::syscall(SYS_gettid));
    }
    return g_thread_id;
}

LogMessage::LogMessage(Level level, const char* file, int line, const char* function)
    : level_(level), file_(file), line_(line), function_(function) {}

LogMessage::~LogMessage() {
    GetLoggerState().Write(level_, file_, line_, function_, stream_.str());
    if (level_ == Level::Fatal) {
        std::abort();
    }
}

} // namespace raft::logging
