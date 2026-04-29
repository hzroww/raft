#pragma once

#include <ostream>
#include <sstream>
#include <string>

namespace raft::logging {

enum class Level {
    Trace = 0,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
};

struct Options {
    Level       min_level     = Level::Info;
    bool        log_to_stderr = true;
    bool        log_to_file   = true;
    std::string file_path     = "raft.log";
};

void Init(const Options& options = Options{});
void Shutdown();

void SetMinLevel(Level level);
bool ShouldLog(Level level);

void SetCurrentThreadName(const std::string& name);
std::string GetCurrentThreadName();
int GetCurrentThreadId();

class LogMessage {
public:
    LogMessage(Level level, const char* file, int line, const char* function);
    ~LogMessage();

    std::ostream& stream() { return stream_; }

private:
    Level              level_;
    const char*        file_;
    int                line_;
    const char*        function_;
    std::ostringstream stream_;
};

} // namespace raft::logging

#define LOG_TRACE() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Trace)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Trace, __FILE__, __LINE__, __func__).stream()

#define LOG_DEBUG() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Debug)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Debug, __FILE__, __LINE__, __func__).stream()

#define LOG_INFO() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Info)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Info, __FILE__, __LINE__, __func__).stream()

#define LOG_WARN() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Warn)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Warn, __FILE__, __LINE__, __func__).stream()

#define LOG_ERROR() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Error)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Error, __FILE__, __LINE__, __func__).stream()

#define LOG_FATAL() \
    if (!::raft::logging::ShouldLog(::raft::logging::Level::Fatal)) ; \
    else ::raft::logging::LogMessage(::raft::logging::Level::Fatal, __FILE__, __LINE__, __func__).stream()
