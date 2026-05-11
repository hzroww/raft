#include "file_raft_storage.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kvserver {

namespace fs = std::filesystem;

namespace {

void EnsureGood(const std::ios& stream, const std::string& action) {
    if (!stream.good()) {
        throw std::runtime_error("failed to " + action);
    }
}

template <typename T>
void WriteBinary(std::ostream& out, T value, const std::string& action) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    EnsureGood(out, action);
}

template <typename T>
bool ReadBinary(std::istream& in, T* value) {
    in.read(reinterpret_cast<char*>(value), sizeof(T));
    return in.good();
}

}  // namespace

FileRaftStorage::FileRaftStorage(std::string data_dir)
    : data_dir_(std::move(data_dir)),
      log_dir_(data_dir_ / "log"),
      hard_state_path_(data_dir_ / "hard.state"),
      snapshot_path_(data_dir_ / "snapshot"),
      config_path_(data_dir_ / "cluster.config") {
    fs::create_directories(log_dir_);
}

bool FileRaftStorage::ReadHardState(raft_core::Term* term,
                                    raft_core::NodeId* voted_for) {
    std::ifstream in(hard_state_path_);
    if (!in.is_open()) {
        return false;
    }

    raft_core::Term persisted_term = 0;
    raft_core::NodeId persisted_vote = raft_core::kNoNode;
    if (!(in >> persisted_term >> persisted_vote)) {
        throw std::runtime_error("failed to read raft hard state");
    }
    if (term) *term = persisted_term;
    if (voted_for) *voted_for = persisted_vote;
    return true;
}

void FileRaftStorage::WriteHardState(raft_core::Term term,
                                     raft_core::NodeId voted_for) {
    fs::create_directories(data_dir_);
    fs::path tmp = TempPath(hard_state_path_);
    {
        std::ofstream out(tmp, std::ios::trunc);
        out << term << ' ' << voted_for << '\n';
        EnsureGood(out, "write raft hard state");
    }
    fs::rename(tmp, hard_state_path_);
}

void FileRaftStorage::AppendEntries(
    const std::vector<raft_core::LogEntry>& entries) {
    if (entries.empty()) {
        return;
    }

    fs::create_directories(log_dir_);
    for (const auto& entry : entries) {
        fs::path final_path = LogPath(entry.index);
        fs::path tmp_path = TempPath(final_path);
        WriteEntryFile(tmp_path, entry);
        fs::rename(tmp_path, final_path);
    }
}

void FileRaftStorage::TruncateSuffix(raft_core::Index from_index) {
    if (!fs::exists(log_dir_)) {
        return;
    }

    for (const auto& dir_entry : fs::directory_iterator(log_dir_)) {
        raft_core::Index index = 0;
        if (ParseLogIndex(dir_entry.path(), &index) && index >= from_index) {
            fs::remove(dir_entry.path());
        }
    }
}

void FileRaftStorage::TruncatePrefix(raft_core::Index through_index) {
    if (!fs::exists(log_dir_)) {
        return;
    }

    for (const auto& dir_entry : fs::directory_iterator(log_dir_)) {
        raft_core::Index index = 0;
        if (ParseLogIndex(dir_entry.path(), &index) && index <= through_index) {
            fs::remove(dir_entry.path());
        }
    }
}

bool FileRaftStorage::EntryAt(raft_core::Index index,
                              raft_core::LogEntry* out) {
    if (index <= 0) {
        return false;
    }
    fs::path path = LogPath(index);
    if (!fs::exists(path)) {
        return false;
    }
    return ReadEntryFile(path, out);
}

void FileRaftStorage::LastIndexTerm(raft_core::Index* index,
                                    raft_core::Term* term) {
    raft_core::Index last_index = 0;
    if (fs::exists(log_dir_)) {
        for (const auto& dir_entry : fs::directory_iterator(log_dir_)) {
            raft_core::Index candidate = 0;
            if (ParseLogIndex(dir_entry.path(), &candidate)) {
                last_index = std::max(last_index, candidate);
            }
        }
    }

    raft_core::Term last_term = 0;
    if (last_index > 0) {
        raft_core::LogEntry entry;
        if (!EntryAt(last_index, &entry)) {
            throw std::runtime_error("failed to read raft log tail");
        }
        last_term = entry.term;
    }
    if (index) *index = last_index;
    if (term) *term = last_term;
}

void FileRaftStorage::SaveSnapshot(raft_core::Index last_included_index,
                                   raft_core::Term last_included_term,
                                   const std::string& data) {
    fs::create_directories(data_dir_);
    fs::path tmp = TempPath(snapshot_path_);
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        WriteBinary(out, last_included_index, "write raft snapshot index");
        WriteBinary(out, last_included_term, "write raft snapshot term");
        uint64_t size = static_cast<uint64_t>(data.size());
        WriteBinary(out, size, "write raft snapshot size");
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
        EnsureGood(out, "write raft snapshot data");
    }
    fs::rename(tmp, snapshot_path_);
}

bool FileRaftStorage::LoadSnapshot(raft_core::Index* last_included_index,
                                   raft_core::Term* last_included_term,
                                   std::string* data) {
    std::ifstream in(snapshot_path_, std::ios::binary);
    if (!in.is_open()) {
        return false;
    }

    raft_core::Index index = 0;
    raft_core::Term term = 0;
    uint64_t size = 0;
    if (!ReadBinary(in, &index) ||
        !ReadBinary(in, &term) ||
        !ReadBinary(in, &size)) {
        throw std::runtime_error("failed to read raft snapshot metadata");
    }

    std::string payload(size, '\0');
    in.read(payload.data(), static_cast<std::streamsize>(size));
    if (!in.good() && !in.eof()) {
        throw std::runtime_error("failed to read raft snapshot data");
    }
    if (static_cast<uint64_t>(in.gcount()) != size) {
        throw std::runtime_error("truncated raft snapshot data");
    }

    if (last_included_index) *last_included_index = index;
    if (last_included_term) *last_included_term = term;
    if (data) *data = std::move(payload);
    return true;
}

fs::path FileRaftStorage::LogPath(raft_core::Index index) const {
    return log_dir_ / LogFileName(index);
}

fs::path FileRaftStorage::TempPath(const fs::path& path) const {
    return path.string() + ".tmp";
}

std::string FileRaftStorage::LogFileName(raft_core::Index index) {
    std::ostringstream out;
    out << std::setw(20) << std::setfill('0') << index << ".entry";
    return out.str();
}

bool FileRaftStorage::ParseLogIndex(const fs::path& path,
                                    raft_core::Index* index) {
    std::string name = path.filename().string();
    constexpr char kSuffix[] = ".entry";
    if (name.size() <= sizeof(kSuffix) - 1 ||
        name.compare(name.size() - (sizeof(kSuffix) - 1),
                     sizeof(kSuffix) - 1,
                     kSuffix) != 0) {
        return false;
    }

    std::string number = name.substr(0, name.size() - (sizeof(kSuffix) - 1));
    try {
        raft_core::Index parsed = static_cast<raft_core::Index>(std::stoll(number));
        if (index) *index = parsed;
        return parsed > 0;
    } catch (const std::exception&) {
        return false;
    }
}

void FileRaftStorage::WriteEntryFile(const fs::path& path,
                                     const raft_core::LogEntry& entry) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    WriteBinary(out, entry.term, "write raft log term");
    WriteBinary(out, entry.index, "write raft log index");
    // Entry type is stored as a uint32 for forward compatibility.
    uint32_t type_val = static_cast<uint32_t>(entry.type);
    WriteBinary(out, type_val, "write raft log type");
    uint64_t size = static_cast<uint64_t>(entry.command.size());
    WriteBinary(out, size, "write raft log payload size");
    out.write(entry.command.data(), static_cast<std::streamsize>(entry.command.size()));
    EnsureGood(out, "write raft log payload");
}

bool FileRaftStorage::ReadEntryFile(const fs::path& path,
                                    raft_core::LogEntry* entry) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return false;
    }

    raft_core::Term term = 0;
    raft_core::Index index = 0;
    uint32_t type_val = 0;
    uint64_t size = 0;
    if (!ReadBinary(in, &term) ||
        !ReadBinary(in, &index) ||
        !ReadBinary(in, &type_val) ||
        !ReadBinary(in, &size)) {
        throw std::runtime_error("failed to read raft log entry metadata");
    }

    std::string command(size, '\0');
    in.read(command.data(), static_cast<std::streamsize>(size));
    if (!in.good() && !in.eof()) {
        throw std::runtime_error("failed to read raft log entry payload");
    }
    if (static_cast<uint64_t>(in.gcount()) != size) {
        throw std::runtime_error("truncated raft log entry payload");
    }

    if (entry) {
        entry->term    = term;
        entry->index   = index;
        entry->type    = static_cast<raft_core::EntryType>(type_val);
        entry->command = std::move(command);
    }
    return true;
}

void FileRaftStorage::SaveConfig(
    const std::vector<raft_core::PeerInfo>& peers) {
    fs::create_directories(data_dir_);
    fs::path tmp = TempPath(config_path_);
    {
        std::ofstream out(tmp, std::ios::trunc);
        EnsureGood(out, "open cluster config for writing");
        for (const auto& p : peers) {
            out << p.id << ' ' << p.ip << ' ' << p.port << '\n';
            EnsureGood(out, "write cluster config entry");
        }
    }
    fs::rename(tmp, config_path_);
}

bool FileRaftStorage::LoadConfig(std::vector<raft_core::PeerInfo>* peers) {
    std::ifstream in(config_path_);
    if (!in.is_open()) {
        return false;
    }
    std::vector<raft_core::PeerInfo> result;
    raft_core::NodeId id;
    std::string ip;
    int port;
    while (in >> id >> ip >> port) {
        raft_core::PeerInfo p;
        p.id   = id;
        p.ip   = std::move(ip);
        p.port = port;
        result.push_back(std::move(p));
    }
    if (!in.eof() && in.fail()) {
        throw std::runtime_error("failed to read cluster config");
    }
    if (peers) *peers = std::move(result);
    return true;
}

}  // namespace kvserver
