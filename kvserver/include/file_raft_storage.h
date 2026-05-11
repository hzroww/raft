#pragma once

#include "raft_storage_iface.h"

#include <filesystem>
#include <string>

namespace kvserver {

class FileRaftStorage : public raft_core::IRaftStorage {
public:
    explicit FileRaftStorage(std::string data_dir);

    bool ReadHardState(raft_core::Term* term,
                       raft_core::NodeId* voted_for) override;
    void WriteHardState(raft_core::Term term,
                        raft_core::NodeId voted_for) override;

    void AppendEntries(const std::vector<raft_core::LogEntry>& entries) override;
    void TruncateSuffix(raft_core::Index from_index) override;
    void TruncatePrefix(raft_core::Index through_index) override;
    bool EntryAt(raft_core::Index index, raft_core::LogEntry* out) override;
    void LastIndexTerm(raft_core::Index* index, raft_core::Term* term) override;

    void SaveSnapshot(raft_core::Index last_included_index,
                      raft_core::Term last_included_term,
                      const std::string& data) override;
    bool LoadSnapshot(raft_core::Index* last_included_index,
                      raft_core::Term* last_included_term,
                      std::string* data) override;

    void SaveConfig(const std::vector<raft_core::PeerInfo>& peers) override;
    bool LoadConfig(std::vector<raft_core::PeerInfo>* peers) override;

private:
    std::filesystem::path LogPath(raft_core::Index index) const;
    std::filesystem::path TempPath(const std::filesystem::path& path) const;
    static std::string LogFileName(raft_core::Index index);
    static bool ParseLogIndex(const std::filesystem::path& path,
                              raft_core::Index* index);
    static void WriteEntryFile(const std::filesystem::path& path,
                               const raft_core::LogEntry& entry);
    static bool ReadEntryFile(const std::filesystem::path& path,
                              raft_core::LogEntry* entry);

    std::filesystem::path data_dir_;
    std::filesystem::path log_dir_;
    std::filesystem::path hard_state_path_;
    std::filesystem::path snapshot_path_;
    std::filesystem::path config_path_;
};

}  // namespace kvserver
