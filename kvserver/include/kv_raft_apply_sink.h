#pragma once

#include "commit_wait_registry.h"
#include "kv_store.h"
#include "raft_apply_iface.h"
#include "raft_node.h"
#include "raft_storage_iface.h"

#include <cstdint>
#include <string>

namespace kvserver {

class KvApplySink : public raft_core::IRaftApplySink {
public:
    // |snapshot_threshold|: trigger a snapshot every this many applied entries.
    // Pass 0 to disable automatic snapshots.
    KvApplySink(raft_core::IRaftStorage* storage,
                KvStore* store,
                CommitWaitRegistry* wait_registry,
                raft_core::RaftNode* raft_node = nullptr,
                uint64_t snapshot_threshold = 0);

    // Must be called after the RaftNode is fully constructed (because
    // RaftNode's constructor calls OnSnapshotInstalled via LoadPersistentLog).
    void SetRaftNode(raft_core::RaftNode* raft_node);

    void OnCommitted(raft_core::Index start_index,
                     raft_core::Index end_index) override;
    void OnSnapshotInstalled(raft_core::Index last_included_index,
                             raft_core::Term last_included_term,
                             const std::string& data) override;

private:
    raft_core::IRaftStorage* storage_;
    KvStore* store_;
    CommitWaitRegistry* wait_registry_;
    raft_core::RaftNode* raft_node_;
    uint64_t snapshot_threshold_;
    uint64_t applied_since_snapshot_ = 0;
};

}  // namespace kvserver
