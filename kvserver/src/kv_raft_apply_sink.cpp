#include "kv_raft_apply_sink.h"

#include "kv_command_codec.h"
#include "log.h"

#include <cstdlib>
#include <utility>

namespace kvserver {

KvApplySink::KvApplySink(raft_core::IRaftStorage* storage,
                         KvStore* store,
                         CommitWaitRegistry* wait_registry,
                         raft_core::RaftNode* raft_node,
                         uint64_t snapshot_threshold)
    : storage_(storage),
      store_(store),
      wait_registry_(wait_registry),
      raft_node_(raft_node),
      snapshot_threshold_(snapshot_threshold) {}

void KvApplySink::SetRaftNode(raft_core::RaftNode* raft_node) {
    raft_node_ = raft_node;
}

void KvApplySink::OnCommitted(raft_core::Index start_index,
                              raft_core::Index end_index) {
    for (raft_core::Index index = start_index; index <= end_index; ++index) {
        raft_core::LogEntry entry;
        if (!storage_ || !storage_->EntryAt(index, &entry)) {
            LOG_ERROR() << "missing committed raft log entry index=" << index;
            std::abort();
        }
        if (entry.command.empty()) {
            continue;
        }

        kv::KvCommand command;
        if (!DecodeCommand(entry.command, &command)) {
            LOG_ERROR() << "failed to decode committed kv command index=" << index;
            std::abort();
        }
        if (!store_) {
            LOG_ERROR() << "kv store is not configured for apply index=" << index;
            std::abort();
        }

        KvResult result = store_->Apply(command);
        if (wait_registry_) {
            wait_registry_->Notify(index, std::move(result));
        }
    }

    applied_since_snapshot_ += static_cast<uint64_t>(end_index - start_index + 1);

    if (snapshot_threshold_ > 0 &&
        applied_since_snapshot_ >= snapshot_threshold_ &&
        raft_node_ && store_) {
        std::string blob = store_->SaveSnapshot();
        raft_node_->TakeSnapshotAsync(end_index, std::move(blob));
        applied_since_snapshot_ = 0;
        LOG_DEBUG() << "triggered async snapshot at index=" << end_index;
    }
}

void KvApplySink::OnSnapshotInstalled(raft_core::Index last_included_index,
                                      raft_core::Term last_included_term,
                                      const std::string& data) {
    if (!store_) {
        LOG_ERROR() << "kv store is not configured for snapshot index="
                    << last_included_index;
        std::abort();
    }
    if (!store_->LoadSnapshot(data)) {
        LOG_ERROR() << "failed to install kv snapshot index="
                    << last_included_index
                    << " term=" << last_included_term;
        std::abort();
    }

    // Reset the counter so we don't trigger a redundant snapshot immediately
    // after restoring one.
    applied_since_snapshot_ = 0;
}

}  // namespace kvserver
