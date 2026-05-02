#include "kv_raft_apply_sink.h"

#include "kv_command_codec.h"
#include "log.h"

#include <cstdlib>
#include <utility>

namespace kvserver {

KvApplySink::KvApplySink(raft_core::IRaftStorage* storage,
                         KvStore* store,
                         CommitWaitRegistry* wait_registry)
    : storage_(storage),
      store_(store),
      wait_registry_(wait_registry) {}

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
}

}  // namespace kvserver
