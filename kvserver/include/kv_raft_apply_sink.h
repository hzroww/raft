#pragma once

#include "commit_wait_registry.h"
#include "kv_store.h"
#include "raft_apply_iface.h"
#include "raft_storage_iface.h"

#include <string>

namespace kvserver {

class KvApplySink : public raft_core::IRaftApplySink {
public:
    KvApplySink(raft_core::IRaftStorage* storage,
                KvStore* store,
                CommitWaitRegistry* wait_registry);

    void OnCommitted(raft_core::Index start_index,
                     raft_core::Index end_index) override;
    void OnSnapshotInstalled(raft_core::Index last_included_index,
                             raft_core::Term last_included_term,
                             const std::string& data) override;

private:
    raft_core::IRaftStorage* storage_;
    KvStore* store_;
    CommitWaitRegistry* wait_registry_;
};

}  // namespace kvserver
