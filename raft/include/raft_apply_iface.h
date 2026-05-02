#pragma once

#include "raft_types.h"

#include <string>

namespace raft_core {

// Notification sink for committed-but-not-yet-applied entries.
//
// The raft node calls OnCommitted whenever |commitIndex| advances past
// previously-applied entries. The receiver (typically a state machine /
// kvserver) is responsible for fetching the actual entry payloads from
// IRaftStorage and applying them.
//
// Threading
//   Invoked exclusively on the raft logic thread, in commit-index order.
//   Implementations MUST return quickly and MUST NOT call back into
//   RaftNode synchronously to avoid re-entrancy.
class IRaftApplySink {
public:
    virtual ~IRaftApplySink() = default;

    // Inclusive range [start_index, end_index]. Both bounds are >= 1.
    // If start_index > end_index, the range is empty (will not be called).
    virtual void OnCommitted(Index start_index, Index end_index) = 0;

    // Called after a durable snapshot has been installed or loaded. The
    // receiver should replace its state machine with |data|.
    virtual void OnSnapshotInstalled(Index              last_included_index,
                                     Term               last_included_term,
                                     const std::string& data) = 0;
};

// No-op apply sink for tests that don't care about commit notifications.
class NullApplySink : public IRaftApplySink {
public:
    void OnCommitted(Index /*start*/, Index /*end*/) override {}
    void OnSnapshotInstalled(Index, Term, const std::string&) override {}
};

}  // namespace raft_core
