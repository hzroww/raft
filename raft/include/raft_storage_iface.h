#pragma once

#include "raft_types.h"

#include <string>
#include <vector>

namespace raft_core {

// Abstract long-term storage for raft persistent state.
//
// The raft node depends ONLY on this interface; it does not know whether
// the backing store is a file, a skiplist-based KV server, or something
// else. The first concrete implementation will live in the future
// `kvserver` module.
//
// Threading
//   All methods are invoked exclusively from the raft logic thread, so
//   implementations do NOT need to be internally thread-safe with respect
//   to raft. They MAY perform synchronous I/O; the raft thread accepts
//   that latency in exchange for not having an asynchronous-persist
//   subsystem.
//
// Errors
//   Persistence failures are considered fatal at this stage and should
//   abort/throw inside the implementation. The interface returns void
//   for write paths to keep raft state-machine code uncluttered with
//   error handling that the spec assumes always succeeds.
class IRaftStorage {
public:
    virtual ~IRaftStorage() = default;

    // -------- Hard state (currentTerm, votedFor) --------
    // Returns true if a previously persisted hard state was loaded; false
    // for a fresh node. Out-params left untouched on a fresh node.
    virtual bool ReadHardState(Term* term, NodeId* voted_for) = 0;
    virtual void WriteHardState(Term term, NodeId voted_for) = 0;

    // -------- Log persistence --------
    // Append the given entries to the persistent log. Caller guarantees
    // the entries are contiguous and follow the current persistent tail.
    virtual void AppendEntries(const std::vector<LogEntry>& entries) = 0;

    // Drop every entry whose index is >= |from_index|. Used when a
    // follower's log conflicts with the leader's view.
    virtual void TruncateSuffix(Index from_index) = 0;

    // Look up a single entry; returns false if not present (e.g. compacted
    // away into a snapshot, or never appended).
    virtual bool EntryAt(Index index, LogEntry* out) = 0;

    // Tail of the persistent log. Returns (0, 0) if the log is empty.
    virtual void LastIndexTerm(Index* index, Term* term) = 0;

    // -------- Snapshots (placeholder for now) --------
    // Concrete implementations are expected later (kvserver). The raft
    // node only calls these on the snapshot code path, which is itself
    // stubbed in this phase.
    virtual void SaveSnapshot(Index              last_included_index,
                              Term               last_included_term,
                              const std::string& data) = 0;

    virtual bool LoadSnapshot(Index*       last_included_index,
                              Term*        last_included_term,
                              std::string* data) = 0;
};

// No-op storage. Useful for unit tests / smoke tests that exercise the
// communication layer without requiring a real persistence backend.
//
// All reads return "fresh node" / "no entry"; all writes are silently
// dropped. Safe to share across raft nodes only because it holds no
// state.
class NullRaftStorage : public IRaftStorage {
public:
    bool ReadHardState(Term* /*term*/, NodeId* /*voted_for*/) override { return false; }
    void WriteHardState(Term /*term*/, NodeId /*voted_for*/) override {}
    void AppendEntries(const std::vector<LogEntry>& /*entries*/) override {}
    void TruncateSuffix(Index /*from_index*/) override {}
    bool EntryAt(Index /*index*/, LogEntry* /*out*/) override { return false; }
    void LastIndexTerm(Index* index, Term* term) override {
        if (index) *index = 0;
        if (term)  *term  = 0;
    }
    void SaveSnapshot(Index, Term, const std::string&) override {}
    bool LoadSnapshot(Index*, Term*, std::string*) override { return false; }
};

}  // namespace raft_core
