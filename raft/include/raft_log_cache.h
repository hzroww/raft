#pragma once

#include "raft_types.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace raft_core {

// In-memory raft log cache.
//
// Role
//   The long-term authority for log entries lives in IRaftStorage
//   (later: kvserver). LogCache is the raft node's local view that
//   answers the hot-path queries needed by election and replication
//   without round-tripping to storage:
//     - lastIndex() / lastTerm()        -> RequestVote, AppendEntries args
//     - termAt(index)                   -> log up-to-date checks
//     - slice(from, to)                 -> AppendEntries.entries
//     - append() / truncateSuffix()     -> mutations driven by the
//                                          raft state machine
//
// Threading
//   Touched only on the raft logic thread.
//
// Indexing convention
//   Raft indices start at 1. We keep entries_[0] meaning "index 1"; an
//   empty cache reports lastIndex() == 0. base_index_ is reserved for a
//   future snapshot-aware variant where we drop the prefix; for this
//   phase we always keep base_index_ == 1.
class LogCache {
public:
    LogCache() = default;

    bool   Empty()     const { return entries_.empty(); }
    size_t Size()      const { return entries_.size(); }

    Index LastIndex() const {
        return entries_.empty() ? 0
                                : entries_.back().index;
    }

    Term LastTerm() const {
        return entries_.empty() ? 0
                                : entries_.back().term;
    }

    // Returns 0 if the index is out of range.
    Term TermAt(Index index) const {
        if (index <= 0) return 0;
        if (index < base_index_) return 0;     // compacted (unused for now)
        size_t off = static_cast<size_t>(index - base_index_);
        if (off >= entries_.size()) return 0;
        return entries_[off].term;
    }

    bool EntryAt(Index index, LogEntry* out) const {
        if (index <= 0) return false;
        if (index < base_index_) return false;
        size_t off = static_cast<size_t>(index - base_index_);
        if (off >= entries_.size()) return false;
        if (out) *out = entries_[off];
        return true;
    }

    // Returns entries whose index is in the half-open range [from, to).
    // Out-of-range bounds are clamped silently. May return fewer entries
    // than requested if the cache does not span the range.
    std::vector<LogEntry> Slice(Index from, Index to) const {
        std::vector<LogEntry> out;
        if (entries_.empty() || from >= to) return out;
        Index lo = from < base_index_ ? base_index_ : from;
        Index hi = to > LastIndex() + 1 ? LastIndex() + 1 : to;
        if (lo >= hi) return out;
        size_t lo_off = static_cast<size_t>(lo - base_index_);
        size_t hi_off = static_cast<size_t>(hi - base_index_);
        out.reserve(hi_off - lo_off);
        for (size_t i = lo_off; i < hi_off; ++i) out.push_back(entries_[i]);
        return out;
    }

    // Append a new entry; index is auto-assigned as LastIndex() + 1.
    Index Append(Term term, std::string command) {
        LogEntry e;
        e.term    = term;
        e.index   = LastIndex() + 1;
        e.command = std::move(command);
        entries_.push_back(std::move(e));
        return entries_.back().index;
    }

    // Append a fully-formed entry (used when receiving from a leader).
    // Caller guarantees |entry.index == LastIndex() + 1|.
    void AppendEntry(LogEntry entry) {
        entries_.push_back(std::move(entry));
    }

    // Drop every entry whose index is >= |from_index|. Safe to call with
    // an index past the tail (no-op).
    void TruncateSuffix(Index from_index) {
        if (entries_.empty()) return;
        if (from_index <= base_index_) {
            entries_.clear();
            return;
        }
        size_t off = static_cast<size_t>(from_index - base_index_);
        if (off >= entries_.size()) return;
        entries_.resize(off);
    }

    void Clear() { entries_.clear(); }

private:
    // Indices are inclusive: entries_[i] has index base_index_ + i.
    Index                 base_index_ = 1;
    std::vector<LogEntry> entries_;
};

}  // namespace raft_core
