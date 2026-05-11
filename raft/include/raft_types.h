#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace raft_core {

// Basic numeric aliases. Kept signed to match raft.proto, which uses int64.
using Term   = int64_t;
using Index  = int64_t;
using NodeId = int64_t;

// Sentinel value for "no vote granted yet" / "unknown leader".
inline constexpr NodeId kNoNode = -1;

// Static description of one cluster member, parsed from configuration.
struct PeerInfo {
    NodeId      id   = kNoNode;
    std::string ip;
    int         port = 0;
};

// Type tag for log entries. Mirrors raft.proto EntryType.
enum class EntryType {
    Normal = 0,  // ordinary user command
    Config = 1,  // cluster membership change (ConfigChange encoded in command)
};

// Type of membership change encoded in a Config log entry.
enum class ConfigChangeType {
    AddPeer    = 0,
    RemovePeer = 1,
};

// Internal C++ representation of a raft log entry. The wire form is
// raft::LogEntry (proto). Conversion happens at the RPC boundary so the
// state machine never deals with protobuf types directly.
struct LogEntry {
    Term        term    = 0;
    Index       index   = 0;
    std::string command;          // opaque user payload (KV op, etc.)
    EntryType   type    = EntryType::Normal;
};

// Payload of a Config log entry, describing a single membership change.
// Serialised into LogEntry.command via raft::ConfigChange proto at the RPC boundary.
struct ConfigChange {
    ConfigChangeType type = ConfigChangeType::AddPeer;
    PeerInfo         peer;
};

// Tri-state of the raft node. Mutated only on the raft logic thread.
enum class RaftState {
    Follower  = 0,
    Candidate = 1,
    Leader    = 2,
};

inline const char* ToString(RaftState s) {
    switch (s) {
        case RaftState::Follower:  return "Follower";
        case RaftState::Candidate: return "Candidate";
        case RaftState::Leader:    return "Leader";
    }
    return "?";
}

}  // namespace raft_core
