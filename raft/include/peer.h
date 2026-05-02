#pragma once

#include "raft_types.h"
#include "rpc_channel.h"

#include "raft.pb.h"

#include <memory>

namespace raft_core {

// One remote raft member, plus the local replication state for it.
//
// Lifetime
//   Owned by RaftNode for the lifetime of the cluster. The underlying
//   RpcChannel spins up its own muduo IO thread; that's accepted for now
//   per the plan ("RPC IO threads remain per-peer; sharing is a future
//   refactor").
//
// Threading
//   |next_index|, |match_index|, |votes_*| are touched only on the raft
//   logic thread. The RpcChannel itself is internally thread-safe.
class Peer {
public:
    Peer(PeerInfo info, int rpc_timeout_ms)
        : info_(std::move(info)),
          channel_(std::make_unique<RpcChannel>(info_.ip, info_.port)),
          stub_(channel_.get()) {
        channel_->setTimeoutMs(rpc_timeout_ms);
    }

    Peer(const Peer&)            = delete;
    Peer& operator=(const Peer&) = delete;

    NodeId             Id()      const { return info_.id; }
    const std::string& Ip()      const { return info_.ip; }
    int                Port()    const { return info_.port; }

    RpcChannel*           Channel() { return channel_.get(); }
    ::raft::RaftRpc_Stub* Stub()    { return &stub_; }

    // Replication state (leader-only meaningful, persisted in memory only).
    Index NextIndex()   const { return next_index_; }
    Index MatchIndex()  const { return match_index_; }
    void  SetNextIndex(Index v)  { next_index_  = v; }
    void  SetMatchIndex(Index v) { match_index_ = v; }

    // Per-election bookkeeping.
    bool VoteReplied()    const { return vote_replied_; }
    bool VoteGranted()    const { return vote_granted_; }
    void RecordVote(bool granted) {
        vote_replied_ = true;
        vote_granted_ = granted;
    }
    void ResetVoteState() {
        vote_replied_ = false;
        vote_granted_ = false;
    }

    // Convenience: re-init replication state when becoming leader.
    void ResetForLeader(Index leader_last_index) {
        next_index_          = leader_last_index + 1;
        match_index_         = 0;
        snapshot_in_flight_  = false;
    }

    // True while a SendSnapshotOnce coroutine is outstanding for this peer.
    // Used to prevent concurrent duplicate InstallSnapshot RPCs.
    bool SnapshotInFlight() const { return snapshot_in_flight_; }
    void SetSnapshotInFlight(bool v) { snapshot_in_flight_ = v; }

private:
    PeerInfo                       info_;
    std::unique_ptr<RpcChannel>    channel_;
    ::raft::RaftRpc_Stub           stub_;

    Index next_index_          = 1;
    Index match_index_         = 0;
    bool  vote_replied_        = false;
    bool  vote_granted_        = false;
    bool  snapshot_in_flight_  = false;
};

}  // namespace raft_core
