#pragma once

#include "peer.h"
#include "raft_apply_iface.h"
#include "raft_log_cache.h"
#include "raft_storage_iface.h"
#include "raft_timer.h"
#include "raft_types.h"

#include "coro/task.h"
#include "rpc_server.h"

#include "raft.pb.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace raft_core {

// Single-threaded raft node.
//
// Architecture
//   - The "main" thread (a.k.a. raft logic thread) is created by Start().
//     It owns all raft state mutations; it runs an event loop:
//       drainQueue() -> checkTimers() -> cv.wait_until(next_deadline)
//   - A separate thread runs the muduo-based RpcServer. Its worker pool
//     dispatches RaftRpc method calls; the server-side handlers below
//     simply Post() the work onto the main thread queue and return.
//   - Each Peer owns its own RpcChannel (per-peer IO thread); RPC
//     completion callbacks resume coroutines via Post() back onto the
//     main thread.
//   - Coroutines run fan-out style: one per peer per election round, one
//     per peer per heartbeat broadcast. They co_await an RpcAwaitable
//     and resume on the main thread.
//
// Storage and apply
//   The node depends on IRaftStorage* and IRaftApplySink* via constructor
//   injection. Both can point at no-op implementations during this
//   phase; the API is shaped so the future kvserver module can implement
//   them without any change to RaftNode.
//
// Thread safety
//   Public methods documented as "thread-safe" can be called from any
//   thread; everything else must be called from the main thread (or via
//   Post() to schedule onto it).
class RaftNode : public ::raft::RaftRpc {
public:
    struct Config {
        NodeId                self_id          = kNoNode;
        std::vector<PeerInfo> peers;            // does NOT include self
        std::string           listen_ip        = "0.0.0.0";
        int                   listen_port      = 0;

        int election_timeout_min_ms = 150;
        int election_timeout_max_ms = 300;
        int heartbeat_interval_ms   = 50;
        int rpc_timeout_ms          = 1000;
        int io_threads              = 1;
        size_t worker_threads       = 4;
        bool start_rpc_server       = true;

        // A Learner is promoted to a voting member once its match_index
        // comes within this many entries of the leader's last log index.
        // 0 means the learner must fully catch up before promotion.
        int learner_lag_tolerance   = 0;

        // Optional services registered on the same RpcServer as RaftRpc.
        // The owner must keep them alive for at least as long as this node.
        std::vector<google::protobuf::Service*> extra_services;
    };

    RaftNode(Config           cfg,
             IRaftStorage*    storage,
             IRaftApplySink*  apply_sink);
    ~RaftNode() override;

    RaftNode(const RaftNode&)            = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // Spawns the RPC server thread + the raft main thread, then returns.
    // Thread-safe; must be called exactly once.
    void Start();

    // Signals shutdown and joins both threads. Idempotent and thread-safe.
    void Stop();

    // Block the calling thread until Stop() finishes. Optional helper.
    void WaitForShutdown();

    // ---- Public introspection (thread-safe) ----
    NodeId    SelfId()       const { return cfg_.self_id; }
    RaftState State()        const { return state_atomic_.load(std::memory_order_acquire); }
    Term      CurrentTerm()  const { return current_term_atomic_.load(std::memory_order_acquire); }
    NodeId    LeaderId()     const { return leader_id_atomic_.load(std::memory_order_acquire); }
    Index     CommitIndex()  const { return commit_index_atomic_.load(std::memory_order_acquire); }
    Index     LastApplied()  const { return last_applied_atomic_.load(std::memory_order_acquire); }

    // Schedule |fn| to run on the raft main thread. Thread-safe.
    void Post(std::function<void()> fn);

    // Client-facing: append a command to the log if this node is leader.
    // The call is thread-safe; it internally posts onto the main thread
    // and blocks the caller until the proposal has been queued (NOT until
    // it is committed). Returns false if the node is not currently the
    // leader.
    bool Propose(std::string command, Index* assigned_index = nullptr);

    // Client-facing: persist a state-machine snapshot and compact the local
    // log prefix covered by |last_included_index|.
    bool TakeSnapshot(Index last_included_index, std::string data);

    // Non-blocking variant: posts the snapshot work onto the main thread and
    // returns immediately. Safe to call from the main thread itself (e.g.
    // from within OnCommitted). Caller cannot observe success/failure.
    void TakeSnapshotAsync(Index last_included_index, std::string data);

    // ---- RaftRpc service handlers (server-side) ----
    // Called by RpcServer worker threads. They Post() the actual work
    // onto the raft main thread and let the closure fire from there.
    void RequestVote(google::protobuf::RpcController*   controller,
                     const ::raft::RequestVoteArgs*     request,
                     ::raft::RequestVoteReply*          response,
                     google::protobuf::Closure*         done) override;

    void AppendEntries(google::protobuf::RpcController* controller,
                       const ::raft::AppendEntriesArgs* request,
                       ::raft::AppendEntriesReply*      response,
                       google::protobuf::Closure*       done) override;

    void InstallSnapshot(google::protobuf::RpcController*   controller,
                         const ::raft::InstallSnapshotArgs* request,
                         ::raft::InstallSnapshotReply*      response,
                         google::protobuf::Closure*         done) override;

    void AddPeer(google::protobuf::RpcController* controller,
                 const ::raft::AddPeerArgs*        request,
                 ::raft::AddPeerReply*             response,
                 google::protobuf::Closure*        done) override;

    void RemovePeer(google::protobuf::RpcController* controller,
                    const ::raft::RemovePeerArgs*     request,
                    ::raft::RemovePeerReply*          response,
                    google::protobuf::Closure*        done) override;

private:
    // ---- Main loop (raft logic thread) ----
    void MainLoop();
    void DrainQueue();
    void CheckTimers();
    DeadlineTimer::TimePoint NextDeadline() const;

    // ---- Server-side RPC handler bodies (run on main thread) ----
    void HandleRequestVote(const ::raft::RequestVoteArgs* req,
                           ::raft::RequestVoteReply*      resp);
    void HandleAppendEntries(const ::raft::AppendEntriesArgs* req,
                             ::raft::AppendEntriesReply*      resp);
    void HandleInstallSnapshot(const ::raft::InstallSnapshotArgs* req,
                               ::raft::InstallSnapshotReply*      resp);
    void HandleAddPeer(const ::raft::AddPeerArgs* req,
                       ::raft::AddPeerReply*      resp);
    void HandleRemovePeer(const ::raft::RemovePeerArgs* req,
                          ::raft::RemovePeerReply*      resp);

    // ---- State transitions ----
    void BecomeFollower(Term new_term, NodeId leader_id);
    void BecomeCandidate();
    void BecomeLeader();

    // Persist current_term_ / voted_for_ via storage. Cheap no-op when
    // storage is the null implementation.
    void PersistHardState();
    void LoadPersistentLog();

    // ---- Coroutines (fan-out) ----
    // One RequestVote attempt against |peer_index| for |election_term|.
    Task RequestVoteOnce(size_t peer_index, Term election_term);
    // One AppendEntries attempt against |peer_index| for |leader_term|.
    Task ReplicateOnce(size_t peer_index, Term leader_term);
    // One InstallSnapshot attempt against |peer_index| for |leader_term|.
    Task SendSnapshotOnce(size_t peer_index, Term leader_term);

    // ---- Replication helpers ----
    void BroadcastAppendEntries();
    void AppendNoopEntry();
    void MaybeAdvanceCommitIndex();
    void ApplyCommittedRange();

    // Core snapshot logic; always runs on main thread.
    void DoTakeSnapshot(Index last_included_index, std::string data);

    // ---- Membership change helpers ----
    // Encode a ConfigChange and append it as an ENTRY_CONFIG log entry.
    void ProposeConfigChange(ConfigChangeType type, const PeerInfo& peer);

    // Apply a committed ENTRY_CONFIG log entry to the in-memory peer list
    // and persist the new configuration.
    void ApplyConfigChange(const LogEntry& entry);

    // After updating match_index for peer[peer_index], check whether it has
    // caught up enough to be promoted from Learner to Voting member.
    void MaybePromoteLearner(size_t peer_index);

    // When a new leader is elected, scan uncommitted log entries for any
    // pending config change entry and reconstruct its state.
    void RecoverPendingConfigChange();

    // If a pending ADD_PEER config entry references a node not yet in
    // peers_, add it as a Learner so replication can continue.
    void ReconstructLearnerIfNeeded(const LogEntry& config_entry);

    // Returns the number of voting (non-Learner) peers.
    int VotingPeerCount() const;

    // Resume poster used by RpcAwaitable. Just forwards to Post().
    std::function<void(std::function<void()>)> ResumePoster();

    // ---- Static config + injected deps ----
    Config           cfg_;
    IRaftStorage*    storage_;
    IRaftApplySink*  apply_sink_;

    // ---- Coroutine queue (the only mutex-protected raft state) ----
    mutable std::mutex                mu_;
    std::condition_variable           cv_;
    std::deque<std::function<void()>> queue_;
    bool                              stop_flag_{false};

    // ---- Threads ----
    std::thread main_thread_;
    std::thread rpc_thread_;
    RpcServer   rpc_server_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopping_{false};

    // ---- Raft persistent-ish state (main thread only) ----
    Term   current_term_ = 0;
    NodeId voted_for_    = kNoNode;
    LogCache log_cache_;

    // ---- Raft volatile state (main thread only) ----
    RaftState state_           = RaftState::Follower;
    NodeId    leader_id_       = kNoNode;
    Index     commit_index_    = 0;
    Index     last_applied_    = 0;
    int       votes_granted_   = 0;
    Term      current_election_term_ = 0;     // term the in-flight election is for

    // ---- Atomic mirrors for thread-safe introspection ----
    std::atomic<RaftState> state_atomic_{RaftState::Follower};
    std::atomic<Term>      current_term_atomic_{0};
    std::atomic<NodeId>    leader_id_atomic_{kNoNode};
    std::atomic<Index>     commit_index_atomic_{0};
    std::atomic<Index>     last_applied_atomic_{0};

    // ---- Membership change state (main thread only) ----
    // True while a config change log entry is in the log but not yet
    // committed.  Prevents a second concurrent config change from being
    // appended while one is still in-flight.
    bool pending_config_change_ = false;

    // Set of NodeIds that are full voting members (excludes self, excludes
    // Learners).  Used by HandleRequestVote to reject votes from nodes that
    // are no longer part of the cluster (Config-Aware Voting).
    std::unordered_set<NodeId> current_members_;

    // ---- Peers ----
    std::vector<std::unique_ptr<Peer>> peers_;

    // ---- Timers ----
    ElectionTimer  election_timer_;
    HeartbeatTimer heartbeat_timer_;
};

}  // namespace raft_core
