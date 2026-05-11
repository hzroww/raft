#include "raft_node.h"

#include "coro/rpc_awaitable.h"
#include "log.h"
#include "rpc_controller.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <stdexcept>
#include <utility>

namespace raft_core {

namespace {

// Fixed name used by raft logging for the main thread.
constexpr const char* kMainThreadName = "raft-main";
constexpr const char* kRpcThreadName  = "raft-rpcsrv";

// Wait sentinel used when no timer is armed (should not normally happen
// outside of stop, but provides a safe upper bound for cv.wait_until).
constexpr std::chrono::milliseconds kIdleWait{500};

Index FirstIndexOfTerm(const LogCache& log, Term term) {
    for (Index idx = log.SnapshotLastIndex() + 1; idx <= log.LastIndex(); ++idx) {
        if (log.TermAt(idx) == term) {
            return idx;
        }
    }
    return 0;
}

Index LastIndexOfTerm(const LogCache& log, Term term) {
    for (Index idx = log.LastIndex(); idx > log.SnapshotLastIndex(); --idx) {
        if (log.TermAt(idx) == term) {
            return idx;
        }
    }
    return 0;
}

}  // namespace

// ===========================================================================
// Construction / lifecycle
// ===========================================================================

RaftNode::RaftNode(Config           cfg,
                   IRaftStorage*    storage,
                   IRaftApplySink*  apply_sink)
    : cfg_(std::move(cfg)),
      storage_(storage),
      apply_sink_(apply_sink),
      election_timer_(cfg_.election_timeout_min_ms, cfg_.election_timeout_max_ms),
      heartbeat_timer_(cfg_.heartbeat_interval_ms) {

    // Build initial peer list from boot config.
    peers_.reserve(cfg_.peers.size());
    for (const auto& info : cfg_.peers) {
        peers_.push_back(std::make_unique<Peer>(info, cfg_.rpc_timeout_ms));
    }

    // Pull persistent state if storage has any. NullRaftStorage returns false.
    if (storage_) {
        Term   t = 0;
        NodeId v = kNoNode;
        if (storage_->ReadHardState(&t, &v)) {
            current_term_ = t;
            voted_for_    = v;
            current_term_atomic_.store(t, std::memory_order_release);
        }

        // If a saved config exists (written after a previous membership
        // change), use it instead of the boot config so the node knows
        // about peers that were added/removed after initial deployment.
        std::vector<PeerInfo> saved;
        if (storage_->LoadConfig(&saved)) {
            peers_.clear();
            for (const auto& info : saved) {
                peers_.push_back(std::make_unique<Peer>(info, cfg_.rpc_timeout_ms));
            }
        }

        LoadPersistentLog();
    }

    // Initialise current_members_ from whatever peer list we ended up with.
    // This set is used for Config-Aware Voting in HandleRequestVote.
    current_members_.insert(cfg_.self_id);
    for (const auto& p : peers_) {
        current_members_.insert(p->Id());
    }
}

RaftNode::~RaftNode() {
    Stop();
    if (rpc_thread_.joinable()) {
        // RpcServer::Run() does not expose a clean shutdown path; detach so
        // process termination tears it down. Acceptable at this stage.
        rpc_thread_.detach();
    }
}

void RaftNode::Start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true)) {
        return;
    }

    if (cfg_.start_rpc_server) {
        rpc_server_.RegisterService(this);
        for (auto* service : cfg_.extra_services) {
            if (service) {
                rpc_server_.RegisterService(service);
            }
        }
        rpc_server_.setIoThreadNum(cfg_.io_threads);
        rpc_server_.setWorkerThreads(cfg_.worker_threads);

        rpc_thread_ = std::thread([this]() {
            raft::logging::SetCurrentThreadName(kRpcThreadName);
            rpc_server_.Run(cfg_.listen_ip, cfg_.listen_port);
        });
    }

    main_thread_ = std::thread([this]() {
        raft::logging::SetCurrentThreadName(kMainThreadName);
        MainLoop();
    });

    LOG_INFO() << "raft node started self_id=" << cfg_.self_id
               << " listen=" << cfg_.listen_ip << ":" << cfg_.listen_port
               << " peers=" << peers_.size();
}

void RaftNode::Stop() {
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        stop_flag_ = true;
    }
    cv_.notify_all();
    if (main_thread_.joinable()) {
        main_thread_.join();
    }
    LOG_INFO() << "raft node stopped self_id=" << cfg_.self_id;
}

void RaftNode::WaitForShutdown() {
    if (main_thread_.joinable()) {
        main_thread_.join();
    }
}

void RaftNode::Post(std::function<void()> fn) {
    {
        std::lock_guard<std::mutex> lk(mu_);
        queue_.push_back(std::move(fn));
    }
    cv_.notify_one();
}

std::function<void(std::function<void()>)> RaftNode::ResumePoster() {
    return [this](std::function<void()> fn) { Post(std::move(fn)); };
}

// ===========================================================================
// Main loop
// ===========================================================================

void RaftNode::MainLoop() {
    // Initial timer setup: we always start as a follower with the election
    // timer armed.
    election_timer_.Reset();
    heartbeat_timer_.Disable();

    while (true) {
        DrainQueue();
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (stop_flag_) break;
        }

        CheckTimers();

        // Wait until something happens: a new task arrived, the next
        // timer is due, or stop was requested.
        auto deadline = NextDeadline();
        std::unique_lock<std::mutex> lk(mu_);
        if (queue_.empty() && !stop_flag_) {
            cv_.wait_until(lk, deadline);
        }
        if (stop_flag_) break;
    }
}

void RaftNode::DrainQueue() {
    std::deque<std::function<void()>> ready;
    {
        std::lock_guard<std::mutex> lk(mu_);
        ready.swap(queue_);
    }
    for (auto& fn : ready) {
        fn();
    }
}

void RaftNode::CheckTimers() {
    auto now = DeadlineTimer::Clock::now();
    if (state_ != RaftState::Leader && election_timer_.Expired(now)) {
        BecomeCandidate();
    }
    if (state_ == RaftState::Leader && heartbeat_timer_.Expired(now)) {
        heartbeat_timer_.Reset(now);
        BroadcastAppendEntries();
    }
}

DeadlineTimer::TimePoint RaftNode::NextDeadline() const {
    auto now = DeadlineTimer::Clock::now();
    DeadlineTimer::TimePoint d = now + kIdleWait;
    if (election_timer_.Armed()) {
        d = std::min(d, election_timer_.Deadline());
    }
    if (heartbeat_timer_.Armed()) {
        d = std::min(d, heartbeat_timer_.Deadline());
    }
    return d;
}

// ===========================================================================
// State transitions (main thread only)
// ===========================================================================

void RaftNode::BecomeFollower(Term new_term, NodeId leader_id) {
    bool term_changed = (new_term > current_term_);
    if (term_changed) {
        current_term_ = new_term;
        voted_for_    = kNoNode;
        current_term_atomic_.store(current_term_, std::memory_order_release);
    }
    state_     = RaftState::Follower;
    leader_id_ = leader_id;
    state_atomic_.store(state_, std::memory_order_release);
    leader_id_atomic_.store(leader_id_, std::memory_order_release);

    election_timer_.Reset();
    heartbeat_timer_.Disable();

    for (auto& p : peers_) {
        p->SetSnapshotInFlight(false);
    }

    if (term_changed) {
        PersistHardState();
    }

    LOG_INFO() << "become Follower term=" << current_term_
               << " leader=" << leader_id_;
}

void RaftNode::BecomeCandidate() {
    current_term_++;
    voted_for_     = cfg_.self_id;
    state_         = RaftState::Candidate;
    leader_id_     = kNoNode;
    votes_granted_ = 1;     // vote for self
    current_election_term_ = current_term_;

    current_term_atomic_.store(current_term_, std::memory_order_release);
    state_atomic_.store(state_, std::memory_order_release);
    leader_id_atomic_.store(leader_id_, std::memory_order_release);

    election_timer_.Reset();
    heartbeat_timer_.Disable();

    for (auto& p : peers_) p->ResetVoteState();

    PersistHardState();

    LOG_INFO() << "become Candidate term=" << current_term_;

    // Single-node cluster (no voting peers): trivially become leader.
    int total  = VotingPeerCount() + 1;
    int quorum = total / 2 + 1;
    if (votes_granted_ >= quorum) {
        BecomeLeader();
        return;
    }

    // Fan-out RequestVote: one fire-and-forget coroutine per voting peer.
    // Learners do not vote and do not receive RequestVote.
    Term election_term = current_term_;
    for (size_t i = 0; i < peers_.size(); ++i) {
        if (!peers_[i]->IsLearner()) {
            RequestVoteOnce(i, election_term);
        }
    }
}

Task RaftNode::RequestVoteOnce(size_t peer_index, Term election_term) {
    auto& peer = *peers_[peer_index];

    ::raft::RequestVoteArgs req;
    req.set_term(election_term);
    req.set_candidateid(cfg_.self_id);
    req.set_lastlogindex(log_cache_.LastIndex());
    req.set_lastlogterm(log_cache_.LastTerm());

    ::raft::RequestVoteReply resp;
    RpcController            ctl;

    co_await MakeRpcAwaitable(
        [&](google::protobuf::Closure* done) {
            peer.Stub()->RequestVote(&ctl, &req, &resp, done);
        },
        ResumePoster());

    // Resumed on raft main thread.
    if (current_term_ != election_term || state_ != RaftState::Candidate) {
        co_return;
    }

    if (ctl.Failed()) {
        LOG_DEBUG() << "RequestVote failed peer=" << peer.Id()
                    << " err=" << ctl.ErrorText();
        peer.RecordVote(false);
        co_return;
    }

    if (resp.term() > current_term_) {
        BecomeFollower(resp.term(), kNoNode);
        co_return;
    }

    peer.RecordVote(resp.votegranted());
    if (resp.votegranted()) {
        votes_granted_++;
        int total  = VotingPeerCount() + 1;
        int quorum = total / 2 + 1;
        LOG_DEBUG() << "vote granted by peer=" << peer.Id()
                    << " votes=" << votes_granted_
                    << "/" << quorum;
        if (votes_granted_ >= quorum && state_ == RaftState::Candidate) {
            BecomeLeader();
        }
    }
}

void RaftNode::BecomeLeader() {
    state_     = RaftState::Leader;
    leader_id_ = cfg_.self_id;

    state_atomic_.store(state_, std::memory_order_release);
    leader_id_atomic_.store(leader_id_, std::memory_order_release);

    election_timer_.Disable();
    heartbeat_timer_.Reset();   // due immediately on next loop iteration? No, after interval.

    Index last = log_cache_.LastIndex();
    for (auto& p : peers_) {
        p->ResetForLeader(last);
    }

    LOG_INFO() << "become Leader term=" << current_term_
               << " last_index=" << last;

    // Scan uncommitted log for any in-flight config change from a previous
    // leader term, and restore pending_config_change_ + learner state.
    RecoverPendingConfigChange();

    // Commit an entry in this term so older entries can be safely advanced.
    AppendNoopEntry();

    // Send an immediate heartbeat/replication round so peers learn the new leader fast.
    BroadcastAppendEntries();
}

void RaftNode::PersistHardState() {
    if (!storage_) return;
    storage_->WriteHardState(current_term_, voted_for_);
}

void RaftNode::LoadPersistentLog() {
    Index snapshot_index = 0;
    Term  snapshot_term = 0;
    std::string snapshot_data;
    if (storage_->LoadSnapshot(&snapshot_index, &snapshot_term, &snapshot_data)) {
        log_cache_.DiscardPrefix(snapshot_index, snapshot_term);
        commit_index_ = snapshot_index;
        last_applied_ = snapshot_index;
        if (apply_sink_) {
            apply_sink_->OnSnapshotInstalled(snapshot_index,
                                             snapshot_term,
                                             snapshot_data);
        }
    }

    Index last_index = 0;
    Term  last_term  = 0;
    storage_->LastIndexTerm(&last_index, &last_term);
    for (Index idx = snapshot_index + 1; idx <= last_index; ++idx) {
        LogEntry entry;
        if (!storage_->EntryAt(idx, &entry)) {
            throw std::runtime_error("persistent raft log is missing an entry");
        }
        if (entry.index != idx) {
            throw std::runtime_error("persistent raft log has a non-contiguous entry");
        }
        log_cache_.AppendEntry(std::move(entry));
    }
    if (last_index > snapshot_index && log_cache_.LastTerm() != last_term) {
        throw std::runtime_error("persistent raft log tail term mismatch");
    }
}

// ===========================================================================
// AppendEntries: leader broadcast + per-peer coroutine
// ===========================================================================

void RaftNode::BroadcastAppendEntries() {
    if (state_ != RaftState::Leader) return;
    Term leader_term = current_term_;
    for (size_t i = 0; i < peers_.size(); ++i) {
        ReplicateOnce(i, leader_term);
    }
}

Task RaftNode::ReplicateOnce(size_t peer_index, Term leader_term) {
    auto& peer = *peers_[peer_index];

    Index next_idx  = peer.NextIndex();
    if (next_idx < 1) next_idx = 1;
    if (next_idx <= log_cache_.SnapshotLastIndex()) {
        if (!peer.SnapshotInFlight()) {
            peer.SetSnapshotInFlight(true);
            SendSnapshotOnce(peer_index, leader_term);
        }
        co_return;
    }
    Index prev_idx  = next_idx - 1;
    Term  prev_term = log_cache_.TermAt(prev_idx);
    auto  entries   = log_cache_.Slice(next_idx, log_cache_.LastIndex() + 1);

    ::raft::AppendEntriesArgs req;
    req.set_term(leader_term);
    req.set_leaderid(cfg_.self_id);
    req.set_prevlogindex(prev_idx);
    req.set_prevlogterm(prev_term);
    req.set_leadercommit(commit_index_);
    for (const auto& e : entries) {
        auto* pe = req.add_entries();
        pe->set_term(e.term);
        pe->set_index(e.index);
        pe->set_command(e.command);
        pe->set_type(static_cast<::raft::EntryType>(static_cast<int>(e.type)));
    }

    ::raft::AppendEntriesReply resp;
    RpcController              ctl;

    co_await MakeRpcAwaitable(
        [&](google::protobuf::Closure* done) {
            peer.Stub()->AppendEntries(&ctl, &req, &resp, done);
        },
        ResumePoster());

    if (state_ != RaftState::Leader || current_term_ != leader_term) {
        co_return;
    }
    if (ctl.Failed()) {
        LOG_DEBUG() << "AppendEntries failed peer=" << peer.Id()
                    << " err=" << ctl.ErrorText();
        co_return;
    }
    if (resp.term() > current_term_) {
        BecomeFollower(resp.term(), kNoNode);
        co_return;
    }

    if (resp.success()) {
        Index sent_last = prev_idx + static_cast<Index>(entries.size());
        if (sent_last > peer.MatchIndex()) {
            peer.SetMatchIndex(sent_last);
            peer.SetNextIndex(sent_last + 1);
            MaybeAdvanceCommitIndex();
            MaybePromoteLearner(peer_index);
        }
    } else {
        Index next = resp.conflictindex();
        if (resp.conflictterm() != 0) {
            Index last_with_term = LastIndexOfTerm(log_cache_, resp.conflictterm());
            if (last_with_term > 0) {
                next = last_with_term + 1;
            }
        }
        if (next < 1) next = 1;
        if (next < peer.NextIndex()) {
            peer.SetNextIndex(next);
        }
    }
}

Task RaftNode::SendSnapshotOnce(size_t peer_index, Term leader_term) {
    auto& peer = *peers_[peer_index];

    if (!storage_) {
        peer.SetNextIndex(log_cache_.SnapshotLastIndex() + 1);
        peer.SetSnapshotInFlight(false);
        co_return;
    }

    Index snapshot_index = 0;
    Term snapshot_term = 0;
    std::string snapshot_data;
    if (!storage_->LoadSnapshot(&snapshot_index, &snapshot_term, &snapshot_data)) {
        peer.SetNextIndex(log_cache_.SnapshotLastIndex() + 1);
        peer.SetSnapshotInFlight(false);
        co_return;
    }

    ::raft::InstallSnapshotArgs req;
    req.set_term(leader_term);
    req.set_leaderid(cfg_.self_id);
    req.set_lastincludedindex(snapshot_index);
    req.set_lastincludedterm(snapshot_term);
    req.set_data(snapshot_data);
    req.set_done(true);

    ::raft::InstallSnapshotReply resp;
    RpcController                ctl;

    co_await MakeRpcAwaitable(
        [&](google::protobuf::Closure* done) {
            peer.Stub()->InstallSnapshot(&ctl, &req, &resp, done);
        },
        ResumePoster());

    // All exit paths below must clear the in-flight flag.
    if (state_ != RaftState::Leader || current_term_ != leader_term) {
        peer.SetSnapshotInFlight(false);
        co_return;
    }
    if (ctl.Failed()) {
        LOG_DEBUG() << "InstallSnapshot failed peer=" << peer.Id()
                    << " err=" << ctl.ErrorText();
        peer.SetSnapshotInFlight(false);
        co_return;
    }
    if (resp.term() > current_term_) {
        peer.SetSnapshotInFlight(false);
        BecomeFollower(resp.term(), kNoNode);
        co_return;
    }

    peer.SetSnapshotInFlight(false);
    if (snapshot_index > peer.MatchIndex()) {
        peer.SetMatchIndex(snapshot_index);
    }
    peer.SetNextIndex(snapshot_index + 1);
    MaybeAdvanceCommitIndex();
}

void RaftNode::AppendNoopEntry() {
    if (state_ != RaftState::Leader) return;
    Index idx = log_cache_.Append(current_term_, "");
    if (storage_) {
        LogEntry e;
        log_cache_.EntryAt(idx, &e);
        storage_->AppendEntries({e});
    }
    MaybeAdvanceCommitIndex();
}

void RaftNode::MaybeAdvanceCommitIndex() {
    if (state_ != RaftState::Leader) return;

    // Only voting (non-Learner) peers count toward the commit quorum.
    std::vector<Index> match;
    match.reserve(peers_.size() + 1);
    match.push_back(log_cache_.LastIndex());          // self
    for (auto& p : peers_) {
        if (!p->IsLearner()) {
            match.push_back(p->MatchIndex());
        }
    }
    std::sort(match.begin(), match.end());

    // For N nodes, the highest index replicated to a majority is at
    // ascending position (N-1)/2.
    size_t pos = (match.size() - 1) / 2;
    Index  candidate = match[pos];

    if (candidate > commit_index_ &&
        log_cache_.TermAt(candidate) == current_term_) {
        commit_index_ = candidate;
        LOG_DEBUG() << "advance commit_index=" << commit_index_;
        ApplyCommittedRange();
    }
}

void RaftNode::ApplyCommittedRange() {
    if (last_applied_ >= commit_index_) return;
    Index start = last_applied_ + 1;
    Index end   = commit_index_;

    // Process each newly committed entry: Config entries are handled here
    // on the raft thread before the range is forwarded to the apply sink.
    for (Index i = start; i <= end; ++i) {
        LogEntry e;
        if (log_cache_.EntryAt(i, &e) && e.type == EntryType::Config) {
            ApplyConfigChange(e);
        }
    }

    last_applied_ = commit_index_;
    if (apply_sink_) {
        apply_sink_->OnCommitted(start, end);
    }
}

// ===========================================================================
// Server-side RPC handlers
// ===========================================================================

void RaftNode::RequestVote(google::protobuf::RpcController* /*controller*/,
                           const ::raft::RequestVoteArgs*   request,
                           ::raft::RequestVoteReply*        response,
                           google::protobuf::Closure*       done) {
    Post([this, request, response, done]() {
        HandleRequestVote(request, response);
        done->Run();
    });
}

void RaftNode::AppendEntries(google::protobuf::RpcController* /*controller*/,
                             const ::raft::AppendEntriesArgs* request,
                             ::raft::AppendEntriesReply*      response,
                             google::protobuf::Closure*       done) {
    Post([this, request, response, done]() {
        HandleAppendEntries(request, response);
        done->Run();
    });
}

void RaftNode::InstallSnapshot(google::protobuf::RpcController* /*controller*/,
                               const ::raft::InstallSnapshotArgs* request,
                               ::raft::InstallSnapshotReply*      response,
                               google::protobuf::Closure*         done) {
    Post([this, request, response, done]() {
        HandleInstallSnapshot(request, response);
        done->Run();
    });
}

void RaftNode::AddPeer(google::protobuf::RpcController* /*controller*/,
                       const ::raft::AddPeerArgs*        request,
                       ::raft::AddPeerReply*             response,
                       google::protobuf::Closure*        done) {
    Post([this, request, response, done]() {
        HandleAddPeer(request, response);
        done->Run();
    });
}

void RaftNode::RemovePeer(google::protobuf::RpcController* /*controller*/,
                          const ::raft::RemovePeerArgs*     request,
                          ::raft::RemovePeerReply*          response,
                          google::protobuf::Closure*        done) {
    Post([this, request, response, done]() {
        HandleRemovePeer(request, response);
        done->Run();
    });
}

void RaftNode::HandleRequestVote(const ::raft::RequestVoteArgs* req,
                                 ::raft::RequestVoteReply*      resp) {
    // Config-Aware Voting: reject votes from nodes that are not part of the
    // current cluster configuration (e.g. nodes that have been removed).
    // Crucially we do NOT update our term here, so a stale removed node
    // cannot force us to step down by sending a high-term RequestVote.
    if (current_members_.find(req->candidateid()) == current_members_.end()) {
        resp->set_term(current_term_);
        resp->set_votegranted(false);
        LOG_DEBUG() << "RequestVote rejected: candidate=" << req->candidateid()
                    << " not in current_members_";
        return;
    }

    if (req->term() > current_term_) {
        BecomeFollower(req->term(), kNoNode);
    }

    bool grant = false;
    if (req->term() < current_term_) {
        grant = false;
    } else if (voted_for_ == kNoNode || voted_for_ == req->candidateid()) {
        // Up-to-date check: candidate's log must be at least as up-to-date
        // as ours. (RaftPaper section 5.4.1.)
        Index my_last_idx  = log_cache_.LastIndex();
        Term  my_last_term = log_cache_.LastTerm();
        bool log_ok =
            (req->lastlogterm() > my_last_term) ||
            (req->lastlogterm() == my_last_term &&
             req->lastlogindex() >= my_last_idx);
        if (log_ok) {
            grant = true;
            voted_for_ = req->candidateid();
            election_timer_.Reset();
            PersistHardState();
        }
    }

    resp->set_term(current_term_);
    resp->set_votegranted(grant);

    LOG_DEBUG() << "RequestVote from candidate=" << req->candidateid()
                << " term=" << req->term()
                << " grant=" << grant
                << " my_term=" << current_term_;
}

void RaftNode::HandleAppendEntries(const ::raft::AppendEntriesArgs* req,
                                   ::raft::AppendEntriesReply*      resp) {
    // Reject stale leaders.
    if (req->term() < current_term_) {
        resp->set_term(current_term_);
        resp->set_success(false);
        return;
    }

    // Step down / acknowledge a (possibly new) leader.
    if (req->term() > current_term_ || state_ != RaftState::Follower) {
        BecomeFollower(req->term(), req->leaderid());
    } else {
        // Same term, already follower: just update leader and reset timer.
        leader_id_ = req->leaderid();
        leader_id_atomic_.store(leader_id_, std::memory_order_release);
        election_timer_.Reset();
    }

    bool accepted = true;
    Index prev_idx = req->prevlogindex();
    Term prev_term = req->prevlogterm();
    if (prev_idx < log_cache_.SnapshotLastIndex()) {
        accepted = false;
        resp->set_conflictindex(log_cache_.SnapshotLastIndex() + 1);
        resp->set_conflictterm(0);
    } else if (prev_idx > 0 && log_cache_.TermAt(prev_idx) == 0) {
        accepted = false;
        resp->set_conflictindex(log_cache_.LastIndex() + 1);
        resp->set_conflictterm(0);
    } else if (prev_idx > 0 && log_cache_.TermAt(prev_idx) != prev_term) {
        accepted = false;
        Term conflict_term = log_cache_.TermAt(prev_idx);
        resp->set_conflictterm(conflict_term);
        resp->set_conflictindex(FirstIndexOfTerm(log_cache_, conflict_term));
    }

    std::vector<LogEntry> to_append;
    Index truncate_from = 0;
    if (accepted) {
        for (int i = 0; i < req->entries_size(); ++i) {
            const auto& pe = req->entries(i);
            Index entry_index = pe.index();
            if (entry_index <= log_cache_.SnapshotLastIndex()) {
                continue;
            }
            Term local_term = log_cache_.TermAt(entry_index);
            if (local_term == pe.term()) {
                continue;
            }
            truncate_from = entry_index;
            break;
        }

        if (truncate_from > 0) {
            log_cache_.TruncateSuffix(truncate_from);
            if (storage_) {
                storage_->TruncateSuffix(truncate_from);
            }
        }

        for (int i = 0; i < req->entries_size(); ++i) {
            const auto& pe = req->entries(i);
            if (pe.index() <= log_cache_.SnapshotLastIndex()) {
                continue;
            }
            if (pe.index() <= log_cache_.LastIndex()) {
                continue;
            }
            if (pe.index() != log_cache_.LastIndex() + 1) {
                accepted = false;
                resp->set_conflictindex(log_cache_.LastIndex() + 1);
                resp->set_conflictterm(0);
                break;
            }
            LogEntry e;
            e.term    = pe.term();
            e.index   = pe.index();
            e.command = pe.command();
            e.type    = static_cast<EntryType>(static_cast<int>(pe.type()));
            log_cache_.AppendEntry(e);
            to_append.push_back(std::move(e));
        }

        if (accepted && storage_ && !to_append.empty()) {
            storage_->AppendEntries(to_append);
        }
    }

    if (accepted) {
        Index leader_commit = req->leadercommit();
        if (leader_commit > commit_index_) {
            commit_index_ = std::min(leader_commit, log_cache_.LastIndex());
            ApplyCommittedRange();
        }
    }

    resp->set_term(current_term_);
    resp->set_success(accepted);
}

void RaftNode::HandleInstallSnapshot(const ::raft::InstallSnapshotArgs* req,
                                     ::raft::InstallSnapshotReply*      resp) {
    if (req->term() < current_term_) {
        resp->set_term(current_term_);
        return;
    }

    if (req->term() > current_term_ || state_ != RaftState::Follower) {
        BecomeFollower(req->term(), req->leaderid());
    } else {
        leader_id_ = req->leaderid();
        leader_id_atomic_.store(leader_id_, std::memory_order_release);
        election_timer_.Reset();
    }

    Index snapshot_index = req->lastincludedindex();
    Term snapshot_term = req->lastincludedterm();
    if (snapshot_index <= commit_index_) {
        resp->set_term(current_term_);
        return;
    }

    bool keep_suffix =
        snapshot_index <= log_cache_.LastIndex() &&
        log_cache_.TermAt(snapshot_index) == snapshot_term;

    if (storage_) {
        storage_->SaveSnapshot(snapshot_index, snapshot_term, req->data());
        if (keep_suffix) {
            storage_->TruncatePrefix(snapshot_index);
        } else {
            storage_->TruncateSuffix(1);
        }
    }

    if (!keep_suffix) {
        log_cache_.Clear();
    }
    log_cache_.DiscardPrefix(snapshot_index, snapshot_term);

    commit_index_ = snapshot_index;
    last_applied_ = snapshot_index;
    if (apply_sink_) {
        apply_sink_->OnSnapshotInstalled(snapshot_index, snapshot_term, req->data());
    }

    resp->set_term(current_term_);
}

void RaftNode::HandleAddPeer(const ::raft::AddPeerArgs* req,
                             ::raft::AddPeerReply*      resp) {
    if (state_ != RaftState::Leader) {
        resp->set_success(false);
        resp->set_message("not leader");
        resp->set_leader_id(leader_id_);
        return;
    }
    if (pending_config_change_) {
        resp->set_success(false);
        resp->set_message("config change already in progress");
        return;
    }

    NodeId new_id = req->node_id();
    if (new_id == cfg_.self_id) {
        resp->set_success(false);
        resp->set_message("cannot add self");
        return;
    }
    for (const auto& p : peers_) {
        if (p->Id() == new_id) {
            resp->set_success(false);
            resp->set_message("peer already exists");
            return;
        }
    }

    PeerInfo info;
    info.id   = new_id;
    info.ip   = req->ip();
    info.port = req->port();

    auto peer = std::make_unique<Peer>(info, cfg_.rpc_timeout_ms);
    peer->SetLearner(true);
    peer->ResetForLeader(log_cache_.LastIndex());

    size_t peer_index = peers_.size();
    peers_.push_back(std::move(peer));

    // Begin replication to the new learner immediately so it can catch up.
    ReplicateOnce(peer_index, current_term_);

    resp->set_success(true);
    resp->set_message("learner added; will be promoted to voting member once caught up");

    LOG_INFO() << "AddPeer: learner id=" << info.id
               << " " << info.ip << ":" << info.port << " added";
}

void RaftNode::HandleRemovePeer(const ::raft::RemovePeerArgs* req,
                                ::raft::RemovePeerReply*      resp) {
    if (state_ != RaftState::Leader) {
        resp->set_success(false);
        resp->set_message("not leader");
        resp->set_leader_id(leader_id_);
        return;
    }
    if (pending_config_change_) {
        resp->set_success(false);
        resp->set_message("config change already in progress");
        return;
    }

    NodeId target_id = req->node_id();

    // Locate the target peer; only voting members can be removed via RemovePeer.
    const Peer* found_peer = nullptr;
    for (const auto& p : peers_) {
        if (p->Id() == target_id && !p->IsLearner()) {
            found_peer = p.get();
            break;
        }
    }
    if (!found_peer) {
        resp->set_success(false);
        resp->set_message("peer not found or is a learner");
        return;
    }

    // Safety: after removal there must still be at least one voting member
    // (the leader itself) able to reach quorum with itself.
    if (VotingPeerCount() <= 1) {
        resp->set_success(false);
        resp->set_message("cannot remove peer: would leave cluster without quorum");
        return;
    }

    ProposeConfigChange(ConfigChangeType::RemovePeer, found_peer->Info());

    resp->set_success(true);
    resp->set_message("config change proposed");

    LOG_INFO() << "RemovePeer: proposed removal of id=" << target_id;
}

// ===========================================================================
// Propose
// ===========================================================================

bool RaftNode::Propose(std::string command, Index* assigned_index) {
    std::promise<std::pair<bool, Index>> p;
    auto                                 fut = p.get_future();

    Post([this, command = std::move(command), &p]() mutable {
        if (state_ != RaftState::Leader) {
            p.set_value({false, 0});
            return;
        }
        Index idx = log_cache_.Append(current_term_, std::move(command));
        if (storage_) {
            LogEntry e;
            log_cache_.EntryAt(idx, &e);
            storage_->AppendEntries({e});
        }
        MaybeAdvanceCommitIndex();
        BroadcastAppendEntries();
        p.set_value({true, idx});
    });

    auto pair = fut.get();
    if (assigned_index) *assigned_index = pair.second;
    return pair.first;
}

void RaftNode::DoTakeSnapshot(Index last_included_index, std::string data) {
    if (!storage_ ||
        last_included_index <= log_cache_.SnapshotLastIndex() ||
        last_included_index > commit_index_ ||
        last_included_index > log_cache_.LastIndex()) {
        return;
    }

    Term last_included_term = log_cache_.TermAt(last_included_index);
    if (last_included_term == 0) {
        return;
    }

    storage_->SaveSnapshot(last_included_index, last_included_term, data);
    storage_->TruncatePrefix(last_included_index);
    log_cache_.DiscardPrefix(last_included_index, last_included_term);

    LOG_DEBUG() << "snapshot taken index=" << last_included_index
                << " term=" << last_included_term;
}

bool RaftNode::TakeSnapshot(Index last_included_index, std::string data) {
    std::promise<bool> p;
    auto               fut = p.get_future();

    Post([this, last_included_index, data = std::move(data), &p]() mutable {
        bool ok = (storage_ &&
                   last_included_index > log_cache_.SnapshotLastIndex() &&
                   last_included_index <= commit_index_ &&
                   last_included_index <= log_cache_.LastIndex() &&
                   log_cache_.TermAt(last_included_index) != 0);
        if (ok) {
            DoTakeSnapshot(last_included_index, std::move(data));
        }
        p.set_value(ok);
    });

    return fut.get();
}

void RaftNode::TakeSnapshotAsync(Index last_included_index, std::string data) {
    Post([this, last_included_index, data = std::move(data)]() mutable {
        DoTakeSnapshot(last_included_index, std::move(data));
    });
}

// ===========================================================================
// Membership change helpers
// ===========================================================================

int RaftNode::VotingPeerCount() const {
    int count = 0;
    for (const auto& p : peers_) {
        if (!p->IsLearner()) ++count;
    }
    return count;
}

void RaftNode::ProposeConfigChange(ConfigChangeType type, const PeerInfo& peer) {
    // Encode the membership change as a protobuf and store it as the
    // command payload of an ENTRY_CONFIG log entry.
    ::raft::ConfigChange cc;
    cc.set_change_type(type == ConfigChangeType::AddPeer
                           ? ::raft::ADD_PEER
                           : ::raft::REMOVE_PEER);
    cc.set_node_id(peer.id);
    cc.set_ip(peer.ip);
    cc.set_port(peer.port);

    std::string command;
    cc.SerializeToString(&command);

    LogEntry e;
    e.term    = current_term_;
    e.index   = log_cache_.LastIndex() + 1;
    e.command = std::move(command);
    e.type    = EntryType::Config;
    log_cache_.AppendEntry(e);

    if (storage_) {
        storage_->AppendEntries({e});
    }

    pending_config_change_ = true;

    MaybeAdvanceCommitIndex();
    BroadcastAppendEntries();

    LOG_INFO() << "ProposeConfigChange type="
               << (type == ConfigChangeType::AddPeer ? "AddPeer" : "RemovePeer")
               << " node_id=" << peer.id
               << " at index=" << e.index;
}

void RaftNode::ApplyConfigChange(const LogEntry& entry) {
    ::raft::ConfigChange cc;
    if (!cc.ParseFromString(entry.command)) {
        LOG_ERROR() << "ApplyConfigChange: failed to parse ConfigChange at index="
                    << entry.index;
        pending_config_change_ = false;
        return;
    }

    NodeId node_id = cc.node_id();

    if (cc.change_type() == ::raft::ADD_PEER) {
        // Promote the matching Learner to a full voting member.
        for (auto& p : peers_) {
            if (p->Id() == node_id && p->IsLearner()) {
                p->SetLearner(false);
                current_members_.insert(node_id);
                LOG_INFO() << "ApplyConfigChange: promoted learner id=" << node_id
                           << " to voting member";
                break;
            }
        }
    } else if (cc.change_type() == ::raft::REMOVE_PEER) {
        auto it = std::find_if(peers_.begin(), peers_.end(),
                               [node_id](const std::unique_ptr<Peer>& p) {
                                   return p->Id() == node_id;
                               });
        if (it != peers_.end()) {
            peers_.erase(it);
        }
        current_members_.erase(node_id);
        LOG_INFO() << "ApplyConfigChange: removed peer id=" << node_id;
    }

    pending_config_change_ = false;

    // Persist the updated list of voting peers so it survives a crash.
    if (storage_) {
        std::vector<PeerInfo> voting_peers;
        voting_peers.reserve(peers_.size());
        for (const auto& p : peers_) {
            if (!p->IsLearner()) {
                voting_peers.push_back(p->Info());
            }
        }
        storage_->SaveConfig(voting_peers);
    }
}

void RaftNode::MaybePromoteLearner(size_t peer_index) {
    if (state_ != RaftState::Leader) return;
    if (pending_config_change_) return;

    auto& peer = *peers_[peer_index];
    if (!peer.IsLearner()) return;

    Index last = log_cache_.LastIndex();
    // Promote once the learner's match_index is within the configured
    // tolerance of the leader's last log index.
    if (peer.MatchIndex() + cfg_.learner_lag_tolerance >= last) {
        LOG_INFO() << "MaybePromoteLearner: learner id=" << peer.Id()
                   << " match_index=" << peer.MatchIndex()
                   << " is caught up; proposing promotion";
        ProposeConfigChange(ConfigChangeType::AddPeer, peer.Info());
    }
}

void RaftNode::RecoverPendingConfigChange() {
    // Scan uncommitted log entries for any in-flight config change left by
    // a previous leader. If found, restore pending_config_change_ so we
    // do not allow a second concurrent change, and reconstruct any Learner
    // Peer whose state lived only in the old leader's memory.
    for (Index i = commit_index_ + 1; i <= log_cache_.LastIndex(); ++i) {
        LogEntry e;
        if (!log_cache_.EntryAt(i, &e)) continue;
        if (e.type == EntryType::Config) {
            pending_config_change_ = true;
            ReconstructLearnerIfNeeded(e);
            LOG_INFO() << "RecoverPendingConfigChange: found uncommitted config entry"
                       << " at index=" << i;
            break;  // single-step: at most one uncommitted config entry
        }
    }
}

void RaftNode::ReconstructLearnerIfNeeded(const LogEntry& config_entry) {
    ::raft::ConfigChange cc;
    if (!cc.ParseFromString(config_entry.command)) return;
    if (cc.change_type() != ::raft::ADD_PEER) return;

    NodeId target_id = cc.node_id();

    // If the peer already exists (possibly from the boot config or a
    // previous reconstruction), do nothing.
    for (const auto& p : peers_) {
        if (p->Id() == target_id) return;
    }

    PeerInfo info;
    info.id   = target_id;
    info.ip   = cc.ip();
    info.port = cc.port();

    auto peer = std::make_unique<Peer>(info, cfg_.rpc_timeout_ms);
    peer->SetLearner(true);
    peer->ResetForLeader(log_cache_.LastIndex());
    peers_.push_back(std::move(peer));

    LOG_INFO() << "ReconstructLearnerIfNeeded: reconstructed learner id=" << target_id;
}

}  // namespace raft_core
