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
        LoadPersistentLog();
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

    // Single-node cluster (no peers): trivially become leader.
    int total  = static_cast<int>(peers_.size()) + 1;
    int quorum = total / 2 + 1;
    if (votes_granted_ >= quorum) {
        BecomeLeader();
        return;
    }

    // Fan-out RequestVote: one fire-and-forget coroutine per peer.
    Term election_term = current_term_;
    for (size_t i = 0; i < peers_.size(); ++i) {
        RequestVoteOnce(i, election_term);
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
        int total  = static_cast<int>(peers_.size()) + 1;
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
        SendSnapshotOnce(peer_index, leader_term);
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
        co_return;
    }

    Index snapshot_index = 0;
    Term snapshot_term = 0;
    std::string snapshot_data;
    if (!storage_->LoadSnapshot(&snapshot_index, &snapshot_term, &snapshot_data)) {
        peer.SetNextIndex(log_cache_.SnapshotLastIndex() + 1);
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

    if (state_ != RaftState::Leader || current_term_ != leader_term) {
        co_return;
    }
    if (ctl.Failed()) {
        LOG_DEBUG() << "InstallSnapshot failed peer=" << peer.Id()
                    << " err=" << ctl.ErrorText();
        co_return;
    }
    if (resp.term() > current_term_) {
        BecomeFollower(resp.term(), kNoNode);
        co_return;
    }

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

    std::vector<Index> match;
    match.reserve(peers_.size() + 1);
    match.push_back(log_cache_.LastIndex());          // self
    for (auto& p : peers_) match.push_back(p->MatchIndex());
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

void RaftNode::HandleRequestVote(const ::raft::RequestVoteArgs* req,
                                 ::raft::RequestVoteReply*      resp) {
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

bool RaftNode::TakeSnapshot(Index last_included_index, std::string data) {
    std::promise<bool> p;
    auto               fut = p.get_future();

    Post([this, last_included_index, data = std::move(data), &p]() mutable {
        if (!storage_ ||
            last_included_index <= log_cache_.SnapshotLastIndex() ||
            last_included_index > commit_index_ ||
            last_included_index > log_cache_.LastIndex()) {
            p.set_value(false);
            return;
        }

        Term last_included_term = log_cache_.TermAt(last_included_index);
        if (last_included_term == 0) {
            p.set_value(false);
            return;
        }

        storage_->SaveSnapshot(last_included_index, last_included_term, data);
        storage_->TruncatePrefix(last_included_index);
        log_cache_.DiscardPrefix(last_included_index, last_included_term);
        p.set_value(true);
    });

    return fut.get();
}

}  // namespace raft_core
