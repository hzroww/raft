// Smoke test for the raft_core RaftNode.
//
// Spins up a 3-node cluster on localhost (ports 19101/19102/19103) and
// verifies:
//   1. Exactly one Leader emerges within a reasonable time.
//   2. The leader's term is consistent across the cluster.
//   3. Propose() succeeds on the leader, fails on a follower.
//   4. The committed range reaches the apply sink on every node.
//
// All nodes use NullRaftStorage; this test only exercises the
// communication + state machine path.

#include "log.h"
#include "raft_apply_iface.h"
#include "raft_node.h"
#include "raft_storage_iface.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace raft_core;

namespace {

class CountingApplySink : public IRaftApplySink {
public:
    void OnCommitted(Index start, Index end) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (end > last_end_) last_end_ = end;
        last_start_ = start;
        total_committed_ += (end - start + 1);
    }
    void OnSnapshotInstalled(Index, Term, const std::string&) override {}
    Index LastEnd() const {
        std::lock_guard<std::mutex> lk(mu_);
        return last_end_;
    }
    int64_t Total() const {
        std::lock_guard<std::mutex> lk(mu_);
        return total_committed_;
    }
private:
    mutable std::mutex mu_;
    Index   last_start_      = 0;
    Index   last_end_        = 0;
    int64_t total_committed_ = 0;
};

struct NodeBundle {
    NullRaftStorage              storage;
    CountingApplySink            sink;
    std::unique_ptr<RaftNode>    node;
};

void Fail(const char* msg) {
    LOG_ERROR() << "FAIL: " << msg;
    std::cout << "FAIL: " << msg << std::endl;
    std::_Exit(1);
}

}  // namespace

int main() {
    raft::logging::Init();
    raft::logging::SetCurrentThreadName("smoke-main");

    constexpr int kBasePort = 19101;
    constexpr int kN        = 3;

    std::vector<PeerInfo> all;
    for (int i = 0; i < kN; ++i) {
        all.push_back({static_cast<NodeId>(i), "127.0.0.1", kBasePort + i});
    }

    std::vector<std::unique_ptr<NodeBundle>> bundles;
    bundles.reserve(kN);

    for (int i = 0; i < kN; ++i) {
        auto bundle = std::make_unique<NodeBundle>();
        RaftNode::Config cfg;
        cfg.self_id     = static_cast<NodeId>(i);
        cfg.listen_ip   = "127.0.0.1";
        cfg.listen_port = kBasePort + i;
        cfg.election_timeout_min_ms = 200;
        cfg.election_timeout_max_ms = 400;
        cfg.heartbeat_interval_ms   = 50;
        cfg.rpc_timeout_ms          = 800;
        for (int j = 0; j < kN; ++j) {
            if (j == i) continue;
            cfg.peers.push_back(all[j]);
        }
        bundle->node = std::make_unique<RaftNode>(std::move(cfg),
                                                  &bundle->storage,
                                                  &bundle->sink);
        bundles.push_back(std::move(bundle));
    }

    for (auto& b : bundles) b->node->Start();

    // Wait up to 5 s for a leader to emerge and stabilise.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int  leader   = -1;
    while (std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int  count = 0;
        int  idx   = -1;
        Term term  = -1;
        bool any_diff_term = false;
        for (int i = 0; i < kN; ++i) {
            if (bundles[i]->node->State() == RaftState::Leader) {
                count++;
                idx  = i;
                if (term == -1) term = bundles[i]->node->CurrentTerm();
                else if (bundles[i]->node->CurrentTerm() != term) any_diff_term = true;
            }
        }
        if (count == 1 && !any_diff_term) {
            leader = idx;
            break;
        }
    }

    if (leader < 0) Fail("no unique leader emerged within 5s");

    LOG_INFO() << "leader emerged: node=" << leader
               << " term="
               << static_cast<long long>(bundles[leader]->node->CurrentTerm());

    // Term agreement: every node should know currentTerm >= leader's term.
    Term lt = bundles[leader]->node->CurrentTerm();
    for (int i = 0; i < kN; ++i) {
        if (bundles[i]->node->CurrentTerm() < lt) {
            Fail("follower currentTerm < leader currentTerm");
        }
    }

    // The new leader first appends a no-op entry, so client commands start
    // after that leader-initialization entry.
    for (int k = 0; k < 3; ++k) {
        Index assigned = 0;
        if (!bundles[leader]->node->Propose("op-" + std::to_string(k), &assigned)) {
            Fail("Propose() failed on leader");
        }
        if (assigned != static_cast<Index>(k + 2)) {
            Fail("unexpected assigned index");
        }
    }

    // Propose on a follower should be rejected.
    int follower = (leader + 1) % kN;
    if (bundles[follower]->node->Propose("rejected", nullptr)) {
        Fail("Propose() unexpectedly succeeded on follower");
    }

    // Wait for replication / commit on every node.
    auto commit_deadline = std::chrono::steady_clock::now() +
                           std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < commit_deadline) {
        bool all_done = true;
        for (int i = 0; i < kN; ++i) {
            if (bundles[i]->sink.LastEnd() < 4) {
                all_done = false;
                break;
            }
        }
        if (all_done) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    for (int i = 0; i < kN; ++i) {
        Index e = bundles[i]->sink.LastEnd();
        LOG_INFO() << "node=" << i << " committed_up_to=" << static_cast<long long>(e);
        if (e < 4) {
            Fail("commit did not propagate to all nodes within 5s");
        }
    }

    LOG_INFO() << "OK: leader=" << leader
               << " term=" << static_cast<long long>(lt)
               << " all 3 commands committed on every node";
    std::cout << "OK" << std::endl;

    // Shutdown is best-effort; RpcServer doesn't expose a clean stop yet.
    for (auto& b : bundles) b->node->Stop();
    std::_Exit(0);
}
