// Four-node Raft follower restart integration test.
//
// This test starts four kv_cluster_main child processes, then exercises the
// follower crash/restart recovery path:
//
//   Phase 1 - bring up the cluster and commit a baseline of writes.
//   Phase 2 - kill one follower F; keep writing. Reads/writes must be
//             unaffected because a 3/4 majority remains.
//   Phase 3 - restart F WITHOUT wiping its data directory. F replays its
//             persistent log on boot, then the leader streams the entries it
//             missed while down. We poll F's status until its commit_index /
//             last_log_index catch up to the leader.
//   Phase 4 - verify every key written across all phases is readable, proving
//             the restarted follower converged with the leader.
//
// Each node exposes a GetNodeStatus RPC that reports term / state /
// commit_index / last_applied / last_log_index plus the tail of its raft log.
// The test dumps a per-node status table before and after the restart so the
// log divergence and subsequent catch-up are visible.

#include "kv.pb.h"
#include "kv_client.h"
#include "log.h"
#include "raft_types.h"
#include "rpc_channel.h"
#include "rpc_controller.h"

#include <muduo/base/Logging.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int kBasePort = 19401;
constexpr int kN        = 4;

struct Child {
    pid_t       pid = -1;
    std::string data_dir;
    std::string conf_path;
};

std::vector<Child> g_children;

class SyncDone : public google::protobuf::Closure {
public:
    void Run() override {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
        cv_.notify_one();
    }

    void Wait() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return done_; });
    }

private:
    std::mutex              mu_;
    std::condition_variable cv_;
    bool                    done_{false};
};

void KillAllChildren() {
    for (auto& c : g_children) {
        if (c.pid > 0) {
            ::kill(c.pid, SIGTERM);
        }
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (auto& c : g_children) {
        if (c.pid <= 0) continue;
        while (std::chrono::steady_clock::now() < deadline) {
            int   status = 0;
            pid_t r      = ::waitpid(c.pid, &status, WNOHANG);
            if (r == c.pid || r < 0) {
                c.pid = -1;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (c.pid > 0) {
            ::kill(c.pid, SIGKILL);
            ::waitpid(c.pid, nullptr, 0);
            c.pid = -1;
        }
    }
}

void CleanupDataDirs() {
    for (auto& c : g_children) {
        std::error_code ec;
        if (!c.data_dir.empty()) {
            std::filesystem::remove_all(c.data_dir, ec);
        }
        if (!c.conf_path.empty()) {
            std::filesystem::remove(c.conf_path, ec);
        }
        if (!c.data_dir.empty()) {
            std::filesystem::remove(c.data_dir + ".log", ec);
        }
    }
}

[[noreturn]] void Fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    KillAllChildren();
    CleanupDataDirs();
    std::_Exit(1);
}

void SignalHandler(int /*sig*/) {
    KillAllChildren();
    CleanupDataDirs();
    std::_Exit(130);
}

void InstallSignalHandlers() {
    struct sigaction sa{};
    sa.sa_handler = SignalHandler;
    sigemptyset(&sa.sa_mask);
    ::sigaction(SIGINT,  &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
}

std::string LocateServerBinary(const char* argv0) {
    std::filesystem::path self;
    try {
        self = std::filesystem::canonical("/proc/self/exe");
    } catch (...) {
        self = argv0;
    }
    auto candidate = self.parent_path() / "kv_cluster_main";
    if (std::filesystem::exists(candidate)) {
        return candidate.string();
    }
    std::filesystem::path p(argv0);
    candidate = p.parent_path() / "kv_cluster_main";
    return candidate.string();
}

void WriteNodeConfig(const std::string& conf_path,
                     int node_id,
                     const std::string& data_dir) {
    std::ofstream f(conf_path);
    if (!f.is_open()) {
        Fail("cannot write config file: " + conf_path);
    }
    f << "node_id   = " << node_id << "\n"
      << "listen_ip = 127.0.0.1\n"
      << "listen_port = " << (kBasePort + node_id) << "\n"
      << "data_dir  = " << data_dir << "\n"
      << "io_threads = 1\n"
      << "worker_threads = 4\n"
      << "snapshot_threshold = 1000\n"
      << "election_timeout_min_ms = 150\n"
      << "election_timeout_max_ms = 300\n"
      << "heartbeat_interval_ms   = 50\n"
      << "\n"
      << "node_count = " << kN << "\n";
    for (int i = 0; i < kN; ++i) {
        f << "node_" << i << "_ip   = 127.0.0.1\n"
          << "node_" << i << "_port = " << (kBasePort + i) << "\n";
    }
}

// |append_log| controls whether the per-node stdout/stderr log file is
// truncated (fresh boot) or appended to (restart, so we keep the history).
pid_t SpawnNode(const std::string& bin,
                const std::string& conf_path,
                const std::string& data_dir,
                bool append_log) {
    pid_t pid = ::fork();
    if (pid < 0) {
        Fail(std::string("fork: ") + std::strerror(errno));
    }
    if (pid == 0) {
        std::string log_path = data_dir + ".log";
        int flags = O_WRONLY | O_CREAT | (append_log ? O_APPEND : O_TRUNC);
        int fd = ::open(log_path.c_str(), flags, 0644);
        if (fd >= 0) {
            ::dup2(fd, STDOUT_FILENO);
            ::dup2(fd, STDERR_FILENO);
            ::close(fd);
        }

        std::vector<char*> args;
        args.push_back(const_cast<char*>(bin.c_str()));
        args.push_back(const_cast<char*>("-c"));
        args.push_back(const_cast<char*>(conf_path.c_str()));
        args.push_back(nullptr);

        ::execv(bin.c_str(), args.data());
        std::cerr << "execv(" << bin << ") failed: " << std::strerror(errno)
                  << "\n";
        std::_Exit(127);
    }
    return pid;
}

std::vector<raft_core::PeerInfo> BuildPeers() {
    std::vector<raft_core::PeerInfo> peers;
    for (int i = 0; i < kN; ++i) {
        peers.push_back({static_cast<raft_core::NodeId>(i),
                         "127.0.0.1",
                         kBasePort + i});
    }
    return peers;
}

// Query a single node's status via the GetNodeStatus RPC. Returns nullopt if
// the node is unreachable (e.g. it is currently down).
std::optional<kv::GetNodeStatusResponse> DirectGetStatus(int node_id) {
    RpcChannel channel("127.0.0.1", kBasePort + node_id);
    channel.setTimeoutMs(800);
    if (!channel.waitUntilConnected(800)) {
        return std::nullopt;
    }

    kv::KvServerRpc_Stub        stub(&channel);
    RpcController               ctl;
    SyncDone                    done;
    kv::GetNodeStatusRequest    request;
    kv::GetNodeStatusResponse   response;

    stub.GetNodeStatus(&ctl, &request, &response, &done);
    done.Wait();
    if (ctl.Failed() || !response.success()) {
        return std::nullopt;
    }
    return response;
}

void PrintClusterStatus(const std::string& label) {
    std::cout << "\n" << label << "\n";
    std::cout << "----------------------------------------------------------------\n";
    for (int node_id = 0; node_id < kN; ++node_id) {
        if (g_children[node_id].pid <= 0) {
            std::cout << "  node=" << node_id << " [DOWN]\n";
            continue;
        }
        auto st = DirectGetStatus(node_id);
        if (!st.has_value()) {
            std::cout << "  node=" << node_id << " [UNREACHABLE]\n";
            continue;
        }
        std::cout << "  node=" << node_id
                  << " state="  << st->state()
                  << " term="   << st->currentterm()
                  << " leader=" << st->leaderid()
                  << " commit=" << st->commitindex()
                  << " applied=" << st->lastapplied()
                  << " last_log=" << st->lastlogindex()
                  << "(t" << st->lastlogterm() << ")"
                  << " entries=" << st->entries_size() << "\n";
    }
    std::cout << "----------------------------------------------------------------\n";
}

// Poll every alive node until one reports itself as Leader. Returns its id.
std::optional<int> WaitForLeader(std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int node_id = 0; node_id < kN; ++node_id) {
            if (g_children[node_id].pid <= 0) {
                continue;
            }
            auto st = DirectGetStatus(node_id);
            if (st.has_value() && st->state() == "Leader" &&
                st->leaderid() == node_id) {
                return node_id;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return std::nullopt;
}

// Pick any alive node that is not the given leader.
std::optional<int> PickFollower(int leader_id) {
    for (int node_id = 0; node_id < kN; ++node_id) {
        if (node_id == leader_id) continue;
        if (g_children[node_id].pid <= 0) continue;
        auto st = DirectGetStatus(node_id);
        if (st.has_value() && st->state() != "Leader") {
            return node_id;
        }
    }
    return std::nullopt;
}

void KillNode(int node_id) {
    Child& child = g_children[node_id];
    if (child.pid <= 0) {
        return;
    }

    std::cout << "killing node " << node_id << " pid=" << child.pid << "\n";
    ::kill(child.pid, SIGTERM);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        int   status = 0;
        pid_t r      = ::waitpid(child.pid, &status, WNOHANG);
        if (r == child.pid || r < 0) {
            child.pid = -1;
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ::kill(child.pid, SIGKILL);
    ::waitpid(child.pid, nullptr, 0);
    child.pid = -1;
}

// Poll |follower_id| until it has caught up to |leader_id| (commit_index and
// last_log_index both >= the leader's at the moment of comparison).
bool WaitForCatchUp(int follower_id, int leader_id,
                    std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto leader   = DirectGetStatus(leader_id);
        auto follower = DirectGetStatus(follower_id);
        if (leader.has_value() && follower.has_value() &&
            follower->commitindex()  >= leader->commitindex() &&
            follower->lastlogindex() >= leader->lastlogindex()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

void WaitUntilPutSucceeds(kvclient::KvClient* client,
                          const std::string& key,
                          const std::string& value,
                          std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string last_error;
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = client->Put(key, value);
        if (r.ok) {
            return;
        }
        last_error = r.error;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    Fail("Put " + key + " did not succeed before timeout, last error: " +
         last_error);
}

void ExpectGet(kvclient::KvClient* client,
               const std::string& key,
               const std::string& want) {
    auto r = client->Get(key);
    if (!r.ok) {
        Fail("Get " + key + " failed: " + r.error);
    }
    if (r.value != want) {
        Fail("Get " + key + " got '" + r.value + "', want '" + want + "'");
    }
}

}  // namespace

int main(int argc, char** argv) {
    raft::logging::Init();
    muduo::Logger::setOutput([](const char*, int){});
    muduo::Logger::setFlush([](){});
    raft::logging::SetCurrentThreadName("follower-restart-test");
    InstallSignalHandlers();

    std::string bin = LocateServerBinary(argv[0]);
    if (!std::filesystem::exists(bin)) {
        std::cerr << "kv_cluster_main not found at " << bin << "\n";
        return 1;
    }
    std::cout << "using server binary: " << bin << "\n";

    std::string run_prefix = "/tmp/raft_follower_restart_test_" +
                             std::to_string(static_cast<long long>(::getpid()));

    g_children.resize(kN);
    for (int i = 0; i < kN; ++i) {
        std::string dir       = run_prefix + "_node_" + std::to_string(i);
        std::string conf_path = run_prefix + "_node_" + std::to_string(i) + ".conf";

        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        if (ec) Fail("create_directories: " + dir + ": " + ec.message());

        WriteNodeConfig(conf_path, i, dir);
        g_children[i].data_dir  = dir;
        g_children[i].conf_path = conf_path;
        g_children[i].pid       = SpawnNode(bin, conf_path, dir, /*append_log=*/false);
        std::cout << "spawned node " << i
                  << " pid=" << g_children[i].pid
                  << " port=" << (kBasePort + i) << "\n";
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    kvclient::KvClientOptions opts;
    opts.max_retries     = 80;
    opts.rpc_timeout_ms  = 1500;
    opts.connect_wait_ms = 3000;
    opts.retry_interval  = std::chrono::milliseconds(250);

    auto peers = BuildPeers();
    kvclient::KvClient client(peers, 0, opts);

    // ---- Phase 1: bring up the cluster + baseline writes. ----
    std::cout << "\n[Phase 1] waiting for leader + baseline writes...\n";
    WaitUntilPutSucceeds(&client, "baseline-key-0", "baseline-val-0",
                         std::chrono::seconds(30));
    constexpr int kBaseline = 10;
    for (int i = 1; i < kBaseline; ++i) {
        std::string k = "baseline-key-" + std::to_string(i);
        std::string v = "baseline-val-" + std::to_string(i);
        auto r = client.Put(k, v);
        if (!r.ok) Fail("baseline Put " + k + " failed: " + r.error);
    }

    auto leader = WaitForLeader(std::chrono::seconds(10));
    if (!leader.has_value()) Fail("could not identify leader after baseline");
    std::cout << "leader is node " << *leader << "\n";
    PrintClusterStatus("=== Phase 1: after baseline writes ===");

    // ---- Phase 2: kill a follower, keep writing. ----
    auto follower = PickFollower(*leader);
    if (!follower.has_value()) Fail("could not pick a follower to kill");
    std::cout << "\n[Phase 2] killing follower node " << *follower
              << ", then writing while it is down...\n";
    KillNode(*follower);

    constexpr int kWhileDown = 15;
    for (int i = 0; i < kWhileDown; ++i) {
        std::string k = "absent-key-" + std::to_string(i);
        std::string v = "absent-val-" + std::to_string(i);
        auto r = client.Put(k, v);
        if (!r.ok) {
            Fail("write while follower down failed for " + k + ": " + r.error);
        }
    }
    // Reads must also still work against the majority.
    ExpectGet(&client, "baseline-key-0", "baseline-val-0");
    ExpectGet(&client, "absent-key-0", "absent-val-0");
    PrintClusterStatus("=== Phase 2: after writing while follower is down ===");

    // ---- Phase 3: restart the follower, keep its data, wait for catch-up. ----
    std::cout << "\n[Phase 3] restarting follower node " << *follower
              << " (data dir preserved)...\n";
    g_children[*follower].pid = SpawnNode(bin,
                                          g_children[*follower].conf_path,
                                          g_children[*follower].data_dir,
                                          /*append_log=*/true);
    std::cout << "restarted node " << *follower
              << " pid=" << g_children[*follower].pid << "\n";

    // Give it a moment to boot + rejoin, then drive a few more writes so the
    // leader replicates the backlog plus fresh entries to it.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    constexpr int kProbe = 5;
    for (int i = 0; i < kProbe; ++i) {
        std::string k = "probe-key-" + std::to_string(i);
        std::string v = "probe-val-" + std::to_string(i);
        auto r = client.Put(k, v);
        if (!r.ok) Fail("probe Put " + k + " failed: " + r.error);
    }

    auto leader_after = WaitForLeader(std::chrono::seconds(10));
    if (!leader_after.has_value()) Fail("no leader during catch-up phase");

    if (!WaitForCatchUp(*follower, *leader_after, std::chrono::seconds(20))) {
        PrintClusterStatus("=== Phase 3: follower FAILED to catch up ===");
        Fail("restarted follower did not catch up to the leader in time");
    }
    PrintClusterStatus("=== Phase 3: after follower restart & catch-up ===");
    std::cout << "follower node " << *follower
              << " caught up with leader node " << *leader_after << "\n";

    // ---- Phase 4: full consistency check. ----
    std::cout << "\n[Phase 4] verifying all keys are readable...\n";
    for (int i = 0; i < kBaseline; ++i) {
        ExpectGet(&client, "baseline-key-" + std::to_string(i),
                  "baseline-val-" + std::to_string(i));
    }
    for (int i = 0; i < kWhileDown; ++i) {
        ExpectGet(&client, "absent-key-" + std::to_string(i),
                  "absent-val-" + std::to_string(i));
    }
    for (int i = 0; i < kProbe; ++i) {
        ExpectGet(&client, "probe-key-" + std::to_string(i),
                  "probe-val-" + std::to_string(i));
    }

    std::cout << "\nPASS: follower crashed, restarted, caught up and the "
                 "cluster stayed consistent\n";
    KillAllChildren();
    CleanupDataDirs();
    return 0;
}
