// Four-node Raft failover integration test.
//
// This test starts four kv_cluster_main child processes, waits for an initial
// leader, kills that leader, and verifies that the remaining three-node
// majority can elect a new leader and continue serving reads/writes.

#include "kv.pb.h"
#include "kv_client.h"
#include "log.h"
#include "raft_types.h"
#include "rpc_channel.h"
#include "rpc_controller.h"

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

constexpr int kBasePort = 19301;
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

pid_t SpawnNode(const std::string& bin,
                const std::string& conf_path,
                const std::string& data_dir) {
    pid_t pid = ::fork();
    if (pid < 0) {
        Fail(std::string("fork: ") + std::strerror(errno));
    }
    if (pid == 0) {
        std::string log_path = data_dir + ".log";
        int fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
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

bool DirectGet(int node_id,
               const std::string& key,
               int64_t request_id,
               kv::GetResponse* response) {
    RpcChannel channel("127.0.0.1", kBasePort + node_id);
    channel.setTimeoutMs(800);
    channel.waitUntilConnected(800);

    kv::KvServerRpc_Stub stub(&channel);
    RpcController        ctl;
    SyncDone             done;
    kv::GetRequest       request;
    request.set_key(key);
    request.set_clientid("leader-probe");
    request.set_requestid(request_id);

    stub.Get(&ctl, &request, response, &done);
    done.Wait();
    return !ctl.Failed();
}

std::optional<int> WaitForLeader(const std::string& committed_key,
                                 std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    int64_t request_id = 1;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int node_id = 0; node_id < kN; ++node_id) {
            if (g_children[node_id].pid <= 0) {
                continue;
            }

            kv::GetResponse response;
            if (!DirectGet(node_id, committed_key, request_id++, &response)) {
                continue;
            }

            if (response.success() && response.leaderid() == node_id) {
                return node_id;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
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
    raft::logging::SetCurrentThreadName("leader-failover-test");
    InstallSignalHandlers();

    std::string bin = LocateServerBinary(argv[0]);
    if (!std::filesystem::exists(bin)) {
        std::cerr << "kv_cluster_main not found at " << bin << "\n";
        return 1;
    }
    std::cout << "using server binary: " << bin << "\n";

    std::string run_prefix = "/tmp/raft_leader_failover_test_" +
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
        g_children[i].pid       = SpawnNode(bin, conf_path, dir);
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

    std::cout << "waiting for initial leader and committing anchor key...\n";
    WaitUntilPutSucceeds(&client, "failover-anchor", "before", std::chrono::seconds(30));

    auto old_leader = WaitForLeader("failover-anchor", std::chrono::seconds(10));
    if (!old_leader.has_value()) {
        Fail("could not identify initial leader");
    }
    std::cout << "initial leader: node " << *old_leader << "\n";

    KillNode(*old_leader);

    std::cout << "waiting for remaining majority to elect a new leader...\n";
    WaitUntilPutSucceeds(&client, "after-leader-kill", "survived", std::chrono::seconds(35));

    auto new_leader = WaitForLeader("after-leader-kill", std::chrono::seconds(10));
    if (!new_leader.has_value()) {
        Fail("could not identify new leader after killing old leader");
    }
    if (*new_leader == *old_leader) {
        Fail("new leader is the killed node");
    }
    std::cout << "new leader: node " << *new_leader << "\n";

    ExpectGet(&client, "failover-anchor", "before");
    ExpectGet(&client, "after-leader-kill", "survived");

    constexpr int kPostFailoverBatch = 8;
    for (int i = 0; i < kPostFailoverBatch; ++i) {
        std::string key = "post-failover-key-" + std::to_string(i);
        std::string val = "post-failover-val-" + std::to_string(i);
        auto r = client.Put(key, val);
        if (!r.ok) {
            Fail("post-failover Put " + key + " failed: " + r.error);
        }
    }
    for (int i = 0; i < kPostFailoverBatch; ++i) {
        std::string key = "post-failover-key-" + std::to_string(i);
        std::string val = "post-failover-val-" + std::to_string(i);
        ExpectGet(&client, key, val);
    }

    std::cout << "PASS: 4-node cluster survived leader process failure\n";
    KillAllChildren();
    CleanupDataDirs();
    return 0;
}
