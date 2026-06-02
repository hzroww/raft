// Multi-process Raft cluster integration test.
//
// Spawns three kv_cluster_main child processes (one per node) on localhost
// ports 19201/19202/19203, then drives them through KvClient to verify:
//   1. A leader is elected within a reasonable time.
//   2. Log replication works end-to-end (Put followed by Get returns the
//      written value via the cluster's commit pipeline).
//   3. Multiple writes are linearised and readable.
//
// Each child process is started with a per-node config file written to the
// same tmp directory as the node's data, using kv_cluster_main's -c flag.
//
// On exit, the test sends SIGTERM to all children, waits for them, and
// removes the per-node data directories.

#include "kv_client.h"
#include "log.h"
#include "raft_types.h"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int kBasePort = 19201;
constexpr int kN        = 3;

struct Child {
    pid_t       pid      = -1;
    std::string data_dir;
    std::string conf_path;
};

std::vector<Child> g_children;

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
        if (c.data_dir.empty()) continue;
        std::error_code ec;
        std::filesystem::remove_all(c.data_dir, ec);
        if (!c.conf_path.empty()) {
            std::filesystem::remove(c.conf_path, ec);
        }
    }
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

[[noreturn]] void Fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    KillAllChildren();
    CleanupDataDirs();
    std::_Exit(1);
}

// Locate kv_cluster_main alongside the current executable.
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

// Write a flat key=value config file for node |node_id| into |conf_path|.
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
        // Child: redirect stdout/stderr to per-node log file.
        std::string log_path = data_dir + ".log";
        int fd = ::open(log_path.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC, 0644);
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
        std::cerr << "execv(" << bin << ") failed: "
                  << std::strerror(errno) << "\n";
        std::_Exit(127);
    }
    return pid;
}

}  // namespace

int main(int argc, char** argv) {
    raft::logging::Init();
    raft::logging::SetCurrentThreadName("cluster-test");

    InstallSignalHandlers();

    std::string bin = LocateServerBinary(argv[0]);
    if (!std::filesystem::exists(bin)) {
        std::cerr << "kv_cluster_main not found at " << bin << "\n";
        return 1;
    }
    std::cout << "using server binary: " << bin << "\n";

    // Prepare data dirs, write per-node config files, and spawn children.
    g_children.resize(kN);
    for (int i = 0; i < kN; ++i) {
        std::string dir       = "/tmp/raft_cluster_test_node_" + std::to_string(i);
        std::string conf_path = "/tmp/raft_cluster_test_node_" + std::to_string(i) + ".conf";

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
                  << " port=" << (kBasePort + i)
                  << " data_dir=" << dir << "\n";
    }

    // Build the client peer list.
    std::vector<raft_core::PeerInfo> peers;
    for (int i = 0; i < kN; ++i) {
        peers.push_back({static_cast<raft_core::NodeId>(i),
                         "127.0.0.1",
                         kBasePort + i});
    }

    // Give the servers a moment to bind their listening sockets.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    kvclient::KvClientOptions opts;
    opts.max_retries     = 60;
    opts.rpc_timeout_ms  = 1500;
    opts.connect_wait_ms = 3000;
    opts.retry_interval  = std::chrono::milliseconds(300);

    kvclient::KvClient client(peers, 0, opts);

    // -------- Election check: keep retrying Put until success or timeout.
    std::cout << "waiting for leader election + first commit...\n";
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(25);
    bool elected  = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = client.Put("__probe__", "1");
        if (r.ok) {
            std::cout << "first Put succeeded (leader elected)\n";
            elected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    if (!elected) Fail("no leader / cluster not responsive within 25s");

    constexpr int kBatch = 10;
    for (int i = 0; i < kBatch; ++i) {
        std::string k = "key-" + std::to_string(i);
        std::string v = "val-" + std::to_string(i);
        auto r = client.Put(k, v);
        if (!r.ok) Fail("Put " + k + " failed: " + r.error);
    }
    std::cout << "Put x " << kBatch << " ok\n";

    for (int i = 0; i < kBatch; ++i) {
        std::string k    = "key-" + std::to_string(i);
        std::string want = "val-" + std::to_string(i);
        auto r = client.Get(k);
        if (!r.ok) Fail("Get " + k + " failed: " + r.error);
        if (r.value != want) {
            Fail("Get " + k + " got '" + r.value + "', want '" + want + "'");
        }
    }
    std::cout << "Get x " << kBatch << " ok (values match)\n";

    // Overwrite + read-after-write.
    {
        auto r = client.Put("key-0", "updated");
        if (!r.ok) Fail("overwrite Put failed: " + r.error);
        auto g = client.Get("key-0");
        if (!g.ok || g.value != "updated") {
            Fail("read-after-write mismatch: ok=" + std::to_string(g.ok) +
                 " value='" + g.value + "'");
        }
    }
    std::cout << "overwrite Put + Get ok\n";

    // Delete + verify miss.
    {
        auto r = client.Delete("key-1");
        if (!r.ok) Fail("Delete failed: " + r.error);
        auto g = client.Get("key-1");
        if (g.ok) Fail("Get after Delete unexpectedly succeeded");
    }
    std::cout << "Delete + Get-after-delete ok\n";

    std::cout << "PASS: raft cluster (3 processes) election + replication\n";

    KillAllChildren();
    CleanupDataDirs();
    return 0;
}
