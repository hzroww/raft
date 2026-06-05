// 4-node Raft 2 vs 2 network partition integration test.
//
// Verifies the following properties:
//   1. A healthy 4-node cluster commits writes normally.
//   2. When nodes {0,1} are partitioned from nodes {2,3}, neither half can
//      form a 3/4 majority, so all write attempts fail or time out.
//   3. After healing the partition the cluster re-elects a leader and
//      continues to commit writes.
//
// Partition mechanism:
//   Each server process is launched with two extra environment variables:
//     RAFT_TEST_NODE_ID=<id>
//     RAFT_TEST_PARTITION_FILE=<path>
//   RpcChannel::CallMethod() consults rpc_test_partition::IsPartitioned()
//   before sending.  When a (src_node_id, dst_port) rule exists in the
//   partition file the RPC is failed immediately with "test partition",
//   simulating a one-way link drop.  All eight cross-partition directed
//   edges are installed to make the split symmetric.
//
//   The test process writes / truncates the partition file at runtime;
//   server processes re-read it on every outgoing RPC call.

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

constexpr int kBasePort = 19501;
constexpr int kN        = 4;

struct Child {
    pid_t       pid = -1;
    std::string data_dir;
    std::string conf_path;
};

std::vector<Child> g_children;
std::string        g_partition_file;

// ---------------------------------------------------------------------------
// Process lifecycle
// ---------------------------------------------------------------------------

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
        if (c.pid > 0) ::kill(c.pid, SIGTERM);
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (auto& c : g_children) {
        if (c.pid <= 0) continue;
        while (std::chrono::steady_clock::now() < deadline) {
            int   status = 0;
            pid_t r      = ::waitpid(c.pid, &status, WNOHANG);
            if (r == c.pid || r < 0) { c.pid = -1; break; }
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
        if (!c.data_dir.empty())  std::filesystem::remove_all(c.data_dir, ec);
        if (!c.conf_path.empty()) std::filesystem::remove(c.conf_path, ec);
        if (!c.data_dir.empty())  std::filesystem::remove(c.data_dir + ".log", ec);
    }
    if (!g_partition_file.empty()) {
        std::error_code ec;
        std::filesystem::remove(g_partition_file, ec);
    }
}

[[noreturn]] void Fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << "\n";
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
    try { self = std::filesystem::canonical("/proc/self/exe"); }
    catch (...) { self = argv0; }
    auto candidate = self.parent_path() / "kv_cluster_main";
    if (std::filesystem::exists(candidate)) return candidate.string();
    candidate = std::filesystem::path(argv0).parent_path() / "kv_cluster_main";
    return candidate.string();
}

void WriteNodeConfig(const std::string& conf_path,
                     int node_id,
                     const std::string& data_dir) {
    std::ofstream f(conf_path);
    if (!f.is_open()) Fail("cannot write config: " + conf_path);
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

// Spawn a kv_cluster_main child.  |partition_file| and |node_id_env| are
// passed via environment variables so that each child's RpcChannel can
// consult the partition rules on every outgoing call.
pid_t SpawnNode(const std::string& bin,
                const std::string& conf_path,
                const std::string& data_dir,
                int                node_id,
                const std::string& partition_file) {
    pid_t pid = ::fork();
    if (pid < 0) Fail(std::string("fork: ") + std::strerror(errno));
    if (pid == 0) {
        // Set partition hook environment variables.
        std::string nid_env  = "RAFT_TEST_NODE_ID=" + std::to_string(node_id);
        std::string file_env = "RAFT_TEST_PARTITION_FILE=" + partition_file;
        ::putenv(const_cast<char*>(nid_env.c_str()));
        ::putenv(const_cast<char*>(file_env.c_str()));

        // Redirect stdout/stderr to per-node log file.
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
        std::cerr << "execv(" << bin << ") failed: " << std::strerror(errno) << "\n";
        std::_Exit(127);
    }
    return pid;
}

// ---------------------------------------------------------------------------
// Partition control
// ---------------------------------------------------------------------------

// Write rules for a symmetric 2 vs 2 split: {group_a} cannot talk to
// {group_b} and vice versa.
void EnablePartition(const std::vector<int>& group_a,
                     const std::vector<int>& group_b) {
    std::ofstream f(g_partition_file, std::ios::trunc);
    if (!f.is_open()) Fail("cannot write partition file: " + g_partition_file);
    // Block all directed edges between the two groups (both directions).
    for (int a : group_a) {
        for (int b : group_b) {
            f << a << " " << (kBasePort + b) << "\n";
            f << b << " " << (kBasePort + a) << "\n";
        }
    }
    f.flush();
}

// Remove all partition rules.
void HealPartition() {
    std::ofstream f(g_partition_file, std::ios::trunc);
    // Empty file = no rules = no partition.
}

// ---------------------------------------------------------------------------
// Cluster status helpers
// ---------------------------------------------------------------------------

std::optional<kv::GetNodeStatusResponse> DirectGetStatus(int node_id) {
    RpcChannel channel("127.0.0.1", kBasePort + node_id);
    channel.setTimeoutMs(800);
    if (!channel.waitUntilConnected(800)) return std::nullopt;

    kv::KvServerRpc_Stub       stub(&channel);
    RpcController              ctl;
    SyncDone                   done;
    kv::GetNodeStatusRequest   request;
    kv::GetNodeStatusResponse  response;

    stub.GetNodeStatus(&ctl, &request, &response, &done);
    done.Wait();
    if (ctl.Failed() || !response.success()) return std::nullopt;
    return response;
}

void PrintClusterStatus(const std::string& label) {
    std::cout << "\n" << label << "\n";
    std::cout << "----------------------------------------------------------------\n";
    for (int id = 0; id < kN; ++id) {
        if (g_children[id].pid <= 0) {
            std::cout << "  node=" << id << " [DOWN]\n";
            continue;
        }
        auto st = DirectGetStatus(id);
        if (!st) { std::cout << "  node=" << id << " [UNREACHABLE]\n"; continue; }
        std::cout << "  node=" << id
                  << " state="  << st->state()
                  << " term="   << st->currentterm()
                  << " leader=" << st->leaderid()
                  << " commit=" << st->commitindex()
                  << " last_log=" << st->lastlogindex()
                  << "(t" << st->lastlogterm() << ")\n";
    }
    std::cout << "----------------------------------------------------------------\n";
}

// Poll every alive node until one reports itself as Leader.
std::optional<int> WaitForLeader(std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int id = 0; id < kN; ++id) {
            if (g_children[id].pid <= 0) continue;
            auto st = DirectGetStatus(id);
            if (st && st->state() == "Leader" && st->leaderid() == id)
                return id;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return std::nullopt;
}

// Block until client.Put() succeeds.
void WaitUntilPutSucceeds(kvclient::KvClient*  client,
                           const std::string&   key,
                           const std::string&   value,
                           std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string last_error;
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = client->Put(key, value);
        if (r.ok) return;
        last_error = r.error;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    Fail("Put '" + key + "' did not succeed before timeout, last error: " + last_error);
}

void ExpectGet(kvclient::KvClient*  client,
               const std::string&   key,
               const std::string&   want) {
    auto r = client->Get(key);
    if (!r.ok)        Fail("Get '" + key + "' failed: " + r.error);
    if (r.value != want)
        Fail("Get '" + key + "' got '" + r.value + "', want '" + want + "'");
}

// Perform a single synchronous Put directly against one node's RPC port.
// Returns the error string (empty = success).
std::string DirectPut(int node_id, const std::string& key,
                      const std::string& value, int64_t request_id) {
    RpcChannel channel("127.0.0.1", kBasePort + node_id);
    channel.setTimeoutMs(2000);
    channel.waitUntilConnected(500);

    kv::KvServerRpc_Stub stub(&channel);
    RpcController        ctl;
    SyncDone             done;
    kv::PutRequest       req;
    kv::PutResponse      resp;

    req.set_key(key);
    req.set_value(value);
    req.set_clientid("partition-probe");
    req.set_requestid(request_id);

    stub.Put(&ctl, &req, &resp, &done);
    done.Wait();

    if (ctl.Failed())    return "rpc failed: " + ctl.ErrorText();
    if (resp.success())  return "";
    return resp.error();
}

}  // namespace

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    raft::logging::Init();
    muduo::Logger::setOutput([](const char*, int){});
    muduo::Logger::setFlush([](){});
    raft::logging::SetCurrentThreadName("partition-test");
    InstallSignalHandlers();

    std::string bin = LocateServerBinary(argv[0]);
    if (!std::filesystem::exists(bin)) {
        std::cerr << "kv_cluster_main not found at " << bin << "\n";
        return 1;
    }
    std::cout << "using server binary: " << bin << "\n";

    std::string run_prefix = "/tmp/raft_network_partition_test_" +
                             std::to_string(static_cast<long long>(::getpid()));
    g_partition_file = run_prefix + ".partition";

    // Start with an empty (inactive) partition file.
    { std::ofstream f(g_partition_file, std::ios::trunc); }

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
        g_children[i].pid       = SpawnNode(bin, conf_path, dir, i, g_partition_file);
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

    std::vector<raft_core::PeerInfo> peers;
    for (int i = 0; i < kN; ++i) {
        peers.push_back({static_cast<raft_core::NodeId>(i), "127.0.0.1", kBasePort + i});
    }
    kvclient::KvClient client(peers, 0, opts);

    // -----------------------------------------------------------------------
    // Phase 1: bring up the cluster and commit a baseline key.
    // -----------------------------------------------------------------------
    std::cout << "\n[Phase 1] waiting for leader and committing baseline...\n";
    WaitUntilPutSucceeds(&client, "partition-baseline", "before",
                         std::chrono::seconds(30));

    auto leader_before = WaitForLeader(std::chrono::seconds(10));
    if (!leader_before) Fail("could not identify initial leader");
    std::cout << "initial leader: node " << *leader_before << "\n";

    PrintClusterStatus("=== Phase 1: cluster healthy ===");

    // -----------------------------------------------------------------------
    // Phase 2: enable 2v2 partition {0,1} vs {2,3}.
    // -----------------------------------------------------------------------
    std::cout << "\n[Phase 2] enabling 2v2 partition: {0,1} | {2,3}...\n";
    EnablePartition({0, 1}, {2, 3});

    // Wait long enough for in-flight RPCs to time out and for at least two
    // election timeout cycles to pass.  The RPC timeout in the server config
    // is 1 s and election_timeout_max is 300 ms, so 3 s is ample.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    PrintClusterStatus("=== Phase 2: during partition ===");

    // Attempt a write to every node.  None should succeed because neither
    // half has a 3/4 majority.
    std::cout << "\n[Phase 2] probing all nodes during partition...\n";
    int64_t req_id = 100;
    bool any_succeeded = false;
    for (int id = 0; id < kN; ++id) {
        std::string err = DirectPut(id, "partition-probe-" + std::to_string(id),
                                    "should-not-commit", req_id++);
        bool ok = err.empty();
        std::cout << "  DirectPut node=" << id
                  << " result=" << (ok ? "SUCCESS (unexpected!)" : ("FAIL: " + err))
                  << "\n";
        if (ok) any_succeeded = true;
    }
    if (any_succeeded) {
        Fail("a write succeeded during 2v2 partition — quorum safety violated");
    }
    std::cout << "  [OK] all writes failed during partition (expected)\n";

    // -----------------------------------------------------------------------
    // Phase 3: heal the partition and wait for recovery.
    // -----------------------------------------------------------------------
    std::cout << "\n[Phase 3] healing partition...\n";
    HealPartition();

    // Give the cluster time to re-elect and commit the post-heal write.
    WaitUntilPutSucceeds(&client, "post-heal", "ok", std::chrono::seconds(40));

    auto leader_after = WaitForLeader(std::chrono::seconds(10));
    if (!leader_after) Fail("no leader after partition heal");
    std::cout << "post-heal leader: node " << *leader_after << "\n";

    PrintClusterStatus("=== Phase 3: after partition healed ===");

    // -----------------------------------------------------------------------
    // Phase 4: consistency check.
    // -----------------------------------------------------------------------
    std::cout << "\n[Phase 4] verifying data consistency...\n";
    ExpectGet(&client, "partition-baseline", "before");
    ExpectGet(&client, "post-heal", "ok");

    std::cout << "\nPASS: 4-node cluster recovered from 2v2 network partition\n";

    KillAllChildren();
    CleanupDataDirs();
    return 0;
}
