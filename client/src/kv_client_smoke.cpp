// Smoke-test / manual integration driver for KvClient.
//
// Usage (requires a running kv_server_main cluster):
//   kv_client_smoke --peers '0@127.0.0.1:8000,1@127.0.0.1:8001,2@127.0.0.1:8002'
//                   [--seed_id 0]
//
// The binary performs a Put, Get, and Delete against the cluster and prints
// PASS or FAIL.  It exercises the full retry / leader-redirect path.

#include "kv_client.h"
#include "log.h"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void PrintUsage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " --peers '0@host:port,...'"
              << " [--seed_id 0]"
              << " [--max_retries 5]"
              << " [--rpc_timeout_ms 3000]\n";
}

bool NextArg(int argc, char** argv, int* i, std::string* out) {
    if (*i + 1 >= argc) return false;
    *out = argv[++(*i)];
    return true;
}

bool ParsePeer(const std::string& spec, raft_core::PeerInfo* out) {
    size_t sep = spec.find('@');
    if (sep == std::string::npos) return false;
    size_t colon = spec.find(':', sep + 1);
    if (colon == std::string::npos) return false;
    out->id   = std::stoll(spec.substr(0, sep));
    out->ip   = spec.substr(sep + 1, colon - sep - 1);
    out->port = std::stoi(spec.substr(colon + 1));
    return !out->ip.empty();
}

std::vector<raft_core::PeerInfo> ParsePeers(const std::string& arg) {
    std::vector<raft_core::PeerInfo> peers;
    std::stringstream ss(arg);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        raft_core::PeerInfo p;
        if (!ParsePeer(item, &p)) {
            throw std::runtime_error("invalid peer spec: " + item);
        }
        peers.push_back(std::move(p));
    }
    return peers;
}

void Fail(const std::string& msg) {
    std::cerr << "FAIL: " << msg << "\n";
    std::exit(1);
}

}  // namespace

int main(int argc, char** argv) {
    raft::logging::Init();
    raft::logging::SetCurrentThreadName("kv-client-smoke");

    std::string peers_arg;
    raft_core::NodeId seed_id  = 0;
    int max_retries     = 5;
    int rpc_timeout_ms  = 3000;

    try {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            std::string val;
            if ((arg == "--peers") && NextArg(argc, argv, &i, &val)) {
                peers_arg = val;
            } else if ((arg == "--seed_id") && NextArg(argc, argv, &i, &val)) {
                seed_id = std::stoll(val);
            } else if ((arg == "--max_retries") && NextArg(argc, argv, &i, &val)) {
                max_retries = std::stoi(val);
            } else if ((arg == "--rpc_timeout_ms") && NextArg(argc, argv, &i, &val)) {
                rpc_timeout_ms = std::stoi(val);
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage(argv[0]);
                return 0;
            } else {
                PrintUsage(argv[0]);
                return 1;
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "argument error: " << ex.what() << "\n";
        PrintUsage(argv[0]);
        return 1;
    }

    if (peers_arg.empty()) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::vector<raft_core::PeerInfo> peers;
    try {
        peers = ParsePeers(peers_arg);
    } catch (const std::exception& ex) {
        std::cerr << "parse peers: " << ex.what() << "\n";
        return 1;
    }
    if (peers.empty()) {
        std::cerr << "no peers parsed\n";
        return 1;
    }

    kvclient::KvClientOptions opts;
    opts.max_retries    = max_retries;
    opts.rpc_timeout_ms = rpc_timeout_ms;

    std::cout << "client_id: ";
    kvclient::KvClient client(peers, seed_id, opts);
    std::cout << client.ClientId() << "\n";

    // --- Put ---
    {
        auto r = client.Put("smoke-key", "hello-world");
        if (!r.ok) {
            Fail("Put failed: " + r.error);
        }
        std::cout << "Put(smoke-key, hello-world) -> ok\n";
    }

    // --- Get ---
    {
        auto r = client.Get("smoke-key");
        if (!r.ok) {
            Fail("Get failed: " + r.error);
        }
        if (r.value != "hello-world") {
            Fail("Get returned unexpected value: " + r.value);
        }
        std::cout << "Get(smoke-key) -> " << r.value << "\n";
    }

    // --- Idempotent Put (same key, new value) ---
    {
        auto r = client.Put("smoke-key", "updated");
        if (!r.ok) {
            Fail("second Put failed: " + r.error);
        }
        std::cout << "Put(smoke-key, updated) -> ok\n";
    }

    // --- Delete ---
    {
        auto r = client.Delete("smoke-key");
        if (!r.ok) {
            Fail("Delete failed: " + r.error);
        }
        std::cout << "Delete(smoke-key) -> ok\n";
    }

    // --- Get after delete should fail ---
    {
        auto r = client.Get("smoke-key");
        if (r.ok) {
            Fail("Get after Delete unexpectedly succeeded");
        }
        std::cout << "Get(smoke-key) after Delete -> not found (expected)\n";
    }

    std::cout << "PASS: kv client smoke\n";
    return 0;
}
