#include "commit_wait_registry.h"
#include "file_raft_storage.h"
#include "kv_raft_apply_sink.h"
#include "kv_server_service.h"
#include "log.h"
#include "raft_node.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

void PrintUsage(const char* program) {
    std::cerr << "Usage: " << program
              << " [--ip 0.0.0.0] [--port 8000] [--node_id 0]"
              << " [--data_dir data/node_0]"
              << " [--raft_peers 0@127.0.0.1:8000,1@127.0.0.1:8001]"
              << " [--io_threads 1] [--worker_threads 4]"
              << " [--snapshot_threshold 1000]\n";
}

bool NextArg(int argc, char** argv, int* index, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

bool ParsePeerSpec(const std::string& spec, raft_core::PeerInfo* out) {
    size_t sep = spec.find('@');
    if (sep == std::string::npos) {
        sep = spec.find('=');
    }
    if (sep == std::string::npos) {
        return false;
    }

    size_t colon = spec.find(':', sep + 1);
    if (colon == std::string::npos) {
        return false;
    }

    out->id = static_cast<raft_core::NodeId>(std::stoll(spec.substr(0, sep)));
    out->ip = spec.substr(sep + 1, colon - sep - 1);
    out->port = std::stoi(spec.substr(colon + 1));
    return !out->ip.empty();
}

std::vector<raft_core::PeerInfo> ParsePeers(const std::string& peers_arg,
                                            int32_t self_id) {
    std::vector<raft_core::PeerInfo> peers;
    if (peers_arg.empty()) {
        return peers;
    }

    std::stringstream input(peers_arg);
    std::string item;
    while (std::getline(input, item, ',')) {
        if (item.empty()) {
            continue;
        }
        raft_core::PeerInfo peer;
        if (!ParsePeerSpec(item, &peer)) {
            throw std::runtime_error("invalid --raft_peers entry: " + item);
        }
        if (peer.id != self_id) {
            peers.push_back(std::move(peer));
        }
    }
    return peers;
}

}  // namespace

int main(int argc, char** argv) {
    std::string ip = "0.0.0.0";
    int         port = 8000;
    int32_t     node_id = 0;
    int         io_threads = 1;
    int         worker_threads = 4;
    uint64_t    snapshot_threshold = 1000;
    std::string data_dir;
    std::string raft_peers;

    try {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            std::string value;
            if (arg == "--ip" && NextArg(argc, argv, &i, &value)) {
                ip = value;
            } else if (arg == "--port" && NextArg(argc, argv, &i, &value)) {
                port = std::stoi(value);
            } else if (arg == "--node_id" && NextArg(argc, argv, &i, &value)) {
                node_id = static_cast<int32_t>(std::stoi(value));
            } else if (arg == "--io_threads" && NextArg(argc, argv, &i, &value)) {
                io_threads = std::stoi(value);
            } else if (arg == "--worker_threads" && NextArg(argc, argv, &i, &value)) {
                worker_threads = std::stoi(value);
            } else if (arg == "--data_dir" && NextArg(argc, argv, &i, &value)) {
                data_dir = value;
            } else if (arg == "--raft_peers" && NextArg(argc, argv, &i, &value)) {
                raft_peers = value;
            } else if (arg == "--snapshot_threshold" && NextArg(argc, argv, &i, &value)) {
                snapshot_threshold = static_cast<uint64_t>(std::stoull(value));
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage(argv[0]);
                return 0;
            } else {
                PrintUsage(argv[0]);
                return 1;
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "invalid argument: " << ex.what() << "\n";
        PrintUsage(argv[0]);
        return 1;
    }

    raft::logging::Init();
    raft::logging::SetCurrentThreadName("kv-main");

    if (data_dir.empty()) {
        data_dir = "data/node_" + std::to_string(node_id);
    }

    auto peers = ParsePeers(raft_peers, node_id);

    kvserver::FileRaftStorage storage(data_dir);
    kvserver::KvStore store;
    kvserver::CommitWaitRegistry wait_registry;
    // RaftNode is constructed below; pass nullptr for now and back-fill via
    // SetRaftNode() after construction so that LoadPersistentLog's call to
    // OnSnapshotInstalled doesn't attempt to use an incomplete RaftNode.
    kvserver::KvApplySink apply_sink(&storage, &store, &wait_registry,
                                     /*raft_node=*/nullptr, snapshot_threshold);
    kvserver::KvServerService service(&store, nullptr, &wait_registry);

    raft_core::RaftNode::Config cfg;
    cfg.self_id = node_id;
    cfg.peers = std::move(peers);
    cfg.listen_ip = ip;
    cfg.listen_port = port;
    cfg.io_threads = io_threads;
    cfg.worker_threads = static_cast<size_t>(worker_threads);
    cfg.extra_services.push_back(&service);

    raft_core::RaftNode raft_node(std::move(cfg), &storage, &apply_sink);
    apply_sink.SetRaftNode(&raft_node);
    service.SetRaftNode(&raft_node);

    LOG_INFO() << "kv server starting node_id=" << node_id
               << " listen=" << ip << ":" << port
               << " data_dir=" << data_dir
               << " io_threads=" << io_threads
               << " worker_threads=" << worker_threads
               << " snapshot_threshold=" << snapshot_threshold;
    raft_node.Start();
    raft_node.WaitForShutdown();
    return 0;
}
