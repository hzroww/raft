#include "kv_raft_server.h"

#include "log.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace kvserver {

namespace {

std::string Trim(const std::string& s) {
    const char* ws = " \t\r\n";
    size_t start = s.find_first_not_of(ws);
    if (start == std::string::npos) return {};
    size_t end = s.find_last_not_of(ws);
    return s.substr(start, end - start + 1);
}

std::unordered_map<std::string, std::string> ParseFlatConfig(
    const std::string& path) {
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        throw std::runtime_error("KvRaftServer: cannot open config file: " + path);
    }

    std::unordered_map<std::string, std::string> kv;
    std::string line;
    while (std::getline(ifs, line)) {
        auto comment = line.find('#');
        if (comment != std::string::npos) {
            line = line.substr(0, comment);
        }
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = Trim(line.substr(0, eq));
        std::string val = Trim(line.substr(eq + 1));
        if (!key.empty()) {
            kv[key] = val;
        }
    }
    return kv;
}

std::string GetStr(const std::unordered_map<std::string, std::string>& kv,
                   const std::string& key,
                   const std::string& def = "") {
    auto it = kv.find(key);
    return it != kv.end() ? it->second : def;
}

int GetInt(const std::unordered_map<std::string, std::string>& kv,
           const std::string& key,
           int def = 0) {
    auto it = kv.find(key);
    if (it == kv.end() || it->second.empty()) return def;
    return std::stoi(it->second);
}

uint64_t GetU64(const std::unordered_map<std::string, std::string>& kv,
                const std::string& key,
                uint64_t def = 0) {
    auto it = kv.find(key);
    if (it == kv.end() || it->second.empty()) return def;
    return static_cast<uint64_t>(std::stoull(it->second));
}

}  // namespace

// static
KvRaftServer::Config KvRaftServer::LoadConfigFromFile(const std::string& path,
                                                       int32_t node_id_override) {
    auto kv = ParseFlatConfig(path);

    Config cfg;

    cfg.node_id = static_cast<int32_t>(GetInt(kv, "node_id", 0));
    if (node_id_override >= 0) {
        cfg.node_id = node_id_override;
    }

    // Accept both rpc.conf-style keys and more explicit names.
    cfg.listen_ip = GetStr(kv, "listen_ip",
                           GetStr(kv, "rpc_server_ip", "0.0.0.0"));
    cfg.listen_port = GetInt(kv, "listen_port",
                             GetInt(kv, "rpc_server_port", 8000));

    cfg.data_dir = GetStr(kv, "data_dir");
    if (cfg.data_dir.empty()) {
        cfg.data_dir = "data/node_" + std::to_string(cfg.node_id);
    }

    cfg.io_threads        = GetInt(kv, "io_threads", 1);
    cfg.worker_threads    = static_cast<size_t>(GetInt(kv, "worker_threads", 4));
    cfg.snapshot_threshold = GetU64(kv, "snapshot_threshold", 1000);

    cfg.election_timeout_min_ms = GetInt(kv, "election_timeout_min_ms", 150);
    cfg.election_timeout_max_ms = GetInt(kv, "election_timeout_max_ms", 300);
    cfg.heartbeat_interval_ms   = GetInt(kv, "heartbeat_interval_ms",   50);

    // Peer topology: node_count + node_<i>_ip / node_<i>_port (rpc.conf style).
    int node_count = GetInt(kv, "node_count", 0);
    for (int i = 0; i < node_count; ++i) {
        if (i == cfg.node_id) continue;
        std::string ip   = GetStr(kv, "node_" + std::to_string(i) + "_ip",
                                  "127.0.0.1");
        int         port = GetInt(kv, "node_" + std::to_string(i) + "_port", 0);
        if (port == 0) continue;
        raft_core::PeerInfo peer;
        peer.id   = static_cast<raft_core::NodeId>(i);
        peer.ip   = std::move(ip);
        peer.port = port;
        cfg.peers.push_back(std::move(peer));
    }

    return cfg;
}

KvRaftServer::KvRaftServer(Config cfg)
    : cfg_(std::move(cfg)),
      storage_(cfg_.data_dir) {
    apply_sink_ = std::make_unique<KvApplySink>(
        &storage_, &store_, &wait_registry_,
        /*raft_node=*/nullptr,
        cfg_.snapshot_threshold);

    service_ = std::make_unique<KvServerService>(
        &store_, /*raft_node=*/nullptr, &wait_registry_);

    raft_core::RaftNode::Config rcfg;
    rcfg.self_id         = cfg_.node_id;
    rcfg.peers           = cfg_.peers;
    rcfg.listen_ip       = cfg_.listen_ip;
    rcfg.listen_port     = cfg_.listen_port;
    rcfg.io_threads      = cfg_.io_threads;
    rcfg.worker_threads  = cfg_.worker_threads;
    rcfg.election_timeout_min_ms = cfg_.election_timeout_min_ms;
    rcfg.election_timeout_max_ms = cfg_.election_timeout_max_ms;
    rcfg.heartbeat_interval_ms   = cfg_.heartbeat_interval_ms;
    rcfg.extra_services.push_back(service_.get());

    raft_node_ = std::make_unique<raft_core::RaftNode>(
        std::move(rcfg), &storage_, apply_sink_.get());

    apply_sink_->SetRaftNode(raft_node_.get());
    service_->SetRaftNode(raft_node_.get());

    LOG_INFO() << "KvRaftServer created"
               << " node_id=" << cfg_.node_id
               << " listen=" << cfg_.listen_ip << ":" << cfg_.listen_port
               << " data_dir=" << cfg_.data_dir
               << " peers=" << cfg_.peers.size()
               << " io_threads=" << cfg_.io_threads
               << " worker_threads=" << cfg_.worker_threads
               << " snapshot_threshold=" << cfg_.snapshot_threshold;
}

KvRaftServer::~KvRaftServer() {
    if (raft_node_) {
        raft_node_->Stop();
    }
}

void KvRaftServer::Start() {
    raft_node_->Start();
}

void KvRaftServer::Stop() {
    raft_node_->Stop();
}

void KvRaftServer::WaitForShutdown() {
    raft_node_->WaitForShutdown();
}

}  // namespace kvserver
