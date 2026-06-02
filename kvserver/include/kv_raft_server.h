#pragma once

#include "commit_wait_registry.h"
#include "file_raft_storage.h"
#include "kv_raft_apply_sink.h"
#include "kv_server_service.h"
#include "kv_store.h"
#include "raft_node.h"
#include "raft_types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace kvserver {

class KvRaftServer {
public:
    struct Config {
        int32_t     node_id             = 0;
        std::string listen_ip           = "0.0.0.0";
        int         listen_port         = 8000;
        std::string data_dir;
        std::vector<raft_core::PeerInfo> peers;
        int         io_threads          = 1;
        size_t      worker_threads      = 4;
        uint64_t    snapshot_threshold  = 1000;
        int         election_timeout_min_ms = 150;
        int         election_timeout_max_ms = 300;
        int         heartbeat_interval_ms   = 50;
    };

    // Parse a flat key=value config file.
    // node_id_override >= 0 overrides the node_id read from the file, which
    // lets a single config file serve all nodes in the cluster when started
    // with --node_id <n>.
    static Config LoadConfigFromFile(const std::string& path,
                                     int32_t node_id_override = -1);

    explicit KvRaftServer(Config cfg);
    ~KvRaftServer();

    KvRaftServer(const KvRaftServer&)            = delete;
    KvRaftServer& operator=(const KvRaftServer&) = delete;

    void Start();
    void Stop();
    void WaitForShutdown();

    raft_core::RaftNode& Raft()  { return *raft_node_; }
    KvStore&             Store() { return store_; }

private:
    // cfg_ must be first so its fields are ready when storage_ is initialised.
    Config cfg_;

    FileRaftStorage    storage_;
    KvStore            store_;
    CommitWaitRegistry wait_registry_;

    // These three are heap-allocated so we can control construction order and
    // perform the mandatory SetRaftNode() back-fill inside the constructor body.
    std::unique_ptr<KvApplySink>     apply_sink_;
    std::unique_ptr<KvServerService> service_;

    // Declared last: constructed last, destroyed first (before apply_sink_ and
    // service_ whose pointers RaftNode holds internally).
    std::unique_ptr<raft_core::RaftNode> raft_node_;
};

}  // namespace kvserver
