#include "commit_wait_registry.h"
#include "file_raft_storage.h"
#include "kv_raft_apply_sink.h"
#include "kv_server_service.h"
#include "log.h"
#include "raft_apply_iface.h"
#include "raft_node.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

namespace {

void Fail(const char* msg) {
    LOG_ERROR() << "FAIL: " << msg;
    std::cout << "FAIL: " << msg << std::endl;
    std::_Exit(1);
}

}  // namespace

int main() {
    raft::logging::Init();
    raft::logging::SetCurrentThreadName("kv-bridge-smoke");

    std::filesystem::path data_dir =
        std::filesystem::temp_directory_path() / "raft-kv-bridge-smoke";
    std::filesystem::remove_all(data_dir);

    kvserver::FileRaftStorage storage(data_dir.string());
    kvserver::KvStore store;
    kvserver::CommitWaitRegistry wait_registry;
    kvserver::KvApplySink apply_sink(&storage, &store, &wait_registry);

    raft_core::RaftNode::Config cfg;
    cfg.self_id = 1;
    cfg.listen_ip = "127.0.0.1";
    cfg.listen_port = 0;
    cfg.election_timeout_min_ms = 20;
    cfg.election_timeout_max_ms = 40;
    cfg.heartbeat_interval_ms = 10;
    cfg.start_rpc_server = false;

    raft_core::RaftNode node(std::move(cfg), &storage, &apply_sink);
    kvserver::KvServerService service(&store, &node, &wait_registry, 2s);

    kv::PutRequest rejected_put;
    rejected_put.set_key("before-start");
    rejected_put.set_value("value");
    rejected_put.set_clientid("client-a");
    rejected_put.set_requestid(1);
    kv::PutResponse rejected_put_resp;
    service.Put(nullptr, &rejected_put, &rejected_put_resp, nullptr);
    if (rejected_put_resp.success() ||
        rejected_put_resp.error() != "not leader" ||
        rejected_put_resp.leaderid() != raft_core::kNoNode) {
        Fail("expected pre-start put to be rejected as not leader");
    }

    node.Start();
    bool became_leader = false;
    for (int i = 0; i < 100; ++i) {
        if (node.State() == raft_core::RaftState::Leader) {
            became_leader = true;
            break;
        }
        std::this_thread::sleep_for(20ms);
    }
    if (!became_leader) {
        Fail("single-node raft did not become leader");
    }

    kv::PutRequest put;
    put.set_key("answer");
    put.set_value("42");
    put.set_clientid("client-a");
    put.set_requestid(2);
    kv::PutResponse put_resp;
    service.Put(nullptr, &put, &put_resp, nullptr);
    if (!put_resp.success() || put_resp.error() != "") {
        Fail("leader put did not commit successfully");
    }

    kv::GetRequest get;
    get.set_key("answer");
    get.set_clientid("client-a");
    get.set_requestid(3);
    kv::GetResponse get_resp;
    service.Get(nullptr, &get, &get_resp, nullptr);
    if (!get_resp.success() || get_resp.value() != "42") {
        Fail("leader get did not observe committed value");
    }

    raft_core::Index last_index = 0;
    raft_core::Term last_term = 0;
    storage.LastIndexTerm(&last_index, &last_term);
    if (last_index < 1 || last_term < 1) {
        Fail("persistent storage did not record raft log tail");
    }

    node.Stop();

    kvserver::FileRaftStorage restarted_storage(data_dir.string());
    raft_core::NullApplySink null_sink;
    raft_core::RaftNode restarted_node(
        raft_core::RaftNode::Config{.self_id = 1},
        &restarted_storage,
        &null_sink);

    std::filesystem::remove_all(data_dir);
    std::cout << "PASS: kv bridge smoke" << std::endl;
    return 0;
}
