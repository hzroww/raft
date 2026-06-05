#include "kv_server_service.h"

#include "kv_command_codec.h"

#include <google/protobuf/service.h>

#include <algorithm>
#include <future>
#include <utility>

namespace kvserver {

namespace {

void Finish(google::protobuf::Closure* done) {
    if (done) {
        done->Run();
    }
}

template <typename Response>
void RejectNotLeader(Response* response, int32_t leader_id) {
    response->set_success(false);
    response->set_error("not leader");
    response->set_leaderid(leader_id);
}

}  // namespace

KvServerService::KvServerService(KvStore* store,
                                 raft_core::RaftNode* raft_node,
                                 CommitWaitRegistry* wait_registry,
                                 raft_core::IRaftStorage* storage,
                                 std::chrono::milliseconds commit_timeout)
    : store_(store),
      raft_node_(raft_node),
      wait_registry_(wait_registry),
      storage_(storage),
      commit_timeout_(commit_timeout) {}

void KvServerService::Put(google::protobuf::RpcController* /*controller*/,
                          const kv::PutRequest*            request,
                          kv::PutResponse*                 response,
                          google::protobuf::Closure*       done) {
    response->set_leaderid(LeaderIdForResponse());

    std::string error;
    if (!request || !ValidateRequest(request->key(),
                                     request->clientid(),
                                     request->requestid(),
                                     &error)) {
        response->set_success(false);
        response->set_error(error.empty() ? "invalid put request" : error);
        Finish(done);
        return;
    }

    if (!IsLeader()) {
        RejectNotLeader(response, LeaderIdForResponse());
        Finish(done);
        return;
    }

    raft_core::Index index = 0;
    kv::KvCommand command = MakePutCommand(request->key(),
                                           request->value(),
                                           request->clientid(),
                                           request->requestid());
    if (!raft_node_->Propose(EncodeCommand(command), &index)) {
        RejectNotLeader(response, LeaderIdForResponse());
        Finish(done);
        return;
    }

    auto future = wait_registry_->FutureFor(index);
    if (future.wait_for(commit_timeout_) != std::future_status::ready) {
        response->set_success(false);
        response->set_error("commit timeout");
        response->set_leaderid(LeaderIdForResponse());
        Finish(done);
        return;
    }

    KvResult result = future.get();
    response->set_success(result.success);
    response->set_error(result.error);
    response->set_leaderid(LeaderIdForResponse());
    Finish(done);
}

void KvServerService::Get(google::protobuf::RpcController* /*controller*/,
                          const kv::GetRequest*            request,
                          kv::GetResponse*                 response,
                          google::protobuf::Closure*       done) {
    response->set_leaderid(LeaderIdForResponse());

    std::string error;
    if (!request || !ValidateRequest(request->key(),
                                     request->clientid(),
                                     request->requestid(),
                                     &error)) {
        response->set_success(false);
        response->set_error(error.empty() ? "invalid get request" : error);
        Finish(done);
        return;
    }

    if (!IsLeader()) {
        RejectNotLeader(response, LeaderIdForResponse());
        Finish(done);
        return;
    }

    KvResult result = store_->Get(request->key());
    response->set_success(result.success);
    response->set_value(result.value);
    response->set_error(result.error);
    response->set_leaderid(LeaderIdForResponse());
    Finish(done);
}

void KvServerService::Delete(google::protobuf::RpcController* /*controller*/,
                             const kv::DeleteRequest*         request,
                             kv::DeleteResponse*              response,
                             google::protobuf::Closure*       done) {
    response->set_leaderid(LeaderIdForResponse());

    std::string error;
    if (!request || !ValidateRequest(request->key(),
                                     request->clientid(),
                                     request->requestid(),
                                     &error)) {
        response->set_success(false);
        response->set_error(error.empty() ? "invalid delete request" : error);
        Finish(done);
        return;
    }

    if (!IsLeader()) {
        RejectNotLeader(response, LeaderIdForResponse());
        Finish(done);
        return;
    }

    raft_core::Index index = 0;
    kv::KvCommand command = MakeDeleteCommand(request->key(),
                                              request->clientid(),
                                              request->requestid());
    if (!raft_node_->Propose(EncodeCommand(command), &index)) {
        RejectNotLeader(response, LeaderIdForResponse());
        Finish(done);
        return;
    }

    auto future = wait_registry_->FutureFor(index);
    if (future.wait_for(commit_timeout_) != std::future_status::ready) {
        response->set_success(false);
        response->set_error("commit timeout");
        response->set_leaderid(LeaderIdForResponse());
        Finish(done);
        return;
    }

    KvResult result = future.get();
    response->set_success(result.success);
    response->set_error(result.error);
    response->set_leaderid(LeaderIdForResponse());
    Finish(done);
}

void KvServerService::GetNodeStatus(
    google::protobuf::RpcController*    /*controller*/,
    const kv::GetNodeStatusRequest*    /*request*/,
    kv::GetNodeStatusResponse*         response,
    google::protobuf::Closure*         done) {
    if (!raft_node_) {
        response->set_success(false);
        response->set_error("raft node is not configured");
        Finish(done);
        return;
    }

    response->set_nodeid(static_cast<int32_t>(raft_node_->SelfId()));
    response->set_state(raft_core::ToString(raft_node_->State()));
    response->set_currentterm(raft_node_->CurrentTerm());
    response->set_leaderid(static_cast<int32_t>(raft_node_->LeaderId()));
    response->set_commitindex(raft_node_->CommitIndex());
    response->set_lastapplied(raft_node_->LastApplied());

    if (!storage_) {
        response->set_success(true);
        Finish(done);
        return;
    }

    // Reading the persistent log must happen on the raft main thread, since
    // IRaftStorage is only safe to touch from there. Hop onto it via Post()
    // and block until the snapshot of recent entries is collected.
    constexpr raft_core::Index kMaxEntries = 50;
    std::promise<void> ready;
    std::future<void>  ready_future = ready.get_future();

    raft_node_->Post([this, response, kMaxEntries, &ready]() {
        raft_core::Index last_index = 0;
        raft_core::Term  last_term  = 0;
        storage_->LastIndexTerm(&last_index, &last_term);
        response->set_lastlogindex(last_index);
        response->set_lastlogterm(last_term);

        raft_core::Index start = std::max<raft_core::Index>(1, last_index - kMaxEntries + 1);
        for (raft_core::Index i = start; i <= last_index; ++i) {
            raft_core::LogEntry entry;
            if (!storage_->EntryAt(i, &entry)) {
                continue;
            }
            kv::LogEntryInfo* info = response->add_entries();
            info->set_index(entry.index);
            info->set_term(entry.term);

            if (entry.type == raft_core::EntryType::Config) {
                info->set_op("CONFIG");
            } else if (entry.command.empty()) {
                info->set_op("NOOP");
            } else {
                kv::KvCommand command;
                if (DecodeCommand(entry.command, &command)) {
                    info->set_op(command.op() == kv::KvCommand::PUT ? "PUT" : "DELETE");
                    info->set_key(command.key());
                    info->set_value(command.value());
                } else {
                    info->set_op("UNKNOWN");
                }
            }
        }
        ready.set_value();
    });

    ready_future.wait();
    response->set_success(true);
    Finish(done);
}

bool KvServerService::ValidateRequest(const std::string& key,
                                      const std::string& client_id,
                                      int64_t            request_id,
                                      std::string*       error) const {
    if (!store_) {
        if (error) *error = "kv store is not configured";
        return false;
    }
    if (!raft_node_) {
        if (error) *error = "raft node is not configured";
        return false;
    }
    if (!wait_registry_) {
        if (error) *error = "commit wait registry is not configured";
        return false;
    }
    if (key.empty()) {
        if (error) *error = "key must not be empty";
        return false;
    }
    if (client_id.empty()) {
        if (error) *error = "clientId must not be empty";
        return false;
    }
    if (request_id <= 0) {
        if (error) *error = "requestId must be positive";
        return false;
    }
    return true;
}

bool KvServerService::IsLeader() const {
    return raft_node_ && raft_node_->State() == raft_core::RaftState::Leader;
}

int32_t KvServerService::LeaderIdForResponse() const {
    if (!raft_node_) {
        return static_cast<int32_t>(raft_core::kNoNode);
    }
    raft_core::NodeId leader = raft_node_->LeaderId();
    return static_cast<int32_t>(leader);
}

}  // namespace kvserver
