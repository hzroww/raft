#include "kv_server_service.h"

#include "kv_command_codec.h"

#include <google/protobuf/service.h>

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
                                 std::chrono::milliseconds commit_timeout)
    : store_(store),
      raft_node_(raft_node),
      wait_registry_(wait_registry),
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
