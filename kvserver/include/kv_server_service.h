#pragma once

#include "commit_wait_registry.h"
#include "kv.pb.h"
#include "kv_store.h"
#include "raft_node.h"
#include "raft_storage_iface.h"

#include <chrono>
#include <cstdint>
#include <string>

namespace kvserver {

class KvServerService : public kv::KvServerRpc {
public:
    KvServerService(KvStore* store,
                    raft_core::RaftNode* raft_node,
                    CommitWaitRegistry* wait_registry,
                    raft_core::IRaftStorage* storage = nullptr,
                    std::chrono::milliseconds commit_timeout =
                        std::chrono::milliseconds(5000));

    void SetRaftNode(raft_core::RaftNode* raft_node) { raft_node_ = raft_node; }
    void SetStorage(raft_core::IRaftStorage* storage) { storage_ = storage; }

    void Put(google::protobuf::RpcController* controller,
             const kv::PutRequest*            request,
             kv::PutResponse*                 response,
             google::protobuf::Closure*       done) override;

    void Get(google::protobuf::RpcController* controller,
             const kv::GetRequest*            request,
             kv::GetResponse*                 response,
             google::protobuf::Closure*       done) override;

    void Delete(google::protobuf::RpcController* controller,
                const kv::DeleteRequest*         request,
                kv::DeleteResponse*              response,
                google::protobuf::Closure*       done) override;

    void GetNodeStatus(google::protobuf::RpcController*    controller,
                       const kv::GetNodeStatusRequest*     request,
                       kv::GetNodeStatusResponse*          response,
                       google::protobuf::Closure*          done) override;

private:
    bool ValidateRequest(const std::string& key,
                         const std::string& client_id,
                         int64_t            request_id,
                         std::string*       error) const;
    bool IsLeader() const;
    int32_t LeaderIdForResponse() const;

    KvStore* store_;
    raft_core::RaftNode* raft_node_;
    CommitWaitRegistry* wait_registry_;
    raft_core::IRaftStorage* storage_;
    std::chrono::milliseconds commit_timeout_;
};

}  // namespace kvserver
