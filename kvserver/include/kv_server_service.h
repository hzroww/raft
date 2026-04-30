#pragma once

#include "commit_wait_registry.h"
#include "kv.pb.h"
#include "kv_store.h"
#include "raft_node.h"

#include <chrono>
#include <cstdint>
#include <string>

namespace kvserver {

class KvServerService : public kv::KvServerRpc {
public:
    KvServerService(KvStore* store,
                    raft_core::RaftNode* raft_node,
                    CommitWaitRegistry* wait_registry,
                    std::chrono::milliseconds commit_timeout =
                        std::chrono::milliseconds(5000));

    void SetRaftNode(raft_core::RaftNode* raft_node) { raft_node_ = raft_node; }

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
    std::chrono::milliseconds commit_timeout_;
};

}  // namespace kvserver
