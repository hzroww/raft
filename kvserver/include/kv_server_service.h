#pragma once

#include "kv.pb.h"
#include "kv_store.h"

#include <cstdint>
#include <string>

namespace kvserver {

class KvServerService : public kv::KvServerRpc {
public:
    KvServerService(KvStore* store, int32_t self_id);

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

    KvStore* store_;
    int32_t  self_id_;
};

}  // namespace kvserver
