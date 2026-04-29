#include "kv_server_service.h"

#include <google/protobuf/service.h>

namespace kvserver {

namespace {

void Finish(google::protobuf::Closure* done) {
    if (done) {
        done->Run();
    }
}

}  // namespace

KvServerService::KvServerService(KvStore* store, int32_t self_id)
    : store_(store), self_id_(self_id) {}

void KvServerService::Put(google::protobuf::RpcController* /*controller*/,
                          const kv::PutRequest*            request,
                          kv::PutResponse*                 response,
                          google::protobuf::Closure*       done) {
    response->set_leaderid(self_id_);

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

    KvResult result = store_->Put(request->key(),
                                  request->value(),
                                  request->clientid(),
                                  request->requestid());
    response->set_success(result.success);
    response->set_error(result.error);
    Finish(done);
}

void KvServerService::Get(google::protobuf::RpcController* /*controller*/,
                          const kv::GetRequest*            request,
                          kv::GetResponse*                 response,
                          google::protobuf::Closure*       done) {
    response->set_leaderid(self_id_);

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

    KvResult result = store_->Get(request->key());
    response->set_success(result.success);
    response->set_value(result.value);
    response->set_error(result.error);
    Finish(done);
}

void KvServerService::Delete(google::protobuf::RpcController* /*controller*/,
                             const kv::DeleteRequest*         request,
                             kv::DeleteResponse*              response,
                             google::protobuf::Closure*       done) {
    response->set_leaderid(self_id_);

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

    KvResult result = store_->Delete(request->key(),
                                     request->clientid(),
                                     request->requestid());
    response->set_success(result.success);
    response->set_error(result.error);
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

}  // namespace kvserver
