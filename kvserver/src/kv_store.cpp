#include "kv_store.h"

#include <utility>

namespace kvserver {

KvResult KvStore::Put(const std::string& key,
                      const std::string& value,
                      const std::string& client_id,
                      int64_t            request_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (IsDuplicateLocked(client_id, request_id)) {
        return {.success = true, .duplicate = true};
    }

    bool inserted = data_.Insert(key, value);
    RememberRequestLocked(client_id, request_id);
    return {.success = true, .existed = !inserted};
}

KvResult KvStore::Get(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mu_);
    std::string value;
    if (!data_.Search(key, value)) {
        return {.success = false, .error = "key not found"};
    }
    return {.success = true, .existed = true, .value = std::move(value)};
}

KvResult KvStore::Delete(const std::string& key,
                         const std::string& client_id,
                         int64_t            request_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (IsDuplicateLocked(client_id, request_id)) {
        return {.success = true, .duplicate = true};
    }

    bool existed = data_.Delete(key);
    RememberRequestLocked(client_id, request_id);
    return {.success = true, .existed = existed};
}

KvResult KvStore::Apply(const kv::KvCommand& command) {
    switch (command.op()) {
        case kv::KvCommand::PUT:
            return Put(command.key(),
                       command.value(),
                       command.clientid(),
                       command.requestid());
        case kv::KvCommand::DELETE:
            return Delete(command.key(), command.clientid(), command.requestid());
        default:
            return {.success = false, .error = "unknown kv command op"};
    }
}

bool KvStore::IsDuplicateLocked(const std::string& client_id,
                                int64_t            request_id) const {
    auto it = last_request_id_.find(client_id);
    return it != last_request_id_.end() && request_id <= it->second;
}

void KvStore::RememberRequestLocked(const std::string& client_id,
                                    int64_t            request_id) {
    auto it = last_request_id_.find(client_id);
    if (it == last_request_id_.end() || request_id > it->second) {
        last_request_id_[client_id] = request_id;
    }
}

}  // namespace kvserver
