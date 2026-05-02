#include "kv_store.h"

#include <cstdint>
#include <cstring>
#include <utility>

namespace kvserver {

namespace {

void AppendU64(std::string* out, uint64_t value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendI64(std::string* out, int64_t value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendString(std::string* out, const std::string& value) {
    AppendU64(out, static_cast<uint64_t>(value.size()));
    out->append(value);
}

bool ReadBytes(const std::string& data, size_t* pos, void* out, size_t size) {
    if (*pos > data.size() || data.size() - *pos < size) {
        return false;
    }
    std::memcpy(out, data.data() + *pos, size);
    *pos += size;
    return true;
}

bool ReadU64(const std::string& data, size_t* pos, uint64_t* value) {
    return ReadBytes(data, pos, value, sizeof(*value));
}

bool ReadI64(const std::string& data, size_t* pos, int64_t* value) {
    return ReadBytes(data, pos, value, sizeof(*value));
}

bool ReadString(const std::string& data, size_t* pos, std::string* value) {
    uint64_t size = 0;
    if (!ReadU64(data, pos, &size) ||
        *pos > data.size() ||
        data.size() - *pos < size) {
        return false;
    }
    value->assign(data.data() + *pos, static_cast<size_t>(size));
    *pos += static_cast<size_t>(size);
    return true;
}

}  // namespace

KvResult KvStore::Put(const std::string& key,
                      const std::string& value,
                      const std::string& client_id,
                      int64_t            request_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (IsDuplicateLocked(client_id, request_id)) {
        return {.success = true, .duplicate = true};
    }

    bool inserted = data_.Insert(key, value);
    data_snapshot_[key] = value;
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
    data_snapshot_.erase(key);
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

std::string KvStore::SaveSnapshot() const {
    std::lock_guard<std::mutex> lk(mu_);

    std::string out;
    AppendU64(&out, static_cast<uint64_t>(data_snapshot_.size()));
    for (const auto& item : data_snapshot_) {
        AppendString(&out, item.first);
        AppendString(&out, item.second);
    }

    AppendU64(&out, static_cast<uint64_t>(last_request_id_.size()));
    for (const auto& item : last_request_id_) {
        AppendString(&out, item.first);
        AppendI64(&out, item.second);
    }
    return out;
}

bool KvStore::LoadSnapshot(const std::string& data) {
    std::unordered_map<std::string, std::string> restored_data;
    std::unordered_map<std::string, int64_t> restored_requests;
    size_t pos = 0;

    uint64_t data_count = 0;
    if (!ReadU64(data, &pos, &data_count)) {
        return false;
    }
    for (uint64_t i = 0; i < data_count; ++i) {
        std::string key;
        std::string value;
        if (!ReadString(data, &pos, &key) ||
            !ReadString(data, &pos, &value)) {
            return false;
        }
        restored_data[std::move(key)] = std::move(value);
    }

    uint64_t request_count = 0;
    if (!ReadU64(data, &pos, &request_count)) {
        return false;
    }
    for (uint64_t i = 0; i < request_count; ++i) {
        std::string client_id;
        int64_t request_id = 0;
        if (!ReadString(data, &pos, &client_id) ||
            !ReadI64(data, &pos, &request_id)) {
            return false;
        }
        restored_requests[std::move(client_id)] = request_id;
    }
    if (pos != data.size()) {
        return false;
    }

    std::lock_guard<std::mutex> lk(mu_);
    for (const auto& item : data_snapshot_) {
        data_.Delete(item.first);
    }
    data_snapshot_ = std::move(restored_data);
    last_request_id_ = std::move(restored_requests);
    for (const auto& item : data_snapshot_) {
        data_.Insert(item.first, item.second);
    }
    return true;
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
