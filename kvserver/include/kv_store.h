#pragma once

#include "kv_log.pb.h"
#include "skip_list.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvserver {

struct KvResult {
    bool        success   = true;
    bool        duplicate = false;
    bool        existed   = false;
    std::string value;
    std::string error;
};

class KvStore {
public:
    KvResult Put(const std::string& key,
                 const std::string& value,
                 const std::string& client_id,
                 int64_t            request_id);

    KvResult Get(const std::string& key) const;

    KvResult Delete(const std::string& key,
                    const std::string& client_id,
                    int64_t            request_id);

    KvResult Apply(const kv::KvCommand& command);

    std::string SaveSnapshot() const;
    bool LoadSnapshot(const std::string& data);

private:
    bool IsDuplicateLocked(const std::string& client_id, int64_t request_id) const;
    void RememberRequestLocked(const std::string& client_id, int64_t request_id);

    mutable std::mutex                     mu_;
    mutable SkipList<std::string, std::string> data_;
    std::unordered_map<std::string, std::string> data_snapshot_;
    std::unordered_map<std::string, int64_t>   last_request_id_;
    
};

}  // namespace kvserver
