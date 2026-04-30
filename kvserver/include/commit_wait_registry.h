#pragma once

#include "kv_store.h"
#include "raft_types.h"

#include <future>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace kvserver {

class CommitWaitRegistry {
public:
    std::future<KvResult> FutureFor(raft_core::Index index);
    void Notify(raft_core::Index index, KvResult result);

private:
    std::mutex mu_;
    std::unordered_map<raft_core::Index, KvResult> completed_;
    std::unordered_map<raft_core::Index, std::vector<std::promise<KvResult>>> waiters_;
};

}  // namespace kvserver
