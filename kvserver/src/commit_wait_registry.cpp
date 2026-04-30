#include "commit_wait_registry.h"

#include <utility>

namespace kvserver {

std::future<KvResult> CommitWaitRegistry::FutureFor(raft_core::Index index) {
    std::lock_guard<std::mutex> lk(mu_);

    std::promise<KvResult> promise;
    auto future = promise.get_future();

    auto completed = completed_.find(index);
    if (completed != completed_.end()) {
        promise.set_value(std::move(completed->second));
        completed_.erase(completed);
        return future;
    }

    waiters_[index].push_back(std::move(promise));
    return future;
}

void CommitWaitRegistry::Notify(raft_core::Index index, KvResult result) {
    std::vector<std::promise<KvResult>> waiters;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = waiters_.find(index);
        if (it == waiters_.end()) {
            completed_[index] = std::move(result);
            return;
        }

        waiters = std::move(it->second);
        waiters_.erase(it);
    }

    for (auto& waiter : waiters) {
        waiter.set_value(result);
    }
}

}  // namespace kvserver
