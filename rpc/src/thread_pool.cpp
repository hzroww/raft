#include "thread_pool.h"
#include "log.h"

#include <exception>
#include <string>

ThreadPool::ThreadPool(size_t numThreads) : stop_(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers_.emplace_back([this, i] {
            raft::logging::SetCurrentThreadName("rpc-worker-" + std::to_string(i));
            LOG_INFO() << "worker thread started index=" << i;
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);
                    condition_.wait(lock, [this] {
                        return stop_ || !tasks_.empty();
                    });
                    if (stop_ && tasks_.empty()) {
                        LOG_INFO() << "worker thread exiting index=" << i;
                        return;
                    }
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                try {
                    task();
                } catch (const std::exception& ex) {
                    LOG_ERROR() << "worker task threw exception index=" << i
                                << " what=" << ex.what();
                } catch (...) {
                    LOG_ERROR() << "worker task threw unknown exception index=" << i;
                }
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    LOG_INFO() << "thread pool shutting down workers=" << workers_.size();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    condition_.notify_all();
    for (auto& w : workers_) w.join();
}

void ThreadPool::Enqueue(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stop_) {
            LOG_WARN() << "enqueue ignored because thread pool is stopping";
            return;
        }
        tasks_.push(std::move(task));
    }
    condition_.notify_one();
}
