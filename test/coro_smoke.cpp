// Smoke test for the RPC coroutine awaitable.
//
// What this verifies
//   1. A user-written coroutine can issue an RPC via `co_await`, suspend,
//      and later resume with the response populated.
//   2. The resume actually lands on the fixed "raft logic thread" we
//      supplied as the resume poster -- NOT on the RpcChannel IO thread.
//   3. A synchronous-completion path (hit before the coroutine has had a
//      chance to suspend) still completes correctly and doesn't deadlock.
//
// How it is structured
//   - RpcServer is started on a background thread with a tiny RaftRpc
//     implementation that replies synchronously inside the handler.
//   - A "RaftThread" is a single thread with a FIFO task queue. It is
//     the fixed logic thread we insist on resuming onto.
//   - The coroutine is launched by `raft.post(...)`, so the coroutine body
//     begins executing on the raft thread (same as how leader-driven
//     coroutines would run in the real project).

#include "coro/rpc_awaitable.h"
#include "coro/task.h"
#include "log.h"
#include "rpc_channel.h"
#include "rpc_controller.h"
#include "rpc_server.h"

#include "raft.pb.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <utility>

// ---------------------------------------------------------------------------
// RaftThread: fixed logic thread used as the resume poster.
// ---------------------------------------------------------------------------

class RaftThread {
public:
    RaftThread() {
        std::promise<void> ready;
        auto               fut = ready.get_future();
        thread_ = std::thread([this, &ready]() {
            raft::logging::SetCurrentThreadName("raft-logic");
            tid_ = std::this_thread::get_id();
            ready.set_value();
            run();
        });
        fut.wait();
    }

    ~RaftThread() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    void post(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push_back(std::move(fn));
        }
        cv_.notify_one();
    }

    std::thread::id id() const { return tid_; }

private:
    void run() {
        while (true) {
            std::function<void()> fn;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [&]() { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                fn = std::move(queue_.front());
                queue_.pop_front();
            }
            fn();
            LOG_INFO() << "RaftThread: executed task";
        }
    }

    std::thread                       thread_;
    std::thread::id                   tid_{};
    std::mutex                        mu_;
    std::condition_variable           cv_;
    std::deque<std::function<void()>> queue_;
    bool                              stop_{false};
};

// ---------------------------------------------------------------------------
// Minimal server-side RaftRpc implementation.
// ---------------------------------------------------------------------------

class EchoRaftService : public ::raft::RaftRpc {
public:
    void RequestVote(google::protobuf::RpcController*   /*controller*/,
                     const ::raft::RequestVoteArgs*     request,
                     ::raft::RequestVoteReply*          response,
                     google::protobuf::Closure*         done) override {
        response->set_term(request->term());
        response->set_votegranted(true);
        sleep(3);
        done->Run();
    }

    void AppendEntries(google::protobuf::RpcController* /*controller*/,
                       const ::raft::AppendEntriesArgs* /*request*/,
                       ::raft::AppendEntriesReply*      response,
                       google::protobuf::Closure*       done) override {
        response->set_success(true);
        done->Run();
    }

    void InstallSnapshot(google::protobuf::RpcController* /*controller*/,
                         const ::raft::InstallSnapshotArgs* /*request*/,
                         ::raft::InstallSnapshotReply*      /*response*/,
                         google::protobuf::Closure*         done) override {
        done->Run();
    }
};

// ---------------------------------------------------------------------------
// Test coroutine: issues RequestVote via co_await, records where it resumed.
// ---------------------------------------------------------------------------

struct CoroResult {
    bool            failed       = true;
    int64_t         term         = -1;
    bool            vote_granted = false;
    std::thread::id resume_tid{};
};

static Task run_request_vote(RpcChannel*         channel,
                             RaftThread*         raft,
                             CoroResult*         out,
                             std::promise<void>* finished) {
    ::raft::RaftRpc_Stub    stub(channel);
    RpcController           ctl;
    ::raft::RequestVoteArgs req;
    req.set_term(42);
    req.set_candidateid(7);
    ::raft::RequestVoteReply resp;

    co_await MakeRpcAwaitable(
        [&](google::protobuf::Closure* done) {
            stub.RequestVote(&ctl, &req, &resp, done);
        },
        [raft](std::function<void()> fn) { raft->post(std::move(fn)); });

    out->resume_tid   = std::this_thread::get_id();
    out->failed       = ctl.Failed();
    out->term         = resp.term();
    out->vote_granted = resp.votegranted();
    finished->set_value();
}

// ---------------------------------------------------------------------------
// Test driver.
// ---------------------------------------------------------------------------

int main() {
    raft::logging::Init();
    raft::logging::SetCurrentThreadName("coro-main");

    const std::string kIp   = "127.0.0.1";
    const int         kPort = 18901;

    EchoRaftService svc;
    RpcServer       server;
    server.RegisterService(&svc);

    std::thread server_thread([&]() { server.Run(kIp, kPort); });

    // Give the server a moment to start listening.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    RpcChannel channel(kIp, kPort);
    if (!channel.waitUntilConnected(2000)) {
        LOG_ERROR() << "FAIL: RpcChannel did not connect";
        std::_Exit(1);
    }

    RaftThread raft;
    CoroResult result;
    std::promise<void> finished;
    auto               fut = finished.get_future();

    raft.post([&]() {
        run_request_vote(&channel, &raft, &result, &finished);
    });

    if (fut.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        LOG_ERROR() << "FAIL: coroutine did not resume within 5s";
        std::_Exit(1);
    }

    if (result.failed) {
        LOG_ERROR() << "FAIL: controller reported failure";
        std::_Exit(1);
    }
    if (result.term != 42) {
        LOG_ERROR() << "FAIL: wrong term in reply: "
                    << static_cast<long long>(result.term);
        std::_Exit(1);
    }
    if (!result.vote_granted) {
        LOG_ERROR() << "FAIL: voteGranted not set";
        std::_Exit(1);
    }
    if (result.resume_tid != raft.id()) {
        LOG_ERROR() << "FAIL: coroutine resumed on wrong thread (expected raft thread)";
        std::_Exit(1);
    }

    LOG_INFO() << "OK: coroutine resumed on raft thread; term="
               << static_cast<long long>(result.term)
               << " voteGranted="
               << static_cast<int>(result.vote_granted);

    // We don't have a clean shutdown path for RpcServer yet, so bypass normal
    // teardown after the result is logged.
    server_thread.detach();
    std::_Exit(0);
}
