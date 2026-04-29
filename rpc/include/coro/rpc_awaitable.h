#pragma once

#include "rpc_closure.h"

#include <google/protobuf/stubs/callback.h>

#include <atomic>
#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

// Thin coroutine awaitable that plugs into the existing protobuf-style
// |done| completion path:
//
//     void CallMethod(..., google::protobuf::Closure* done);
//
// The awaitable does NOT own the request/response/controller. The caller
// is expected to keep those alive for the entire `co_await` expression
// (stack locals in the coroutine frame are fine). On resume, the caller
// reads the outcome from its own `controller.Failed()` / `response`.
//
// Thread model
// ------------
// The RPC framework's `done->Run()` generally fires on the RpcChannel's
// IO thread (and in some failure paths inline on the caller's thread
// while still inside CallMethod). In a raft context we do NOT want to
// continue executing raft protocol state-machine code on the RPC IO
// thread, and we do NOT want to spin up yet another thread just to host
// coroutine resumption. Instead, the awaitable requires the caller to
// supply a |resume_poster| that posts a task onto the fixed raft logic
// thread (typically an EventLoop::runInLoop or similar). The coroutine
// therefore resumes on whichever thread the poster runs tasks on.
//
// The |resume_poster| itself must be thread-safe, because it will be
// invoked from the RpcChannel IO thread.
//
// Race with synchronous completion
// --------------------------------
// `RpcChannel::CallMethod()` can invoke `done->Run()` synchronously from
// inside the launcher call (e.g. on serialize failure). That means the
// closure may fire BEFORE `await_suspend()` finishes recording state.
// The awaiter handles this with a small shared `State` and a mutex:
//
//   - In the closure:  mark `completed = true`; only post the resume if
//     `await_suspend()` has already marked `suspended = true`.
//   - In await_suspend: after calling the launcher, under the same lock
//     check `completed`. If already completed, return `false` (don't
//     suspend, continue running inline). Otherwise mark `suspended = true`
//     and return `true`.
//
// This guarantees exactly one of the two paths actually resumes the
// coroutine, regardless of whether `done->Run()` fires synchronously or
// asynchronously.
class RpcAwaitable {
public:
    // Launches the actual RPC. Must eventually cause |done->Run()| to be
    // called exactly once (CallMethod / its completion paths already do
    // this). Typical usage:
    //
    //     [&](google::protobuf::Closure* done) {
    //         stub.RequestVote(&ctl, &req, &resp, done);
    //     }
    using Launcher = std::function<void(google::protobuf::Closure*)>;

    // Posts |fn| onto the raft logic thread. Must be thread-safe: it is
    // called from whatever thread ends up invoking |done->Run()|.
    using ResumePoster = std::function<void(std::function<void()>)>;

    RpcAwaitable(Launcher launcher, ResumePoster resume_poster)
        : launcher_(std::move(launcher)),
          resume_poster_(std::move(resume_poster)) {}

    // Always suspend first; we make the fast-path decision inside
    // await_suspend() after we know whether the RPC completed inline.
    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
        auto state            = std::make_shared<State>();
        state->handle         = h;
        state->resume_poster  = resume_poster_;
        state_                = state;

        auto* done = new RpcClosure([state]() {
            bool should_post_resume = false;
            {
                std::lock_guard<std::mutex> lk(state->mu);
                state->completed = true;
                should_post_resume = state->suspended;
            }
            if (should_post_resume) {
                auto h = state->handle;
                state->resume_poster([h]() { h.resume(); });
            }
            // else: await_suspend() will observe |completed| and keep
            // running inline without suspending.
        });

        // Fire the RPC. This may invoke |done->Run()| synchronously.
        launcher_(done);

        std::lock_guard<std::mutex> lk(state->mu);
        if (state->completed) {
            return false;    // don't suspend; coroutine keeps running
        }
        state->suspended = true;
        return true;         // park until resume_poster fires us
    }

    // The awaitable does not synthesise a result value. The caller reads
    // the RPC outcome from its own controller / response objects, same
    // as the non-coroutine API.
    void await_resume() const noexcept {}

private:
    struct State {
        std::mutex                          mu;
        bool                                completed = false;
        bool                                suspended = false;
        std::coroutine_handle<>             handle{};
        ResumePoster                        resume_poster;
    };

    Launcher               launcher_;
    ResumePoster           resume_poster_;
    std::shared_ptr<State> state_;
};

// Convenience factory so callers can write:
//
//     co_await MakeRpcAwaitable(
//         [&](google::protobuf::Closure* done) {
//             stub.RequestVote(&ctl, &req, &resp, done);
//         },
//         raftPoster);
inline RpcAwaitable MakeRpcAwaitable(RpcAwaitable::Launcher     launcher,
                                     RpcAwaitable::ResumePoster resume_poster) {
    return RpcAwaitable(std::move(launcher), std::move(resume_poster));
}
