#pragma once

#include <coroutine>
#include <exception>
#include <utility>

// Minimal fire-and-forget coroutine return type.
//
// This type is intentionally tiny: it exists only to let user code write
// coroutine functions that `co_await` an RpcAwaitable. It does NOT implement
// a general-purpose scheduler, chained `co_await Task<T>`, results, or
// cancellation.
//
// Semantics
//   - `initial_suspend() == suspend_never`:  the coroutine body starts
//     running eagerly when the function is invoked, on the caller's thread.
//   - `final_suspend()  == suspend_never`:  when the coroutine hits
//     `co_return` (or falls off the end), the coroutine frame is destroyed
//     automatically. The returned `Task` object itself does not own the
//     coroutine frame and does not destroy it.
//
// This is enough for the current use case: a "raft logic thread" calls
// a coroutine function; the coroutine either runs to completion inline
// (no `co_await` actually suspended) or parks on an RPC awaitable and is
// later resumed on the same raft logic thread via the awaitable's `post`.
class Task {
public:
    struct promise_type {
        Task                 get_return_object() noexcept { return Task{}; }
        std::suspend_never   initial_suspend()   noexcept { return {}; }
        std::suspend_never   final_suspend()     noexcept { return {}; }
        void                 return_void()       noexcept {}
        void                 unhandled_exception()        { std::terminate(); }
    };
};
