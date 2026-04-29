#pragma once

#include <chrono>
#include <random>

namespace raft_core {

// Deadline-based timer used by the raft main-loop.
//
// The main loop's wait_until() needs the soonest deadline across all
// timers; rather than having the timer drive a callback on its own
// thread, we just expose Deadline() and Expired(now). Reset() is called
// when the relevant raft event happens (e.g. heartbeat received).
class DeadlineTimer {
public:
    using Clock     = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    DeadlineTimer() = default;

    // Re-arm with the next firing time |timeout| from |now|.
    void Reset(Clock::duration timeout, TimePoint now = Clock::now()) {
        deadline_ = now + timeout;
        armed_    = true;
    }

    void Disable() { armed_ = false; }

    bool Armed() const { return armed_; }

    bool Expired(TimePoint now = Clock::now()) const {
        return armed_ && now >= deadline_;
    }

    TimePoint Deadline() const { return deadline_; }

private:
    TimePoint deadline_{};
    bool      armed_{false};
};

// Election timer: a randomised timeout in [min_ms, max_ms]. Reset every
// time the follower hears from the current leader (or grants a vote).
class ElectionTimer {
public:
    ElectionTimer(int min_ms, int max_ms)
        : min_ms_(min_ms),
          max_ms_(max_ms),
          rng_(std::random_device{}()),
          dist_(min_ms, max_ms) {}

    void Reset(DeadlineTimer::TimePoint now = DeadlineTimer::Clock::now()) {
        timer_.Reset(std::chrono::milliseconds(dist_(rng_)), now);
    }

    void Disable() { timer_.Disable(); }
    bool Expired(DeadlineTimer::TimePoint now = DeadlineTimer::Clock::now()) const {
        return timer_.Expired(now);
    }
    bool Armed() const { return timer_.Armed(); }
    DeadlineTimer::TimePoint Deadline() const { return timer_.Deadline(); }

    int MinMs() const { return min_ms_; }
    int MaxMs() const { return max_ms_; }

private:
    int                                    min_ms_;
    int                                    max_ms_;
    std::mt19937                           rng_;
    std::uniform_int_distribution<int>     dist_;
    DeadlineTimer                          timer_;
};

// Heartbeat timer: fixed interval, only armed while the node is leader.
class HeartbeatTimer {
public:
    explicit HeartbeatTimer(int interval_ms) : interval_ms_(interval_ms) {}

    void Reset(DeadlineTimer::TimePoint now = DeadlineTimer::Clock::now()) {
        timer_.Reset(std::chrono::milliseconds(interval_ms_), now);
    }

    void Disable() { timer_.Disable(); }
    bool Expired(DeadlineTimer::TimePoint now = DeadlineTimer::Clock::now()) const {
        return timer_.Expired(now);
    }
    bool Armed() const { return timer_.Armed(); }
    DeadlineTimer::TimePoint Deadline() const { return timer_.Deadline(); }

    int IntervalMs() const { return interval_ms_; }

private:
    int           interval_ms_;
    DeadlineTimer timer_;
};

}  // namespace raft_core
