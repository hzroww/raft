#pragma once

#include <google/protobuf/service.h>

#include <muduo/net/TcpClient.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoopThread.h>
#include <muduo/net/TimerId.h>
#include <muduo/base/Timestamp.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Client-side RPC channel backed by muduo::net::TcpClient.
//
// One RpcChannel owns one persistent TCP connection to a remote RpcServer.
// Multiple concurrent RPC calls on the same channel are multiplexed over a
// single connection using RpcHeader.request_id.
//
// Lifecycle
//   1. Construct with peer address; the channel spins up its own IO thread
//      (EventLoopThread) and kicks off an async connect.
//   2. Hand the channel to a protobuf-generated Stub.
//   3. Call any stub method. CallMethod is non-blocking and thread-safe.
//      The |done| closure fires on the channel's IO thread, so keep it short
//      or dispatch heavy work to your own pool.
class RpcChannel : public google::protobuf::RpcChannel {
public:
    static constexpr int kDefaultTimeoutMs = 3000;

    RpcChannel(const std::string& ip, int port);
    ~RpcChannel() override;

    // Per-call timeout in milliseconds (default 3 s).
    void setTimeoutMs(int ms) { timeoutMs_ = ms; }

    // Block the caller until the connection is established (or |waitMs|
    // elapses).  Useful right after construction in single-threaded tests.
    // Returns true if connected.
    bool waitUntilConnected(int waitMs = kDefaultTimeoutMs);

    // The IO thread's event loop. Exposed so coroutine awaiters (timers,
    // schedulers) can post work onto the same thread the channel uses.
    muduo::net::EventLoop* loop() const { return loop_; }

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController*          controller,
                    const google::protobuf::Message*          request,
                    google::protobuf::Message*                response,
                    google::protobuf::Closure*                done) override;

private:
    struct PendingCall {
        google::protobuf::RpcController* controller = nullptr;
        google::protobuf::Message*       response   = nullptr;
        google::protobuf::Closure*       done       = nullptr;
        std::string                      serviceName;
        std::string                      methodName;
        muduo::net::TimerId              timerId;
        bool                             hasTimer   = false;
    };

    void onConnection(const muduo::net::TcpConnectionPtr& conn);
    void onMessage(const muduo::net::TcpConnectionPtr& conn,
                   muduo::net::Buffer*                 buf,
                   muduo::Timestamp                    ts);

    void sendInLoop(std::string frame);
    void flushPendingSends();
    void failAllPending(const std::string& reason);
    void finishCall(uint64_t           requestId,
                    bool               ok,
                    const std::string& body,
                    const std::string& errMsg);

    std::string ip_;
    int         port_;
    int         timeoutMs_{kDefaultTimeoutMs};

    std::unique_ptr<muduo::net::EventLoopThread> ioThread_;
    muduo::net::EventLoop*                       loop_{nullptr};
    std::unique_ptr<muduo::net::TcpClient>       client_;

    // --- Connection readiness (main-thread visible) ---
    mutable std::mutex       connMutex_;
    std::condition_variable  connCv_;
    bool                     connected_{false};

    // --- Pending in-flight calls, keyed by request_id ---
    std::mutex                                 pendingMutex_;
    std::unordered_map<uint64_t, PendingCall>  pendingCalls_;
    std::atomic<uint64_t>                      nextRequestId_{1};

    // --- Frames queued while the connection was not yet up ---
    std::mutex              queueMutex_;
    std::deque<std::string> pendingSendQueue_;
};
