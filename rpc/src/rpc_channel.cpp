#include "rpc_channel.h"

#include "rpc_codec.h"
#include "rpc_controller.h"
#include "log.h"

#include <google/protobuf/descriptor.h>

#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>

#include <chrono>
#include <utility>

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::EventLoopThread;
using muduo::net::InetAddress;
using muduo::net::TcpClient;
using muduo::net::TcpConnectionPtr;

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

RpcChannel::RpcChannel(const std::string& ip, int port)
    : ip_(ip), port_(port) {
    ioThread_ = std::make_unique<EventLoopThread>(
        EventLoopThread::ThreadInitCallback(), "rpc-channel-io");
    loop_ = ioThread_->startLoop();     // blocks until the loop is running
    LOG_INFO() << "rpc channel created peer=" << ip_ << ':' << port_;

    // Create TcpClient inside the IO loop so all muduo callbacks fire there.
    loop_->runInLoop([this]() {
        InetAddress serverAddr(ip_, static_cast<uint16_t>(port_));
        client_ = std::make_unique<TcpClient>(loop_, serverAddr, "RpcChannel");

        client_->setConnectionCallback(
            [this](const TcpConnectionPtr& conn) { onConnection(conn); });
        client_->setMessageCallback(
            [this](const TcpConnectionPtr& conn,
                   Buffer*                 buf,
                   muduo::Timestamp        ts) {
                onMessage(conn, buf, ts);
            });

        client_->enableRetry();
        client_->connect();
    });
}

RpcChannel::~RpcChannel() {
    // Fail any in-flight calls before tearing everything down.
    LOG_INFO() << "rpc channel shutting down peer=" << ip_ << ':' << port_;
    failAllPending("RpcChannel destroyed");

    if (loop_) {
        loop_->runInLoop([this]() {
            if (client_) client_->disconnect();
        });
    }
    // EventLoopThread dtor will quit() the loop and join the thread.
    ioThread_.reset();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

bool RpcChannel::waitUntilConnected(int waitMs) {
    std::unique_lock<std::mutex> lk(connMutex_);
    if (connected_) return true;
    connCv_.wait_for(lk, std::chrono::milliseconds(waitMs),
                     [this]() { return connected_; });
    if (!connected_) {
        LOG_WARN() << "wait for rpc channel connection timed out peer="
                   << ip_ << ':' << port_
                   << " wait_ms=" << waitMs;
    }
    return connected_;
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController*          controller,
                            const google::protobuf::Message*          request,
                            google::protobuf::Message*                response,
                            google::protobuf::Closure*                done) {
    std::string body;
    if (!request->SerializeToString(&body)) {
        LOG_ERROR() << "serialize request failed service="
                    << method->service()->full_name()
                    << " method=" << method->name();
        static_cast<RpcController*>(controller)->SetFailed("serialize request failed");
        if (done) done->Run();
        return;
    }

    uint64_t reqId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);

    mprrpc::RpcHeader header;
    header.set_request_id(reqId);
    header.set_service_name(method->service()->full_name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(body.size()));
    std::string frame = rpc_codec::encode(header, body);

    // Register the pending entry before we schedule anything on the IO loop.
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        PendingCall pc;
        pc.controller = controller;
        pc.response   = response;
        pc.done       = done;
        pc.serviceName = method->service()->full_name();
        pc.methodName  = method->name();
        pendingCalls_.emplace(reqId, std::move(pc));
    }

    // Arm timeout + send on the IO loop (both are only safe there).
    int timeoutMs = timeoutMs_;
    loop_->runInLoop([this, reqId, timeoutMs, frame = std::move(frame)]() mutable {
        muduo::net::TimerId tid = loop_->runAfter(
            timeoutMs / 1000.0,
            [this, reqId]() { finishCall(reqId, false, "", "RPC timeout"); });

        {
            std::lock_guard<std::mutex> lk(pendingMutex_);
            auto it = pendingCalls_.find(reqId);
            if (it != pendingCalls_.end()) {
                it->second.timerId  = tid;
                it->second.hasTimer = true;
            }
        }

        sendInLoop(std::move(frame));
    });
}

// ---------------------------------------------------------------------------
// IO-thread callbacks
// ---------------------------------------------------------------------------

void RpcChannel::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        {
            std::lock_guard<std::mutex> lk(connMutex_);
            connected_ = true;
        }
        connCv_.notify_all();
        LOG_INFO() << "rpc channel connected peer=" << conn->peerAddress().toIpPort();

        // Flush any frames queued before the connection came up.
        flushPendingSends();
    } else {
        {
            std::lock_guard<std::mutex> lk(connMutex_);
            connected_ = false;
        }
        LOG_WARN() << "rpc channel disconnected peer=" << ip_ << ':' << port_;
        // Don't fail pending calls here: TcpClient's retry will try to
        // reconnect, and the timeout timer will clean up stragglers.
    }
}

void RpcChannel::onMessage(const TcpConnectionPtr& /*conn*/,
                           Buffer*                 buf,
                           muduo::Timestamp        /*ts*/) {
    while (true) {
        mprrpc::RpcHeader header;
        std::string       body;
        if (!rpc_codec::tryDecode(buf, &header, &body)) break;
        finishCall(header.request_id(), true, body, "");
    }
}

// ---------------------------------------------------------------------------
// Helpers (all run on the IO loop unless noted)
// ---------------------------------------------------------------------------

void RpcChannel::sendInLoop(std::string frame) {
    TcpConnectionPtr conn = client_ ? client_->connection() : nullptr;
    if (conn && conn->connected()) {
        conn->send(frame);
    } else {
        std::lock_guard<std::mutex> lk(queueMutex_);
        LOG_DEBUG() << "queueing rpc frame until connection is ready peer="
                    << ip_ << ':' << port_
                    << " queued_frames=" << (pendingSendQueue_.size() + 1);
        pendingSendQueue_.emplace_back(std::move(frame));
    }
}

void RpcChannel::flushPendingSends() {
    std::deque<std::string> drained;
    {
        std::lock_guard<std::mutex> lk(queueMutex_);
        drained.swap(pendingSendQueue_);
    }
    TcpConnectionPtr conn = client_ ? client_->connection() : nullptr;
    if (!conn || !conn->connected()) {
        // Connection vanished between callbacks; push them back.
        std::lock_guard<std::mutex> lk(queueMutex_);
        for (auto& f : drained) pendingSendQueue_.emplace_back(std::move(f));
        return;
    }
    LOG_DEBUG() << "flushing queued rpc frames count=" << drained.size()
                << " peer=" << conn->peerAddress().toIpPort();
    for (auto& f : drained) conn->send(f);
}

void RpcChannel::finishCall(uint64_t           requestId,
                            bool               ok,
                            const std::string& body,
                            const std::string& errMsg) {
    PendingCall call;
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        auto it = pendingCalls_.find(requestId);
        if (it == pendingCalls_.end()) return; // already finished
        call = std::move(it->second);
        pendingCalls_.erase(it);
    }

    if (call.hasTimer && loop_) {
        // cancel() on an already-fired timer is a no-op, which is what we want.
        loop_->cancel(call.timerId);
    }

    if (ok) {
        if (!call.response->ParseFromString(body)) {
            LOG_ERROR() << "parse response failed request_id=" << requestId
                        << " service=" << call.serviceName
                        << " method=" << call.methodName
                        << " body_size=" << body.size();
            static_cast<RpcController*>(call.controller)
                ->SetFailed("parse response failed");
        }
    } else {
        LOG_WARN() << "rpc call failed request_id=" << requestId
                   << " service=" << call.serviceName
                   << " method=" << call.methodName
                   << " error=" << errMsg;
        static_cast<RpcController*>(call.controller)->SetFailed(errMsg);
    }

    if (call.done) call.done->Run();
}

void RpcChannel::failAllPending(const std::string& reason) {
    std::unordered_map<uint64_t, PendingCall> snapshot;
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        snapshot.swap(pendingCalls_);
    }
    if (!snapshot.empty()) {
        LOG_WARN() << "failing pending rpc calls count=" << snapshot.size()
                   << " reason=" << reason;
    }
    for (auto& [id, call] : snapshot) {
        if (call.hasTimer && loop_) loop_->cancel(call.timerId);
        static_cast<RpcController*>(call.controller)->SetFailed(reason);
        if (call.done) call.done->Run();
    }
}
