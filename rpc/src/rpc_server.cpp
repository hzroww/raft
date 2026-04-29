#include "rpc_server.h"

#include "rpc_codec.h"
#include "rpc_controller.h"
#include "rpc_closure.h"
#include "thread_pool.h"
#include "log.h"

#include <google/protobuf/descriptor.h>

#include <muduo/net/InetAddress.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::InetAddress;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

RpcServer::RpcServer()  = default;
RpcServer::~RpcServer() = default;

void RpcServer::RegisterService(google::protobuf::Service* service) {
    const auto* desc = service->GetDescriptor();
    service_map_[desc->full_name()] = service;
    LOG_INFO() << "registered rpc service name=" << desc->full_name();
}

void RpcServer::Run(const std::string& ip, int port) {
    raft::logging::SetCurrentThreadName("rpc-server-main");
    loop_ = std::make_unique<EventLoop>();
    pool_ = std::make_unique<ThreadPool>(workerThreads_);

    InetAddress listenAddr(ip, static_cast<uint16_t>(port));
    server_ = std::make_unique<TcpServer>(loop_.get(), listenAddr, "RpcServer");

    if (ioThreadNum_ > 1) {
        server_->setThreadNum(ioThreadNum_);
    }

    server_->setMessageCallback(
        [this](const TcpConnectionPtr& conn,
               Buffer*                 buf,
               muduo::Timestamp        ts) {
            onMessage(conn, buf, ts);
        });

    server_->start();
    LOG_INFO() << "rpc server started ip=" << ip
               << " port=" << port
               << " io_threads=" << ioThreadNum_
               << " worker_threads=" << workerThreads_
               << " services=" << service_map_.size();
    loop_->loop();
}

void RpcServer::onMessage(const TcpConnectionPtr& conn,
                          Buffer*                 buf,
                          muduo::Timestamp        /*receiveTime*/) {
    // A single callback may contain several frames; drain them all.
    while (true) {
        mprrpc::RpcHeader header;
        std::string       body;
        if (!rpc_codec::tryDecode(buf, &header, &body)) break;

        // Move heavy work off the IO thread.
        pool_->Enqueue(
            [this, conn, header = std::move(header), body = std::move(body)]() mutable {
                processRequest(conn, std::move(header), std::move(body));
            });
    }
}

void RpcServer::processRequest(TcpConnectionPtr  conn,
                               mprrpc::RpcHeader header,
                               std::string       body) {
    const std::string& serviceName = header.service_name();
    const std::string& methodName  = header.method_name();
    uint64_t           requestId   = header.request_id();

    auto serviceIt = service_map_.find(serviceName);
    if (serviceIt == service_map_.end()) {
        LOG_WARN() << "service not found request_id=" << requestId
                   << " service=" << serviceName
                   << " method=" << methodName;
        mprrpc::RpcHeader rsp;
        rsp.set_request_id(requestId);
        rsp.set_args_size(0);
        conn->send(rpc_codec::encode(rsp, ""));
        return;
    }

    google::protobuf::Service* service     = serviceIt->second;
    const auto*                serviceDesc = service->GetDescriptor();
    const auto*                methodDesc  = serviceDesc->FindMethodByName(methodName);
    if (!methodDesc) {
        LOG_WARN() << "method not found request_id=" << requestId
                   << " service=" << serviceName
                   << " method=" << methodName;
        mprrpc::RpcHeader rsp;
        rsp.set_request_id(requestId);
        rsp.set_args_size(0);
        conn->send(rpc_codec::encode(rsp, ""));
        return;
    }

    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(methodDesc).New());
    if (!request->ParseFromString(body)) {
        LOG_WARN() << "request parse failed request_id=" << requestId
                   << " service=" << serviceName
                   << " method=" << methodName
                   << " body_size=" << body.size();
        mprrpc::RpcHeader rsp;
        rsp.set_request_id(requestId);
        rsp.set_args_size(0);
        conn->send(rpc_codec::encode(rsp, ""));
        return;
    }

    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(methodDesc).New());

    auto* controller = new RpcController();
    auto* rawReq     = request.release();
    auto* rawResp    = response.release();

    auto* done = new RpcClosure(
        [this, conn, requestId, controller, rawReq, rawResp, serviceName, methodName]() {
            if (controller->Failed()) {
                LOG_ERROR() << "rpc handler failed request_id=" << requestId
                            << " service=" << serviceName
                            << " method=" << methodName
                            << " error=" << controller->ErrorText();
            } else {
                LOG_DEBUG() << "rpc handler completed request_id=" << requestId
                            << " service=" << serviceName
                            << " method=" << methodName;
            }
            sendResponse(conn, requestId, *rawResp);
            delete controller;
            delete rawReq;
            delete rawResp;
        });

    service->CallMethod(methodDesc, controller, rawReq, rawResp, done);
}

void RpcServer::sendResponse(const TcpConnectionPtr&          conn,
                             uint64_t                         requestId,
                             const google::protobuf::Message& response) {
    std::string body;
    response.SerializeToString(&body);

    mprrpc::RpcHeader rsp;
    rsp.set_request_id(requestId);
    rsp.set_args_size(static_cast<uint32_t>(body.size()));

    // TcpConnection::send() is thread-safe: it defers to the IO loop.
    conn->send(rpc_codec::encode(rsp, body));
}
