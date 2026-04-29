#pragma once

#include <google/protobuf/service.h>

#include "rpc.pb.h"    // mprrpc::RpcHeader

#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/base/Timestamp.h>

#include <memory>
#include <string>
#include <unordered_map>

class ThreadPool;

// RPC server built on top of muduo::net::TcpServer.
//
// Usage:
//     RpcServer srv;
//     srv.RegisterService(&myService);
//     srv.setIoThreadNum(4);     // muduo IO threads (accept + read/write)
//     srv.setWorkerThreads(8);   // worker pool that runs user handlers
//     srv.Run("0.0.0.0", 8001);  // blocking
class RpcServer {
public:
    RpcServer();
    ~RpcServer();

    void RegisterService(google::protobuf::Service* service);

    // Number of muduo IO threads (default 1: everything on main loop).
    void setIoThreadNum(int n) { ioThreadNum_ = n; }

    // Number of background worker threads for handler execution (default 4).
    // Handlers are dispatched here so the muduo IO thread is never blocked by
    // user code or slow protobuf (de)serialisation.
    void setWorkerThreads(size_t n) { workerThreads_ = n; }

    // Blocking: starts listening on |ip|:|port| and runs the event loop until
    // the process is terminated.
    void Run(const std::string& ip, int port);

private:
    void onMessage(const muduo::net::TcpConnectionPtr& conn,
                   muduo::net::Buffer*                 buf,
                   muduo::Timestamp                    receiveTime);

    void processRequest(muduo::net::TcpConnectionPtr conn,
                        mprrpc::RpcHeader            header,
                        std::string                  body);

    void sendResponse(const muduo::net::TcpConnectionPtr& conn,
                      uint64_t                             requestId,
                      const google::protobuf::Message&     response);

    std::unordered_map<std::string, google::protobuf::Service*> service_map_;

    int    ioThreadNum_{1};
    size_t workerThreads_{4};

    // Owned during Run()
    std::unique_ptr<muduo::net::EventLoop> loop_;
    std::unique_ptr<muduo::net::TcpServer> server_;
    std::unique_ptr<ThreadPool>            pool_;
};
