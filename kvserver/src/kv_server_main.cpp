#include "kv_server_service.h"

#include "log.h"
#include "rpc_server.h"

#include <cstdint>
#include <cstddef>
#include <exception>
#include <iostream>
#include <string>

namespace {

void PrintUsage(const char* program) {
    std::cerr << "Usage: " << program
              << " [--ip 0.0.0.0] [--port 8000] [--node_id 0]"
              << " [--io_threads 1] [--worker_threads 4]\n";
}

bool NextArg(int argc, char** argv, int* index, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::string ip = "0.0.0.0";
    int         port = 8000;
    int32_t     node_id = 0;
    int         io_threads = 1;
    int         worker_threads = 4;

    try {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            std::string value;
            if (arg == "--ip" && NextArg(argc, argv, &i, &value)) {
                ip = value;
            } else if (arg == "--port" && NextArg(argc, argv, &i, &value)) {
                port = std::stoi(value);
            } else if (arg == "--node_id" && NextArg(argc, argv, &i, &value)) {
                node_id = static_cast<int32_t>(std::stoi(value));
            } else if (arg == "--io_threads" && NextArg(argc, argv, &i, &value)) {
                io_threads = std::stoi(value);
            } else if (arg == "--worker_threads" && NextArg(argc, argv, &i, &value)) {
                worker_threads = std::stoi(value);
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage(argv[0]);
                return 0;
            } else {
                PrintUsage(argv[0]);
                return 1;
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "invalid argument: " << ex.what() << "\n";
        PrintUsage(argv[0]);
        return 1;
    }

    raft::logging::Init();
    raft::logging::SetCurrentThreadName("kv-main");

    kvserver::KvStore store;
    kvserver::KvServerService service(&store, node_id);

    RpcServer server;
    server.setIoThreadNum(io_threads);
    server.setWorkerThreads(static_cast<size_t>(worker_threads));
    server.RegisterService(&service);

    LOG_INFO() << "kv server starting node_id=" << node_id
               << " listen=" << ip << ":" << port
               << " io_threads=" << io_threads
               << " worker_threads=" << worker_threads;
    server.Run(ip, port);
    return 0;
}
