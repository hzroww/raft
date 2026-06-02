#include "kv_raft_server.h"
#include "log.h"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

void PrintUsage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " [-c <config_file>] [--node_id <n>]\n"
              << "  -c  <file>  path to flat key=value config (default: kv_node.conf)\n"
              << "  --node_id <n>  override the node_id in the config file\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string config_path   = "kv_node.conf";
    int32_t     node_id_override = -1;

    try {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if ((arg == "-c" || arg == "--config") && i + 1 < argc) {
                config_path = argv[++i];
            } else if (arg == "--node_id" && i + 1 < argc) {
                node_id_override = static_cast<int32_t>(std::stoi(argv[++i]));
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
    raft::logging::SetCurrentThreadName("kv-cluster-main");

    try {
        auto cfg = kvserver::KvRaftServer::LoadConfigFromFile(config_path,
                                                              node_id_override);
        kvserver::KvRaftServer server(std::move(cfg));
        server.Start();
        server.WaitForShutdown();
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
