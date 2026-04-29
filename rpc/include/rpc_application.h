#pragma once

#include <string>
#include <unordered_map>
#include <stdexcept>

// Singleton that loads a flat key=value config file and exposes node addresses.
//
// Config file example (comments start with '#'):
//   rpc_server_ip   = 0.0.0.0
//   rpc_server_port = 8001
//
//   node_count = 3
//   node_0_ip  = 127.0.0.1
//   node_0_port = 8001
//   node_1_ip  = 127.0.0.1
//   node_1_port = 8002
//   node_2_ip  = 127.0.0.1
//   node_2_port = 8003
class RpcApplication {
public:
    static void Init(int argc, char** argv);
    static RpcApplication& GetInstance();

    std::string GetServerIp()   const;
    int         GetServerPort() const;

    // Peer address for Raft node |index| (0-based).
    std::string GetNodeIp(int index)   const;
    int         GetNodePort(int index) const;
    int         GetNodeCount()         const;

    std::string Get(const std::string& key,
                    const std::string& defaultVal = "") const;

private:
    RpcApplication() = default;
    RpcApplication(const RpcApplication&) = delete;
    RpcApplication& operator=(const RpcApplication&) = delete;

    void loadConfig(const std::string& path);

    std::unordered_map<std::string, std::string> config_;
};
