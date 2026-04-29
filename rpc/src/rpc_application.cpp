#include "rpc_application.h"

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <cstdlib>

static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return {};
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

void RpcApplication::loadConfig(const std::string& path) {
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        throw std::runtime_error("RpcApplication: cannot open config file: " + path);
    }

    std::string line;
    while (std::getline(ifs, line)) {
        // Strip comments
        auto pos = line.find('#');
        if (pos != std::string::npos) line = line.substr(0, pos);

        auto eq = line.find('=');
        if (eq == std::string::npos) continue;

        std::string key = trim(line.substr(0, eq));
        std::string val = trim(line.substr(eq + 1));
        if (!key.empty()) {
            config_[key] = val;
        }
    }
}

void RpcApplication::Init(int argc, char** argv) {
    std::string configPath = "rpc.conf"; // default
    for (int i = 1; i < argc - 1; ++i) {
        if (std::string(argv[i]) == "-c" || std::string(argv[i]) == "--config") {
            configPath = argv[i + 1];
            break;
        }
    }
    GetInstance().loadConfig(configPath);
}

RpcApplication& RpcApplication::GetInstance() {
    static RpcApplication instance;
    return instance;
}

std::string RpcApplication::Get(const std::string& key,
                                  const std::string& defaultVal) const {
    auto it = config_.find(key);
    return it != config_.end() ? it->second : defaultVal;
}

std::string RpcApplication::GetServerIp() const {
    return Get("rpc_server_ip", "0.0.0.0");
}

int RpcApplication::GetServerPort() const {
    return std::stoi(Get("rpc_server_port", "8000"));
}

std::string RpcApplication::GetNodeIp(int index) const {
    return Get("node_" + std::to_string(index) + "_ip", "127.0.0.1");
}

int RpcApplication::GetNodePort(int index) const {
    return std::stoi(Get("node_" + std::to_string(index) + "_port", "8000"));
}

int RpcApplication::GetNodeCount() const {
    return std::stoi(Get("node_count", "0"));
}
