#include "rpc_test_partition.h"

#include <cstdlib>
#include <fstream>
#include <string>

namespace rpc_test_partition {

namespace {

// Lazily read RAFT_TEST_NODE_ID once per process; -1 means "not set".
int LocalNodeId() {
    static int id = []() -> int {
        const char* env = std::getenv("RAFT_TEST_NODE_ID");
        if (!env || env[0] == '\0') return -1;
        try { return std::stoi(env); } catch (...) { return -1; }
    }();
    return id;
}

// Read RAFT_TEST_PARTITION_FILE on every call so the test process can
// toggle partitions without restarting the server processes.
const char* PartitionFile() {
    return std::getenv("RAFT_TEST_PARTITION_FILE");
}

}  // namespace

bool IsPartitioned(int dst_port) {
    int src = LocalNodeId();
    if (src < 0) return false;

    const char* path = PartitionFile();
    if (!path || path[0] == '\0') return false;

    std::ifstream f(path);
    if (!f.is_open()) return false;

    int rule_src = -1;
    int rule_dst = -1;
    while (f >> rule_src >> rule_dst) {
        if (rule_src == src && rule_dst == dst_port) return true;
    }
    return false;
}

}  // namespace rpc_test_partition
