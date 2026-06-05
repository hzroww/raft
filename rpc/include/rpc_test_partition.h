#pragma once

// Test-only network partition hook.
//
// This module is compiled into rpc_framework but is completely inert at
// runtime unless the environment variable RAFT_TEST_PARTITION_FILE is set.
// When it is set, RpcChannel::CallMethod() calls IsPartitioned() before
// sending; if the call returns true the RPC is failed immediately with
// "test partition" instead of being put on the wire.
//
// Partition file format:
//   One rule per line:  <src_node_id> <dst_port>
//   Both values are decimal integers.
//   A call is blocked when the calling process's RAFT_TEST_NODE_ID matches
//   src_node_id AND the channel's remote port matches dst_port.
//
// All state is process-local.  Each server process (kv_cluster_main) reads
// RAFT_TEST_NODE_ID once at first use.  The partition file is re-read on
// every call so that the test process can enable/disable partitions at
// runtime simply by writing or truncating the file.

namespace rpc_test_partition {

// Returns true if the current process (identified by env RAFT_TEST_NODE_ID)
// is partitioned from the remote node listening on |dst_port|.
// Always returns false when RAFT_TEST_PARTITION_FILE is not set.
bool IsPartitioned(int dst_port);

}  // namespace rpc_test_partition
