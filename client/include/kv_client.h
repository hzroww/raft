#pragma once

#include "kv.pb.h"
#include "raft_types.h"
#include "rpc_channel.h"
#include "rpc_controller.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace kvclient {

// Tunables passed to KvClient at construction time.
struct KvClientOptions {
    // Maximum number of retry attempts per user call (not counting the first try).
    int max_retries = 5;

    // Time to sleep between consecutive retries.
    std::chrono::milliseconds retry_interval{100};

    // Per-RPC timeout forwarded to RpcChannel.
    int rpc_timeout_ms = 3000;

    // How long to wait for the TCP connection to be established.
    int connect_wait_ms = 3000;
};

// Result returned from every KvClient operation.
struct KvResult {
    bool        ok    = false;
    std::string value;   // populated for Get on success
    std::string error;
};

// Synchronous KV client that speaks to a Raft-backed KV cluster.
//
// Thread safety: KvClient is NOT thread-safe. Use one instance per thread
// or guard externally.
//
// Client identity:
//   clientId  = <local IP>:<instance-unique suffix> picked at construction
//               using getifaddrs; falls back to 127.0.0.1 when no non-loopback
//               interface is available.
//   requestId = monotonically increasing int64, allocated once per user-level
//               operation and held constant across all retries of that call.
class KvClient {
public:
    // |peers|    – full cluster peer list (id, ip, port). Used for leader
    //             redirection after "not leader" responses.
    // |seed_id|  – NodeId of the peer to connect to initially.  Must exist
    //             in |peers|.
    explicit KvClient(std::vector<raft_core::PeerInfo> peers,
                      raft_core::NodeId                seed_id,
                      KvClientOptions                  opts = {});
    ~KvClient() = default;

    KvClient(const KvClient&)            = delete;
    KvClient& operator=(const KvClient&) = delete;

    KvResult Put(const std::string& key, const std::string& value);
    KvResult Get(const std::string& key);
    KvResult Delete(const std::string& key);

    // Exposed for testing; returns the client identity string.
    const std::string& ClientId() const { return client_id_; }

private:
    // Rebuild the RpcChannel + Stub pointed at |peer_id|.
    // Returns false if the peer is not in the peer table.
    bool ConnectTo(raft_core::NodeId peer_id);

    // Execute a synchronous RPC via the current stub, honouring the per-RPC
    // timeout.  Returns false if the RpcController reports failure.
    template <typename Req, typename Resp>
    bool SyncCall(void (kv::KvServerRpc_Stub::*method)(
                      google::protobuf::RpcController*,
                      const Req*, Resp*, google::protobuf::Closure*),
                  const Req& req, Resp* resp);

    // Core retry loop shared by Put/Get/Delete.
    // |build_req| populates the request with the per-call requestId already set.
    // |extract|   extracts the KvResult from the response after a successful RPC.
    template <typename Req, typename Resp>
    KvResult CallWithRetry(
        std::function<Req(int64_t)> build_req,
        std::function<KvResult(const Resp&)> extract,
        void (kv::KvServerRpc_Stub::*method)(
            google::protobuf::RpcController*,
            const Req*, Resp*, google::protobuf::Closure*));

    // Choose a non-loopback IPv4 address via getifaddrs; returns "127.0.0.1"
    // if none is found.
    static std::string PickLocalIp();

    std::vector<raft_core::PeerInfo>       peers_;
    KvClientOptions                        opts_;
    std::string                            client_id_;
    std::atomic<int64_t>                   next_request_id_{1};

    raft_core::NodeId                      current_peer_id_;
    std::unique_ptr<RpcChannel>            channel_;
    std::unique_ptr<kv::KvServerRpc_Stub>  stub_;
};

}  // namespace kvclient
