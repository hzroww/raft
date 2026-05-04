#include "kv_client.h"
#include "log.h"

#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace kvclient {

// ---------------------------------------------------------------------------
// Synchronisation helper: lets a callback on the RpcChannel IO thread unblock
// the caller thread that is waiting for the RPC to complete.
// ---------------------------------------------------------------------------
namespace {

class SyncDone : public google::protobuf::Closure {
public:
    void Run() override {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
        cv_.notify_one();
    }

    void Wait() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return done_; });
    }

private:
    std::mutex              mu_;
    std::condition_variable cv_;
    bool                    done_{false};
};

}  // namespace

// ---------------------------------------------------------------------------
// PickLocalIp
// ---------------------------------------------------------------------------

std::string KvClient::PickLocalIp() {
    struct ifaddrs* ifa_list = nullptr;
    if (::getifaddrs(&ifa_list) != 0 || ifa_list == nullptr) {
        return "127.0.0.1";
    }

    std::string result;
    for (struct ifaddrs* ifa = ifa_list; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }
        auto* sin = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
        uint32_t addr = ntohl(sin->sin_addr.s_addr);
        // Skip loopback (127.x.x.x).
        if ((addr >> 24) == 127) {
            continue;
        }
        char buf[INET_ADDRSTRLEN];
        if (::inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf))) {
            result = buf;
            break;
        }
    }
    ::freeifaddrs(ifa_list);

    return result.empty() ? "127.0.0.1" : result;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

KvClient::KvClient(std::vector<raft_core::PeerInfo> peers,
                   raft_core::NodeId                seed_id,
                   KvClientOptions                  opts)
    : peers_(std::move(peers)), opts_(std::move(opts)) {
    // Build a stable client identity: "<local-ip>-<seed-node-id>" to avoid
    // collisions between multiple clients on the same machine.
    client_id_ = PickLocalIp() + "-" + std::to_string(seed_id);

    if (!ConnectTo(seed_id)) {
        throw std::invalid_argument(
            "KvClient: seed_id " + std::to_string(seed_id) +
            " not found in peers list");
    }
    LOG_INFO() << "kv client initialised client_id=" << client_id_
               << " seed=" << seed_id;
}

// ---------------------------------------------------------------------------
// ConnectTo
// ---------------------------------------------------------------------------

bool KvClient::ConnectTo(raft_core::NodeId peer_id) {
    const raft_core::PeerInfo* target = nullptr;
    for (const auto& p : peers_) {
        if (p.id == peer_id) {
            target = &p;
            break;
        }
    }
    if (!target) {
        LOG_WARN() << "kv client: peer_id=" << peer_id << " not in peer table";
        return false;
    }

    // Destroy old channel first (RpcChannel dtor joins its IO thread).
    stub_.reset();
    channel_.reset();

    channel_ = std::make_unique<RpcChannel>(target->ip, target->port);
    channel_->setTimeoutMs(opts_.rpc_timeout_ms);
    channel_->waitUntilConnected(opts_.connect_wait_ms);

    stub_ = std::make_unique<kv::KvServerRpc_Stub>(channel_.get());
    current_peer_id_ = peer_id;

    LOG_INFO() << "kv client connected to peer=" << peer_id
               << " addr=" << target->ip << ":" << target->port;
    return true;
}

// ---------------------------------------------------------------------------
// SyncCall
// ---------------------------------------------------------------------------

template <typename Req, typename Resp>
bool KvClient::SyncCall(
    void (kv::KvServerRpc_Stub::*method)(google::protobuf::RpcController*,
                                          const Req*, Resp*,
                                          google::protobuf::Closure*),
    const Req& req, Resp* resp) {
    RpcController ctl;
    SyncDone      done;
    (stub_.get()->*method)(&ctl, &req, resp, &done);
    done.Wait();
    return !ctl.Failed();
}

// ---------------------------------------------------------------------------
// CallWithRetry
// ---------------------------------------------------------------------------

template <typename Req, typename Resp>
KvResult KvClient::CallWithRetry(
    std::function<Req(int64_t)> build_req,
    std::function<KvResult(const Resp&)> extract,
    void (kv::KvServerRpc_Stub::*method)(google::protobuf::RpcController*,
                                          const Req*, Resp*,
                                          google::protobuf::Closure*)) {
    // Allocate a requestId exactly once for this user-level call.
    int64_t req_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);

    int attempt = 0;
    while (true) {
        // --- Build and send the RPC ---
        Req  req  = build_req(req_id);
        Resp resp;

        bool rpc_ok = SyncCall(method, req, &resp);

        if (rpc_ok && resp.success()) {
            return extract(resp);
        }

        // --- Classify the failure ---
        bool retryable = false;
        raft_core::NodeId redirect_to = raft_core::kNoNode;

        if (!rpc_ok) {
            // Transport/timeout failure; retry to same or any peer.
            retryable = true;
        } else if (resp.error() == "not leader") {
            retryable    = true;
            redirect_to  = static_cast<raft_core::NodeId>(resp.leaderid());
        } else if (resp.error() == "commit timeout") {
            // The command may already be committed; replaying with the same
            // requestId is safe because the server deduplicates by (clientId,
            // requestId).
            retryable = true;
        }
        // Validation errors ("key must not be empty", "requestId must be
        // positive", …) are not retryable.

        if (!retryable || attempt >= opts_.max_retries) {
            return {.ok    = false,
                    .error = rpc_ok ? resp.error() : "rpc failed"};
        }
        ++attempt;

        // --- Leader redirection ---
        if (redirect_to != raft_core::kNoNode &&
            redirect_to != current_peer_id_) {
            LOG_INFO() << "kv client: redirecting to leader=" << redirect_to
                       << " attempt=" << attempt;
            if (!ConnectTo(redirect_to)) {
                // Leader id unknown in our table; try a round-robin fallback.
                raft_core::NodeId fallback = raft_core::kNoNode;
                for (const auto& p : peers_) {
                    if (p.id != current_peer_id_) {
                        fallback = p.id;
                        break;
                    }
                }
                if (fallback != raft_core::kNoNode) {
                    ConnectTo(fallback);
                }
            }
        } else if (!rpc_ok) {
            // Transport failure; cycle to the next peer in the table.
            raft_core::NodeId next = raft_core::kNoNode;
            bool found_current     = false;
            for (const auto& p : peers_) {
                if (found_current) {
                    next = p.id;
                    break;
                }
                if (p.id == current_peer_id_) {
                    found_current = true;
                }
            }
            if (next == raft_core::kNoNode && !peers_.empty()) {
                next = peers_.front().id;
            }
            if (next != raft_core::kNoNode && next != current_peer_id_) {
                ConnectTo(next);
            }
        }

        std::this_thread::sleep_for(opts_.retry_interval);
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

KvResult KvClient::Put(const std::string& key, const std::string& value) {
    return CallWithRetry<kv::PutRequest, kv::PutResponse>(
        [&](int64_t rid) {
            kv::PutRequest req;
            req.set_key(key);
            req.set_value(value);
            req.set_clientid(client_id_);
            req.set_requestid(rid);
            return req;
        },
        [](const kv::PutResponse& r) -> KvResult {
            return {.ok = r.success(), .error = r.error()};
        },
        &kv::KvServerRpc_Stub::Put);
}

KvResult KvClient::Get(const std::string& key) {
    return CallWithRetry<kv::GetRequest, kv::GetResponse>(
        [&](int64_t rid) {
            kv::GetRequest req;
            req.set_key(key);
            req.set_clientid(client_id_);
            req.set_requestid(rid);
            return req;
        },
        [](const kv::GetResponse& r) -> KvResult {
            return {.ok = r.success(), .value = r.value(), .error = r.error()};
        },
        &kv::KvServerRpc_Stub::Get);
}

KvResult KvClient::Delete(const std::string& key) {
    return CallWithRetry<kv::DeleteRequest, kv::DeleteResponse>(
        [&](int64_t rid) {
            kv::DeleteRequest req;
            req.set_key(key);
            req.set_clientid(client_id_);
            req.set_requestid(rid);
            return req;
        },
        [](const kv::DeleteResponse& r) -> KvResult {
            return {.ok = r.success(), .error = r.error()};
        },
        &kv::KvServerRpc_Stub::Delete);
}

}  // namespace kvclient
