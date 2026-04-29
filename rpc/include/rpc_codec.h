#pragma once

// Wire frame layout (all fields big-endian):
//
//   ┌──────────────────┬─────────────────────────────┬──────────────┐
//   │  header_pb_size  │      RpcHeader (protobuf)   │  body (pb)   │
//   │     4 bytes      │    header_pb_size bytes     │ args_size B  │
//   └──────────────────┴─────────────────────────────┴──────────────┘
//
// args_size is stored inside RpcHeader.args_size.
// request_id is stored inside RpcHeader.request_id and echoed in responses.

#include "rpc.pb.h"   // mprrpc::RpcHeader (generated from proto/rpc.proto)

#include <muduo/net/Buffer.h>

#include <string>
#include <arpa/inet.h>
#include <cstring>

namespace rpc_codec {

// Encode a request or response into a wire frame.
inline std::string encode(const mprrpc::RpcHeader& header,
                          const std::string&       body) {
    std::string header_str;
    header.SerializeToString(&header_str);

    uint32_t header_size_net = htonl(static_cast<uint32_t>(header_str.size()));

    std::string frame;
    frame.reserve(4 + header_str.size() + body.size());
    frame.append(reinterpret_cast<const char*>(&header_size_net), 4);
    frame.append(header_str);
    frame.append(body);
    return frame;
}

// Try to decode one complete frame from *buf.
// Returns true and fills *header + *body on success;
// returns false and leaves *buf untouched if the frame is incomplete.
inline bool tryDecode(muduo::net::Buffer* buf,
                      mprrpc::RpcHeader*  header,
                      std::string*        body) {
    if (buf->readableBytes() < 4) return false;

    uint32_t header_size_net;
    std::memcpy(&header_size_net, buf->peek(), 4);
    uint32_t header_size = ntohl(header_size_net);

    // Sanity guard: header shouldn't exceed 64 KiB.
    if (header_size == 0 || header_size > 64 * 1024) return false;

    if (buf->readableBytes() < 4 + header_size) return false;

    if (!header->ParseFromArray(buf->peek() + 4,
                                static_cast<int>(header_size))) {
        return false;
    }

    uint32_t args_size = header->args_size();
    if (buf->readableBytes() < 4 + header_size + args_size) return false;

    buf->retrieve(4 + header_size);
    *body = buf->retrieveAsString(args_size);
    return true;
}

} // namespace rpc_codec
