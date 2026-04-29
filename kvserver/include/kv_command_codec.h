#pragma once

#include "kv_log.pb.h"

#include <string>

namespace kvserver {

inline std::string EncodeCommand(const kv::KvCommand& command) {
    return command.SerializeAsString();
}

inline bool DecodeCommand(const std::string& payload, kv::KvCommand* command) {
    return command && command->ParseFromString(payload);
}

inline kv::KvCommand MakePutCommand(const std::string& key,
                                    const std::string& value,
                                    const std::string& client_id,
                                    int64_t            request_id) {
    kv::KvCommand command;
    command.set_op(kv::KvCommand::PUT);
    command.set_key(key);
    command.set_value(value);
    command.set_clientid(client_id);
    command.set_requestid(request_id);
    return command;
}

inline kv::KvCommand MakeDeleteCommand(const std::string& key,
                                       const std::string& client_id,
                                       int64_t            request_id) {
    kv::KvCommand command;
    command.set_op(kv::KvCommand::DELETE);
    command.set_key(key);
    command.set_clientid(client_id);
    command.set_requestid(request_id);
    return command;
}

}  // namespace kvserver
