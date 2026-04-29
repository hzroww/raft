cmake_minimum_required(VERSION 3.14)
project(raft-kv-db CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# ---- muduo (shipped under third_party/muduo_install) ----
set(MUDUO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party/muduo_install)
find_path(MUDUO_INCLUDE_DIR muduo/net/TcpServer.h
    HINTS ${MUDUO_ROOT}/include
    NO_DEFAULT_PATH)
find_library(MUDUO_NET_LIB   NAMES muduo_net   HINTS ${MUDUO_ROOT}/lib NO_DEFAULT_PATH)
find_library(MUDUO_BASE_LIB  NAMES muduo_base  HINTS ${MUDUO_ROOT}/lib NO_DEFAULT_PATH)
if(NOT MUDUO_INCLUDE_DIR OR NOT MUDUO_NET_LIB OR NOT MUDUO_BASE_LIB)
    message(FATAL_ERROR
        "muduo not found under ${MUDUO_ROOT}. "
        "Make sure third_party/muduo_install contains include/ and lib/.")
endif()

include_directories(
    ${Protobuf_INCLUDE_DIRS}
    ${MUDUO_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${CMAKE_CURRENT_SOURCE_DIR}/log/include
    ${CMAKE_CURRENT_SOURCE_DIR}/rpc/include
    ${CMAKE_CURRENT_SOURCE_DIR}/kv_db/include
    ${CMAKE_CURRENT_SOURCE_DIR}/raft/include
    ${CMAKE_CURRENT_SOURCE_DIR}/kvserver/include
    # Populated when server/ client/ modules are added:
    ${CMAKE_CURRENT_SOURCE_DIR}/server/include
    ${CMAKE_CURRENT_SOURCE_DIR}/client/include
)

# ---------------------------------------------------------------------------
# Protobuf code generation
# ---------------------------------------------------------------------------
set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)

set(PROTO_SRCS
    ${PROTO_SRC_DIR}/rpc.pb.cc
    ${PROTO_SRC_DIR}/raft.pb.cc
    ${PROTO_SRC_DIR}/kv.pb.cc
    ${PROTO_SRC_DIR}/kv_log.pb.cc
)
set(PROTO_HDRS
    ${PROTO_SRC_DIR}/rpc.pb.h
    ${PROTO_SRC_DIR}/raft.pb.h
    ${PROTO_SRC_DIR}/kv.pb.h
    ${PROTO_SRC_DIR}/kv_log.pb.h
)

add_custom_command(
    OUTPUT  ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS    --cpp_out=${PROTO_SRC_DIR}
            -I${PROTO_SRC_DIR}
            ${PROTO_SRC_DIR}/rpc.proto
            ${PROTO_SRC_DIR}/raft.proto
            ${PROTO_SRC_DIR}/kv.proto
            ${PROTO_SRC_DIR}/kv_log.proto
    DEPENDS ${PROTO_SRC_DIR}/rpc.proto
            ${PROTO_SRC_DIR}/raft.proto
            ${PROTO_SRC_DIR}/kv.proto
            ${PROTO_SRC_DIR}/kv_log.proto
    COMMENT "Generating protobuf sources"
)

add_library(proto_gen STATIC ${PROTO_SRCS})
target_link_libraries(proto_gen PUBLIC ${Protobuf_LIBRARIES})

# ---------------------------------------------------------------------------
# kv_db library  (Arena + SkipList + LRU)
# ---------------------------------------------------------------------------
add_library(kv_db STATIC
    kv_db/src/arena.cpp
)
target_include_directories(kv_db PUBLIC kv_db/include)

# ---------------------------------------------------------------------------
# raft_log library  (lightweight logging)
# ---------------------------------------------------------------------------
add_library(raft_log STATIC
    log/src/log.cpp
)
target_include_directories(raft_log PUBLIC log/include)
target_link_libraries(raft_log PUBLIC Threads::Threads)

# ---------------------------------------------------------------------------
# rpc_framework library  (RPC layer on top of muduo)
# ---------------------------------------------------------------------------
set(RPC_SRCS
    rpc/src/thread_pool.cpp
    rpc/src/rpc_server.cpp
    rpc/src/rpc_channel.cpp
    rpc/src/rpc_application.cpp
)

add_library(rpc_framework STATIC ${RPC_SRCS})
target_include_directories(rpc_framework PUBLIC
    rpc/include
    proto
    ${MUDUO_INCLUDE_DIR}
)
target_link_libraries(rpc_framework PUBLIC
    proto_gen
    raft_log
    ${MUDUO_NET_LIB}
    ${MUDUO_BASE_LIB}
    Threads::Threads
)

# ---------------------------------------------------------------------------
# raft_core library  (raft node: communication + state machine)
# ---------------------------------------------------------------------------
add_library(raft_core STATIC
    raft/src/raft_node.cpp
)
target_include_directories(raft_core PUBLIC
    raft/include
    proto
)
target_link_libraries(raft_core PUBLIC
    rpc_framework
    proto_gen
    raft_log
    Threads::Threads
)

# ---------------------------------------------------------------------------
# kv_server library  (client-facing KV RPC + in-memory state machine)
# ---------------------------------------------------------------------------
add_library(kv_server STATIC
    kvserver/src/kv_store.cpp
    kvserver/src/kv_server_service.cpp
)
target_include_directories(kv_server PUBLIC
    kvserver/include
    kv_db/include
    proto
)
target_link_libraries(kv_server PUBLIC
    rpc_framework
    proto_gen
    kv_db
    raft_log
    Threads::Threads
)

# ---------------------------------------------------------------------------
# Example / smoke-test executables
# ---------------------------------------------------------------------------
add_executable(kv_server_main kvserver/src/kv_server_main.cpp)
target_link_libraries(kv_server_main PRIVATE kv_server)
set_target_properties(kv_server_main PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin
)

add_executable(coro_smoke test/coro_smoke.cpp)
target_link_libraries(coro_smoke PRIVATE rpc_framework)
set_target_properties(coro_smoke PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin
)

add_executable(raft_node_smoke test/raft_node_smoke.cpp)
target_link_libraries(raft_node_smoke PRIVATE raft_core)
set_target_properties(raft_node_smoke PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin
)
