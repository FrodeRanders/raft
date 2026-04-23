cmake_minimum_required(VERSION 3.20)

project(raft_cpp LANGUAGES CXX)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED)
find_package(absl CONFIG REQUIRED)

set(RAFT_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/../../raft-wire/src/main/proto/raft.proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${GENERATED_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${RAFT_PROTO})

add_library(raftcpp_proto STATIC
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_include_directories(raftcpp_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(raftcpp_proto PUBLIC
    absl::base
    absl::check
    absl::log
    absl::log_internal_check_op
    absl::log_internal_message
    absl::strings
    protobuf::libprotobuf
)

add_library(raftcpp_transport STATIC
    src/raft_node.cpp
    src/rpc_handler.cpp
)

target_include_directories(raftcpp_transport PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(raftcpp_transport PUBLIC
    raftcpp_proto
)

target_compile_definitions(raftcpp_transport PUBLIC
    BOOST_ERROR_CODE_HEADER_ONLY
)

add_executable(raft_cpp_smoke
    src/main.cpp
)

target_link_libraries(raft_cpp_smoke PRIVATE
    raftcpp_transport
)

if(MSVC)
    target_compile_options(raftcpp_proto PRIVATE /W4)
    target_compile_options(raftcpp_transport PRIVATE /W4)
    target_compile_options(raft_cpp_smoke PRIVATE /W4)
else()
    target_compile_options(raftcpp_proto PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(raftcpp_transport PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(raft_cpp_smoke PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    target_compile_options(raftcpp_proto PRIVATE -Wno-nullability-extension)
endif()
