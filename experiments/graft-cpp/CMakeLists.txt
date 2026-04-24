cmake_minimum_required(VERSION 3.20)

project(graft LANGUAGES CXX)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(Catch2 3 QUIET)

set(RAFT_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/../../raft-wire/src/main/proto/raft.proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${GENERATED_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${RAFT_PROTO})

add_library(graft_proto STATIC
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_include_directories(graft_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(graft_proto PUBLIC
    absl::base
    absl::check
    absl::log
    absl::log_internal_check_op
    absl::log_internal_message
    absl::strings
    protobuf::libprotobuf
)

add_library(graft_transport STATIC
    src/app/cli.cpp
    src/core/raft_node.cpp
    src/runtime/raft_runtime.cpp
    src/runtime/rpc_handler.cpp
    src/storage/persistent_state_store.cpp
    src/transport/raft_server.cpp
)

target_include_directories(graft_transport PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(graft_transport PUBLIC
    graft_proto
)

target_compile_definitions(graft_transport PUBLIC
    BOOST_ERROR_CODE_HEADER_ONLY
)

add_executable(graft_smoke
    src/main.cpp
)

target_link_libraries(graft_smoke PRIVATE
    graft_transport
)

if(Catch2_FOUND)
    include(Catch)

    add_executable(graft_unit_tests
        tests/persistent_state_store_tests.cpp
        tests/raft_node_election_tests.cpp
        tests/raft_node_membership_tests.cpp
        tests/raft_node_state_machine_tests.cpp
        tests/test_main.cpp
    )

    target_link_libraries(graft_unit_tests PRIVATE
        graft_transport
        Catch2::Catch2
    )

    catch_discover_tests(graft_unit_tests)
else()
    message(WARNING "Catch2 v3 was not found; graft_unit_tests will not be built. Install it with e.g. `sudo port install catch2`.")
endif()

if(MSVC)
    target_compile_options(graft_proto PRIVATE /W4)
    target_compile_options(graft_transport PRIVATE /W4)
    target_compile_options(graft_smoke PRIVATE /W4)
    if(Catch2_FOUND)
        target_compile_options(graft_unit_tests PRIVATE /W4)
    endif()
else()
    target_compile_options(graft_proto PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(graft_transport PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(graft_smoke PRIVATE -Wall -Wextra -Wpedantic)
    if(Catch2_FOUND)
        target_compile_options(graft_unit_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    target_compile_options(graft_proto PRIVATE -Wno-nullability-extension)
endif()
