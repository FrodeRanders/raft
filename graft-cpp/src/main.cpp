#include "graft/app/cli.hpp"

#include "google/protobuf/stubs/common.h"

int main(int argc, char** argv) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    const int exit_code = graft::run_cli(argc, argv);
    google::protobuf::ShutdownProtobufLibrary();
    return exit_code;
}
