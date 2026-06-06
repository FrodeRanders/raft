#include "graft/app/cli.hpp"

#include "google/protobuf/stubs/common.h"

int main(int argc, char** argv) {
    // Protobuf has process-wide initialization/shutdown hooks.  Keeping them in
    // main() avoids leaking that dependency into applications embedding graft as
    // a library rather than running this demo/smoke-test executable.
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    const int exit_code = graft::run_cli(argc, argv);
    google::protobuf::ShutdownProtobufLibrary();
    return exit_code;
}
