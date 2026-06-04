#define CATCH_CONFIG_RUNNER
#include <catch2/catch_session.hpp>

#include "google/protobuf/stubs/common.h"

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    const int result = Catch::Session().run(argc, argv);
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
