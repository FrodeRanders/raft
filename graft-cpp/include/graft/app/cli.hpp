#pragma once

namespace graft {

// Small command-line front-end used by smoke tests and demos.
//
// The production-facing library types live under core/runtime/transport.  This
// function is deliberately kept as a thin shell entry point so applications can
// embed the Raft machinery without depending on argv parsing or demo commands.
int run_cli(int argc, char** argv);

} // namespace graft
