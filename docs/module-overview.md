## Module Overview

This repository is organized as a Maven reactor with a small set of focused modules.

### Dependency direction
- `raft-dist` assembles the runnable jar from runtime, applications, telemetry, and transport.
- application modules depend on shared runtime contracts plus the abstractions they need.
- runtime depends on core-facing abstractions and supporting modules, but not on concrete transport implementations.
- core depends on membership, state-machine, storage, and wire abstractions.
- transport, storage, membership, telemetry, and wire are reusable support modules.

### Modules
- `raft-wire`
  Protocol models, protobuf schema, generated wire types, and `ProtoMapper`.
- `raft-membership`
  Cluster configuration, membership-transition commands, and configuration snapshot wrapping.
- `raft-state-machine`
  Snapshot/apply/query state-machine contracts and simple adapters.
- `raft-storage`
  In-memory and file-backed log and persistent-state stores.
- `raft-core`
  `RaftNode`, message handling, and transport interfaces.
- `raft-transport-netty`
  Netty-based client/server transport implementation and transport factory.
- `raft-telemetry`
  Telemetry exporters and related support code.
- `raft-runtime`
  Bootstrap/runtime wiring, adapters, CLI support, authentication, authorization, and request policy hooks.
- `raft-app-kv`
  Key/value sample application.
- `raft-app-reference`
  Reference-data application.
- `raft-dist`
  Runnable distribution and entrypoint.
- `raft-tests`
  Reactor-level test suite.

### Current intent
- `raft-core` stays small and reusable.
- application-specific behavior lives in app modules, not in core.
- concrete transport and persistence implementations sit behind separate module boundaries.
