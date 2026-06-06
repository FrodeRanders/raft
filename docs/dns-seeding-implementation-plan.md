# DNS Seeding Implementation Plan

## Goal
Make DNS SRV based discovery the production/default seeding model for Raft nodes while preserving the existing localhost development mode where each node is supplied explicitly as `id@host:port[/role]`.

DNS discovery is only a bootstrap input. Once a node has contacted the cluster, committed Raft membership remains authoritative.

## Runtime Contract
- Keep explicit peer specs for local development and current tests.
- Add DNS SRV startup using a service name such as `_raft._tcp.raft.local`.
- Add advertised endpoint configuration so a node can bind `0.0.0.0:7000` while publishing `raft-1.raft.local:7000`.
- Never bootstrap a new cluster implicitly on DNS failure.
- Require explicit bootstrap intent with `bootstrapNewCluster` or an initial cluster size policy.
- Use stable logical peer ids, defaulting to the first DNS label of each SRV target when no explicit id is supplied.

## Java Work
1. Add a seed provider abstraction in `raft-runtime`.
2. Implement static peer-spec seeding.
3. Implement DNS SRV seeding.
4. Extend `RuntimeConfiguration` and `raft-dist` startup to support:
   - `raft.node.id`
   - `raft.bind.host`
   - `raft.bind.port`
   - `raft.advertise.host`
   - `raft.advertise.port`
   - `raft.cluster.srv`
   - `raft.bootstrap.new-cluster`
5. Keep existing positional peer-spec startup working.
6. Add Java unit tests for peer id derivation, explicit static seeds, and DNS resolver behavior through an injectable resolver.

## C++ Work
1. Add the same seed provider concepts under `graft-cpp/include/graft/runtime`.
2. Preserve existing positional peer-spec commands.
3. Add option/env based startup for:
   - node id
   - bind endpoint
   - advertised endpoint
   - cluster SRV name
   - explicit bootstrap
4. Implement SRV lookup using a small resolver boundary. Prefer an injectable resolver for tests; decide separately whether production lookup uses c-ares or platform resolver APIs.
5. Add C++ unit tests for static seeding and resolver-provided SRV answers.

## Docker Work
1. Add a Java runtime Dockerfile for the `raft-dist` jar.
2. Add a C++ runtime Dockerfile for `graft_smoke`.
3. Add Docker Compose examples:
   - Java-only cluster with CoreDNS SRV records.
   - C++-only cluster with CoreDNS SRV records.
   - Mixed Java/C++ cluster with shared wire protocol.
4. Use the same environment variable contract in both images.

## Jepsen Work
1. Keep current localhost/static peer mode as the baseline.
2. Add a Docker/SRV mode only after Compose smoke is stable.
3. Run existing KV/register workloads against Java-only and mixed Java/C++ SRV-seeded clusters.
4. Extend nemesis coverage only after startup/discovery is reliable.

## Verification Order
1. Java unit tests for seed provider behavior.
2. Existing Java test suite.
3. C++ seed provider tests.
4. Existing C++ tests.
5. Docker Compose smoke tests.
6. Mixed Java/C++ smoke suite.
7. Jepsen local/static baseline.
8. Jepsen Docker/SRV mode.
