FROM rust:1-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY raft-wire /src/raft-wire
COPY graft-rust /src/graft-rust

RUN cargo build --release --manifest-path graft-rust/Cargo.toml -p graft-app-kv

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /src/graft-rust/target/release/graft-kv /app/graft-kv

COPY docker/entrypoint-rust.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 7000
ENTRYPOINT ["/app/entrypoint.sh"]
