FROM ubuntu:24.04 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       cmake \
       libabsl-dev \
       libboost-dev \
       libprotobuf-dev \
       protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY graft-cpp /src/graft-cpp
COPY raft-wire /src/raft-wire
RUN cmake -S graft-cpp -B graft-cpp/build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build graft-cpp/build --target graft_smoke

FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       libabsl-dev \
       libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /src/graft-cpp/build/graft_smoke /app/graft_smoke

EXPOSE 7000
ENTRYPOINT ["/app/graft_smoke"]
