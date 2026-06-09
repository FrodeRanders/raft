fn main() {
    prost_build::compile_protos(
        &["../../raft-wire/src/main/proto/raft.proto"],
        &["../../raft-wire/src/main/proto/"],
    )
    .unwrap();
}
