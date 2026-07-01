fn main() {
    prost_build::compile_protos(
        &["proto/raft.proto"],
        &["proto/"],
    )
    .unwrap();
}
