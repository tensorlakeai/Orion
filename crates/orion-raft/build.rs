fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);

    tonic_prost_build::configure().compile_with_config(
        config,
        &["../../proto/orion/raft/v1/raft.proto"],
        &["../../proto"],
    )?;
    Ok(())
}
