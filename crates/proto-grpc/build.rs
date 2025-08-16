#[cfg(feature = "generate")]
fn main() {
    let b = proto_build::Boilerplate::create();

    // Generating gRPC stubs for Flow also brings in Gazette protobufs,
    // and generates their stubs.
    let proto_build = b.resolve_flow_targets();

    tonic_build::configure()
        .out_dir(&b.src_dir)
        .build_client(true)
        .build_server(true)
        .client_mod_attribute("capture", "#[cfg(feature = \"capture_client\")]")
        .client_mod_attribute("consumer", "#[cfg(feature = \"consumer_client\")]")
        .client_mod_attribute("derive", "#[cfg(feature = \"derive_client\")]")
        .client_mod_attribute("flow", "#[cfg(feature = \"flow_client\")]")
        .client_mod_attribute("materialize", "#[cfg(feature = \"materialize_client\")]")
        .client_mod_attribute("protocol", "#[cfg(feature = \"broker_client\")]")
        .client_mod_attribute("runtime", "#[cfg(feature = \"runtime_client\")]")
        .extern_path(".capture", "::proto_flow::capture")
        .extern_path(".consumer", "::proto_gazette::consumer")
        .extern_path(".derive", "::proto_flow::derive")
        .extern_path(".flow", "::proto_flow::flow")
        .extern_path(".materialize", "::proto_flow::materialize")
        .extern_path(".ops", "::proto_flow::ops")
        .extern_path(".protocol", "::proto_gazette::broker")
        .extern_path(".recoverylog", "::proto_gazette::recoverylog")
        .extern_path(".runtime", "::proto_flow::runtime")
        .server_mod_attribute("capture", "#[cfg(feature = \"capture_server\")]")
        .server_mod_attribute("consumer", "#[cfg(feature = \"consumer_server\")]")
        .server_mod_attribute("derive", "#[cfg(feature = \"derive_server\")]")
        .server_mod_attribute("flow", "#[cfg(feature = \"flow_server\")]")
        .server_mod_attribute("materialize", "#[cfg(feature = \"materialize_server\")]")
        .server_mod_attribute("protocol", "#[cfg(feature = \"broker_server\")]")
        .server_mod_attribute("runtime", "#[cfg(feature = \"runtime_server\")]")
        .compile_protos(&proto_build, &b.proto_include())
        .expect("tonic build failed");
}

#[cfg(not(feature = "generate"))]
fn main() {}
