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
        .client_mod_attribute("flow", "#[cfg(feature = \"flow_client\")]")
        .client_mod_attribute("materialize", "#[cfg(feature = \"materialize_client\")]")
        .client_mod_attribute("protocol", "#[cfg(feature = \"broker_client\")]")
        .extern_path(".capture", "::proto_flow::capture")
        .extern_path(".consumer", "::proto_gazette::consumer")
        .extern_path(".flow", "::proto_flow::flow")
        .extern_path(".materialize", "::proto_flow::materialize")
        .extern_path(".protocol", "::proto_gazette::broker")
        .extern_path(".recoverylog", "::proto_gazette::recoverylog")
        .server_mod_attribute("capture", "#[cfg(feature = \"capture_server\")]")
        .server_mod_attribute("consumer", "#[cfg(feature = \"consumer_server\")]")
        .server_mod_attribute("flow", "#[cfg(feature = \"flow_server\")]")
        .server_mod_attribute("materialize", "#[cfg(feature = \"materialize_server\")]")
        .server_mod_attribute("protocol", "#[cfg(feature = \"broker_server\")]")
        .compile(&proto_build, &b.proto_include())
        .expect("tonic build failed");
}
