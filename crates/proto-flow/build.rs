fn main() {
    let b = proto_build::Boilerplate::create();
    let proto_build = b.resolve_flow_targets();

    prost_build::Config::new()
        .out_dir(&b.src_dir)
        .file_descriptor_set_path(&b.descriptor_path)
        .compile_well_known_types()
        .extern_path(".consumer", "::proto_gazette::consumer")
        .extern_path(".google.protobuf", "::pbjson_types")
        .extern_path(".protocol", "::proto_gazette::broker")
        .extern_path(".recoverylog", "::proto_gazette::recoverylog")
        .compile_protos(&proto_build, &b.proto_include())
        .expect("failed to compile protobuf");

    pbjson_build::Builder::new()
        .register_descriptors(&std::fs::read(b.descriptor_path).expect("read descriptors"))
        .unwrap()
        .build(&[".flow", ".capture", ".materialize"])
        .expect("building pbjson");
}
