#[cfg(feature = "generate")]
fn main() {
    let b = proto_build::Boilerplate::create();
    let proto_build = b.resolve_gazette_targets();

    prost_build::Config::new()
        .out_dir(&b.src_dir)
        .bytes(&["AppendRequest.content", "ReadResponse.content"])
        .file_descriptor_set_path(&b.descriptor_path)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&proto_build, &b.proto_include())
        .expect("failed to compile protobuf");

    pbjson_build::Builder::new()
        .out_dir(&b.src_dir)
        .register_descriptors(&std::fs::read(b.descriptor_path).expect("read descriptors"))
        .unwrap()
        .build(&[".protocol", ".consumer", ".recoverylog"])
        .expect("building pbjson");
}

#[cfg(not(feature = "generate"))]
fn main() {}
