fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=src/ffi/mod.rs");
    println!("cargo:rerun-if-changed=src/ffi/simd-doc.hpp");
    println!("cargo:rerun-if-changed=src/ffi/simdjson.cpp");
    println!("cargo:rerun-if-changed=src/ffi/simdjson.h");

    cxx_build::bridge("src/ffi/mod.rs")
        .cargo_metadata(true)
        .define("NDEBUG", None)
        .define("SIMDJSON_DISABLE_DEPRECATED_API", "1")
        .define("SIMDJSON_EXCEPTIONS", "1")
        // Disabled implementations for older architectures we don't need.
        .define("SIMDJSON_IMPLEMENTATION_WESTMERE", "0")
        .define("SIMDJSON_IMPLEMENTATION_PPC64", "0")
        .include(format!("{}/../..", env!("CARGO_MANIFEST_DIR")))
        .file("src/ffi/simdjson.cpp")
        .file("src/ffi/simd-doc.cpp")
        .std("c++2a")
        .compile("simdjson");
}
