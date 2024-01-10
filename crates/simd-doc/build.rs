fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=src/ffi/mod.rs");
    println!("cargo:rerun-if-changed=src/ffi/simd-doc.h");
    println!("cargo:rerun-if-changed=src/ffi/simdjson.cpp");
    println!("cargo:rerun-if-changed=src/ffi/simdjson.h");

    cxx_build::bridge("src/ffi/mod.rs")
        .cargo_metadata(true)
        .define("NDEBUG", None)
        .define("SIMDJSON_DISABLE_DEPRECATED_API", "1")
        .define("SIMDJSON_EXCEPTIONS", "1")
        // Build implementations for ICELAKE, HASWELL, and ARM64 the (default).
        .define("SIMDJSON_IMPLEMENTATION_WESTMERE", "0")
        .define("SIMDJSON_IMPLEMENTATION_PPC64", "0")
        .define("SIMDJSON_IMPLEMENTATION_FALLBACK", "0")
        .file("src/ffi/simdjson.cpp")
        .std("c++20")
        .compile("simdjson");
}
