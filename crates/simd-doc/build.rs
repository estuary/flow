fn main() {
    cxx_build::bridge("src/lib.rs")
        .cargo_metadata(true)
        .define("SIMDJSON_DISABLE_DEPRECATED_API", "1")
        .define("SIMDJSON_EXCEPTIONS", "1")
        .file("src/simd-doc.cpp")
        .file("src/simdjson.cpp")
        .flag_if_supported("-std=c++17")
        .flag_if_supported("/std:c++20") // error C7555: use of designated initializers requires at least '/std:c++20'
        /*
        .flag_if_supported("-DNDEBUG")
        .flag_if_supported("-DSIMD")
        .flag_if_supported("-O3")
        .flag_if_supported("/DNDEBUG")
        .flag_if_supported("/O2")
        */
        .compile("simdjson");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/simd-doc.cpp");
    println!("cargo:rerun-if-changed=src/simd-doc.h");
    println!("cargo:rerun-if-changed=src/simdjson.cpp");
    println!("cargo:rerun-if-changed=src/simdjson.hpp");
}

/*
fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/simdjson.cpp")
        .flag_if_supported("-std=c++14")
        .compile("simd-doc");

    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=src/simdjson.cc");
    println!("cargo:rerun-if-changed=src/simdjson_ffi.h");
}
*/
