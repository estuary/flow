extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(cbindgen::Config {
            language: cbindgen::Language::C,
            enumeration: cbindgen::EnumConfig {
                prefix_with_name: true,
                ..cbindgen::EnumConfig::default()
            },
            ..cbindgen::Config::default()
        })
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("bindings.h");
}
