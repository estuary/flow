extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    //let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    for wrapper in ["rocksdb.h", "sqlite3.h"].iter() {
        // Tell cargo to invalidate the built crate whenever the wrappers change.
        println!("cargo:rerun-if-changed={}", wrapper);

        // The bindgen::Builder is the main entry point
        // to bindgen, and lets you build up options for
        // the resulting bindings.
        let bindings = bindgen::Builder::default()
            // The input header we would like to generate
            // bindings for.
            .header(*wrapper)
            // Tell cargo to invalidate the built crate whenever any of the
            // included header files changed.
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            // Finish the builder and generate the bindings.
            .generate()
            // Unwrap the Result and panic on failure.
            .expect("Unable to generate bindings");

        bindings
            .write_to_file(out_dir.join(wrapper).with_extension("rs"))
            .expect("Couldn't write bindings!");
    }
}
