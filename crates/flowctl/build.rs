fn main() {
    // If we set CARGO_PKG_VERSION this way, then it will override the default value, which is
    // taken from the `version` in Cargo.toml.
    if let Ok(val) = std::env::var("FLOW_VERSION") {
        println!("cargo:rustc-env=CARGO_PKG_VERSION={}", val);
        eprintln!("using FLOW_VERSION: {}", val);
    }
    println!("cargo:rerun-if-env-changed=FLOW_VERSION");
}
