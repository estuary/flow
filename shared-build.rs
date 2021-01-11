// The purpose of this is to use the value of the `FLOW_VERSION` variable in order to set
// `CARGO_PKG_VERSION`. `CARGO_PKG_VERSION` is the standard env variable that cargo always sets
// to the current version. Normally that version is taken from the Cargo.toml file, but since
// we want to determine the version dynamically, we need to set this variable manually.
//
// Note that it does not work to set/export `CARGO_PKG_VERSION` in any other way besides in this
// build script. Cargo will simply overwrite the variable, unless it's set by the build script of
// the current crate.
fn main() {
    if let Ok(vers) = std::env::var("FLOW_VERSION") {
        println!("cargo:rustc-env=CARGO_PKG_VERSION={}", vers);
    }
}
