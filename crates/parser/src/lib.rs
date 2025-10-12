mod config;
mod decorate;
mod format;
mod input;

pub use self::config::{
    Compression, ErrorThreshold, Format, JsonPointer, ParseConfig, character_separated, protobuf,
};
pub use self::format::{Output, ParseError, Parser, parse};
pub use self::input::Input;

#[cfg(test)]
mod test {
    pub fn path(rel: impl AsRef<std::path::Path>) -> std::path::PathBuf {
        let cargo_manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let cargo_workspace = cargo_manifest.parent().unwrap().parent().unwrap();

        // Bazel: detect test execution context and compute relative path.
        if let (Ok(bazel_srcdir), Ok(bazel_ws)) = (
            std::env::var("TEST_SRCDIR"),
            std::env::var("TEST_WORKSPACE"),
        ) {
            std::path::PathBuf::from(bazel_srcdir)
                .join(bazel_ws)
                .join(cargo_manifest.strip_prefix(cargo_workspace).unwrap())
                .join(rel)
        } else {
            cargo_manifest.join(rel)
        }
    }
}
