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
        test_support::test_resource_path!(rel.as_ref())
    }
}
