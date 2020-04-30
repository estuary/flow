mod error;
mod nodejs;

pub use error::Error;
pub use nodejs::Service as NodeJsService;

pub type Result<T> = std::result::Result<T, Error>;
