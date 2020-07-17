mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub mod combine;
pub mod extract;
pub mod nodejs;
pub mod state;
pub mod transform;

mod service;
pub use service::build as build_service;

mod framing;
pub use framing::{parse_record_batch, RecordBatch};

pub use nodejs::Service as NodeJsHandle;
