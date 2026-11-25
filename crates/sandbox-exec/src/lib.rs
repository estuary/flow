//! Runs one shell command inside a sandbox, relaying its stdio as frames
//! over the wrapper's own input and output streams. See README.md.

mod codec;
mod wrapper;

pub use codec::{Header, Input, MAX_PAYLOAD, Output, PROTOCOL_VERSION};
pub use wrapper::{Config, WORKSPACE_PATTERN, is_valid_workspace, run};
