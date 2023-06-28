mod task_runtime;
pub use task_runtime::TaskRuntime;

mod task_service;
pub use task_service::TaskService;

pub mod derive;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
