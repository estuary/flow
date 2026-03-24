use std::collections::BTreeMap;

use crate::ResourcePath;

/// Represents a capture binding that was added, removed, or modified by a
/// discover.
#[derive(Debug, PartialEq, Clone)]
pub struct Changed {
    /// The name of the target collection for the binding.
    pub target: crate::Collection,
    /// Whether the binding is disabled.
    pub disable: bool,
    /// Optional reason describing a non-obvious change that was made.
    pub reason: Option<String>,
}
/// Represents a set of changes resulting from a discover.
pub type Changes = BTreeMap<ResourcePath, Changed>;
