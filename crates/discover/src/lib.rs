mod bindings;
mod collections;
mod parse;

pub use bindings::{Merged, merge_bindings};
pub use collections::merge_collections;
pub use parse::{Binding, Collection, Discovered, inline_bindings, parse};

/// Options of a discover merge.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Add newly-discovered bindings as disabled.
    pub update_only: bool,
    /// Reset a collection whose key is changed by the discover.
    pub reset_on_key_change: bool,
}

impl Options {
    /// Options of a discover of the capture `model`, which `is_live` if the
    /// capture has been published. Its `autoDiscover` stanza, if present,
    /// determines the options. Otherwise a live capture adds new bindings
    /// as disabled, and a new capture adds them as enabled.
    pub fn for_capture(model: &models::CaptureDef, is_live: bool) -> Self {
        match &model.auto_discover {
            Some(policy) => Self {
                update_only: !policy.add_new_bindings,
                reset_on_key_change: policy.evolve_incompatible_collections,
            },
            None => Self {
                update_only: is_live,
                reset_on_key_change: false,
            },
        }
    }
}
