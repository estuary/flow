//! Installs the interned collections of a message which this crate built.
//!
//! Builders in this crate always construct specs and Validate requests in
//! indirect form, interning each binding's collection as the binding is built.
//! The `indirect-specs` shard flag then gates a single post-process: when it's
//! unset, `install_*` converts the message to inline form, which is
//! byte-for-byte the encoding that predates indirect specs.
//!
//! Inlining is transitional. Once indirect form is universal, the
//! `indirect_specs` argument and its branch are deleted.

use proto_flow::{capture, derive, flow, materialize};

pub use proto_flow::linked::Interner;

/// Define `$install`, which installs `interner` into `$msg` and then
/// converts it to inline form unless `indirect_specs`.
macro_rules! install {
    ($install:ident, $msg:ty) => {
        pub fn $install(msg: &mut $msg, interner: Interner, indirect_specs: bool) {
            msg.install(interner);

            if !indirect_specs {
                msg.inline();
            }
        }
    };
}

install!(install_capture_spec, flow::CaptureSpec);
install!(install_materialization_spec, flow::MaterializationSpec);
install!(install_derivation, flow::collection_spec::Derivation);
install!(install_capture_validate, capture::request::Validate);
install!(install_materialize_validate, materialize::request::Validate);
install!(install_derive_validate, derive::request::Validate);
