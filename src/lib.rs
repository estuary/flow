// Increase limit for use by select! in derive_api.rs.
#![recursion_limit = "256"]

pub mod catalog;
pub mod derive;
pub mod doc;
pub mod labels;
pub mod materialization;
pub mod runtime;
pub mod serve;
pub mod testing;
