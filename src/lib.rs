// Increase limit for use by select! in derive_api.rs.
#![recursion_limit = "256"]
#![feature(btree_drain_filter)]
#![feature(map_first_last)]

pub mod catalog;
pub mod derive;
pub mod doc;
pub mod runtime;
pub mod serve;
pub mod testing;
