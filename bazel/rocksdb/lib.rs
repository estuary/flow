// Wrapper for librocksdb-sys that includes vendored bindings
// This replaces the build.rs-generated bindings with pre-generated ones

#![allow(
    clippy::all,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]

include!("bindings.rs");
