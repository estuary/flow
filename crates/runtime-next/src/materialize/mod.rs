//! Materialize module: leader-side coordination (`leader`) and per-shard
//! transaction loop (`shard`) live in sibling submodules.

pub mod leader;
pub mod shard;

//#[allow(dead_code)]
//mod shard_poc;
