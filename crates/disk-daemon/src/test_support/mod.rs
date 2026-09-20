//! Infrastructure the crate's own tests need, and nothing a case asserts.
//!
//! Two kinds of test here reach outside the process. [`device`] serves a real `ublk`
//! device, which needs privilege and therefore a child process. [`broker`] runs
//! against a real Gazette broker, which needs a data plane. Both are prerequisites of
//! the host rather than of the code, and both fail with what to do about it rather
//! than skipping.
//!
//! [`allocated`] needs nothing of the host. It is here because the replay, buffer,
//! and broker cases all report a rebuilt image the same way.
//!
//! The cases themselves live beside the code they cover, because that is where a
//! reader of `owner/` or `journal/` looks for what its module promises. A module
//! whose cases outgrew it keeps them in a sibling file of the same path, such as
//! `chunk.rs` with `chunk/test.rs`, so a case's name does not move when its file
//! does.
//!
//! Probing a journal is `e2e_support`'s, which the black-box suite uses too.
//!
//! That suite, in `tests/`, covers the daemon as a whole, over [`crate::client`] and
//! its journals, and shares nothing with this.

pub mod broker;
pub mod device;

/// Each allocated block of `image`, paired with the byte it is filled with: a
/// readable report of what a replay left allocated.
pub fn allocated(image: &crate::image::Image) -> Vec<(u32, u8)> {
    let mut block = vec![0u8; crate::BLOCK_SIZE as usize];

    image
        .allocated()
        .iter()
        .map(|index| {
            image.read_at(index, &mut block).unwrap();
            (index, block[0])
        })
        .collect()
}
