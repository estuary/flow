//! Per-binding backfill-truncation boundaries: each binding tracks its latest
//! observed truncation boundary clock.
//!
//! Boundaries classify ingress rather than tagging combiner entries: the scan
//! drops source documents below the boundary, Loaded rows split into fresh vs.
//! stale existence-only fronts, and a boundary advance triggers
//! `Accumulator::truncate` to reclassify what's already accumulated.

use proto_gazette::uuid;

#[derive(Debug)]
pub(super) struct Boundaries(Vec<Option<uuid::Clock>>);

impl Boundaries {
    pub(super) fn new(n_bindings: usize) -> Self {
        Self(vec![None; n_bindings])
    }

    /// Observe `binding`'s backfill boundary, returning whether it genuinely
    /// advanced. Begins are re-delivered on every Load and only ever move
    /// forward, so a duplicate or older begin returns `false` and is a no-op.
    pub(super) fn observe_begin(&mut self, binding: usize, begin: uuid::Clock) -> bool {
        let latest = &mut self.0[binding];
        if latest.is_some_and(|l| begin <= l) {
            return false;
        }
        *latest = Some(begin);
        true
    }

    /// Whether `binding` has observed a truncation boundary. Without one no
    /// Loaded row needs classification, so a Loaded row's UUID clock is consulted
    /// only once its binding is truncating.
    pub(super) fn has_boundary(&self, binding: usize) -> bool {
        self.0[binding].is_some()
    }

    /// Whether `clock` is stale for `binding`: true iff a boundary exists and
    /// `clock` is below it. Without a boundary, nothing is stale.
    pub(super) fn is_stale(&self, binding: usize, clock: uuid::Clock) -> bool {
        matches!(self.0[binding], Some(boundary) if clock < boundary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock(v: u64) -> uuid::Clock {
        uuid::Clock::from_u64(v)
    }

    #[test]
    fn no_boundary_is_never_stale() {
        let b = Boundaries::new(1);
        assert!(!b.has_boundary(0));
        for c in [1, 100, u64::MAX] {
            assert!(!b.is_stale(0, clock(c)));
        }
    }

    #[test]
    fn boundary_classifies_by_clock() {
        let mut b = Boundaries::new(1);
        assert!(b.observe_begin(0, clock(500))); // advanced
        assert!(b.has_boundary(0));
        assert!(b.is_stale(0, clock(499))); // below → stale
        assert!(!b.is_stale(0, clock(500))); // at boundary → fresh
        assert!(!b.is_stale(0, clock(999)));

        // Advancing the boundary reclassifies the prior boundary clock as stale.
        assert!(b.observe_begin(0, clock(800)));
        assert!(b.is_stale(0, clock(500)));
        assert!(b.is_stale(0, clock(799)));
        assert!(!b.is_stale(0, clock(800)));
    }

    #[test]
    fn repeated_or_older_begin_does_not_advance() {
        let mut b = Boundaries::new(1);
        assert!(b.observe_begin(0, clock(500)));
        assert!(!b.observe_begin(0, clock(500))); // duplicate
        assert!(!b.observe_begin(0, clock(300))); // older
        assert!(!b.is_stale(0, clock(500)));
    }

    #[test]
    fn bindings_advance_independently() {
        let mut b = Boundaries::new(2);
        assert!(b.observe_begin(0, clock(500)));
        assert!(b.is_stale(0, clock(499)));
        assert!(!b.has_boundary(1));
        assert!(!b.is_stale(1, clock(1))); // binding 1 has no boundary
    }
}
