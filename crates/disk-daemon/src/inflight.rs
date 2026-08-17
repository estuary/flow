//! Serialization of overlapping mutations against one disk's image.

/// Block ranges of the mutations one disk has in flight, keyed by the device
/// request tag which owns each.
///
/// Chunks reach the journal in the order the owner accepted them. The image must
/// be modified in that same order, or a rebuilt disk would not match what the
/// client saw. Only an overlap can reorder two mutations, and a filesystem does
/// not issue overlapping concurrent writes. This therefore holds only the handful
/// of ranges a device queue has open, and it is expected never to refuse one. A
/// linear scan over a `Vec` is the right shape for that.
#[derive(Default)]
pub struct InFlight {
    active: Vec<(u16, std::ops::Range<u32>)>,
    /// Mutations refused by [`InFlight::begin`], in arrival order.
    waiting: std::collections::VecDeque<(u16, std::ops::Range<u32>)>,
}

impl InFlight {
    /// Begin `tag` over `range`. `false` means an earlier mutation covers one
    /// of those blocks, and `tag` is returned by a later [`InFlight::end`].
    pub fn begin(&mut self, tag: u16, range: std::ops::Range<u32>) -> bool {
        if self.blocked(&range, self.waiting.len()) {
            self.waiting.push_back((tag, range));
            return false;
        }
        self.active.push((tag, range));
        true
    }

    /// End `tag`, returning the tags which may now begin in arrival order.
    pub fn end(&mut self, tag: u16) -> Vec<u16> {
        let position = self
            .active
            .iter()
            .position(|(active, _)| *active == tag)
            .expect("an ending mutation is in flight");
        self.active.swap_remove(position);

        let mut started = Vec::new();
        let mut index = 0;

        while index < self.waiting.len() {
            if self.blocked(&self.waiting[index].1.clone(), index) {
                index += 1;
                continue;
            }
            let entry = self.waiting.remove(index).expect("index is in bounds");
            started.push(entry.0);
            self.active.push(entry);
        }
        started
    }

    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.waiting.is_empty()
    }

    /// Whether `range` collides with a mutation in flight, or with one of the
    /// first `earlier` waiting mutations. The second test keeps two waiting
    /// writes to one block in arrival order.
    fn blocked(&self, range: &std::ops::Range<u32>, earlier: usize) -> bool {
        self.active
            .iter()
            .map(|(_, active)| active)
            .chain(
                self.waiting
                    .iter()
                    .take(earlier)
                    .map(|(_, waiting)| waiting),
            )
            .any(|other| other.start < range.end && range.start < other.end)
    }
}

#[cfg(test)]
mod test {
    use super::InFlight;

    #[test]
    fn test_disjoint_mutations_all_begin() {
        let mut flight = InFlight::default();

        assert!(flight.begin(0, 0..4));
        assert!(flight.begin(1, 4..8));
        assert!(flight.begin(2, 100..101));

        assert!(flight.end(1).is_empty());
        assert!(flight.end(0).is_empty());
        assert!(flight.end(2).is_empty());
        assert!(flight.is_empty());
    }

    #[test]
    fn test_an_overlapping_mutation_waits_for_the_one_it_covers() {
        let mut flight = InFlight::default();

        assert!(flight.begin(0, 8..16));
        // Abutting ranges do not overlap. A shared block does.
        assert!(flight.begin(1, 16..24));
        assert!(!flight.begin(2, 15..17));

        assert!(flight.end(0).is_empty());
        assert_eq!(flight.end(1), vec![2]);
        assert!(flight.end(2).is_empty());
        assert!(flight.is_empty());
    }

    #[test]
    fn test_waiting_mutations_keep_arrival_order() {
        let mut flight = InFlight::default();

        assert!(flight.begin(0, 0..2));
        // Three rewrites of the same block, which must apply in order.
        assert!(!flight.begin(1, 0..2));
        assert!(!flight.begin(2, 0..2));
        assert!(!flight.begin(3, 0..2));

        assert_eq!(flight.end(0), vec![1]);
        assert_eq!(flight.end(1), vec![2]);
        assert_eq!(flight.end(2), vec![3]);
        assert!(flight.end(3).is_empty());
        assert!(flight.is_empty());
    }

    #[test]
    fn test_one_ending_mutation_releases_several() {
        let mut flight = InFlight::default();

        assert!(flight.begin(0, 0..64));
        // Each waits on tag 0 alone, so all three begin together.
        assert!(!flight.begin(1, 0..1));
        assert!(!flight.begin(2, 10..11));
        assert!(!flight.begin(3, 63..70));
        // This one also waits on tag 3, so it stays behind.
        assert!(!flight.begin(4, 65..66));

        assert_eq!(flight.end(0), vec![1, 2, 3]);
        assert!(!flight.is_empty());

        assert_eq!(flight.end(3), vec![4]);
        assert!(flight.end(1).is_empty());
        assert!(flight.end(2).is_empty());
        assert!(flight.end(4).is_empty());
        assert!(flight.is_empty());
    }

    /// Replay a mixed sequence of begins and ends. After every step, assert the
    /// invariant this guard exists for.
    #[test]
    fn test_no_two_overlapping_mutations_are_ever_active() {
        let mut flight = InFlight::default();
        let ranges = [0..4, 2..6, 4..8, 0..1, 6..12, 3..5, 20..24, 1..2];

        let mut active: Vec<u16> = Vec::new();
        let mut pending: Vec<u16> = Vec::new();

        for (tag, range) in ranges.iter().enumerate() {
            let tag = tag as u16;
            if flight.begin(tag, range.clone()) {
                active.push(tag);
            } else {
                pending.push(tag);
            }
            assert_disjoint(&flight, &ranges);
        }

        while let Some(tag) = active.pop() {
            for started in flight.end(tag) {
                assert!(pending.contains(&started));
                active.push(started);
            }
            assert_disjoint(&flight, &ranges);
        }
        assert!(flight.is_empty());
    }

    fn assert_disjoint(flight: &InFlight, ranges: &[std::ops::Range<u32>]) {
        for (index, (tag, range)) in flight.active.iter().enumerate() {
            assert_eq!(range, &ranges[*tag as usize]);

            for (other, peer) in flight.active.iter().skip(index + 1) {
                assert!(
                    range.end <= peer.start || peer.end <= range.start,
                    "tags {tag} and {other} are both in flight over {range:?} and {peer:?}",
                );
            }
        }
    }
}
