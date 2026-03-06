use proto_flow::shuffle;

/// Rotate a UUID clock into a high-entropy 32-bit r-clock value.
///
/// XORs the high 60-bit timestamp (shifted down by 4) with the 4-bit sequence
/// counter, then bit-reverses the result. This distributes temporally-adjacent
/// clocks across the full u32 range, enabling balanced r-clock-based partitioning
/// of read-only derivation transforms.
///
/// Matches Go's `rotateClock` in go/shuffle/subscriber.go.
pub fn rotate_clock(clock: proto_gazette::uuid::Clock) -> u32 {
    let raw = clock.to_g1582_ns100();
    (((raw >> 4) ^ (raw & 0xf)) as u32).reverse_bits()
}

/// Find which member(s) should receive a document based on its key hash and r-clock.
pub fn route_to_members(
    key_hash: u32,
    r_clock: u32,
    filter_r_clocks: bool,
    members: &[shuffle::Member],
) -> impl Iterator<Item = usize> + '_ {
    members.iter().enumerate().filter_map(move |(i, member)| {
        let range = member.range.as_ref()?;

        if key_hash < range.key_begin || key_hash > range.key_end {
            return None;
        }
        if filter_r_clocks && (r_clock < range.r_clock_begin || r_clock > range.r_clock_end) {
            return None;
        }
        Some(i)
    })
}

/// Find which member(s) overlap a bounding box in (key_hash, r_clock) space.
///
/// Used to route ACK_TXN documents to all Queue members that may have received
/// the producer's preceding CONTINUE_TXN documents.
pub fn route_to_members_by_bbox<'a>(
    bbox: &'a super::producer::BoundingBox,
    filter_r_clocks: bool,
    members: &'a [shuffle::Member],
) -> impl Iterator<Item = usize> + 'a {
    let bbox = *bbox;
    members.iter().enumerate().filter_map(move |(i, member)| {
        let range = member.range.as_ref()?;

        // Check key_hash range overlap.
        if bbox.key_hash_max < range.key_begin || bbox.key_hash_min > range.key_end {
            return None;
        }
        // Check r_clock range overlap (when filtering is enabled).
        if filter_r_clocks
            && (bbox.r_clock_max < range.r_clock_begin || bbox.r_clock_min > range.r_clock_end)
        {
            return None;
        }
        Some(i)
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::flow;

    #[test]
    fn test_rotate_clock_regression() {
        use proto_gazette::uuid::Clock;

        // Port of TestClockRotationRegression from go/shuffle/subscriber_test.go.
        // Raw clock values: the low 4 bits are the sequence counter,
        // the upper bits are the timestamp in 100ns intervals.
        assert_eq!(rotate_clock(Clock::from_u64(0)), 0);

        // Incrementing the sequence counter modulates the MSBs of the output.
        assert_eq!(rotate_clock(Clock::from_u64(1)), 0x80000000);
        assert_eq!(rotate_clock(Clock::from_u64(2)), 0x40000000);
        assert_eq!(rotate_clock(Clock::from_u64(3)), 0xC0000000);
        assert_eq!(rotate_clock(Clock::from_u64(4)), 0x20000000);
    }

    #[test]
    fn test_route_to_members() {
        let members = vec![
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0x7FFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0x80000000,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        // Low key hash routes to member 0.
        let out: Vec<_> = route_to_members(0x10000000, 0, false, &members).collect();
        assert_eq!(out.as_slice(), &[0]);

        // High key hash routes to member 1.
        let out: Vec<_> = route_to_members(0x90000000, 0, false, &members).collect();
        assert_eq!(out.as_slice(), &[1]);

        // r-clock filtering: only member 0 has matching r-clock range.
        let members_rclock = vec![
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0x7FFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0x80000000,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        let out: Vec<_> = route_to_members(0x50000000, 0x10000000, true, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[0]);

        let out: Vec<_> = route_to_members(0x50000000, 0x90000000, true, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[1]);

        // Without r-clock filtering, both match.
        let out: Vec<_> =
            route_to_members(0x50000000, 0x90000000, false, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[0, 1]);
    }

    #[test]
    fn test_route_to_members_by_bbox() {
        use super::super::producer::BoundingBox;

        let members = vec![
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0x7FFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0x80000000,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        // Bbox entirely in member 0's key range.
        let bbox = BoundingBox {
            key_hash_min: 0x10000000,
            key_hash_max: 0x20000000,
            r_clock_min: 0,
            r_clock_max: 0,
        };
        let out: Vec<_> = route_to_members_by_bbox(&bbox, false, &members).collect();
        assert_eq!(out.as_slice(), &[0]);

        // Bbox spanning both members' key ranges.
        let bbox = BoundingBox {
            key_hash_min: 0x70000000,
            key_hash_max: 0x90000000,
            r_clock_min: 0,
            r_clock_max: 0,
        };
        let out: Vec<_> = route_to_members_by_bbox(&bbox, false, &members).collect();
        assert_eq!(out.as_slice(), &[0, 1]);

        // Empty bbox routes to no members.
        let out: Vec<_> = route_to_members_by_bbox(&BoundingBox::EMPTY, false, &members).collect();
        assert_eq!(out.as_slice(), &[] as &[usize]);

        // Bbox with r_clock filtering.
        let members_rclock = vec![
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0x7FFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Member {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0x80000000,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        // Bbox r_clock range only overlaps member 0.
        let bbox = BoundingBox {
            key_hash_min: 0,
            key_hash_max: 0xFFFFFFFF,
            r_clock_min: 0x10000000,
            r_clock_max: 0x20000000,
        };
        let out: Vec<_> = route_to_members_by_bbox(&bbox, true, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[0]);

        // Bbox r_clock range spans both members.
        let bbox = BoundingBox {
            key_hash_min: 0,
            key_hash_max: 0xFFFFFFFF,
            r_clock_min: 0x70000000,
            r_clock_max: 0x90000000,
        };
        let out: Vec<_> = route_to_members_by_bbox(&bbox, true, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[0, 1]);

        // Without r_clock filtering, both match regardless of r_clock range.
        let bbox = BoundingBox {
            key_hash_min: 0,
            key_hash_max: 0xFFFFFFFF,
            r_clock_min: 0x10000000,
            r_clock_max: 0x20000000,
        };
        let out: Vec<_> = route_to_members_by_bbox(&bbox, false, &members_rclock).collect();
        assert_eq!(out.as_slice(), &[0, 1]);
    }
}
