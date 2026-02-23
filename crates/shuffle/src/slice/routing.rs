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
}
