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

/// A fraction of keys have their routing "remapped" away from the shard
/// owning their key-hash range to one spread uniformly over the key space.
/// Shards otherwise own solid key ranges, so a Slice reading journals
/// aligned with its shard's range appends only to its own Log and nothing
/// couples its read rate to other shards'. Journals on a busy machine are then
/// read slower than the rest, that shard's log falls behind in wall-clock, and
/// each transaction mixes documents of widely differing age. Remapping a
/// trickle of every Slice's documents into every Log lets a lagging Log's
/// clock-ordered merge hold back the Appends of a Slice which has raced ahead,
/// back-pressuring it towards the slowest shard.
///
/// A key is remapped when its hash's low REMAP_BITS, read as a bucket in
/// [0, REMAP_BUCKETS), fall below REMAP_THRESHOLD: the remapped fraction is
/// REMAP_THRESHOLD / REMAP_BUCKETS.
const REMAP_BITS: u32 = 8;
const REMAP_BUCKETS: u32 = 1 << REMAP_BITS;
const REMAP_BUCKET_MASK: u32 = REMAP_BUCKETS - 1;
const REMAP_THRESHOLD: u32 = 4; // 4 / 256 = 1.56%

/// Remap the routing of the fraction of keys selected above by swapping the
/// hash's 16-bit halves. Every Slice must route a key identically, so this is
/// a pure function of the hash.
///
/// The remap exists solely within `route_to_shards`: the key hash a connector
/// or log entry observes is always the true hash of the packed key.
fn remap_key_hash(key_hash: u32) -> u32 {
    if (key_hash & REMAP_BUCKET_MASK) < REMAP_THRESHOLD {
        key_hash.rotate_left(16)
    } else {
        key_hash
    }
}

/// Find which shard(s) should receive a document based on its key hash and r-clock.
/// Remapped keys (see REMAP_BITS et al) route by a remapped hash in the key
/// dimension; the r_clock dimension is unaffected.
pub fn route_to_shards(
    key_hash: u32,
    r_clock: u32,
    filter_r_clocks: bool,
    shards: &[shuffle::Shard],
) -> impl Iterator<Item = usize> + '_ {
    let key_hash = remap_key_hash(key_hash);

    shards.iter().enumerate().filter_map(move |(i, shard)| {
        let range = shard.range.as_ref()?;

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
    fn test_route_to_shards() {
        let shards = vec![
            shuffle::Shard {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0x7FFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Shard {
                range: Some(flow::RangeSpec {
                    key_begin: 0x80000000,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        // Low key hash routes to shard 0. Low bytes are >= REMAP_THRESHOLD
        // throughout, so no fixture here is remapped.
        let out: Vec<_> = route_to_shards(0x10000010, 0, false, &shards).collect();
        assert_eq!(out.as_slice(), &[0]);

        // High key hash routes to shard 1.
        let out: Vec<_> = route_to_shards(0x90000010, 0, false, &shards).collect();
        assert_eq!(out.as_slice(), &[1]);

        // r-clock filtering: only shard 0 has matching r-clock range.
        let shards_rclock = vec![
            shuffle::Shard {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0,
                    r_clock_end: 0x7FFFFFFF,
                }),
                ..Default::default()
            },
            shuffle::Shard {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: 0xFFFFFFFF,
                    r_clock_begin: 0x80000000,
                    r_clock_end: 0xFFFFFFFF,
                }),
                ..Default::default()
            },
        ];

        let out: Vec<_> = route_to_shards(0x50000010, 0x10000000, true, &shards_rclock).collect();
        assert_eq!(out.as_slice(), &[0]);

        let out: Vec<_> = route_to_shards(0x50000010, 0x90000000, true, &shards_rclock).collect();
        assert_eq!(out.as_slice(), &[1]);

        // Without r-clock filtering, both match.
        let out: Vec<_> = route_to_shards(0x50000010, 0x90000000, false, &shards_rclock).collect();
        assert_eq!(out.as_slice(), &[0, 1]);
    }

    fn key_shards(n: u32) -> Vec<shuffle::Shard> {
        // Tile the key space evenly across `n` shards, full r_clock range.
        (0..n)
            .map(|i| {
                let step = (u32::MAX as u64 + 1) / n as u64;
                shuffle::Shard {
                    range: Some(flow::RangeSpec {
                        key_begin: (i as u64 * step) as u32,
                        key_end: (((i as u64 + 1) * step) - 1) as u32,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    }),
                    ..Default::default()
                }
            })
            .collect()
    }

    #[test]
    fn test_route_to_shards_remaps_keys() {
        let shards = key_shards(4);
        let key_hash = 0x1000C302; // Low byte < REMAP_THRESHOLD.
        let remapped = remap_key_hash(key_hash);
        let home_of = |h: u32| (h >> 30) as usize; // 4 equal ranges tile on the top two bits.
        assert_ne!(home_of(remapped), home_of(key_hash));

        // A remapped key routes by its remapped hash, not its own range home.
        let out: Vec<_> = route_to_shards(key_hash, 0, false, &shards).collect();
        assert_eq!(out.as_slice(), &[home_of(remapped)]);

        // The adjacent non-remapped key routes home.
        let out: Vec<_> = route_to_shards(key_hash + 2, 0, false, &shards).collect();
        assert_eq!(out.as_slice(), &[home_of(key_hash)]);
    }

    #[test]
    fn test_route_to_shards_remap_composes_with_r_clocks() {
        // Shards tile r_clock with full key ranges: a remapped key still
        // broadcasts to all shards when unfiltered, and routes by its r_clock
        // when filtered.
        let shards: Vec<_> = [(0, 0x7FFFFFFF), (0x80000000, u32::MAX)]
            .into_iter()
            .map(|(r_clock_begin, r_clock_end)| shuffle::Shard {
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: u32::MAX,
                    r_clock_begin,
                    r_clock_end,
                }),
                ..Default::default()
            })
            .collect();

        let out: Vec<_> = route_to_shards(0x10000302, 0x10000000, false, &shards).collect();
        assert_eq!(out.as_slice(), &[0, 1]);

        let out: Vec<_> = route_to_shards(0x10000302, 0x90000000, true, &shards).collect();
        assert_eq!(out.as_slice(), &[1]);
    }

    #[test]
    fn test_remap_key_hash_is_identity_outside_threshold() {
        assert_eq!(remap_key_hash(0x10000004), 0x10000004);
        assert_eq!(remap_key_hash(0x900000FF), 0x900000FF);
    }

    #[test]
    fn test_remap_key_hash_remaps_selected_keys() {
        // A remapped key swaps its 16-bit halves. The routed hash's top byte
        // is the true hash's second byte, so the (biased) selecting byte
        // doesn't bias which range a remapped key lands in.
        assert_eq!(remap_key_hash(0x1000C302), 0xC3021000);
        assert_eq!(remap_key_hash(0x1000C302) >> 24, (0x1000C302 >> 8) & 0xFF);
    }
}
