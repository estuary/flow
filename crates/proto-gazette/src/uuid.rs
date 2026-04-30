pub use ::uuid::Uuid;

/// Producer is the unique node identifier portion of a v1 UUID.
/// Gazette uses Producer to identify distinct writers of collection data,
/// as the key of a vector clock.
#[derive(Clone, Copy, Ord, PartialOrd, PartialEq, Eq)]
pub struct Producer(pub [u8; 6]);

impl std::hash::Hash for Producer {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Pack the 6 bytes into a u64 so that passthrough hashers can
        // use the already-random Producer ID directly.
        let [a, b, c, d, e, f] = self.0;
        let v = (a as u64) << 40
            | (b as u64) << 32
            | (c as u64) << 24
            | (d as u64) << 16
            | (e as u64) << 8
            | (f as u64);
        state.write_u64(v);
    }
}

/// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of sequence
/// counter. Both the timestamp and counter are monotonic (will never decrease),
/// and each Tick increments the Clock. For UUID generation, Clock provides a
/// total ordering over UUIDs of a given Producer.
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct Clock(u64);

// Flags are the 10 least-significant bits of the v1 UUID clock sequence,
// which Gazette employs for representing message transaction semantics.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Flags(pub u16);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to parse document UUID {0:?}")]
    UUIDParse(String, #[source] ::uuid::Error),
    #[error("UUID {0} is not a V1 UUID")]
    UUIDNotV1(::uuid::Uuid),
}

impl Producer {
    #[inline]
    pub fn from_bytes(v: [u8; 6]) -> Self {
        assert_eq!(
            v[0] & 0x01,
            0x01,
            "Per RFC 4122, the multicast bit must be set to mark that a Producer is not a real MAC address",
        );
        Self(v)
    }

    pub fn from_i64(v: i64) -> Self {
        let v = v.to_be_bytes();
        Self::from_bytes([v[0], v[1], v[2], v[3], v[4], v[5]])
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 6] {
        &self.0
    }

    pub fn as_i64(&self) -> i64 {
        let v = self.0;
        i64::from_be_bytes([v[0], v[1], v[2], v[3], v[4], v[5], 0, 0])
    }
}

impl Clock {
    #[inline]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn from_unix(seconds: u64, nanos: u32) -> Self {
        Self(((seconds * 10_000_000 + (nanos as u64) / 100) + G1582NS100) << 4)
    }

    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Clock(v)
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn from_time(t: std::time::SystemTime) -> Self {
        let unix = t.duration_since(std::time::UNIX_EPOCH).unwrap();
        Self::from_unix(unix.as_secs(), unix.subsec_nanos())
    }

    #[inline]
    pub fn update(&mut self, next: Self) {
        if next.0 > self.0 {
            self.0 = next.0
        }
    }

    #[inline]
    pub fn tick(&mut self) -> Self {
        self.0 += 1;
        *self
    }

    #[inline]
    pub fn to_unix(&self) -> (u64, u32) {
        // Each tick is 100ns relative to unix epoch.
        let ticks = (self.0 >> 4).saturating_sub(G1582NS100);
        let seconds = ticks / 10_000_000;
        // We also include the four counter bits as increments of 4 nanoseconds each.
        let nanos = (ticks % 10_000_000) * 100 + ((self.0 & 0xf) << 2);
        (seconds, nanos as u32)
    }

    #[inline]
    pub fn to_time(&self) -> std::time::SystemTime {
        let (seconds, nanos) = self.to_unix();
        let unix =
            std::time::Duration::from_secs(seconds) + std::time::Duration::from_nanos(nanos as u64);
        std::time::UNIX_EPOCH + unix
    }

    #[inline]
    pub fn to_g1582_ns100(&self) -> u64 {
        self.0
    }

    /// Converts the clock to a protobuf timestamp, returning None if the timestamp
    /// would overflow.
    pub fn to_pb_json_timestamp(&self) -> Option<pbjson_types::Timestamp> {
        let (seconds, nanos) = self.to_unix();
        let seconds = i64::try_from(seconds).ok()?;
        let nanos = i32::try_from(nanos).ok()?;

        Some(pbjson_types::Timestamp { seconds, nanos })
    }

    /// Saturating difference of `a - b` expressed as `Duration`.
    pub fn delta(a: Self, b: Self) -> std::time::Duration {
        let (a_s, a_n) = a.to_unix();
        let (b_s, b_n) = b.to_unix();
        let a = std::time::Duration::new(a_s, a_n);
        let b = std::time::Duration::new(b_s, b_n);
        a.saturating_sub(b)
    }

    pub const UNIX_EPOCH: Self = Clock::from_unix(0, 0);
}

impl Default for Clock {
    fn default() -> Self {
        Self::UNIX_EPOCH
    }
}

impl std::ops::Add<Clock> for Clock {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [b1, b2, b3, b4, b5, b6] = self.as_bytes();
        write!(
            f,
            "Producer({b1:02x}:{b2:02x}:{b3:02x}:{b4:02x}:{b5:02x}:{b6:02x})",
        )
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (seconds, nanos) = self.to_unix();
        write!(f, "Clock({}s {}ns)", seconds, nanos)
    }
}

impl Flags {
    pub const ACK_TXN: Self = Self(crate::message_flags::ACK_TXN as u16);
    pub const CONTINUE_TXN: Self = Self(crate::message_flags::CONTINUE_TXN as u16);
    pub const OUTSIDE_TXN: Self = Self(crate::message_flags::OUTSIDE_TXN as u16);

    #[inline]
    pub fn is_ack(&self) -> bool {
        self.0 & (crate::message_flags::ACK_TXN as u16) != 0
    }

    #[inline]
    pub fn is_continue(&self) -> bool {
        self.0 & (crate::message_flags::CONTINUE_TXN as u16) != 0
    }

    #[inline]
    pub fn is_outside(&self) -> bool {
        // OUTSIDE_TXN is zero, so detect absence of CONTINUE_TXN (0x1) or ACK_TXN (0x2).
        self.0 & 0x3 == crate::message_flags::OUTSIDE_TXN as u16
    }
}

impl std::fmt::Debug for Flags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut flags = Vec::new();
        if self.is_outside() {
            flags.push("OUTSIDE_TXN");
        }
        if self.is_continue() {
            flags.push("CONTINUE_TXN");
        }
        if self.is_ack() {
            flags.push("ACK_TXN");
        }
        write!(f, "Flags({:x} ({}))", self.0, flags.join("|"))
    }
}

// G1582NS100 is the time interval between 15 Oct 1582 (RFC 4122)
// and 1 Jan 1970 (Unix epoch), in units of 100 nanoseconds.
const G1582NS100: u64 = 122_192_928_000_000_000;

/// Parse a v1 UUID into its Producer, Clock, and Flags.
pub fn parse(u: uuid::Uuid) -> Result<(Producer, Clock, Flags), Error> {
    if u.get_version_num() != 1 {
        return Err(Error::UUIDNotV1(u));
    }
    let (c_low, c_mid, c_high, seq_node_id) = u.as_fields();

    let clock = (c_low as u64) << 4          // Clock low bits.
            | (c_mid as u64) << 36                  // Clock middle bits.
            | (c_high as u64) << 52                 // Clock high bits.
            | ((seq_node_id[0] as u64) >> 2) & 0xf; // High 4 bits of sequence number.

    // 6 bytes of big-endian node ID.
    let producer: [u8; 6] = seq_node_id[2..8].try_into().unwrap();

    let flags = ((seq_node_id[0] as u16) & 0x3) << 8 // High 2 bits of flags.
            | (seq_node_id[1] as u16); // Low 8 bits of flags.

    Ok((Producer(producer), Clock(clock), Flags(flags)))
}

/// Parse a v1 UUID string into its Producer, Clock, and Flags.
pub fn parse_str(s: &str) -> Result<(Producer, Clock, Flags), Error> {
    parse(uuid::Uuid::parse_str(s).map_err(|err| Error::UUIDParse(s.to_string(), err))?)
}

/// Build a V1 UUID from a Producer, Clock, and Flags.
pub fn build(p: Producer, c: Clock, f: Flags) -> uuid::Uuid {
    assert!(f.0 <= 0x3ff, "only 10 low bits may be used for flags");

    let p1 = ((c.0 >> 4) as u32).to_be_bytes(); // Clock low bits.
    let p2 = ((c.0 >> 36) as u16).to_be_bytes(); // Clock middle bits.
    let p3 = ((c.0 >> 52) as u16 | 0x1000).to_be_bytes(); // Clock high bits + version 1.
    let p4 = (((c.0 << 10) as u16) & 0x3c00 | f.0 | 0x8000).to_be_bytes(); // Clock sequence + flags + variant 1.

    uuid::Uuid::from_bytes([
        p1[0], p1[1], p1[2], p1[3], p2[0], p2[1], p3[0], p3[1], p4[0], p4[1], p.0[0], p.0[1],
        p.0[2], p.0[3], p.0[4], p.0[5],
    ])
}

/// The successful outcome of a message sequencing determination.
#[derive(Debug)]
pub enum SequenceOutcome {
    /// This OUTSIDE_TXN message is committed.
    OutsideCommit,
    /// This OUTSIDE_TXN message is already acknowledged. This case is expected
    /// under at-least-once journal semantics, as a producer may append a
    /// chunk of one or more OUTSIDE_TXN messages in duplicate.
    OutsideDuplicate,
    /// This CONTINUE_TXN message begins a new producer transaction sequence.
    ContinueBeginSpan,
    /// This CONTINUE_TXN message extends a producer transaction sequence.
    ContinueExtendSpan,
    /// This CONTINUE_TXN message has a lesser Clock than a preceding CONTINUE_TXN
    /// of this pending transaction. This case is expected under at-least-once
    /// journal semantics, as a producer may append a chunk of one or more
    /// CONTINUE_TXN messages in duplicate.
    ContinueDuplicate,
    /// This ACK_TXN rolls back a pending transaction sequence,
    /// re-establishing the last-committed Clock for the producer.
    AckCleanRollback,
    /// This ACK_TXN attempts to roll back to before a preceding commit,
    /// which is tolerated but indicates a likely loss of exactly-once semantics.
    /// Producers are expected to store ACK_TXN in a durable write-head log before
    /// writing them to journals, making this case impossible in normal operation
    /// (but possible in certain disaster recovery scenarios).
    AckDeepRollback,
    /// This ACK_TXN commits a non-empty sequence of preceding CONTINUE_TXN messages.
    AckCommit,
    /// This ACK_TXN commits, but had no preceding CONTINUE_TXN messages
    /// (they were before our read offset). This occurs during conservative
    /// reads when a producer's transaction data predates the read start.
    AckEmpty,
    /// This ACK_TXN re-establishes the last-committed Clock for the producer,
    /// and had no preceding CONTINUE_TXN messages. This case is expected under
    /// at-least-once journal semantics, as a producer may append the ACK_TXN
    /// message multiple times.
    AckDuplicate,
}

#[derive(Debug, thiserror::Error)]
pub enum SequenceError {
    /// This OUTSIDE_TXN message attempts to commit a preceding span of
    /// CONTINUE_TXN messages, but this is disallowed (ACK_TXN must be used).
    #[error("unexpected OUTSIDE_TXN with a preceding unacknowledged CONTINUE_TXN")]
    OutsideWithPrecedingContinue,
    /// This ACK_TXN message attempts to only partially commit a preceding span
    /// of CONTINUE_TXN messages, but this is disallowed (ACK_TXN must commit all or none).
    #[error("unexpected ACK_TXN which only partially acknowledges preceding CONTINUE_TXN messages")]
    AckPartialCommit,
}

/// Sequence a Producer message having `flags` and `clock` against the producer's
/// `last_commit` and `max_continue` Clocks.
///
/// On success returns (SequenceOutcome, next `last_commit`, next `max_continue`).
/// On failure returns a SequenceError.
#[inline]
pub fn sequence(
    flags: Flags,             // Flags of this message.
    clock: Clock,             // Clock of this message.
    last_commit: &mut Clock,  // Last committed Clock of the producer.
    max_continue: &mut Clock, // Max Clock of a current-transaction CONTINUE_TXN, or zero if none.
) -> Result<SequenceOutcome, SequenceError> {
    if flags.is_outside() {
        if *max_continue != Clock::zero() {
            Err(SequenceError::OutsideWithPrecedingContinue)
        } else if clock <= *last_commit {
            Ok(SequenceOutcome::OutsideDuplicate)
        } else {
            *last_commit = clock;
            Ok(SequenceOutcome::OutsideCommit)
        }
    } else if flags.is_continue() {
        if clock <= *last_commit || clock <= *max_continue {
            Ok(SequenceOutcome::ContinueDuplicate)
        } else if *max_continue == Clock::zero() {
            *max_continue = clock;
            Ok(SequenceOutcome::ContinueBeginSpan)
        } else {
            *max_continue = clock;
            Ok(SequenceOutcome::ContinueExtendSpan)
        }
    } else if flags.is_ack() {
        if clock == *last_commit {
            if *max_continue == Clock::zero() {
                Ok(SequenceOutcome::AckDuplicate)
            } else {
                *max_continue = Clock::zero();
                Ok(SequenceOutcome::AckCleanRollback)
            }
        } else if clock < *last_commit {
            if *max_continue == Clock::zero() {
                // If there are no pending CONTINUEs, then likely a duplicate
                // observed under a conservative re-read of journal content
                // (from a lower-bound starting offset).
                Ok(SequenceOutcome::AckDuplicate)
            } else {
                // Given pending CONTINUEs (which are not possible under a
                // conservative re-read of journal content), this is a deep
                // rollback.
                *last_commit = clock;
                *max_continue = Clock::zero();
                Ok(SequenceOutcome::AckDeepRollback)
            }
        } else if *max_continue == Clock::zero() {
            *last_commit = clock;
            Ok(SequenceOutcome::AckEmpty)
        } else if *max_continue <= clock {
            *last_commit = clock;
            *max_continue = Clock::zero();
            Ok(SequenceOutcome::AckCommit)
        } else {
            Err(SequenceError::AckPartialCommit)
        }
    } else {
        unreachable!("Flags is always one of OUTSIDE_TXN, CONTINUE_TXN, or ACK_TXN")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_producer_conversions() {
        let a = Producer::from_bytes([8 | 1, 6, 7, 5, 3, 9]);
        let b: i64 = 650214914308767744;

        assert_eq!(a.as_i64(), b);
        assert_eq!(Producer::from_i64(b), a);
    }

    #[test]
    fn test_clock_lifecycle() {
        let mut c = Clock::UNIX_EPOCH;
        assert_eq!(c.0, 0x1b21dd2138140000);
        assert_eq!(c.to_unix(), (0, 0));

        // UNIX_EPOCH is Clock's default.
        assert_eq!(Clock::default().to_unix(), (0, 0));

        // Each tick increments the clock.
        c.tick();
        c.tick();
        assert_eq!(c.0, 0x1b21dd2138140002);

        // Updates take the maximum value of the observed Clocks (Clock is monotonic).
        c.update(Clock::from_unix(10, 0));
        assert_eq!(c.0, 0x1b21dd2197721000);
        c.update(Clock::from_unix(5, 0));
        assert_eq!(c.0, 0x1b21dd2197721000); // Not changed.

        assert_eq!(c.to_unix(), (10, 0));

        c.tick();
        assert_eq!(c.to_unix(), (10, 4));
        c.tick();
        assert_eq!(c.to_unix(), (10, 8));

        for _ in 0..16 {
            c.tick();
        }
        assert_eq!(c.to_unix(), (10, 108));
    }

    #[test]
    fn test_build_from_parts_and_parse() {
        const SECONDS: u64 = 1567304621;
        const NANOS: u32 = 981273734;
        const FLAGS: u16 = 0b1010101010; // 682.

        let p_in = Producer::from_bytes([8 | 1, 6, 7, 5, 3, 9]);

        // Craft an interesting Clock fixture which uses the full bit-range
        // and includes clock sequence increments.
        let mut c_in = Clock::UNIX_EPOCH;
        c_in.update(Clock::from_unix(SECONDS, NANOS));
        assert_eq!(c_in.to_unix(), (SECONDS, 981273700)); // Rounded to 100's of nanos.

        c_in.tick();
        c_in.tick();

        let id = build(p_in, c_in, Flags(FLAGS));

        // Verify compatibility with `uuid` crate.
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(id.get_version(), Some(uuid::Version::Mac));
        assert_eq!(
            id.get_timestamp().map(|ts| ts.to_unix()),
            Some((
                SECONDS,
                (NANOS / 100) * 100, // Rounded down to nearest 100ns.
            ))
        );
        assert_eq!(id.get_timestamp().map(|ts| ts.to_gregorian().1), Some(2730));

        let (p_out, c_out, f_out) = parse(id).unwrap();

        assert_eq!(p_in, p_out);
        assert_eq!(c_in, c_out);
        assert_eq!(Flags(FLAGS), f_out);
    }

    const O: Flags = Flags(crate::message_flags::OUTSIDE_TXN as u16);
    const C: Flags = Flags(crate::message_flags::CONTINUE_TXN as u16);
    const A: Flags = Flags(crate::message_flags::ACK_TXN as u16);

    fn clk(v: u64) -> Clock {
        Clock::from_u64(v)
    }

    #[test]
    fn test_sequence_outcomes() {
        use SequenceOutcome::*;

        // (flags, clock, last_commit, max_continue, expected outcome, expected (last_commit, max_continue))
        let ok_cases: &[(Flags, u64, u64, u64, SequenceOutcome, (u64, u64))] = &[
            // OUTSIDE
            (O, 10, 5, 0, OutsideCommit, (10, 0)),
            (O, 10, 10, 0, OutsideDuplicate, (10, 0)),
            (O, 5, 10, 0, OutsideDuplicate, (10, 0)),
            // CONTINUE
            (C, 10, 5, 0, ContinueBeginSpan, (5, 10)),
            (C, 20, 5, 10, ContinueExtendSpan, (5, 20)),
            (C, 10, 5, 10, ContinueDuplicate, (5, 10)),
            (C, 8, 5, 10, ContinueDuplicate, (5, 10)),
            (C, 5, 10, 0, ContinueDuplicate, (10, 0)), // clock <= last_commit
            (C, 10, 10, 0, ContinueDuplicate, (10, 0)), // clock == last_commit
            // ACK
            (A, 20, 5, 20, AckCommit, (20, 0)),
            (A, 30, 5, 20, AckCommit, (30, 0)),
            (A, 20, 10, 0, AckEmpty, (20, 0)),
            (A, 10, 10, 0, AckDuplicate, (10, 0)),
            (A, 10, 10, 20, AckCleanRollback, (10, 0)),
            (A, 3, 10, 0, AckDuplicate, (10, 0)), // No pending CONTINUEs: stale ACK, not rollback.
            (A, 3, 10, 20, AckDeepRollback, (3, 0)), // Pending CONTINUEs: genuine deep rollback.
        ];
        for (flags, clock, lc_in, mc_in, expected, (lc_out, mc_out)) in ok_cases {
            let (mut lc, mut mc) = (clk(*lc_in), clk(*mc_in));
            let outcome = sequence(*flags, clk(*clock), &mut lc, &mut mc).unwrap();
            assert_eq!(
                std::mem::discriminant(&outcome),
                std::mem::discriminant(expected),
                "flags={flags:?} clock={clock} lc={lc_in} mc={mc_in}: expected {expected:?}, got {outcome:?}"
            );
            assert_eq!((lc.as_u64(), mc.as_u64()), (*lc_out, *mc_out));
        }
    }

    #[test]
    fn test_sequence_errors() {
        use SequenceError::*;

        let err_cases: &[(Flags, u64, u64, u64, SequenceError)] = &[
            (O, 20, 5, 15, OutsideWithPrecedingContinue),
            (A, 15, 5, 20, AckPartialCommit),
        ];
        for (flags, clock, lc_in, mc_in, expected) in err_cases {
            let (mut lc, mut mc) = (clk(*lc_in), clk(*mc_in));
            let err = sequence(*flags, clk(*clock), &mut lc, &mut mc).unwrap_err();
            assert_eq!(
                std::mem::discriminant(&err),
                std::mem::discriminant(expected),
                "flags={flags:?} clock={clock} lc={lc_in} mc={mc_in}: expected {expected:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn test_sequence_lifecycle() {
        use SequenceOutcome::*;

        fn run(lc: u64, mc: u64, steps: &[(Flags, u64, SequenceOutcome)]) {
            let (mut lc, mut mc) = (clk(lc), clk(mc));
            for &(f, c, ref expect) in steps {
                let outcome = sequence(f, clk(c), &mut lc, &mut mc).unwrap();
                assert_eq!(
                    std::mem::discriminant(&outcome),
                    std::mem::discriminant(expect),
                    "at clock={c}: expected {expect:?}, got {outcome:?}"
                );
            }
        }

        // CONTINUE x2, duplicate, ACK commit, OUTSIDE, dup OUTSIDE, stale ACK.
        run(
            0,
            0,
            &[
                (C, 10, ContinueBeginSpan),
                (C, 20, ContinueExtendSpan),
                (C, 15, ContinueDuplicate),
                (A, 20, AckCommit),
                (O, 30, OutsideCommit),
                (O, 30, OutsideDuplicate),
                (A, 20, AckDuplicate), // No pending CONTINUEs, so stale ACK is a duplicate.
            ],
        );

        // CONTINUE x2, clean rollback, new transaction after rollback.
        run(
            10,
            0,
            &[
                (C, 20, ContinueBeginSpan),
                (C, 30, ContinueExtendSpan),
                (A, 10, AckCleanRollback),
                (C, 40, ContinueBeginSpan),
                (A, 40, AckCommit),
            ],
        );

        // Empty ACK (missed CONTINUEs), then normal transaction.
        run(
            0,
            0,
            &[
                (A, 10, AckEmpty),
                (C, 20, ContinueBeginSpan),
                (A, 20, AckCommit),
            ],
        );
    }

    #[test]
    fn test_uuid_parse_errors() {
        let err = parse_str("not-a-uuid").unwrap_err();
        assert!(matches!(
            err,
            Error::UUIDParse(s, _) if s == "not-a-uuid"
        ));

        let err = parse(::uuid::Uuid::nil()).unwrap_err();
        assert!(matches!(err, Error::UUIDNotV1(_)));
    }
}
