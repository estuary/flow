/// Producer is the unique node identifier portion of a v1 UUID.
/// Gazette uses Producer to identify distinct writers of collection data,
/// as the key of a vector clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer(pub [u8; 6]);

/// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of sequence
/// counter. Both the timestamp and counter are monotonic (will never decrease),
/// and each Tick increments the Clock. For UUID generation, Clock provides a
/// total ordering over UUIDs of a given Producer.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct Clock(u64);

// Flags are the 10 least-significant bits of the v1 UUID clock sequence,
// which Gazette employs for representing message transaction semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub const fn from_unix(seconds: u64, nanos: u32) -> Self {
        Self(((seconds * 10_000_000 + (nanos as u64) / 100) + G1582NS100) << 4)
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
    pub fn tick(&mut self) {
        self.0 += 1;
    }

    #[inline]
    pub fn to_unix(&self) -> (u64, u32) {
        // Each tick is 100ns relative to unix epoch.
        let ticks = (self.0 >> 4) - G1582NS100;
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

    pub const UNIX_EPOCH: Self = Clock::from_unix(0, 0);
}

impl Default for Clock {
    fn default() -> Self {
        Self::UNIX_EPOCH
    }
}

impl Flags {
    #[inline]
    pub fn is_ack(&self) -> bool {
        self.0 & (crate::message_flags::ACK_TXN as u16) != 0
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

    #[test]
    fn test_uuid_parse_errors() {
        let err = parse_str("not-a-uuid").unwrap_err();
        assert!(matches!(
            err,
            Error::UUIDParse(s, _) if s == "not-a-uuid"
        ));

        let err = parse(::uuid::Uuid::new_v4()).unwrap_err();
        assert!(matches!(err, Error::UUIDNotV1(_)));
    }
}
