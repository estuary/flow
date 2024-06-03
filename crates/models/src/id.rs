use std::fmt::Write;
use std::str::FromStr;

// Estuary epoch is the first representable timestamp in generated IDs.
// This could be zero, but subtracting |estuary_epoch| results in the
// high bit being zero for the next ~34 years,
// making ID representations equivalent for both signed and
// unsigned 64-bit integers.
const ESTUARY_EPOCH_MILLIS: u64 = 1_600_000_000_000;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 8]);

impl Id {
    pub fn new(b: [u8; 8]) -> Self {
        Self(b)
    }
    pub fn zero() -> Self {
        Self::new([0; 8])
    }
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 8]
    }
    pub fn as_array(&self) -> [u8; 8] {
        self.0
    }

    pub fn from_parts(timestamp: u64, seq: u16, shard: u16) -> Self {
        let int_val = timestamp << 23 | (seq as u64) << 10 | shard as u64;
        Self::new(int_val.to_be_bytes())
    }

    pub fn into_parts(self) -> (u64, u16, u16) {
        const SEQ_MASK: u64 = (1u64 << 13) - 1;
        const SHARD_MASK: u64 = (1u64 << 10) - 1;

        let int_val = u64::from_be_bytes(self.0);
        let timestamp = int_val >> 23;

        let seq = ((int_val >> 10) & SEQ_MASK) as u16;
        let shard = (int_val & SHARD_MASK) as u16;
        (timestamp, seq, shard)
    }
}

impl std::str::FromStr for Id {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.replace(':', "");
        let u = u64::from_str_radix(&s, 16)?;
        Ok(Self(u.to_be_bytes()))
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::with_capacity(23);

        for (ind, b) in self.0.iter().enumerate() {
            if ind != 0 && f.alternate() {
                s.push(':');
            }
            write!(&mut s, "{b:02x}").unwrap();
        }

        f.write_str(&s)
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl schemars::JsonSchema for Id {
    fn schema_name() -> String {
        String::from("Id")
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        String::json_schema(gen)
    }
}

impl serde::Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{self}").serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let str_val = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        Id::from_str(str_val.as_ref()).map_err(|err| D::Error::custom(format!("invalid id: {err}")))
    }
}

/// Generates unique IDs that are compatible with the `flowid` generation in postgres.
#[derive(Debug, Clone)]
pub struct IdGenerator {
    shard: u16,
    seq: u16,
    last_timestamp: u64,
}
impl IdGenerator {
    /// Return a new generator with the given shard id.
    pub fn new(shard: u16) -> Self {
        Self {
            shard,
            seq: 0,
            last_timestamp: 0,
        }
    }

    /// Generate and return the next unique id.
    pub fn next(&mut self) -> Id {
        let mut timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        timestamp -= ESTUARY_EPOCH_MILLIS;
        // Ensure that the timestamp is monotonically increasing, which is not guaranteed
        // by the system time.
        timestamp = timestamp.max(self.last_timestamp);

        if timestamp == self.last_timestamp {
            self.seq += 1;
            if self.seq >= (1 << 13) - 1 {
                // TODO: handle this case more gracefully
                panic!("sequence overflow");
            }
        } else {
            self.seq = 0;
            self.last_timestamp = timestamp;
        }
        Id::from_parts(timestamp, self.seq, self.shard)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_id_generation() {
        let mut gen = IdGenerator::new(789);

        let mut prev_id = gen.next();
        for i in 0..5000 {
            let id = gen.next();
            let (timestamp, seq, shard) = id.into_parts();
            assert_eq!(gen.shard, shard, "shard mismatch");
            assert_eq!(gen.last_timestamp, timestamp, "timestamp mismatch");

            assert!(id > prev_id, "ids must increase monotonically");
            let round_tripped = Id::from_parts(timestamp, seq, shard);
            assert_eq!(id, round_tripped, "round trip failed at {i}");
            prev_id = id;
        }
    }
}
