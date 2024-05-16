use std::fmt::Write;
use std::str::FromStr;

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
            if ind != 0 {
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
