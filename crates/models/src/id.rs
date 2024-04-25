#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 8]);

impl Id {
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 8]
    }
    pub fn new(b: [u8; 8]) -> Self {
        Self(b)
    }
    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, hex::FromHexError> {
        let vec_bytes = hex::decode(hex)?;
        let exact: [u8; 8] = vec_bytes
            .as_slice()
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)?;

        Ok(Id(exact))
    }
}

impl std::str::FromStr for Id {
    type Err = hex::FromHexError;

    // TODO: from_hex should probably also accept strings with colons
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let no_colons = s.replace(':', "");
        Id::from_hex(&no_colons)
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", i64::from_be_bytes(self.0))
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
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
        Id::from_hex(str_val.as_ref()).map_err(|err| D::Error::custom(format!("invalid id: {err}")))
    }
}
