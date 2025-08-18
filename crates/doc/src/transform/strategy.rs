/// Strategy defines the transformation to apply to a document location.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Strategy {
    /// Remove causes the document location to be removed altogether.
    Remove,
}

// Custom serialize/deserialize to handle string format
impl serde::Serialize for Strategy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Strategy::Remove => serializer.serialize_str("remove"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Strategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "remove" => Ok(Strategy::Remove),
            _ => Err(serde::de::Error::custom(format!("unknown transform strategy: {}", s))),
        }
    }
}

impl std::convert::TryFrom<&serde_json::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &serde_json::Value) -> std::result::Result<Self, Self::Error> {
        <Strategy as serde::Deserialize>::deserialize(v)
    }
}