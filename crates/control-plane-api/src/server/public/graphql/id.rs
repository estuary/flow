use std::{fmt::Debug, hash::Hash};
pub trait TypedId:
    PartialEq + Eq + Hash + Debug + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static
{
}

impl<T> TypedId for T where
    T: PartialEq
        + Eq
        + Hash
        + Debug
        + Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static
{
}

/// A wrapper type for opaque IDs that are serialized as JSON and then base64 encoded.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphqlId<T: TypedId>(T);
impl<T: TypedId> std::ops::Deref for GraphqlId<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: TypedId> GraphqlId<T> {
    pub fn new(id: T) -> Self {
        GraphqlId(id)
    }

    pub fn encode(&self) -> String {
        let ser = serde_json::to_string(&self.0).expect("id type must serialize without error");
        base64::encode(&ser)
    }

    pub fn decode(encoded: &str) -> anyhow::Result<Self> {
        let decoded = base64::decode(encoded)?;
        let deserialized = serde_json::from_slice(&decoded)?;
        Ok(GraphqlId(deserialized))
    }
}

impl<T: TypedId> serde::Serialize for GraphqlId<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let tmp = self.encode();
        tmp.serialize(serializer)
    }
}

impl<'a, T: TypedId> serde::Deserialize<'a> for GraphqlId<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let str_val = std::borrow::Cow::<'a, str>::deserialize(deserializer)?;
        GraphqlId::decode(&str_val).map_err(|e| serde::de::Error::custom(e))
    }
}
