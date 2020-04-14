use super::deserialize_cow_str;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Document<'d> {
    #[serde(borrow, deserialize_with = "deserialize_cow_str")]
    pub key: Cow<'d, str>,

    #[serde(borrow)]
    pub value: &'d RawValue,

    // Expiration time of the document, as RFC3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    // Key to retreive, or prefix thereof.
    pub key: String,
    // Whether to match all documents having |key| as a key prefix.
    pub prefix: bool,
}
