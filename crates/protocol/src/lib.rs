/*
pub mod protocol {
    tonic::include_proto!("protocol");
}
*/

pub mod consumer;
pub mod flow;
pub mod protocol;
pub mod recoverylog;

mod read;

use serde::{Deserializer, Serializer};

/// This exists to enable conditional serialization of optional u32 fields where 0 represents a
/// missing or unset value. See `build.rs` for references to this function in the serde attributes.
fn u32_is_0(i: &u32) -> bool {
    *i == 0
}

pub fn deserialize_duration<'a, D>(d: D) -> Result<Option<prost_types::Duration>, D::Error>
where
    D: Deserializer<'a>,
{
    let dur: Option<std::time::Duration> = humantime_serde::deserialize(d)?;
    Ok(dur.map(Into::into))
}
// serialize_duration is lossy: it only serializes whole numbers of positive seconds.
// If the Duration is negative, None is returned. If the Duration includes nanos,
// they're dropped. This is because:
// * std::time::Duration only represents positive durations, and indeed all durations
//   within Gazette & Flow are never negative.
// * The protobuf mapping of prost_type::Duration to JSON requires that they be
//   fractional seconds, with an "s" suffix. Meanwhile, humantime parses only
//   integer time segments (including "s"). Therefor, we use integer seconds as
//   a lowest common denominator.
pub fn serialize_duration<S>(dur: &Option<prost_types::Duration>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match dur {
        Some(prost_types::Duration { seconds, .. }) if *seconds >= 0 => {
            s.serialize_str(&format!("{}s", seconds))
        }
        _ => s.serialize_none(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use flow::{inference, CollectionSpec, Inference, Projection};

    #[test]
    fn test_serde_round_trip_of_collection_spec() {
        fn s(i: &str) -> String {
            String::from(i)
        }

        let expected = CollectionSpec {
            name: String::from("testCollection"),
            schema_uri: String::from("test://test/schema.json"),
            key_ptrs: vec![String::from("/a"), String::from("/b")],
            uuid_ptr: s(""),
            partition_fields: Vec::new(),
            ack_json_template: Vec::new(),
            journal_spec: None,
            projections: vec![
                Projection {
                    ptr: s("/a"),
                    field: s("field_a"),
                    user_provided: true,
                    is_partition_key: false,
                    is_primary_key: true,
                    inference: Some(Inference {
                        title: s("the title from a"),
                        description: s(""),
                        types: vec![s("string")],
                        must_exist: true,
                        string: Some(inference::String {
                            content_type: s(""),
                            format: s("email"),
                            is_base64: false,
                            max_length: 0,
                        }),
                    }),
                },
                Projection {
                    ptr: s("/b"),
                    field: s("b"),
                    user_provided: false,
                    is_partition_key: false,
                    is_primary_key: true,
                    inference: Some(Inference {
                        title: s(""),
                        description: s("the description from b"),
                        types: vec![s("integer")],
                        must_exist: true,
                        string: None,
                    }),
                },
            ],
        };

        let serialized = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(serialized);
        let actual = serde_json::from_str::<CollectionSpec>(&serialized).unwrap();
        assert_eq!(expected, actual);
    }
}
