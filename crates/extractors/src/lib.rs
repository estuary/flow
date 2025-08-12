use proto_flow::flow;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key {key} has no corresponding projection")]
    KeyNotFound { key: String },
    #[error("field {field} is not a projection")]
    FieldNotFound { field: String },
    #[error("projection does not have inference")]
    InferenceNotFound,
    #[error("failed to parse inferred projection default value")]
    ParseDefault(#[source] serde_json::Error),
}
type Result<T> = std::result::Result<T, Error>;

/// Map a protobuf flow::SerPolicy into an equivalent doc::SerPolicy.
pub fn map_policy(policy: &flow::SerPolicy) -> doc::SerPolicy {
    let proto_flow::flow::SerPolicy {
        str_truncate_after,
        nested_obj_truncate_after,
        array_truncate_after,
    } = policy;

    fn zero_to_max(i: u32) -> usize {
        if i == 0 {
            usize::MAX
        } else {
            i as usize
        }
    }

    doc::SerPolicy {
        str_truncate_after: zero_to_max(*str_truncate_after),
        array_truncate_after: zero_to_max(*array_truncate_after),
        nested_obj_truncate_after: zero_to_max(*nested_obj_truncate_after),
    }
}

/// for_key returns Extractors initialized for the composite key of JSON pointers.
pub fn for_key<S: AsRef<str>>(
    key: &[S],
    projections: &[flow::Projection],
    policy: &doc::SerPolicy,
) -> Result<Vec<doc::Extractor>> {
    // Order projections so that explicit (user-defined) projections are walked first.
    let mut projections: Vec<_> = projections.iter().collect();
    projections.sort_by_key(|p| !p.explicit);

    key.iter()
        .map(AsRef::as_ref)
        .map(|key| match projections.iter().find(|p| key == p.ptr) {
            Some(p) => for_projection(p, policy),
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        })
        .collect()
}

/// for_fields returns Extractors initialized for the given fields.
pub fn for_fields<S: AsRef<str>>(
    fields: &[S],
    projections: &[flow::Projection],
    policy: &doc::SerPolicy,
) -> Result<Vec<doc::Extractor>> {
    fields
        .iter()
        .map(AsRef::as_ref)
        .map(
            |field| match projections.binary_search_by_key(&field, |p| &p.field) {
                Ok(index) => for_projection(&projections[index], policy),
                Err(_) => Err(Error::FieldNotFound {
                    field: field.to_string(),
                }),
            },
        )
        .collect()
}

/// for_projection returns an Extractor for the given Projection.
pub fn for_projection(
    projection: &flow::Projection,
    policy: &doc::SerPolicy,
) -> Result<doc::Extractor> {
    let Some(inf) = projection.inference.as_ref() else {
        return Err(Error::InferenceNotFound);
    };

    // Special-case for date-time extracted from the clock component of a UUID.
    // Compare to assemble::inference_uuid_v1_date_time().
    if matches!(inf,
        flow::Inference {
            string:
                Some(flow::inference::String {
                    format,
                    content_encoding,
                    ..
                }),
            ..
        } if format == "date-time" && content_encoding == "uuid")
    {
        return Ok(doc::Extractor::for_uuid_v1_date_time(&projection.ptr));
    }
    if projection.ptr == doc::TRUNCATION_INDICATOR_PTR {
        return Ok(doc::Extractor::for_truncation_indicator());
    }

    let default = if inf.default_json != "" {
        serde_json::from_slice(&inf.default_json).map_err(Error::ParseDefault)?
    } else {
        serde_json::Value::Null
    };

    Ok(doc::Extractor::with_default(
        &projection.ptr,
        policy,
        default,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_projection_mapping() {
        let policy = doc::SerPolicy::truncate_strings(1234);

        let mut projections: Vec<flow::Projection> = serde_json::from_value(json!([
            {"field": "the/key", "ptr": "/the/key", "inference": {"default": "the/key"}},
            {"field": "user_key", "ptr": "/the/key", "explicit": true, "inference": {"default": "user_key"}},
            {"field": "foo", "ptr": "/foo", "inference": {"default": 32}},
            {"field": "user_bar", "ptr": "/bar/baz", "explicit": true, "inference": {}},
            {"field": "flow_published_at", "ptr": "/_meta/uuid", "inference": {"string": {"format": "date-time", "contentEncoding": "uuid"}}},
        ]))
        .unwrap();
        projections.sort_by(|l, r| l.field.cmp(&r.field));

        insta::assert_debug_snapshot!(for_key(&["/the/key", "/bar/baz"], &projections, &policy).unwrap(), @r###"
        [
            Extractor {
                ptr: Pointer(
                    [
                        Property(
                            "the",
                        ),
                        Property(
                            "key",
                        ),
                    ],
                ),
                policy: SerPolicy {
                    str_truncate_after: 1234,
                    array_truncate_after: 18446744073709551615,
                    nested_obj_truncate_after: 18446744073709551615,
                },
                default: String("user_key"),
                magic: None,
            },
            Extractor {
                ptr: Pointer(
                    [
                        Property(
                            "bar",
                        ),
                        Property(
                            "baz",
                        ),
                    ],
                ),
                policy: SerPolicy {
                    str_truncate_after: 1234,
                    array_truncate_after: 18446744073709551615,
                    nested_obj_truncate_after: 18446744073709551615,
                },
                default: Null,
                magic: None,
            },
        ]
        "###);

        insta::assert_debug_snapshot!(for_fields(&["user_bar", "foo", "flow_published_at"], &projections, &policy).unwrap(), @r###"
        [
            Extractor {
                ptr: Pointer(
                    [
                        Property(
                            "bar",
                        ),
                        Property(
                            "baz",
                        ),
                    ],
                ),
                policy: SerPolicy {
                    str_truncate_after: 1234,
                    array_truncate_after: 18446744073709551615,
                    nested_obj_truncate_after: 18446744073709551615,
                },
                default: Null,
                magic: None,
            },
            Extractor {
                ptr: Pointer(
                    [
                        Property(
                            "foo",
                        ),
                    ],
                ),
                policy: SerPolicy {
                    str_truncate_after: 1234,
                    array_truncate_after: 18446744073709551615,
                    nested_obj_truncate_after: 18446744073709551615,
                },
                default: Number(32),
                magic: None,
            },
            Extractor {
                ptr: Pointer(
                    [
                        Property(
                            "_meta",
                        ),
                        Property(
                            "uuid",
                        ),
                    ],
                ),
                policy: SerPolicy {
                    str_truncate_after: 18446744073709551615,
                    array_truncate_after: 18446744073709551615,
                    nested_obj_truncate_after: 18446744073709551615,
                },
                default: Null,
                magic: Some(
                    UuidV1DateTime,
                ),
            },
        ]
        "###);
    }
}
