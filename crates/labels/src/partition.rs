use crate::{
    expect_one, expect_one_u32, set_value, Error, LabelSet, FIELD_PREFIX, KEY_BEGIN, KEY_BEGIN_MIN,
    KEY_END,
};
use proto_gazette::broker::Label;
use serde_json::Value;
use std::fmt::Write;

/// Encode logical partition field values and their key range.
/// Values are drawn from `fields` and corresponding `extractors`
/// and extracted from `doc`.
///
// `fields` must be in sorted order and have the
// same length as `extractors`, or encode_partition_labels panics.
pub fn encode_field_range<'f, N: doc::AsNode>(
    mut set: LabelSet,
    key_begin: u32,
    key_end: u32,
    fields: &[impl AsRef<str>],
    extractors: &[doc::Extractor],
    doc: &N,
) -> Result<LabelSet, Error> {
    assert_eq!(fields.len(), extractors.len());

    for i in 0..fields.len() {
        let field = fields[i].as_ref();

        if i > 0 && field <= fields[i - 1].as_ref() {
            panic!("fields are not in sorted order");
        }
        set = match extractors[i].query(doc) {
            Ok(value) => add_value(set, field, value)?,
            Err(value) => add_value(set, field, value.as_ref())?,
        };
    }

    Ok(encode_key_range(set, key_begin, key_end))
}

/// Add a logically-partitioned field and value to the LabelSet.
pub fn add_value<N: doc::AsNode>(set: LabelSet, field: &str, value: &N) -> Result<LabelSet, Error> {
    Ok(crate::add_value(
        set,
        &format!("{FIELD_PREFIX}{}", field),
        &encode_field_value(String::new(), value)?,
    ))
}

/// Decode logical partition field values and their key range.
pub fn decode_field_range(set: &LabelSet) -> Result<((u32, u32), Vec<Value>), Error> {
    let key_range = decode_key_range(set)?;
    let mut values = Vec::new();

    for Label { name, value, .. } in &set.labels {
        if name.starts_with(FIELD_PREFIX) {
            values.push(decode_field_value(value)?);
        }
    }

    Ok((key_range, values))
}

/// Encode a begin / end key range into a LabelSet.
pub fn encode_key_range(mut set: LabelSet, key_begin: u32, key_end: u32) -> LabelSet {
    let fmt = |v: u32| format!("{v:08x}");

    set = set_value(set, crate::KEY_BEGIN, &fmt(key_begin));
    set = set_value(set, crate::KEY_END, &fmt(key_end));
    set
}

/// Decode a begin / end key range from a LabelSet.
pub fn decode_key_range(set: &LabelSet) -> Result<(u32, u32), Error> {
    let key_begin = expect_one_u32(set, KEY_BEGIN)?;
    let key_end = expect_one_u32(set, KEY_END)?;
    Ok((key_begin, key_end))
}

/// Encode a partitioned field value by appending into the given
/// String and returning the result. Encoded values are suitable
/// for embedding within Journal names as well as label values.
///
/// * Booleans append either %_true or %_false
/// * Integers append their base-10 encoding with a `%_` prefix, as in `%_-1234`.
/// * Null appends %_null.
/// * String values append their URL query-encoding.
///
/// Note that types *other* than strings all use a common %_ prefix, which can
/// never be produced by a query-encoded string and thus allows for unambiguously
/// mapping a partition value back into its JSON value.
#[inline]
pub fn encode_field_value<N: doc::AsNode>(mut b: String, value: &N) -> Result<String, Error> {
    match value.as_node() {
        doc::Node::Null => b.push_str("%_null"),
        doc::Node::Bool(true) => b.push_str("%_true"),
        doc::Node::Bool(false) => b.push_str("%_false"),
        doc::Node::PosInt(p) => write!(b, "%_{p}").unwrap(),
        doc::Node::NegInt(n) => write!(b, "%_{n}").unwrap(),
        doc::Node::String(s) => write!(b, "{}", super::percent_encoding(s)).unwrap(),
        doc::Node::Array(_) | doc::Node::Bytes(_) | doc::Node::Float(_) | doc::Node::Object(_) => {
            return Err(Error::InvalidValueType)
        }
    };
    Ok(b)
}

/// Decode a partitioned field value into a dynamic Value variant.
pub fn decode_field_value(value: &str) -> Result<Value, Error> {
    Ok(if value == "%_null" {
        Value::Null
    } else if value == "%_true" {
        Value::Bool(true)
    } else if value == "%_false" {
        Value::Bool(false)
    } else if value.starts_with("%_-") {
        Value::Number(i64::from_str_radix(&value[2..], 10)?.into())
    } else if value.starts_with("%_") {
        Value::Number(u64::from_str_radix(&value[2..], 10)?.into())
    } else {
        Value::String(
            percent_encoding::percent_decode_str(value)
                .decode_utf8()?
                .to_string(),
        )
    })
}

/// Build the journal name suffix that's implied by the LabelSet.
/// This suffix is appended to the journal template's base name
/// to form a complete journal name.
pub fn name_suffix(set: &LabelSet) -> Result<String, Error> {
    let mut s = String::new();

    // We're relying on the fact that labels are always in lexicographic order.
    for label in &set.labels {
        if !label.name.starts_with(FIELD_PREFIX) {
            continue;
        }

        s.push_str(&label.name[FIELD_PREFIX.len()..]); // Field without label prefix.
        s.push('=');
        s.push_str(&label.value);
        s.push('/')
    }
    s.push_str("pivot=");

    let key_begin = expect_one(&set, KEY_BEGIN)?;

    // As a prettified special case, and for historical reasons, we represent the
    // KeyBeginMin value of "00000000" as just "00". This is safe because "00"
    // will naturally order before all other splits, as "00000000" would.
    // All other key splits are their normal 8-byte padded hexidecimal encodings.
    if key_begin == KEY_BEGIN_MIN {
        s.push_str("00");
    } else {
        s.push_str(key_begin);
    }

    Ok(s)
}

/// Extract a journal's templated name prefix.
pub fn name_prefix<'n>(name: &'n str, set: &LabelSet) -> Option<&'n str> {
    let count = set
        .labels
        .iter()
        .filter(|Label { name, .. }| name.starts_with(FIELD_PREFIX))
        .count();

    name.rsplitn(count + 2, "/").skip(count + 1).next()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::build_set;
    use serde_json::{json, Value};

    #[test]
    fn test_partition_value_encoding_round_trip() {
        let cases = [
            (Value::Null, "%_null"),
            (Value::Bool(true), "%_true"),
            (Value::Bool(false), "%_false"),
            (Value::Number(123u64.into()), "%_123"),
            (Value::Number((-123i64).into()), "%_-123"),
            (Value::Number(u64::MAX.into()), "%_18446744073709551615"),
            (Value::Number(i64::MIN.into()), "%_-9223372036854775808"),
            // Strings that *look* like other scalar types.
            (json!("null"), "null"),
            (json!("%_null"), "%25_null"),
            (json!("true"), "true"),
            (json!("false"), "false"),
            (json!("123"), "123"),
            (json!("-123"), "-123"),
            (json!("hello, world!"), "hello%2C%20world%21"),
            (json!("Baz!@\"Bing\""), "Baz%21%40%22Bing%22"),
            (
                json!("no.no&no-no@no$yes_yes();"),
                "no.no%26no-no%40no%24yes_yes%28%29%3B",
            ),
            (
                json!("http://example/path?q1=v1&q2=v2;ex%20tra"),
                "http%3A%2F%2Fexample%2Fpath%3Fq1%3Dv1%26q2%3Dv2%3Bex%2520tra",
            ),
        ];

        for (fixture, expect) in cases {
            let actual = encode_field_value(String::new(), &fixture).unwrap();
            assert_eq!(actual, expect);

            let recovered = decode_field_value(&actual).unwrap();
            assert_eq!(recovered, fixture);
        }
    }

    #[test]
    fn test_encoding() {
        let fields = ["Loo", "bar", "foo", "z"];
        let policy = doc::SerPolicy::debug();
        let extractors = [
            doc::Extractor::new("/a", &policy),
            doc::Extractor::new("/b", &policy),
            doc::Extractor::new("/c", &policy),
            doc::Extractor::new("/d", &policy),
        ];
        let doc = serde_json::json!({
            "a": "Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra",
            "b": -123,
            "c": true,
            "d": "bye! 👋",
        });

        let set = encode_field_range(
            build_set([("pass", "through")]),
            0x12341234,
            0x56785678,
            &fields,
            &extractors,
            &doc,
        )
        .unwrap();

        insta::assert_json_snapshot!(set, @r###"
        {
          "labels": [
            {
              "name": "estuary.dev/field/Loo",
              "value": "Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra"
            },
            {
              "name": "estuary.dev/field/bar",
              "value": "%_-123"
            },
            {
              "name": "estuary.dev/field/foo",
              "value": "%_true"
            },
            {
              "name": "estuary.dev/field/z",
              "value": "bye%21%20%F0%9F%91%8B"
            },
            {
              "name": "estuary.dev/key-begin",
              "value": "12341234"
            },
            {
              "name": "estuary.dev/key-end",
              "value": "56785678"
            },
            {
              "name": "pass",
              "value": "through"
            }
          ]
        }
        "###);

        let name = format!("base/journal/name/{}", name_suffix(&set).unwrap());

        assert_eq!(
            name,
            "base/journal/name/Loo=Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra/bar=%_-123/foo=%_true/z=bye%21%20%F0%9F%91%8B/pivot=12341234"
        );
        assert_eq!(name_prefix(&name, &set), Some("base/journal/name"));
    }

    #[test]
    fn test_decode_cases() {
        let case = |set| match decode_field_range(&set) {
            Ok(ok) => serde_json::to_value(ok).unwrap(),
            Err(err) => serde_json::Value::String(err.to_string()),
        };

        let model = build_set([
            (crate::KEY_BEGIN, "10001000"),
            (crate::KEY_END, "20002000"),
            ("estuary.dev/field/Bool", "%_true"),
            ("estuary.dev/field/String", "hi%21%20%F0%9F%91%8B"),
            ("estuary.dev/field/messy", "Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra"),
            ("estuary.dev/field/the_int", "%_-8675309"),
        ]);

        insta::assert_json_snapshot!(
            case(model.clone()),
            @r###"
        [
          [
            268439552,
            536879104
          ],
          [
            true,
            "hi! 👋",
            "Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra",
            -8675309
          ]
        ]
        "###
        );

        // Required labels are missing.
        let set = crate::remove(model.clone(), crate::KEY_BEGIN);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/key-begin (got [])""###);
        let set = crate::remove(model.clone(), crate::KEY_END);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/key-end (got [])""###);

        // Key labels are not hex.
        let set = crate::set_value(model.clone(), crate::KEY_BEGIN, "0000000z");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"0000000z\" for label estuary.dev/key-begin""###);

        // Partition value is malformed.
        let set = crate::set_value(
            model.clone(),
            "estuary.dev/field/the_int",
            "%_-867_not_an_int",
        );
        insta::assert_json_snapshot!(case(set),
            @r###""failed to parse label value as integer""###);
    }
}
