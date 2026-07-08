use crate::{
    Error, FIELD_PREFIX, KEY_BEGIN, KEY_END, LabelSet, expect_one, expect_one_u32, set_value,
};
use proto_gazette::broker::Label;
use std::fmt::Write;

/// Encode partition field values extracted from `doc` as `estuary.dev/field/` labels.
/// `fields` and `extractors` must have the same length, and `fields` must be sorted.
#[inline]
pub fn encode_extracted_fields_labels<N: json::AsNode>(
    mut set: LabelSet,
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
            Ok(value) => encode_field_label(set, field, value)?,
            Err(value) => encode_field_label(set, field, value.as_ref())?,
        };
    }

    Ok(set)
}

/// Append partition field values to a journal name, extracted directly from `doc`.
/// This is the name-building counterpart to `encode_extracted_fields_labels`.
#[inline]
pub fn append_extracted_fields_name_suffix<N: json::AsNode>(
    mut name: String,
    fields: &[impl AsRef<str>],
    extractors: &[doc::Extractor],
    doc: &N,
) -> Result<String, Error> {
    assert_eq!(fields.len(), extractors.len());

    for i in 0..fields.len() {
        let field = fields[i].as_ref();

        if i > 0 && field <= fields[i - 1].as_ref() {
            panic!("fields are not in sorted order");
        }
        name.push_str(field);
        name.push('=');

        match extractors[i].query(doc) {
            Ok(value) => name = encode_field_value(name, value)?,
            Err(value) => name = encode_field_value(name, value.as_ref())?,
        };
        name.push('/');
    }
    Ok(name)
}

/// Append partition field values to a journal name from an existing LabelSet.
/// Unlike `append_extracted_fields_name_suffix`, this works from pre-encoded labels.
#[inline]
pub fn append_fields_name_suffix(mut name: String, set: &LabelSet) -> String {
    // Note that labels are always in lexicographic order.
    for label in &set.labels {
        if !label.name.starts_with(FIELD_PREFIX) {
            continue;
        }

        name.push_str(&label.name[FIELD_PREFIX.len()..]); // Field without label prefix.
        name.push('=');
        name.push_str(&label.value); // Label value is already encoded.
        name.push('/')
    }
    name
}

/// Encode a single partition field value into the LabelSet with the `estuary.dev/field/` prefix.
#[inline]
pub fn encode_field_label<N: json::AsNode>(
    set: LabelSet,
    field: &str,
    value: &N,
) -> Result<LabelSet, Error> {
    Ok(crate::add_value(
        set,
        &format!("{FIELD_PREFIX}{}", field),
        &encode_field_value(String::new(), value)?,
    ))
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
pub fn encode_field_value<N: json::AsNode>(mut b: String, value: &N) -> Result<String, Error> {
    match value.as_node() {
        json::Node::Null => b.push_str("%_null"),
        json::Node::Bool(true) => b.push_str("%_true"),
        json::Node::Bool(false) => b.push_str("%_false"),
        json::Node::PosInt(p) => write!(b, "%_{p}").unwrap(),
        json::Node::NegInt(n) => write!(b, "%_{n}").unwrap(),
        json::Node::String(s) => write!(b, "{}", super::percent_encoding(s)).unwrap(),
        json::Node::Array(_)
        | json::Node::Bytes(_)
        | json::Node::Float(_)
        | json::Node::Object(_) => return Err(Error::InvalidValueType),
    };
    Ok(b)
}

/// Decode partition field values from a LabelSet.
/// `fields` are bare field names (without `FIELD_PREFIX`) and must be sorted.
#[inline]
pub fn decode_fields_labels<S: AsRef<str>>(
    set: &LabelSet,
    fields: &[S],
) -> Result<Vec<serde_json::Value>, Error> {
    let mut values = Vec::with_capacity(fields.len());

    for (i, field) in fields.iter().enumerate() {
        let field = field.as_ref();
        if i > 0 && field <= fields[i - 1].as_ref() {
            panic!("fields are not in sorted order");
        }

        let label_name = format!("{FIELD_PREFIX}{field}");
        let encoded = expect_one(set, &label_name)?;
        values.push(decode_field_value(encoded)?);
    }

    Ok(values)
}

#[inline]
pub fn decode_field_value(value: &str) -> Result<serde_json::Value, Error> {
    Ok(if value == "%_null" {
        serde_json::Value::Null
    } else if value == "%_true" {
        serde_json::Value::Bool(true)
    } else if value == "%_false" {
        serde_json::Value::Bool(false)
    } else if value.starts_with("%_-") {
        serde_json::Value::Number(i64::from_str_radix(&value[2..], 10)?.into())
    } else if value.starts_with("%_") {
        serde_json::Value::Number(u64::from_str_radix(&value[2..], 10)?.into())
    } else {
        serde_json::Value::String(
            percent_encoding::percent_decode_str(value)
                .decode_utf8()?
                .to_string(),
        )
    })
}

/// Encode a begin / end key range as `estuary.dev/key-begin` and `estuary.dev/key-end` labels.
pub fn encode_key_range_labels(mut set: LabelSet, key_begin: u32, key_end: u32) -> LabelSet {
    let fmt = |v: u32| format!("{v:08x}");

    set = set_value(set, crate::KEY_BEGIN, &fmt(key_begin));
    set = set_value(set, crate::KEY_END, &fmt(key_end));
    set
}

/// Append the key range `pivot=` segment to a journal name.
#[inline]
pub fn append_key_range_name_suffix(mut name: String, key_begin: u32) -> String {
    // Only key_begin is included in the journal name. key_end is included in
    // its labels, but may change over the journal's lifecycle as it's split.
    //
    // As a prettified special case, and for historical reasons, we represent the
    // u32::MIN value as just "00" instead of "00000000". This is safe because "00"
    // will naturally order before all other splits, as "00000000" would.
    // All other key splits are their normal 8-byte padded hexadecimal encodings.
    name.push_str("pivot=");
    if key_begin == u32::MIN {
        name.push_str("00");
    } else {
        name.push_str(&format!("{key_begin:08x}"));
    }
    name
}

/// Decode a begin / end key range from `estuary.dev/key-begin` and `estuary.dev/key-end` labels.
#[inline]
pub fn decode_key_range_labels(set: &LabelSet) -> Result<(u32, u32), Error> {
    let key_begin = expect_one_u32(set, KEY_BEGIN)?;
    let key_end = expect_one_u32(set, KEY_END)?;
    Ok((key_begin, key_end))
}

/// Extract a journal's templated name prefix by stripping off the
/// partition field and key range components.
/// Labels are required to count the number of field segments to strip.
pub fn name_prefix<'n>(name: &'n str, set: &LabelSet) -> Option<&'n str> {
    let count = set
        .labels
        .iter()
        .filter(|Label { name, .. }| name.starts_with(FIELD_PREFIX))
        .count();

    name.rsplitn(count + 2, "/").skip(count + 1).next()
}

/// Build the complete journal name implied by its `template_name`
/// prefix and its LabelSet.
pub fn full_name(template_name: &str, set: &LabelSet) -> Result<String, Error> {
    let key_begin = expect_one_u32(&set, KEY_BEGIN)?;

    let mut s = String::new();
    s.push_str(template_name);
    s.push('/');

    s = append_fields_name_suffix(s, set);
    s = append_key_range_name_suffix(s, key_begin);

    Ok(s)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::build_set;
    use serde_json::{Value, json};

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
        let doc = json!({
            "a": "Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra",
            "b": -123,
            "c": true,
            "d": "bye! 👋",
        });

        // Encode field labels, then compose with key range labels.
        let set = encode_extracted_fields_labels(
            build_set([("pass", "through")]),
            &fields,
            &extractors,
            &doc,
        )
        .unwrap();
        let set = encode_key_range_labels(set, 0x12341234, 0x56785678);

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

        // Both name-building paths (from extractors vs from labels) produce
        // identical suffixes, since they encode the same underlying values.
        let from_extractors = append_key_range_name_suffix(
            append_extracted_fields_name_suffix(String::new(), &fields, &extractors, &doc).unwrap(),
            0x12341234,
        );
        let from_labels = append_key_range_name_suffix(
            append_fields_name_suffix(String::new(), &set),
            0x12341234,
        );
        assert_eq!(from_extractors, from_labels);

        let name = format!("base/journal/name/{from_extractors}");
        assert_eq!(
            name,
            "base/journal/name/Loo=Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra/bar=%_-123/foo=%_true/z=bye%21%20%F0%9F%91%8B/pivot=12341234"
        );
        assert_eq!(name_prefix(&name, &set), Some("base/journal/name"));

        // full_name is a convenience for building a full journal name from its
        // template name prefix and labels.
        assert_eq!(name, full_name("base/journal/name", &set).unwrap())
    }

    #[test]
    fn test_key_range_name_suffix() {
        let cases = [
            (u32::MIN, "pivot=00"), // Prettified special case.
            (1, "pivot=00000001"),
            (0x12341234, "pivot=12341234"),
            (u32::MAX, "pivot=ffffffff"),
        ];
        for (key_begin, expect) in cases {
            assert_eq!(
                append_key_range_name_suffix(String::new(), key_begin),
                expect
            );
        }
    }

    #[test]
    fn test_decode_cases() {
        let fields = ["Bool", "String", "messy", "the_int"];

        let model = build_set([
            (crate::KEY_BEGIN, "10001000"),
            (crate::KEY_END, "20002000"),
            ("estuary.dev/field/Bool", "%_true"),
            ("estuary.dev/field/String", "hi%21%20%F0%9F%91%8B"),
            (
                "estuary.dev/field/messy",
                "Ba%2Bz%21%40_%22Bi.n%2Fg%22%20http%3A%2F%2Fexample-host%2Fpath%3Fq1%3Dv1%26q2%3Dv%2B2%3Bex%25%25tra",
            ),
            ("estuary.dev/field/the_int", "%_-8675309"),
        ]);

        // Field values decode independently of key range labels.
        insta::assert_json_snapshot!(
            decode_fields_labels(&model, &fields).unwrap(),
            @r###"
        [
          true,
          "hi! 👋",
          "Ba+z!@_\"Bi.n/g\" http://example-host/path?q1=v1&q2=v+2;ex%%tra",
          -8675309
        ]
        "###
        );

        assert_eq!(
            decode_key_range_labels(&model).unwrap(),
            (0x10001000, 0x20002000),
        );

        // Key range decode error cases.
        let key_errors: &[(LabelSet, &str)] = &[
            (
                crate::remove(model.clone(), crate::KEY_BEGIN),
                "expected one label for estuary.dev/key-begin (got [])",
            ),
            (
                crate::remove(model.clone(), crate::KEY_END),
                "expected one label for estuary.dev/key-end (got [])",
            ),
            (
                crate::set_value(model.clone(), crate::KEY_BEGIN, "0000000z"),
                r#"invalid value "0000000z" for label estuary.dev/key-begin"#,
            ),
        ];
        for (set, expect) in key_errors {
            assert_eq!(
                decode_key_range_labels(set).unwrap_err().to_string(),
                *expect
            );
        }

        // Field decode error cases.
        let field_errors: &[(LabelSet, &str)] = &[(
            crate::set_value(
                model.clone(),
                "estuary.dev/field/the_int",
                "%_-867_not_an_int",
            ),
            "failed to parse label value as integer",
        )];
        for (set, expect) in field_errors {
            assert_eq!(
                decode_fields_labels(set, &fields).unwrap_err().to_string(),
                *expect,
            );
        }
    }
}
