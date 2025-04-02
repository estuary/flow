use super::{Error, RecordName, RecordSchema, Schema, FLOW_EXTRA_NAME};
use doc::{AsNode, Field, Fields, Node};

/// Encode `node` at `loc` with the given `schema` into buffer `b`.
pub fn encode<'s, 'n, N: AsNode>(
    loc: json::Location,
    b: &mut Vec<u8>,
    schema: &'s Schema,
    node: &'n N,
) -> Result<(), Error> {
    if !maybe_encode(loc, b, schema, node)? {
        return Err(Error::NotMatched {
            ptr: loc.pointer_str().to_string(),
            expected: schema.clone(),
            actual: serde_json::to_value(&doc::SerPolicy::noop().on(node)).unwrap(),
        });
    }
    Ok(())
}

/// Encode a `key` extracted from `root` with the given `schema` into buffer `b`.
pub fn encode_key<'s, 'n, N: AsNode>(
    b: &mut Vec<u8>,
    schema: &'s Schema,
    root: &'n N,
    key: &[doc::Pointer],
) -> Result<(), Error> {
    let Schema::Record(key_record) = schema else {
        return Err(Error::KeySchemaMalformed);
    };
    let Some(parts_field) = key_record.fields.first() else {
        return Err(Error::KeySchemaMalformed);
    };
    let Schema::Record(parts_record) = &parts_field.schema else {
        return Err(Error::KeySchemaMalformed);
    };
    if parts_record.fields.len() != key.len() {
        return Err(Error::KeyComponentsMismatch {
            expected: parts_record.fields.len(),
            actual: key.len(),
        });
    }

    let loc = json::Location::Root;
    for (ptr, field) in key.iter().zip(parts_record.fields.iter()) {
        if let Some(node) = ptr.query(root) {
            encode(loc.push_prop(&field.name), b, &field.schema, node)?;
        } else if let Some(default) = &field.default {
            encode(loc.push_prop(&field.name), b, &field.schema, default)?;
        } else {
            encode(
                loc.push_prop(&field.name),
                b,
                &field.schema,
                &serde_json::Value::Null,
            )?;
        }
    }

    Ok(())
}

/// Attempt to encode `node` at `loc` with the given `schema` into buffer `b`.
/// Returns true if the encoding was successful, or false if the schema
/// did not match and `b` was not extended.
fn maybe_encode<'s, 'n, N: AsNode>(
    loc: json::Location,
    b: &mut Vec<u8>,
    schema: &'s Schema,
    node: &'n N,
) -> Result<bool, Error> {
    match (schema, node.as_node()) {
        (Schema::Union(union), _) => {
            assert!(union.variants().len() < 64);
            b.push(0);

            for variant in union.variants() {
                if maybe_encode(loc, b, variant, node)? {
                    return Ok(true);
                }
                *b.last_mut().unwrap() += 2; // Increment by one, two's complement.
            }
            Ok(false)
        }
        (Schema::Null, Node::Null) => Ok(true),

        (Schema::Boolean, Node::Bool(v)) => {
            b.push(v as u8);
            Ok(true)
        }

        (Schema::Long, Node::NegInt(v)) => {
            zig_zag(b, v);
            Ok(true)
        }
        (Schema::Long, Node::PosInt(v)) => {
            zig_zag(b, v as i64);
            Ok(true)
        }
        (Schema::Long, Node::Float(v)) if v.fract() == 0.0 => {
            zig_zag(b, v as i64);
            Ok(true)
        }

        (Schema::Double, Node::NegInt(v)) => {
            b.extend_from_slice(&(v as f64).to_le_bytes());
            Ok(true)
        }
        (Schema::Double, Node::PosInt(v)) => {
            b.extend_from_slice(&(v as f64).to_le_bytes());
            Ok(true)
        }
        (Schema::Double, Node::Float(v)) => {
            b.extend_from_slice(&v.to_le_bytes());
            Ok(true)
        }
        (Schema::Double, Node::String(v)) => {
            let v = v
                .parse::<f64>()
                .map_err(|err| Error::ParseFloat(v.to_string(), err))?;
            b.extend_from_slice(&v.to_le_bytes());
            Ok(true)
        }

        (Schema::String, Node::String(v)) => {
            zig_zag(b, v.len() as i64);
            b.extend(v.as_bytes());
            Ok(true)
        }
        (Schema::String, Node::NegInt(v)) => {
            let v = format!("{v}");
            zig_zag(b, v.len() as i64);
            b.extend(v.as_bytes());
            Ok(true)
        }
        (Schema::String, Node::PosInt(v)) => {
            let v = format!("{v}");
            zig_zag(b, v.len() as i64);
            b.extend(v.as_bytes());
            Ok(true)
        }

        (Schema::Date, Node::String(s)) => {
            let Ok(date) = time::Date::parse(s, &TIME_FORMAT_DATE) else {
                return Ok(false);
            };
            let primitive_date_time = time::PrimitiveDateTime::new(date, time::Time::MIDNIGHT);
            let offset_date_time = primitive_date_time.assume_utc();
            let duration_since_epoch = offset_date_time.unix_timestamp();
            let days_since_epoch = duration_since_epoch / 86_400; // Number of seconds in a day

            zig_zag(b, days_since_epoch);
            Ok(true)
        }
        (Schema::Duration, Node::String(s)) => {
            let Some(caps) = ISO8601_DURATION.captures(s) else {
                return Ok(false);
            };
            let years: u32 = caps.get(1).map_or(0, |m| m.as_str().parse().unwrap());
            let months: u32 = caps.get(2).map_or(0, |m| m.as_str().parse().unwrap());
            let days: u32 = caps.get(3).map_or(0, |m| m.as_str().parse().unwrap());
            let hours: u32 = caps.get(4).map_or(0, |m| m.as_str().parse().unwrap());
            let minutes: u32 = caps.get(5).map_or(0, |m| m.as_str().parse().unwrap());
            let seconds: f64 = caps.get(6).map_or(0.0, |m| m.as_str().parse().unwrap());

            let months: u32 = years * 12 + months;
            let millis: u32 = hours * 3_600_000 + minutes * 60_000 + (seconds * 1_000.0) as u32;

            b.extend_from_slice(&months.to_le_bytes());
            b.extend_from_slice(&days.to_le_bytes());
            b.extend_from_slice(&millis.to_le_bytes());
            Ok(true)
        }
        (Schema::TimestampMicros, Node::String(s)) => {
            let Ok(dt) =
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
            else {
                return Ok(false);
            };
            let dt = dt.unix_timestamp_nanos() / 1_000; // Nanos to microseconds.
            zig_zag(b, dt as i64);
            Ok(true)
        }
        (Schema::Uuid, Node::String(s)) => {
            zig_zag(b, s.len() as i64);
            b.extend(s.as_bytes());
            Ok(true)
        }

        // A RawJSON encodes the JSON serialization of `node` as a string.
        (
            Schema::Record(RecordSchema {
                name: RecordName { name, .. },
                ..
            }),
            _,
        ) if name == "RawJSON" => {
            let enc = serde_json::to_vec(&doc::SerPolicy::noop().on(node)).unwrap();
            zig_zag(b, enc.len() as i64);
            b.extend_from_slice(enc.as_slice());
            Ok(true)
        }
        (Schema::Record(schema), Node::Object(fields)) => {
            // RawJSON encodes the JSON serialization of `node` as a string.
            if schema.name.name == "RawJSON" {
                let enc = serde_json::to_vec(&doc::SerPolicy::noop().on(node)).unwrap();
                zig_zag(b, enc.len() as i64);
                b.extend_from_slice(enc.as_slice());
                return Ok(true);
            }

            let mut extra: Vec<(&'n str, &'n N)> = Vec::new();
            let mut schema_it = schema.fields.iter();
            let mut field_it = fields.iter();

            let mut maybe_schema = schema_it.next();
            let mut maybe_field = field_it.next();

            // Perform an ordered merge over `schema_it` and `field_it`.
            while let Some(schema) = maybe_schema {
                match maybe_field
                    .as_ref()
                    .map(|field| (field.property().cmp(&schema.name), field))
                {
                    None | Some((std::cmp::Ordering::Greater, _)) => {
                        // Schematized field is not present in this object instance.

                        if schema.name == FLOW_EXTRA_NAME {
                            let Schema::Map(map_schema) = &schema.schema else {
                                return Err(Error::ExtraPropertiesMap);
                            };
                            // This field is constructed as the last schematized field of the schema.
                            // Any remaining object fields are implicitly "extra".
                            while let Some(field) = maybe_field {
                                extra.push((field.property(), field.value()));
                                maybe_field = field_it.next();
                            }

                            if !extra.is_empty() {
                                zig_zag(b, extra.len() as i64);
                                for (name, value) in extra.iter() {
                                    zig_zag(b, name.len() as i64); // Key length.
                                    b.extend(name.as_bytes()); // Key content.
                                    encode(loc.push_prop(name), b, &map_schema.types, *value)?;
                                }
                            }
                            zig_zag(b, 0); // Close map.
                        } else {
                            // Encode the default value or NULL.
                            encode(
                                loc.push_prop(&schema.name),
                                b,
                                &schema.schema,
                                schema.default.as_ref().unwrap_or(&serde_json::Value::Null),
                            )?;
                        }
                        maybe_schema = schema_it.next();
                    }
                    Some((std::cmp::Ordering::Less, field)) => {
                        extra.push((field.property(), field.value()));
                        maybe_field = field_it.next();
                    }
                    Some((std::cmp::Ordering::Equal, field)) => {
                        encode(
                            loc.push_prop(&field.property()),
                            b,
                            &schema.schema,
                            field.value(),
                        )?;
                        maybe_schema = schema_it.next();
                        maybe_field = field_it.next();
                    }
                }
            }
            Ok(true)
        }
        (Schema::Map(map_schema), Node::Object(fields)) => {
            if fields.len() != 0 {
                zig_zag(b, fields.len() as i64);
                for field in fields.iter() {
                    zig_zag(b, field.property().len() as i64); // Key length.
                    b.extend(field.property().as_bytes()); // Key content.
                    encode(
                        loc.push_prop(field.property()),
                        b,
                        &map_schema.types,
                        field.value(),
                    )?;
                }
            }
            zig_zag(b, 0); // Close map.
            Ok(true)
        }
        (Schema::Array(array_schema), Node::Array(items)) => {
            if !items.is_empty() {
                zig_zag(b, items.len() as i64);
                for (index, item) in items.iter().enumerate() {
                    encode(loc.push_item(index), b, &array_schema.items, item)?;
                }
            }
            zig_zag(b, 0); // Close array.
            Ok(true)
        }

        _ => Ok(false),
    }
}

fn zig_zag(b: &mut Vec<u8>, z: i64) {
    let mut z = ((z << 1) ^ (z >> 63)) as u64;

    loop {
        if z <= 0x7F {
            b.push((z & 0x7F) as u8);
            break;
        } else {
            b.push((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
    }
}

lazy_static::lazy_static! {
    // The set of allowed characters in an AVRO field name.
    static ref ISO8601_DURATION : regex::Regex = regex::Regex::new(r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?").unwrap();
    static ref TIME_FORMAT_DATE : Vec<time::format_description::FormatItem<'static>> = time::format_description::parse("[year]-[month]-[day]").unwrap();
}

#[cfg(test)]
mod test {
    use serde_json::json;

    #[test]
    fn test_basic_types() {
        let fixture = json!({
          "type": "object",
          "properties": {
            "a": {"type": "null"},
            "b": {"type": "boolean"},
            "c1_neg": {"type": "integer"},
            "c2_pos": {"type": "integer"},
            "c3_def": {"type": "integer", "default": 42},
            "d1_neg": {"type": "number"},
            "d2_pos": {"type": "number"},
            "d3_f64": {"type": "number"},
            "d4_str": {"oneOf": [{"type": "number"}, {"type": "string", "format": "number"}]},
            "e1_str": {"type": "string"},
            "e2_str_def": {"type": "string", "default": "the-default"},
            "e4_pos_int": {"oneOf": [{"type": "integer"}, {"type": "string", "format": "integer"}]},
            "e5_neg_int": {"oneOf": [{"type": "integer"}, {"type": "string", "format": "integer"}]},
            "e6_int_str": {"oneOf": [{"type": "integer"}, {"type": "string", "format": "integer"}]},
            "f": {"type": "string", "format": "date"},
            "g": {"type": "string", "format": "duration"},
            "h": {"type": "string", "format": "date-time"},
            "i": {"type": "string", "format": "uuid"},
            "j": {"oneOf": [{"type": "number"}, {"type": "string"}]},
            "k": {"type": "object", "additionalProperties": {"type": "integer"}},
            "l": {"type": "array", "items": {"type": "boolean"}},
            "m1_with_addl": {"type": "array", "items": {"type": "object", "properties": {"d": {"type": "boolean"}, "f": {"type": "boolean"}}}},
            "m2_no_addl": {"type": "object", "properties": {"d": {"type": "boolean"}}, "additionalProperties": false},
            "m2_disallowed_field": {"type": "object", "properties": {"not valid": {"type": "boolean"}}, "additionalProperties": false},
          },
          "required": ["b", "c2_pos", "e1_str", "j", "m1_with_addl"],
        });
        let key_ptrs = &["/c2_pos", "/c3_def", "/f", "/i"];
        let key_ptrs: Vec<_> = key_ptrs.iter().map(|p| doc::Pointer::from_str(p)).collect();

        let (key, value) = crate::json_schema_to_avro(&fixture.to_string(), &key_ptrs).unwrap();

        insta::assert_json_snapshot!(json!({
            "key": &key,
            "value": &value,
        }));

        let instance = json!({
            "a": null,
            "b": true,
            "c1_neg": i64::MIN,
            "c2_pos": i64::MAX,
            "d1_neg": i64::MIN,
            "d2_pos": i64::MAX,
            "d3_f64": 3.14159,
            "d4_str": "123.456",
            "e1_str": "a string",
            "e4_pos_int": i64::MAX,
            "e5_neg_int": i64::MIN,
            "e6_int_str": "123456",
            "f": "2022-05-10",
            "g": "P1Y2M3DT4H5M6.789S",
            "h": "2019-03-06T00:00:00Z",
            "i": "a0b0c0d0-1000-2000-3000-400000000000",
            "j": "raw json",
            "k": {"jenny": 8675309},
            "l": [true, false],
            "m1_with_addl": [
                {},
                {"a": "leading extra", "d": true},
                {"e": "middle extra", "d": true, "f": true},
                {"z": "trailing extra", "f": true},
            ],
            "m2_no_addl": {"d": true},
            "m2_disallowed_field": {"not valid": true},
            "zz - ex": ["extra", 3],
        });

        let mut b = Vec::new();
        super::encode(json::Location::Root, &mut b, &value, &instance).unwrap();
        insta::assert_snapshot!(to_hex(&b));

        let recovered = apache_avro::from_avro_datum(&value, &mut &b[..], None).unwrap();
        insta::assert_debug_snapshot!(recovered);

        let mut b = Vec::new();
        super::encode_key(&mut b, &key, &instance, &key_ptrs).unwrap();
        insta::assert_snapshot!(to_hex(&b));

        let recovered = apache_avro::from_avro_datum(&key, &mut &b[..], None).unwrap();
        insta::assert_debug_snapshot!(recovered);
    }

    fn to_hex(v: &[u8]) -> String {
        hexdump::hexdump_iter(v)
            .map(|line| format!(" {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
