use super::{FLOW_EXTRA_NAME, FLOW_KEY_NAME};
use apache_avro::schema as avro;
use json::schema::formats::Format;
use json::schema::types;
use std::fmt::Write;

/// Map a Shape at the given location into an AVRO schema.
/// If the location is not required and has no default, it may implicitly be none.
pub fn shape_to_avro(loc: json::Location, shape: doc::Shape, required: bool) -> avro::Schema {
    let mut type_ = shape.type_;

    // Is this location nullable ? NULL may union with any other schema.
    let nullable = if shape.type_.overlaps(types::NULL) || (!required && shape.default.is_none()) {
        type_ = type_ - types::NULL;
        true
    } else {
        false
    };

    let base = if type_.overlaps(types::STRING) {
        match (type_ - types::STRING, shape.string.format) {
            // If the location may contain a fractional number, map to a double.
            // This could lose precision of a string number, but we're already using f64's,
            // and these are typically representing Nan or Inf.
            (types::INT_OR_FRAC | types::FRACTIONAL, Some(Format::Number | Format::Integer)) => {
                avro::Schema::Double
            }
            // Or, if the location may contain an integer, map to a string.
            // A string integer may not fit in a 64-bit signed integer.
            (types::INTEGER, Some(Format::Number | Format::Integer)) => avro::Schema::String,
            // String which holds an RFC-3339 date.
            (types::INVALID, Some(Format::Date)) => avro::Schema::Date,
            // String which holds an RFC-3339 date-time.
            (types::INVALID, Some(Format::DateTime)) => avro::Schema::TimestampMicros,
            // String which holds a canonical UUID.
            (types::INVALID, Some(Format::Uuid)) => avro::Schema::Uuid,
            // String which holds an ISO-8601 duration.
            (types::INVALID, Some(Format::Duration)) => avro::Schema::Duration,
            // A regular string.
            (types::INVALID, _) => avro::Schema::String,
            // Other combinations fall back to JSON encoding.
            _ => raw_json_schema(loc),
        }
    } else {
        match type_ {
            types::ARRAY => array_to_avro(loc, shape.array),
            types::BOOLEAN => avro::Schema::Boolean,
            types::INTEGER => avro::Schema::Long,
            types::INT_OR_FRAC | types::FRACTIONAL => avro::Schema::Double,
            types::OBJECT => object_to_avro(loc, shape.object),
            // Other combinations fall back to JSON encoding.
            _ => raw_json_schema(loc),
        }
    };

    if !nullable || matches!(base, avro::Schema::Null) {
        base
    } else {
        avro::Schema::Union(avro::UnionSchema::new(vec![base, avro::Schema::Null]).unwrap())
    }
}

// Map a key extracted from a Shape into a flattened Avro Record.
pub fn key_to_avro(key: &[doc::Pointer], shape: doc::Shape) -> avro::Schema {
    let loc_root = json::Location::Root;
    let loc_key = loc_root.push_prop("Key");
    let loc_parts = loc_key.push_prop("Parts");

    let mut parts = Vec::new();

    // Map each key component into a field with a structured name.
    for (position, ptr) in key.iter().enumerate() {
        let name = format!("p{}", position + 1);
        let (shape, _) = shape.locate(ptr);

        let default = shape.default.as_ref().map(|d| d.0.clone());
        let schema = shape_to_avro(loc_parts.push_prop(&name), shape.clone(), true);

        parts.push(avro::RecordField {
            aliases: None,
            custom_attributes: Default::default(),
            default,
            doc: None,
            name,
            order: avro::RecordFieldOrder::Ascending,
            position,
            schema,
        });
    }

    // Wrap the record of key components in a single-field parent record.
    let parts_schema = avro::Schema::Record(avro::RecordSchema {
        name: location_to_name(loc_parts),
        aliases: None,
        doc: None,
        fields: parts,
        attributes: Default::default(),
        lookup: Default::default(),
    });

    avro::Schema::Record(avro::RecordSchema {
        name: location_to_name(loc_key),
        aliases: None,
        doc: None,
        fields: vec![avro::RecordField {
            aliases: None,
            custom_attributes: Default::default(),
            default: None,
            doc: None,
            name: FLOW_KEY_NAME.to_string(),
            order: avro::RecordFieldOrder::Ascending,
            position: 0,
            schema: parts_schema,
        }],
        attributes: Default::default(),
        lookup: Default::default(),
    })
}

// Map an Object Shape into an Avro schema (a Record or Map).
fn object_to_avro(loc: json::Location, obj: doc::shape::ObjShape) -> avro::Schema {
    let mut fields: Vec<avro::RecordField> = Vec::new();
    let mut extra = doc::Shape::nothing();

    for pattern in obj.pattern_properties {
        extra = doc::Shape::union(extra, pattern.shape);
    }
    if let Some(addl) = obj.additional_properties {
        extra = doc::Shape::union(extra, *addl);
    } else {
        extra = doc::Shape::union(extra, doc::Shape::anything());
    }

    // If there are no explicit properties, but this object may have pattern or
    // additional properties, then interpret it as an Avro map.
    if extra.type_ != types::INVALID && obj.properties.is_empty() {
        let schema = shape_to_avro(loc, extra, true);
        return avro::Schema::map(schema);
    }

    // Otherwise, build a Record which may have a placeholder
    // field for additional dynamic properties.

    for prop in obj.properties {
        if !AVRO_FIELD_RE.is_match(&prop.name) {
            extra = doc::Shape::union(extra, prop.shape);
            continue; // Cannot be represented under Avro's name restrictions.
        }
        let default = prop.shape.default.as_ref().map(|d| d.0.clone());
        let schema = shape_to_avro(loc.push_prop(&prop.name), prop.shape, prop.is_required);

        fields.push(avro::RecordField {
            aliases: None,
            custom_attributes: Default::default(),
            default,
            doc: None,
            name: prop.name.to_string(),
            order: avro::RecordFieldOrder::Ascending,
            position: fields.len(),
            schema,
        })
    }

    // If extra dynamic properties are possible, add a trailing field for them.
    // This field MUST appear last in the record schema.

    if extra.type_ != types::INVALID {
        let schema = shape_to_avro(loc.push_prop(FLOW_EXTRA_NAME), extra, true);
        let schema = avro::Schema::map(schema);

        fields.push(avro::RecordField {
            aliases: None,
            custom_attributes: Default::default(),
            default: None,
            doc: None,
            name: FLOW_EXTRA_NAME.to_string(),
            order: avro::RecordFieldOrder::Ascending,
            position: fields.len(),
            schema,
        })
    }

    avro::Schema::Record(avro::RecordSchema {
        name: location_to_name(loc),
        aliases: None,
        doc: None,
        fields,
        attributes: Default::default(),
        lookup: Default::default(),
    })
}

// Map an Array Shape into an Avro Array.
fn array_to_avro(loc: json::Location, shape: doc::shape::ArrayShape) -> avro::Schema {
    let mut items = doc::Shape::nothing();

    if let Some(addl) = shape.additional_items {
        items = doc::Shape::union(items, *addl);
    } else if !matches!(shape.max_items, Some(m) if m as usize <= shape.tuple.len()) {
        items = doc::Shape::union(items, doc::Shape::anything());
    }

    for shape in shape.tuple {
        items = doc::Shape::union(items, shape);
    }

    let items = shape_to_avro(loc.push_prop("_items"), items, true);
    avro::Schema::array(items)
}

// Map a location into a special Schema holding a string-encoded JSON value.
fn raw_json_schema(loc: json::Location) -> avro::Schema {
    let fields = vec![avro::RecordField {
        aliases: None,
        custom_attributes: Default::default(),
        default: None,
        doc: None,
        name: "json".to_string(),
        order: avro::RecordFieldOrder::Ignore,
        position: 0,
        schema: avro::Schema::String,
    }];

    avro::Schema::Record(avro::RecordSchema {
        name: location_to_name(loc.push_prop("RawJSON")),
        aliases: None,
        doc: None,
        fields,
        attributes: Default::default(),
        lookup: Default::default(),
    })
}

// Map a Location into an Avro dot-separated namespace and name.
fn location_to_name(loc: json::Location) -> avro::Name {
    let name = loc.fold(String::new(), move |loc, mut n: String| {
        match loc {
            json::Location::Root => n.push_str("root"),
            json::Location::Property(json::LocatedProperty { name, .. }) => {
                n.push('.');
                n.push_str(name);
            }
            json::Location::Item(json::LocatedItem { index, .. }) => {
                write!(&mut n, ".{index}").unwrap();
            }
            _ => unreachable!(),
        };
        n
    });

    avro::Name::new(&name).unwrap()
}

lazy_static::lazy_static! {
    // The set of allowed characters in an AVRO field name.
    pub static ref AVRO_FIELD_RE : regex::Regex = regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap();
}

#[cfg(test)]
mod test {
    use serde_json::json;

    #[test]
    fn test_complex_schema() {
        let fixture = json!({
          "type": "object",
          "properties": {
            "a_bool": {"type": "boolean"},
            "a_date": {"type": "string", "format": "date"},
            "a_date_time": {"type": "string", "format": "date-time"},
            "a_duration": {"type": "string", "format": "duration"},
            "a_fractional": {"type": "number", "not": {"type": "integer"}},
            "a_integer_like": {"oneOf": [{"type": "integer"}, {"type": "string", "format": "integer"}]},
            "a_null": {"type": "null"},
            "a_number_like":  {"oneOf": [{"type": "number"}, {"type": "string", "format": "number"}]},
            "a_number_like_mixed1": {"oneOf": [{"type": "integer"}, {"type": "string", "format": "number"}]},
            "a_number_like_mixed2": {"oneOf": [{"type": "number"}, {"type": "string", "format": "integer"}]},
            "a_str": {"type": "string", "default": "xyz"},
            "an_int": {"type": "integer", "default": 32},
            "arr_items": {"type": "array", "items": {"type": "boolean"}},
            "arr_tuple": {
              "type": "array",
              "items": [{"type": "number"}, {"type": "string", "format": "number"}],
              "maxItems": 2,
            },
            "arr_tuple_items": {
              "type": "array",
              "items": [{"type": "integer"}, {"type": "integer"}],
              "additionalItems": {"type": "number"},
            },
            "map": {
                "type": "object",
                "additionalProperties": {"type": "boolean"},
            },
            "obj": {
              "type": "object",
              "properties": {
                "bar": {
                  "type": "object",
                  "properties": {"a_const": {"const": 42}, "filtered invalid avro name": {"const": 52}},
                  "additionalProperties": {"type": "integer"},
                  "patternProperties": {
                    "str_.*": {"type": "string", "format": "integer"},
                  },
                  "required": ["a_const"],
                },
                "foo": {"type": "boolean"},
              },
              "required": ["foo", "bar"],
              "additionalProperties": false,
            },
          },
          "required": ["an_int", "a_bool", "a_date", "obj"],
        }).to_string();

        let key = &["/a_bool", "/obj/a_map/a_const"];
        let key: Vec<_> = key.iter().map(|p| doc::Pointer::from_str(p)).collect();
        insta::assert_json_snapshot!(schema_test(&fixture, &key));
    }

    fn schema_test(json_schema: &str, key: &[doc::Pointer]) -> serde_json::Value {
        let (key, value) = crate::json_schema_to_avro(json_schema, key).unwrap();

        json!({
            "key": &key,
            "value": &value,
        })
    }
}
