use super::{count_nodes, Cursor, Error, Result};
use crate::{
    shape::limits,
    shape::{
        limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT, limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
        schema::to_schema, X_COMPLEXITY_LIMIT,
    },
    AsNode, HeapNode, SerPolicy, Shape,
};
use json::schema::index::IndexBuilder;
use serde_json::Value as JsonValue;

pub fn json_schema_merge<'alloc, L: AsNode, R: AsNode>(
    cur: Cursor<'alloc, '_, '_, '_, '_, L, R>,
) -> Result<HeapNode<'alloc>> {
    let Cursor {
        tape,
        loc,
        full: _,
        lhs,
        rhs,
        alloc,
    } = cur;

    let lhs = lhs
        .map(|n| serde_json::to_value(SerPolicy::noop().on_lazy(&n)).unwrap())
        .unwrap_or(serde_json::Value::Bool(false));
    let rhs = serde_json::to_value(SerPolicy::noop().on_lazy(&rhs)).unwrap();

    *tape = &tape[count_nodes(&rhs)..];

    let left = shape_from_node(lhs).map_err(|e| Error::with_location(e, loc))?;
    let right = shape_from_node(rhs).map_err(|e| Error::with_location(e, loc))?;

    let complexity_limit =
        if let Some(JsonValue::Number(limit)) = right.annotations.get(X_COMPLEXITY_LIMIT) {
            limit
                .as_u64()
                .and_then(|l| {
                    if l >= 1 && l <= 100_000 {
                        Some(l as usize)
                    } else {
                        None
                    }
                })
                .unwrap_or(DEFAULT_SCHEMA_COMPLEXITY_LIMIT)
        } else {
            DEFAULT_SCHEMA_COMPLEXITY_LIMIT
        };

    const X_GEN_ID: &str = "x-collection-generation-id";

    let mut merged_shape = match (
        left.annotations.get(X_GEN_ID),
        right.annotations.get(X_GEN_ID),
    ) {
        (Some(JsonValue::String(l_gen_id)), Some(JsonValue::String(r_gen_id))) => {
            match l_gen_id.cmp(r_gen_id) {
                std::cmp::Ordering::Equal => Shape::union(left, right),
                std::cmp::Ordering::Less => right, // LHS is an older generation and is reset.
                std::cmp::Ordering::Greater => left, // RHS is a stale update of an older generation and is discarded.
            }
        }
        (_, Some(JsonValue::String(gen_id))) | (Some(JsonValue::String(gen_id)), _) => {
            // Perform a merged reduction, retaining the generation ID available from only one side.
            // Shape::union intersects annotations and retains only those having equal key/values.
            let gen_id = JsonValue::String(gen_id.clone());
            let mut merged = Shape::union(left, right);
            merged.annotations.insert(X_GEN_ID.to_string(), gen_id);
            merged
        }
        _ => Shape::union(left, right),
    };

    limits::enforce_shape_complexity_limit(
        &mut merged_shape,
        complexity_limit,
        DEFAULT_SCHEMA_DEPTH_LIMIT,
    );

    // Convert back from `Shape` into `HeapNode`.
    let merged_doc = serde_json::to_value(to_schema(merged_shape)).unwrap();
    let merged_doc = HeapNode::from_serde(merged_doc, alloc).unwrap();
    Ok(merged_doc)
}

fn shape_from_node(node: serde_json::Value) -> Result<Shape> {
    let url = url::Url::parse("json-schema-reduction:///").unwrap();

    let schema = json::schema::build::build_schema::<crate::Annotation>(url.clone(), &node)
        .map_err(|e| Error::JsonSchemaMerge {
            detail: format!("{e:#}"),
        })?;

    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    Ok(Shape::infer(
        index.must_fetch(&url).map_err(|e| Error::JsonSchemaMerge {
            detail: format!("{e:#}"),
        })?,
        &index,
    ))
}

#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;

    #[test]
    fn test_merge_json_schemas() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "jsonSchemaMerge" } }),
            vec![
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 5,
                        "minLength": 5
                    }),
                    expect: Ok(json!({
                        "type": "string",
                        "maxLength": 5,
                        "minLength": 5
                    })),
                },
                Partial {
                    rhs: json!("oops!"),
                    expect: Err(Error::JsonSchemaMerge { detail: "at schema 'json-schema-reduction:///': expected a schema".to_string() }),
                },
                Partial {
                    rhs: json!({
                        "type": "foo"
                    }),
                    expect: Err(Error::JsonSchemaMerge {
                        detail: r#"at keyword 'type' of schema 'json-schema-reduction:///': expected a type or array of types: invalid type name: 'foo'"#.to_owned(),
                    }),
                },
                Partial {
                    rhs: json!({
                        "type": "string",
                        "minLength": 8,
                        "maxLength": 10
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 10,
                    })),
                },
            ],
        )
    }

    #[test]
    fn test_merge_json_schema_numeric_bounds_floats_vs_integers() {
        // This scenario comes up when schemas are inferred for data having a mix of numeric values
        // like `1` vs `1.0`. Inference will include the `.0` if the input was a float, but not if it
        // was an integer. When we reduce the schemas, we should always prefer the decimal value if the
        // two are otherwise equal. In reality, we ought never see `type: integer` with minimum/maximum
        // values that are decimals. But that is technically possible, which this test demonstrates.
        run_reduce_cases(
            json!({ "reduce": { "strategy": "jsonSchemaMerge" } }),
            vec![
                Partial {
                    rhs: json!({
                        "type": "integer",
                        "maximum": 5,
                        "minimum": 4
                    }),
                    expect: Ok(json!({
                        "type": "integer",
                        "maximum": 5,
                        "minimum": 4
                    })),
                },
                Partial {
                    rhs: json!({
                        "type": "integer",
                        "maximum": 5.0,
                        "minimum": 4.0
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "integer",
                        "maximum": 5.0,
                        "minimum": 4.0
                    })),
                },
                // Further reductions of integer values should keep the decimal
                Partial {
                    rhs: json!({
                        "type": "integer",
                        "maximum": 5,
                        "minimum": 4
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "integer",
                        "maximum": 5.0,
                        "minimum": 4.0
                    })),
                },
                // Except when the integer values are less restrictive than the decimals
                Partial {
                    rhs: json!({
                        "type": "integer",
                        "maximum": 6,
                        "minimum": 3
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "integer",
                        "maximum": 6,
                        "minimum": 3
                    })),
                },
            ],
        )
    }

    #[test]
    fn test_merge_with_reset() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "jsonSchemaMerge" } }),
            vec![
                // Initial schema without a generation ID.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 5,
                        "minLength": 5
                    }),
                    expect: Ok(json!({
                        "type": "string",
                        "maxLength": 5,
                        "minLength": 5
                    })),
                },
                // Generation ID is retained if LHS omits it.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 6,
                        "minLength": 6,
                        "x-collection-generation-id": "0011223344556677",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 6,
                        "minLength": 5,
                        "x-collection-generation-id": "0011223344556677",
                    })),
                },
                // Another reduction of the same generation ID.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 4,
                        "minLength": 4,
                        "x-collection-generation-id": "0011223344556677",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 6,
                        "minLength": 4,
                        "x-collection-generation-id": "0011223344556677",
                    })),
                },
                // Reset! Old schema is dropped.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 10,
                        "minLength": 10,
                        "x-collection-generation-id": "1122334455667788",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 10,
                        "minLength": 10,
                        "x-collection-generation-id": "1122334455667788",
                    })),
                },
                // Stale update of older generation ID is ignored.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 1,
                        "minLength": 1,
                        "x-collection-generation-id": "0011223344556677",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 10,
                        "minLength": 10,
                        "x-collection-generation-id": "1122334455667788",
                    })),
                },
                // Update at current generation ID.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 5,
                        "minLength": 5,
                        "x-collection-generation-id": "1122334455667788",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 10,
                        "minLength": 5,
                        "x-collection-generation-id": "1122334455667788",
                    })),
                },
                // Reset once more.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 100,
                        "minLength": 100,
                        "x-collection-generation-id": "2233445566778899",
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 100,
                        "minLength": 100,
                        "x-collection-generation-id": "2233445566778899",
                    })),
                },
                // Generation ID is retained if RHS omits it.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 200,
                        "minLength": 200,
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 200,
                        "minLength": 100,
                        "x-collection-generation-id": "2233445566778899",
                    })),
                },
                // Or if it's the wrong type.
                Partial {
                    rhs: json!({
                        "type": "string",
                        "maxLength": 50,
                        "minLength": 50,
                        "x-collection-generation-id": null,
                    }),
                    expect: Ok(json!({
                        "$schema": "https://json-schema.org/draft/2019-09/schema",
                        "type": "string",
                        "maxLength": 200,
                        "minLength": 50,
                        "x-collection-generation-id": "2233445566778899",
                    })),
                },
            ],
        )
    }
}
