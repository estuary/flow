use super::{count_nodes, Cursor, Error, Result};
use crate::{
    shape::limits,
    shape::{
        limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT, limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
        schema::to_schema,
    },
    AsNode, HeapNode, SerPolicy, Shape,
};
use json::schema::index::IndexBuilder;

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

    let mut merged_shape = Shape::union(left, right);
    limits::enforce_shape_complexity_limit(
        &mut merged_shape,
        DEFAULT_SCHEMA_COMPLEXITY_LIMIT,
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
}
