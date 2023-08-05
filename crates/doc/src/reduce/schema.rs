use super::{count_nodes_heap, Cursor, Error, Result};
use crate::{shape::limits, shape::schema::SchemaBuilder, AsNode, HeapNode, Shape};
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

    let (lhs, rhs) = (lhs.into_heap_node(alloc), rhs.into_heap_node(alloc));

    *tape = &tape[count_nodes_heap(&rhs)..];

    // Ensure that we're working with objects on both sides
    // Question: Should we actually relax this to support
    // reducing valid schemas like "true" and "false"?
    let (
        lhs @ HeapNode::Object(_),
        rhs @ HeapNode::Object(_)
    ) = (lhs, rhs) else {
        return Err(Error::with_location(Error::JsonSchemaMergeWrongType { detail: None }, loc) )
    };

    let left = shape_from_node(lhs).map_err(|e| Error::with_location(e, loc))?;
    let right = shape_from_node(rhs).map_err(|e| Error::with_location(e, loc))?;

    let mut merged_shape = Shape::union(left, right);
    limits::enforce_field_count_limits(&mut merged_shape, json::Location::Root);

    // Union together the LHS and RHS, and convert back from `Shape` into `HeapNode`.
    let merged_doc = serde_json::to_value(&SchemaBuilder::new(merged_shape).root_schema())
        .and_then(|value| HeapNode::from_serde(value, alloc))
        .map_err(|e| {
            Error::with_location(
                Error::JsonSchemaMergeWrongType {
                    detail: Some(e.to_string()),
                },
                loc,
            )
        })?;

    Ok(merged_doc)
}

fn shape_from_node<'a, N: AsNode>(node: N) -> Result<Shape> {
    // Should this be something more specific/useful?
    let url = url::Url::parse("json-schema-reduction:///").unwrap();

    let serialized =
        serde_json::to_value(node.as_node()).map_err(|e| Error::JsonSchemaMergeWrongType {
            detail: Some(e.to_string()),
        })?;

    let schema = json::schema::build::build_schema::<crate::Annotation>(url.clone(), &serialized)
        .map_err(|e| Error::JsonSchemaMergeWrongType {
        detail: Some(e.to_string()),
    })?;

    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    Ok(Shape::infer(
        index
            .must_fetch(&url)
            .map_err(|e| Error::JsonSchemaMergeWrongType {
                detail: Some(e.to_string()),
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
                    expect: Err(Error::JsonSchemaMergeWrongType { detail: None }),
                },
                Partial {
                    rhs: json!({
                        "type": "foo"
                    }),
                    expect: Err(Error::JsonSchemaMergeWrongType {
                        detail: Some(
                            r#"at keyword 'type' of schema 'json-schema-reduction:///': expected a type or array of types: invalid type name: 'foo'"#.to_owned(),
                        ),
                    }),
                },
                Partial {
                    rhs: json!({
                        "type": "string",
                        "minLength": 8,
                        "maxLength": 10
                    }),
                    expect: Ok(json!({
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "string",
                        "minLength": 5,
                        "maxLength": 10,
                    })),
                },
            ],
        )
    }
}
