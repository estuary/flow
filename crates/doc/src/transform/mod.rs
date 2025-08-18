use super::{
    AsNode, BumpVec, Field, Fields, HeapField, HeapNode, Node, Valid,
};

pub mod strategy;
pub use strategy::Strategy;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    // Currently no error variants, but we keep the enum for future extensibility
    #[error("transform error")]
    _Placeholder,
}

type Result<T> = std::result::Result<T, Error>;

/// Transform a document in-place using transformation annotations from its validation.
/// Returns false if the entire document should be removed, true otherwise.
pub fn transform<'alloc>(
    node: &mut HeapNode<'alloc>,
    valid: Valid,
    alloc: &'alloc bumpalo::Bump,
) -> Result<bool> {
    let tape = valid.extract_transform_annotations();
    
    // If no transformations at all, keep the node unchanged
    if tape.iter().all(|t| t.is_none()) {
        return Ok(true);
    }
    
    let tape = &mut tape.as_slice();
    let keep = transform_with_tape(node, tape, json::Location::Root, alloc)?;
    
    assert!(tape.is_empty());
    Ok(keep)
}

type Index<'a> = &'a [Option<(&'a Strategy, u64)>];

// Transform a node in-place, returning false if it should be removed
fn transform_with_tape<'alloc>(
    node: &mut HeapNode<'alloc>,
    tape: &mut Index<'_>,
    loc: json::Location<'_>,
    alloc: &'alloc bumpalo::Bump,
) -> Result<bool> {
    // Check if current location has a transform annotation
    if let Some((strategy, _)) = tape.first().and_then(|t| *t) {
        match strategy {
            Strategy::Remove => {
                // Skip this node and all its children in the tape
                *tape = &tape[count_nodes(node)..];
                return Ok(false);  // Signal removal
            }
        }
    }

    // No transform at this location - increment tape and transform children
    *tape = &tape[1..];
    
    match node {
        HeapNode::Object(fields) => {
            // We need to rebuild the fields to remove items
            let mut new_fields = BumpVec::with_capacity_in(fields.len(), alloc);
            
            // Process each field
            let len = fields.len();
            for _ in 0..len {
                let field = fields.remove(0);
                let property = field.property;
                let mut value = field.value;
                
                let child_loc = loc.push_prop(&property);
                let keep = transform_with_tape(&mut value, tape, child_loc, alloc)?;
                
                if keep {
                    new_fields.push(HeapField { property, value }, alloc);
                }
                // If !keep, the field is removed
            }
            
            // Replace with transformed fields
            *fields = new_fields;
            Ok(true)  // Keep the object itself
        }
        HeapNode::Array(items) => {
            // We need to rebuild the array to remove items
            let mut new_items = BumpVec::with_capacity_in(items.len(), alloc);
            
            // Process each item
            let len = items.len();
            for index in 0..len {
                let mut item = items.remove(0);
                
                let child_loc = loc.push_item(index);
                let keep = transform_with_tape(&mut item, tape, child_loc, alloc)?;
                
                if keep {
                    new_items.push(item, alloc);
                }
                // If !keep, the item is removed
            }
            
            // Replace with transformed items
            *items = new_items;
            Ok(true)  // Keep the array itself
        }
        // Scalar values pass through unchanged
        _ => Ok(true),
    }
}

fn count_nodes<N: AsNode>(node: &N) -> usize {
    match node.as_node() {
        Node::Bool(_)
        | Node::Bytes(_)
        | Node::Float(_)
        | Node::NegInt(_)
        | Node::Null
        | Node::PosInt(_)
        | Node::String(_) => 1,

        Node::Array(v) => count_nodes_items(v),
        Node::Object(v) => count_nodes_fields::<N>(v),
    }
}

fn count_nodes_items<N: AsNode>(items: &[N]) -> usize {
    items.iter().fold(1, |c, vv| c + count_nodes(vv))
}

fn count_nodes_fields<N: AsNode>(fields: &N::Fields) -> usize {
    fields
        .iter()
        .fold(1, |c, field| c + count_nodes(field.value()))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Validator;
    use json::schema::build::build_schema;
    use serde_json::json;

    #[test]
    fn test_remove_object_property() {
        let schema = json!({
            "type": "object",
            "properties": {
                "keep": {"type": "string"},
                "remove": {
                    "type": "string",
                    "transform": "remove"
                }
            }
        });

        let doc = json!({
            "keep": "value1",
            "remove": "value2"
        });

        let curi = url::Url::parse("http://example/schema").unwrap();
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();
        
        let valid = validator.validate(None, &doc).unwrap().ok().unwrap();
        let mut heap_doc = HeapNode::from_node(&doc, &alloc);
        
        let kept = transform(&mut heap_doc, valid, &alloc).unwrap();
        assert!(kept);
        
        // Verify "remove" property was removed
        match heap_doc {
            HeapNode::Object(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].property.as_str(), "keep");
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_remove_array_item() {
        let schema = json!({
            "type": "array",
            "items": [
                {"type": "string"},
                {
                    "type": "string",
                    "transform": "remove"
                },
                {"type": "string"}
            ]
        });

        let doc = json!(["first", "second", "third"]);

        let curi = url::Url::parse("http://example/schema").unwrap();
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();
        
        let valid = validator.validate(None, &doc).unwrap().ok().unwrap();
        let mut heap_doc = HeapNode::from_node(&doc, &alloc);
        
        let kept = transform(&mut heap_doc, valid, &alloc).unwrap();
        assert!(kept);
        
        // Verify second item was removed
        match heap_doc {
            HeapNode::Array(items) => {
                assert_eq!(items.len(), 2);
                match (&items[0], &items[1]) {
                    (HeapNode::String(s1), HeapNode::String(s2)) => {
                        assert_eq!(s1.as_str(), "first");
                        assert_eq!(s2.as_str(), "third");
                    }
                    _ => panic!("Expected strings"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_remove_entire_document() {
        let schema = json!({
            "type": "object",
            "transform": "remove"
        });

        let doc = json!({"any": "content"});

        let curi = url::Url::parse("http://example/schema").unwrap();
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();
        
        let valid = validator.validate(None, &doc).unwrap().ok().unwrap();
        let mut heap_doc = HeapNode::from_node(&doc, &alloc);
        
        let kept = transform(&mut heap_doc, valid, &alloc).unwrap();
        
        // Verify entire document was removed
        assert!(!kept);
    }
    
    #[test]
    fn test_object_format_rejected() {
        // Test that the old object format is rejected
        let schema = json!({
            "type": "object",
            "transform": {"strategy": "remove"}
        });

        let curi = url::Url::parse("http://example/schema").unwrap();
        let result = build_schema::<crate::Annotation>(curi, &schema);
        
        // Should fail to build the schema with object format
        assert!(result.is_err());
    }
}