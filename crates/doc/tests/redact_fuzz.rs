use doc::AsNode;

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

#[test]
fn fuzz_redaction() {
    // Expect that `doc` contains matches the schematized redaction rules.
    // On success, also returns the computed tape length for verification.
    fn is_fully_redacted(doc: &doc::HeapNode) -> Result<i32, ()> {
        match doc {
            doc::HeapNode::String(val) if val.starts_with("sha256:") => Ok(1),
            doc::HeapNode::String(_) => Err(()), // Should be hashed.
            doc::HeapNode::Bool(_) => Err(()),   // Should be blocked.
            doc::HeapNode::Array(_, items) => {
                let mut total_length = 1;
                for item in items.iter() {
                    total_length += is_fully_redacted(item)?;
                }
                Ok(total_length)
            }
            doc::HeapNode::Object(_, fields) => {
                let mut total_length = 1;
                for field in fields.iter() {
                    total_length += is_fully_redacted(&field.value)?;
                }
                Ok(total_length)
            }
            // Everything else should be unchanged.
            _ => Ok(1),
        }
    }

    fn inner_test(input: ArbitraryValue) -> bool {
        let schema = serde_json::json!({
          "$anchor": "Node",
          "anyOf": [
            // Recursive case, which matches anything.
            {
              "items": {"$ref": "#Node"},
              "additionalProperties": {"$ref": "#Node"},
            },
            // Block all bools.
            {"type": "boolean", "redact": {"strategy": "block"}},
            // Hash all strings.
            {"type": "string", "redact": {"strategy": "sha256"}},

            // Block subtrees having 3-5 properties.
            {
              "type": "object",
              "minProperties": 3,
              "maxProperties": 5,
              "redact": {"strategy": "block"},
            },
            // Hash subtrees having 6-8 items.
            {
              "type": "array",
              "minItems": 6,
              "maxItems": 8,
              "redact": {"strategy": "sha256"},
            }
          ]
        });

        let curi = url::Url::parse("http://example/schema").unwrap();
        let mut validator =
            doc::Validator::new(doc::validation::build_schema(curi, &schema).unwrap()).unwrap();

        let alloc = doc::Allocator::new();
        let doc = doc::HeapNode::from_node(&input.0, &alloc);
        let mut doc = doc::HeapNode::new_array(&alloc, [doc].into_iter()); // Wrap to never remove root.

        let pre_tape_length = doc.tape_length();
        let valid = validator.validate(None, &doc).unwrap().ok().unwrap();
        let outcome = doc::redact::redact(&mut doc, valid.outcomes(), &alloc, &[]).unwrap();

        let expected_tape_length = match outcome {
            doc::redact::Outcome::Unchanged => pre_tape_length,
            doc::redact::Outcome::Modified { tape_delta } => pre_tape_length + tape_delta,
            doc::redact::Outcome::Remove { tape_length: _ } => unreachable!(),
        };

        if let Ok(tape_length) = is_fully_redacted(&doc) {
            assert_eq!(tape_length, expected_tape_length);
            true
        } else {
            false
        }
    }

    quickcheck::QuickCheck::new()
        .gen(quickcheck::Gen::new(500))
        .quickcheck(inner_test as fn(ArbitraryValue) -> bool);
}
