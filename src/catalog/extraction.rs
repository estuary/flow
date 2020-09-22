use super::{Error, Result, DB};
use std::fmt::{self, Display};

/// Verifies that all locations used either as collection primary keys or as shuffle keys point to
/// locations that are guaranteed to exist and have valid scalar types. Locations are valid if they
/// have only one possible type (besides null) that is either "integer", "string", or "boolean".
/// Object, arrays, and floats may not be used for these keys.
pub fn verify_extracted_fields(db: &DB) -> Result<()> {
    let errors = collect_errors(db)?;
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::InvalidCollectionKeys(errors))
    }
}

fn collect_errors(db: &DB) -> Result<Vec<KeyError>> {
    // Check all collection primary keys to ensure that they have valid inferences
    let mut stmt = db.prepare(
        "SELECT schema_uri, location_ptr, source, types_json, error
            FROM collection_keys
            WHERE error IS NOT NULL;",
    )?;
    let results = stmt
        .query_map(rusqlite::NO_PARAMS, |row| {
            Ok(KeyError {
                location_ptr: row.get("location_ptr")?,
                source: row.get("source")?,
                schema_uri: row.get("schema_uri")?,
                inferred_types_json: row.get("types_json")?,
                message: row.get("error")?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(results)
}

#[derive(Debug, PartialEq)]
pub struct KeyError {
    location_ptr: String,
    source: String,
    message: String,
    schema_uri: String,
    inferred_types_json: Option<String>,
}
impl Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Invalid {}: '{}' (from {}). {}",
            self.source, self.location_ptr, self.schema_uri, self.message
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::catalog::{init_db_schema, open};
    use itertools::Itertools;

    #[test]
    fn all_key_errors_are_returned() {
        let db = open(":memory:").unwrap();
        init_db_schema(&db).unwrap();

        db.execute_batch(
            r##"
            INSERT INTO resources (resource_id, content_type, content, is_processed)
            VALUES (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'0123', TRUE);

            INSERT INTO collections
                (collection_id, resource_id, collection_name, schema_uri, key_json)
            VALUES
                (1, 1, 'okA', 'test://schema.json', '["/a", "/b"]'),
                (2, 1, 'okB', 'test://schema.json', '["/a", "/b"]'),
                (3, 1, 'okC', 'test://schema.json', '["/a", "/b"]'),

                (98, 1, 'bad/a', 'test://bad/schema.json', '["/missing_inference", "/object"]'),
                (99, 1, 'bad/b', 'test://bad/schema.json', '["/missing_inference", "/object"]');

            INSERT INTO derivations
                (collection_id, register_schema_uri, register_initial_json)
            VALUES
                (2, 'test://whatever', '{}'),
                (3, 'test://anything', '{}'),
                (98, 'test://anything', '{}'),
                (99, 'test://anything', '{}');

            INSERT INTO lambdas (lambda_id, runtime, inline) VALUES (1, 'remote', 'http://test.test/foo');

            INSERT INTO transforms
                (transform_id, derivation_id, transform_name, source_collection_id, publish_id,
                        source_schema_uri, shuffle_key_json)
            VALUES
                (1, 2, 'goodTransformA', 1, 1, 'test://tsource/schema.json', '["/goodKeyA", "/goodKeyB"]'),
                (2, 3, 'goodTransformB', 2, 1, NULL, '["/goodKeyC"]'),

                (3, 98, 'badTransformA', 1, 1, NULL, '["/multi_types", "/may_not_exist"]'),
                (4, 99, 'badTransformB', 2, 1, 'test://badTrans/schema.json', '["/float", "/array"]');

            INSERT INTO projections
                (collection_id, field, location_ptr, user_provided)
            VALUES
                (2, 'a', '/a', TRUE),
                (2, 'b', '/b', TRUE),
                (99, 'missing_inference', '/missing_inference', TRUE),
                (99, 'object', '/object', TRUE),
                (99, 'multi_types', '/multi_types', TRUE);

            INSERT INTO partitions
                (collection_id, field)
            VALUES
                (2, 'a'),
                (2, 'b'),
                (99, 'missing_inference'),
                (99, 'object'),
                (99, 'multi_types');

            INSERT INTO inferences
                (schema_uri, location_ptr, types_json, must_exist)
            VALUES
                -- These are all valid to use as keys
                ('test://schema.json', '/a', '["string"]', TRUE),
                ('test://schema.json', '/b', '["integer", "null"]', TRUE),
                ('test://tsource/schema.json', '/goodKeyA', '["boolean"]', TRUE),
                ('test://tsource/schema.json', '/goodKeyB', '["string", "null"]', TRUE),
                ('test://schema.json', '/goodKeyC', '["integer"]', TRUE),

                -- All invalid to use as keys
                ('test://bad/schema.json', '/object', '["object"]', TRUE),
                ('test://bad/schema.json', '/multi_types', '["string", "integer"]', TRUE),
                ('test://bad/schema.json', '/may_not_exist', '["integer"]', FALSE),

                ('test://badTrans/schema.json', '/float', '["number"]', TRUE),
                ('test://badTrans/schema.json', '/array', '["array"]', TRUE);
         "##,
        )
        .expect("setup failed");

        let errors = collect_errors(&db).expect("failed to verify fields");
        insta::assert_display_snapshot!(errors.into_iter().join("\n"));
    }
}
