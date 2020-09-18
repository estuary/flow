use super::{Error, Result, DB};
use crate::catalog::materialization::TypesWrapper;
use estuary_json::schema::types;

/// Verifies that all locations used either as collection primary keys or as shuffle keys point to
/// locations that are guaranteed to exist and have valid scalar types. Locations are valid if they
/// have only one possible type (besides null) that is either "integer", "string", or "boolean".
/// Object, arrays, and floats may not be used for these keys.
pub fn verify_extracted_fields(db: &DB) -> Result<()> {
    // Check all collection primary keys to ensure that they have valid inferences
    let mut stmt = db.prepare(
        "SELECT c.collection_name, c.schema_uri, keys.value as ptr, i.types_json, i.must_exist
        FROM collections AS c, JSON_EACH(c.key_json) AS keys
        LEFT JOIN inferences AS i ON c.collection_id = i.collection_id AND keys.value = i.location_ptr
        "
    )?;
    let rows = stmt.query(rusqlite::NO_PARAMS)?;
    check_rows("primary", rows)?;

    // Check all transform shuffle keys to ensure that they have valid inferences
    let mut stmt = db.prepare(
        "SELECT collections.collection_name, coalesce(t.source_schema_uri, collections.schema_uri) as schema_uri, keys.value as ptr, i.types_json, i.must_exist
        FROM transforms AS t, JSON_EACH(t.shuffle_key_json) AS keys
        JOIN collections ON collections.collection_id = t.source_collection_id
        LEFT JOIN inferences AS i ON t.source_collection_id = i.collection_id AND keys.value = i.location_ptr;
        "
    )?;
    let rows = stmt.query(rusqlite::NO_PARAMS)?;
    check_rows("shuffle", rows)?;

    Ok(())
}

fn check_rows(key_type: &str, mut rows: rusqlite::Rows) -> Result<()> {
    let invalid_key_types = types::ARRAY | types::OBJECT | types::NUMBER;
    while let Some(row) = rows.next()? {
        let types_json: Option<TypesWrapper> = row.get("types_json")?;
        let must_exist: Option<bool> = row.get("must_exist")?;

        if !must_exist.unwrap_or_default() {
            return Err(error(
                key_type,
                row,
                "The pointer location is not guaranteed to exist by the schema.",
            )?);
        }

        if let Some(TypesWrapper(types)) = types_json {
            if types.overlaps(invalid_key_types) {
                let flavor = format!(
                    "Location may have possible values: {}",
                    types.to_json_array()
                );
                return Err(error(key_type, row, &flavor)?);
            }
        } else {
            unreachable!("must_exist was true, but there was no type information for the field");
        }
    }
    Ok(())
}

// Ok I'll admit that it's pretty weird to return a result where the success value is an error, but
// we are reading this information from the database to populate the error fields, so who knows
// what may happen.
fn error(key_type: &str, row: &rusqlite::Row, msg_flavor: &str) -> Result<Error> {
    let collection_name: String = row.get("collection_name")?;
    let schema_uri: String = row.get("schema_uri")?;
    let ptr: String = row.get("ptr")?;

    let context = format!("{} key of collection: '{}'", key_type, collection_name);
    let msg = format!("{} The key pointer must point to a location within the schema that has a single value type of integer, string, boolean, or null.", msg_flavor);
    Ok(Error::ExtractedFieldErr {
        schema_uri,
        ptr,
        context,
        msg,
    })
}
