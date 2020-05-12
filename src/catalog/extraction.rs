use super::{sql_params, Error, Result, Schema, DB};
use crate::doc::{inference, Pointer, SchemaIndex};
use estuary_json::schema::types;
use serde_json::Value;
use url::Url;

pub fn verify_extracted_fields(db: &DB) -> Result<()> {
    // Compile and index all Schemas.
    let schemas = Schema::compile_all(db)?;
    let mut index = SchemaIndex::new();
    for schema in &schemas {
        index.add(&schema)?;
    }

    let mut stmt = db.prepare(
        "SELECT schema_uri, JSON_GROUP_ARRAY(JSON_ARRAY(ptr, is_key, context))
                FROM schema_extracted_fields
                GROUP BY schema_uri;",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    while let Some(r) = rows.next()? {
        let (uri, fields): (Url, Value) = (r.get(0)?, r.get(1)?);

        let schema = index.must_fetch(&uri)?;
        let shape = inference::Shape::infer(schema, &index);
        let fields: Vec<(String, u8, String)> = serde_json::from_value(fields)?;

        // Verify each field JSON-Pointer resolves to a known schema shape,
        // which is always a scalar.
        for (ptr, is_key, context) in fields.into_iter() {
            let (shape, must_exist) = match shape.locate(&Pointer::from(&ptr)) {
                Some((shape, must_exist)) => (shape, must_exist),
                None => {
                    return Err(Error::ExtractedFieldErr {
                        context,
                        schema_uri: uri.into_string(),
                        ptr,
                        msg: "field not found in schema".to_owned(),
                    });
                }
            };

            if is_key == 0 {
                // Un-keyed fields are null-able, and may be of any JSON type.
                continue;
            }

            if !must_exist {
                return Err(Error::ExtractedFieldErr {
                    context,
                    schema_uri: uri.into_string(),
                    ptr,
                    msg: "the schema does not verify that the field always exists (check 'type', 'required', or 'minItems'?)"
                        .to_owned(),
                });
            }

            if shape
                .type_
                .overlaps(types::ARRAY | types::OBJECT | types::NUMBER)
            {
                return Err(Error::ExtractedFieldErr {
                    context,
                    schema_uri: uri.into_string(),
                    ptr,
                    msg: "keyed fields cannot extract from schema locations which could be an array, object, or float"
                        .to_owned(),
                });
            }
        }
    }

    Ok(())
}
