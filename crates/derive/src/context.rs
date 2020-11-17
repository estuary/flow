use super::lambda;
use super::nodejs;
use catalog::{self, sql_params};
use doc::{Pointer, SchemaIndex};
use std::sync::Arc;
use url::Url;

pub struct Transform {
    pub transform_id: i32,
    pub source_schema: Url,
    pub update: lambda::Lambda,
    pub publish: lambda::Lambda,

    // Index of this Transform within its owning array.
    // This makes it easy to map back to an index from a &Transform.
    pub index: usize,
}

pub struct Context {
    pub transforms: Vec<Transform>,
    pub schema_index: &'static SchemaIndex<'static>,

    pub derivation_id: i32,
    pub derivation_name: String,
    pub derivation_schema: Url,
    pub derivation_key: Arc<[Pointer]>,

    pub register_schema: Url,
    pub register_initial: serde_json::Value,
}

impl Context {
    pub fn build_from_catalog(
        db: &catalog::DB,
        derivation: &str,
        schema_index: &'static SchemaIndex<'static>,
        node: &nodejs::NodeRuntime,
    ) -> Result<Context, catalog::Error> {
        let (derivation_id, derived_schema, derived_key, register_schema, register_initial): (
            i32,
            Url,
            serde_json::Value,
            Url,
            serde_json::Value,
        ) = db
            .prepare(
                "SELECT
                    collection_id,
                    schema_uri,
                    key_json,
                    register_schema_uri,
                    register_initial_json
                FROM collections
                NATURAL JOIN derivations
                WHERE collection_name = ?",
            )?
            .query_row(sql_params![derivation], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .map_err(|err| catalog::Error::At {
                loc: format!("querying for derived collection {:?}", derivation),
                detail: Box::new(err.into()),
            })?;

        let derived_key: Vec<String> = serde_json::from_value(derived_key)?;
        let derived_key = derived_key.iter().map(|s| s.into()).collect::<Vec<_>>();

        // Load all transforms of this derivation.
        let mut transforms = Vec::new();
        let mut stmt = db.prepare(
            "SELECT
            transform_id,             -- 0
            source_schema_uri,        -- 1
            update_runtime,           -- 2
            publish_runtime,          -- 3
            update_inline,            -- 4 (needed for 'remote')
            publish_inline            -- 5
        FROM transform_details
            WHERE derivation_id = ?;
        ",
        )?;
        let mut rows = stmt.query(sql_params![derivation_id])?;

        while let Some(r) = rows.next()? {
            let (transform_id, source_schema, update_runtime, publish_runtime): (
                i32,
                Url,
                Option<String>,
                Option<String>,
            ) = (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?);

            let update = match update_runtime.as_deref() {
                None => lambda::Lambda::Noop,
                Some("nodeJS") => node.new_update_lambda(transform_id),
                Some("remote") => lambda::Lambda::new_web_json(r.get::<_, Url>(4)?),
                Some(rt) => panic!("transform {} has invalid runtime {:?}", transform_id, rt),
            };

            let publish = match publish_runtime.as_deref() {
                None => lambda::Lambda::Noop,
                Some("nodeJS") => node.new_publish_lambda(transform_id),
                Some("remote") => lambda::Lambda::new_web_json(r.get::<_, Url>(5)?),
                Some(rt) => panic!("transform {} has invalid runtime {:?}", transform_id, rt),
            };

            transforms.push(Transform {
                transform_id,
                source_schema,
                update,
                publish,

                index: 0, // Filled out below.
            })
        }
        // Index Transforms on their order.
        for (i, l) in transforms.iter_mut().enumerate() {
            l.index = i;
        }

        Ok(Context {
            transforms,
            schema_index,
            derivation_id,
            derivation_name: derivation.to_owned(),
            derivation_schema: derived_schema,
            derivation_key: derived_key.into(),
            register_schema,
            register_initial,
        })
    }
}
