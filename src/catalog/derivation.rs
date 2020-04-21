use super::{Collection, Resource, Result, Schema, Lambda};
use crate::specs::build as specs;
use crate::doc::Pointer;
use rusqlite::{params as sql_params, Connection as DB};
use std::convert::TryFrom;

/// Derivation represents a derivation of a catalog Collection.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Derivation {
    pub collection: Collection,
}

impl Derivation {
    pub fn register(db: &DB, collection: Collection, spec: &specs::Derivation) -> Result<Derivation> {
        db.prepare_cached("INSERT INTO derivations (collection_id, parallelism) VALUES (?, ?)")?
            .execute(sql_params![collection.id, spec.parallelism])?;

        let derivation = Derivation{collection};

        for spec in &spec.bootstrap {
            derivation.register_bootstrap(db, spec)?;
        }
        for spec in &spec.transform {
            derivation.register_transform(db, spec)?;
        }
        Ok(derivation)
    }

    fn register_bootstrap(&self, db: &DB, spec: &specs::Lambda) -> Result<()> {
        let lambda = Lambda::register(db, self.collection.resource, spec)?;

        db.prepare_cached("INSERT INTO bootstraps (derivation_id, lambda_id) VALUES (?, ?)")?
            .execute(sql_params![self.collection.id, lambda.id])?;
        Ok(())
    }

    fn register_transform(&self, db: &DB, spec: &specs::Transform) -> Result<()> {
        let source = Collection::query(db, &spec.source, self.collection.resource)?;

        // Register optional source schema.
        let mut schema_uri: Option<String> = None;

        if let Some(uri) = &spec.source_schema {
            let uri = self.collection.resource.join(db, uri)?;
            let schema = Schema::register(db, uri.clone())?;
            Resource::register_import(db, self.collection.resource, schema.resource)?;
            schema_uri = Some(uri.into_string())
        }

        // If shuffle key is None, default to the source key.
        let shuffle_key = match &spec.shuffle.key {
            Some(key) => key.clone(),
            None => source.key(db)?,
        };
        for key in shuffle_key.iter() {
            Pointer::try_from(key)?;
        }
        let shuffle_key = serde_json::to_string(&shuffle_key)?;

        let lambda = Lambda::register(db, self.collection.resource, &spec.lambda)?;

        db.prepare_cached(
            "INSERT INTO transforms (
                        derivation_id,
                        source_collection_id,
                        source_schema_uri,
                        shuffle_key_json,
                        shuffle_broadcast,
                        shuffle_choose,
                        lambda_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?
        .execute(sql_params![
            self.collection.id,
            source.id,
            schema_uri,
            shuffle_key,
            spec.shuffle.broadcast,
            spec.shuffle.choose,
            lambda.id,
        ])?;

        Ok(())
    }
}

