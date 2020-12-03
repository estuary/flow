use crate::{specs, Collection, Endpoint, Error, Resource, Result, Scope, DB};
use itertools::Itertools;
use json::schema::types;
use std::collections::{HashMap, HashSet};

pub struct Materialization {
    pub id: i64,
    pub resource: Resource,
}

impl Materialization {
    pub fn register(scope: Scope, spec: &specs::Materialization) -> Result<Materialization> {
        let endpoint = scope
            .push_prop("endpoint")
            .then(|s| Endpoint::get_by_name(s, &spec.endpoint))?;
        let collection = scope
            .push_prop("source")
            .then(|s| Collection::get_imported_by_name(s, spec.source.name.as_ref()))?;

        let fields = scope.push_prop("fields").then(|scope| match &spec.fields {
            specs::MaterializationFields::Include(include) => {
                resolve_projections_including(scope, collection.id, include.as_slice())
            }
            specs::MaterializationFields::Exclude(exclude) => {
                resolve_projections_excluding(scope, collection.id, exclude.as_slice())
            }
        })?;
        let fields_json = serde_json::to_string(&fields)?;

        let mut stmt = scope.db.prepare_cached(
            "INSERT INTO materializations
                (resource_id, endpoint_id, target_entity, source_collection_id, fields_json)
            VALUES (?, ?, ?, ?, ?);",
        )?;

        let resource = scope.resource();
        stmt.execute(rusqlite::params![
            resource.id,
            endpoint.id,
            spec.target.as_str(),
            collection.id,
            fields_json
        ])?;
        let id = scope.db.last_insert_rowid();

        validate_materialization_fields(scope.db, id)?;
        Ok(Materialization { id, resource })
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{}", .0.iter().join("\n"))]
pub struct MaterializationFieldsError(Vec<String>);

// Queries the materialization_invalid_projections view and returns any errors found there.
// This must only be called after the materialization has been inserted.
fn validate_materialization_fields(db: &DB, materialization_id: i64) -> Result<()> {
    use rusqlite::OptionalExtension;
    let mut stmt = db.prepare_cached(
        "select error from materialization_invalid_projections where materialization_id = ?;",
    )?;

    let mut errors = Vec::new();
    if let Some(mut rows) = stmt
        .query(rusqlite::params![materialization_id])
        .optional()?
    {
        while let Some(row) = rows.next()? {
            let err: String = row.get(0)?;
            errors.push(err);
        }
    }

    if !errors.is_empty() {
        Err(MaterializationFieldsError(errors).into())
    } else {
        Ok(())
    }
}

fn resolve_projections_including(
    scope: Scope,
    collection_id: i64,
    include: &[String],
) -> Result<Vec<String>> {
    // We only need to validate that each field actually has a valid projection. Technically, the
    // database schema already enforces this invariant, but we'll do up front explicitly so that we
    // can produce good error messages with suggestions for the nearest match.
    for field in include {
        // TODO: another place for using a multi-error
        scope.push_prop(field).then(|scope| {
            let (nearest, osa_distance) =
                get_closest_match_projection(scope.db, collection_id, field.as_str())?;
            if field != &nearest {
                Err(Error::missing_projection(
                    field.clone(),
                    Some((nearest, osa_distance)),
                ))
            } else {
                Ok(())
            }
        })?;
    }
    Ok(include.to_owned())
}

fn include_default_projection(
    current_by_location: &HashMap<String, (String, bool)>,
    types: types::Set,
    location_ptr: &str,
) -> bool {
    !current_by_location.contains_key(location_ptr)
        && (types.is_single_scalar_type() || location_ptr.is_empty())
}

fn resolve_projections_excluding(
    scope: Scope,
    collection_id: i64,
    exclude: &[String],
) -> Result<Vec<String>> {
    let mut by_location = HashMap::new();
    let mut stmt = scope.db.prepare_cached("SELECT field, location_ptr, user_provided, types_json, is_primary_key FROM projected_fields WHERE collection_id = ?;")?;
    let mut rows = stmt.query(rusqlite::params![collection_id])?;

    // We'll remove the excluded fields from this set as they're encountered, and return
    // an error if there are any fields left here after iterating all the projections. If there are
    // any remaining, then it's because no projection exists with that field name.
    let mut excluded_fields = exclude.iter().cloned().collect::<HashSet<_>>();

    while let Some(row) = rows.next()? {
        let field: String = row.get(0)?;
        let location_ptr: String = row.get(1)?;
        let user_provided: bool = row.get(2)?;
        let types_json: String = row.get(3)?;
        let is_primary_key: bool = row.get(4)?;

        let types: types::Set = serde_json::from_str(&types_json)?;
        if !excluded_fields.remove(&field) {
            if user_provided || include_default_projection(&by_location, types, &location_ptr) {
                by_location.insert(location_ptr, (field, is_primary_key));
            }
        }
    }

    // TODO: (phil) consider creating a multi-error variant and using that here
    // User has provided at least one field name to exclude, but no projection exists with that name.
    if let Some(unmatched_field) = excluded_fields.into_iter().next() {
        let closest_match =
            get_closest_match_projection(scope.db, collection_id, &unmatched_field)?;
        return Err(Error::missing_projection(
            unmatched_field.to_string(),
            Some(closest_match),
        ));
    }

    // Sort the projections in a reasonable order. The most important thing is that this ordering
    // is deterministic and consistent across subsequent catalog builds. But we try to also make
    // the ordering reasonable by sorting by whether the field is part of the key and then by
    // location, except for the empty location pointer, which always goes last. This is to match
    // the convention of putting primary key columns first in SQL DDL.
    let sorted_results = by_location
        .into_iter()
        .sorted_by(|(a_loc, (_, a_is_key)), (b_loc, (_, b_is_key))| {
            let mut loc_ordering = a_loc.cmp(b_loc);
            // Is either location the root pointer? If so, then reverse so that the root pointer is
            // always sorted last.
            if a_loc.is_empty() || b_loc.is_empty() {
                loc_ordering = loc_ordering.reverse();
            }
            a_is_key.cmp(&b_is_key).reverse().then(loc_ordering)
        })
        .map(|(_, v)| v.0)
        .collect::<Vec<_>>();
    Ok(sorted_results)
}

fn get_flow_document_projection(db: &DB, collection_id: i64) -> Result<String> {
    let query = "SELECT field
        FROM projections
        WHERE collection_id = ? AND location_ptr = ''
        ORDER BY user_provided, field DESC
        LIMIT 1;";
    db.query_row(query, rusqlite::params![collection_id], |r| r.get(0))
        .map_err(Into::into)
}

fn get_closest_match_projection(
    db: &DB,
    collection_id: i64,
    field: &str,
) -> rusqlite::Result<(String, i64)> {
    let query = "SELECT field, osa_distance(field, ?) AS osa_dist
        FROM projections
        WHERE collection_id = ?
        ORDER BY osa_dist ASC
        LIMIT 1;";
    db.query_row(query, rusqlite::params![field, collection_id], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })
}

#[deprecated = "transitioning to Materialization"]
#[derive(Debug, Copy, Clone)]
pub struct MaterializationTarget {
    pub id: i64,
}

impl MaterializationTarget {
    pub fn register(
        scope: &Scope,
        target_name: &str,
        spec: &specs::MaterializationTarget,
    ) -> crate::Result<MaterializationTarget> {
        let uri = match &spec.config {
            specs::MaterializationConfig::Postgres(connection) => connection.uri.clone(),
            specs::MaterializationConfig::Sqlite(connection) => {
                if let Err(url::ParseError::RelativeUrlWithoutBase) =
                    url::Url::parse(connection.uri.as_str())
                {
                    let canonical_path = std::env::current_dir()?.join(connection.uri.as_str());
                    log::debug!(
                        "resolved path: '{}' for sqlite materialization target: '{}'",
                        canonical_path.display(),
                        target_name
                    );
                    canonical_path.display().to_string()
                } else {
                    connection.uri.clone()
                }
            }
        };

        let mut stmt = scope.db.prepare_cached(
            "INSERT INTO materialization_targets (
                target_name,
                target_type,
                target_uri
            ) VALUES (?, ?, ?);",
        )?;
        let params = rusqlite::params![target_name, spec.config.type_name(), uri,];
        stmt.execute(params)?;

        let id = scope.db.last_insert_rowid();
        Ok(MaterializationTarget { id })
    }

    pub fn get_by_name(db: &DB, name: &str) -> crate::Result<MaterializationTarget> {
        let mut stmt = db.prepare_cached(
            "SELECT target_id FROM materialization_targets WHERE target_name = ?;",
        )?;
        let params = rusqlite::params![name];
        let id: i64 = stmt.query_row(params, |row| row.get(0))?;
        Ok(MaterializationTarget { id })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{create, dump_tables, test_register, Error};

    #[test]
    fn register_returns_error_when_materialization_fields_are_missing_root_document() {
        let yaml = r##"
            collections:
              - name: foo
                key: [/id]
                schema:
                  type: object
                  properties:
                    id: { type: integer }
                    wee: { type: string }
                    woo: { type: number }
                  required: [id, wee]
            endpoints:
              testDB:
                postgres: 'postgres://flow:flow@flow.test:5432/flow'
            materializations:
              - source:
                  name: foo
                endpoint: testDB
                target: test_table
                fields:
                  include:
                    - id
                    - wee
            "##;
        let err = test_register(yaml)
            .expect_err("expected an error")
            .unlocate();

        let expected_err = "Materialization must include a projection of the root document (location pointer of an empty string)";
        match err {
            Error::InvalidMaterialization(MaterializationFieldsError(messages)) => {
                assert_eq!(vec![expected_err.to_string()], messages);
            }
            other => panic!("expected invalidMaterialization error, got: {:?}", other),
        }
    }

    #[test]
    fn materialization_is_registered_with_exclude_fields() {
        let yaml = r##"
            collections:
              - name: foo
                key: [/id]
                schema:
                  type: object
                  properties:
                    id: { type: integer }
                    wee: { type: string }
                    woo: { type: number }
                  required: [id, wee]
                projections:
                  wat: /woo
            endpoints:
              testDB:
                postgres: 'postgres://flow:flow@flow.test:5432/flow'
            materializations:
              - source:
                  name: foo
                endpoint: testDB
                target: test_table
                fields:
                  exclude:
                    - wee
            "##;
        let db = test_register(yaml).expect("failed to register");
        let results = dump_tables(&db, &["materializations", "materializations_json"]).unwrap();
        insta::assert_yaml_snapshot!(results);
    }

    #[test]
    fn materialization_is_registered_with_include_fields() {
        let yaml = r##"
            collections:
              - name: foo
                key: [/id]
                schema:
                  type: object
                  properties:
                    id: { type: integer }
                    wee: { type: string }
                    woo: { type: number }
                  required: [id, wee]
                projections:
                  wat: /woo
            endpoints:
              testDB:
                postgres: 'postgres://flow:flow@flow.test:5432/flow'
            materializations:
              - source:
                  name: foo
                endpoint: testDB
                target: test_table
                fields:
                  include:
                    - id
                    - flow_document
                    - wat
            "##;
        let db = test_register(yaml).expect("failed to register");
        let results = dump_tables(&db, &["materializations", "materializations_json"]).unwrap();
        insta::assert_yaml_snapshot!(results);
    }

    #[test]
    fn materialization_is_registered_with_default_fields() {
        let yaml = r##"
            collections:
              - name: foo
                key: [/id]
                schema:
                  type: object
                  properties:
                    id: { type: integer }
                    wee: { type: string }
                    woo: { type: number }
                  required: [id, wee]
                projections:
                  wat: /woo
            endpoints:
              testDB:
                postgres: 'postgres://flow:flow@flow.test:5432/flow'
            materializations:
              - source:
                  name: foo
                endpoint: testDB
                target: test_table
            "##;
        let db = test_register(yaml).expect("failed to register");
        let results = dump_tables(&db, &["materializations", "materializations_json"]).unwrap();
        insta::assert_yaml_snapshot!(results);
    }

    #[test]
    fn get_by_name_returns_extant_target() {
        let db = create(":memory:").unwrap();
        db.execute_batch(
            r##"
                         insert into materialization_targets
                             (target_id, target_name, target_type, target_uri)
                         values
                             (997, 'someTarget', 'sqlite', 'file:///canary.db');
                         "##,
        )
        .expect("setup failed");
        let target = MaterializationTarget::get_by_name(&db, "someTarget")
            .expect("failed to get materialization target");
        assert_eq!(997, target.id);
    }

    #[test]
    fn get_by_name_returns_err_when_named_target_does_not_exist() {
        let db = create(":memory:").unwrap();
        let err = MaterializationTarget::get_by_name(&db, "nonExistant")
            .expect_err("expected an error from get_by_name");
        assert!(matches!(
            err,
            Error::SQLiteErr(rusqlite::Error::QueryReturnedNoRows)
        ));
    }
}
