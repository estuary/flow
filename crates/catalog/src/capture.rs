use crate::{specs, Collection, Endpoint, Error, Resource, Result, Scope, DB};

#[derive(Debug, Copy, Clone)]
pub struct Capture {
    id: i64,
    resource: Resource,
}

impl Capture {
    pub fn register(scope: Scope, spec: &specs::Capture) -> Result<Capture> {
        let (endpoint_id, source_entity): (Option<i64>, &str) = match &spec.source {
            specs::CaptureSource::External { endpoint, target } => {
                let resolved = scope
                    .push_prop("endpoint")
                    .then(|scope| Endpoint::get_by_name(scope, endpoint.as_str()))?;
                (Some(resolved.id), target.as_str())
            }
            specs::CaptureSource::Builtin(builtin_source) => (None, builtin_source.type_name()),
        };

        let collection = scope
            .push_prop("target")
            .then(|scope| Collection::get_imported_by_name(scope, spec.target.name.as_ref()))?;
        let resource = scope.resource();

        let mut stmt = scope.db.prepare_cached("INSERT INTO captures
                                               (resource_id, endpoint_id, source_entity, target_collection_id)
                                               VALUES (?, ?, ?, ?);")?;

        stmt.execute(rusqlite::params![
            resource.id,
            endpoint_id,
            source_entity,
            collection.id
        ])?;
        let id = scope.db.last_insert_rowid();

        Ok(Capture { id, resource })
    }
}

#[cfg(test)]
mod test {
    use crate::{dump_tables, test_register};

    #[test]
    fn capture_is_registered_for_flow_ingester() {
        let yaml = r##"
            collections:
              - name: foo
                key: [/id]
                schema:
                  type: object
                  properties:
                    id: { type: integer }
                  required: [id]

            captures:
              - source: flow-ingester
                target: { name: foo }
            "##;
        let db = test_register(yaml).expect("failed to register");
        let results = dump_tables(&db, &["endpoints", "captures"]).unwrap();
        insta::assert_yaml_snapshot!(results);
    }
}
