use crate::{specs, Resource, Result, Scope};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Endpoint {
    pub id: i64,
    pub resource: Resource,
}

impl Endpoint {
    pub fn register(scope: Scope, endpoint_name: &str, spec: &specs::Endpoint) -> Result<Endpoint> {
        let endpoint_type = spec.type_name();
        let uri = spec.uri();
        let resource = scope.resource();
        let mut stmt = scope.db.prepare_cached(
            "INSERT INTO endpoints (resource_id, endpoint_name, endpoint_type, endpoint_uri) VALUES (?, ?, ?, ?);",
        )?;
        stmt.execute(rusqlite::params![
            resource.id,
            endpoint_name,
            endpoint_type,
            uri
        ])?;
        let id = scope.db.last_insert_rowid();
        Ok(Endpoint { id, resource })
    }

    pub fn get_by_name(scope: Scope, endpoint_name: &str) -> Result<Endpoint> {
        scope
            .db
            .query_row(
                "select endpoint_id, resource_id from endpoints where endpoint_name = ?;",
                rusqlite::params![endpoint_name],
                |row| {
                    Ok(Endpoint {
                        id: row.get(0)?,
                        resource: Resource { id: row.get(1)? },
                    })
                },
            )
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{create, dump_table, ContentType, Resource};
    use url::Url;

    #[test]
    fn endpoint_is_registered() {
        let db = create(":memory:").unwrap();
        let resource_url = Url::parse("test://foo.bar/flow.yaml").unwrap();
        let resource = Resource::register_content(
            &db,
            ContentType::CatalogSpec,
            &resource_url,
            b"bogus content",
        )
        .unwrap();

        let scope = Scope::for_test(&db, resource.id);

        let uri = specs::Endpoint::Postgres(specs::EndpointUri::new(
            "postgres://foo:bar@baz.test:5432/flow",
        ));
        let endpoint =
            Endpoint::register(scope, "testName", &uri).expect("failed to register endpoint");
        let endpoints = dump_table(&db, "endpoints").unwrap();
        insta::assert_yaml_snapshot!(endpoints);

        let result =
            Endpoint::get_by_name(scope, "testName").expect("failed to get endpoint by name");
        assert_eq!(endpoint, result);
    }
}
