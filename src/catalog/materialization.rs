use crate::catalog::{self, specs, Scope, DB};

#[derive(Debug, Copy, Clone)]
pub struct MaterializationTarget {
    pub id: i64,
}

impl MaterializationTarget {
    pub fn register(
        scope: &Scope,
        target_name: &str,
        spec: &specs::MaterializationTarget,
    ) -> catalog::Result<MaterializationTarget> {
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

    pub fn get_by_name(db: &DB, name: &str) -> catalog::Result<MaterializationTarget> {
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
    use crate::catalog::{self, Error};

    #[test]
    fn get_by_name_returns_extant_target() {
        let db = catalog::create(":memory:").unwrap();
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
        let db = catalog::create(":memory:").unwrap();
        let err = MaterializationTarget::get_by_name(&db, "nonExistant")
            .expect_err("expected an error from get_by_name");
        assert!(matches!(
            err,
            Error::SQLiteErr(rusqlite::Error::QueryReturnedNoRows)
        ));
    }
}
