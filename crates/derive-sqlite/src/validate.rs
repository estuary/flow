use super::{dbutil, is_url_to_generate, Config, Param, Transform};
use anyhow::Context;
use proto_flow::{
    derive::{request, response},
    flow,
};

pub fn parse_validate(
    validate: request::Validate,
) -> anyhow::Result<(Vec<String>, Vec<Transform>)> {
    let request::Validate {
        connector_type: _,
        collection: _,
        config_json,
        transforms,
        shuffle_key_types: _,
        project_root: _,
        import_map: _,
        last_collection: _,
        last_version: _,
    } = &validate;

    let config: Config = serde_json::from_str(&config_json)
        .with_context(|| format!("failed to parse SQLite configuration: {config_json}"))?;

    let transforms: Vec<Transform> = transforms
        .into_iter()
        .map(|transform| {
            let request::validate::Transform {
                name,
                collection: source,
                lambda_config_json,
                shuffle_lambda_config_json: _,
                backfill: _,
            } = transform;

            let source = source.as_ref().unwrap();
            let params = source
                .projections
                .iter()
                .map(Param::new)
                .collect::<Result<Vec<_>, _>>()?;

            let block: String = serde_json::from_str(&lambda_config_json).with_context(|| {
                format!("failed to parse SQLite lambda block: {lambda_config_json}")
            })?;

            Ok(Transform {
                name: name.clone(),
                block,
                source: source.name.clone(),
                params,
            })
        })
        .collect::<Result<_, anyhow::Error>>()?;

    Ok((config.migrations, transforms))
}

pub fn do_validate(
    migrations: &[String],
    transforms: &[Transform],
) -> anyhow::Result<response::Validated> {
    let (conn, _checkpoint) = dbutil::open(":memory:", migrations)?;
    let transform_stacks = dbutil::build_transforms(&conn, &transforms)?;

    let mut generated_files: Vec<(String, String)> = Vec::new();

    // Look for any migrations we must generate.
    for block in migrations {
        if is_url_to_generate(&block) {
            generated_files.push((block.to_string(), MIGRATION_STUB.to_string()));
        }
    }

    // Look for any transform lambdas we must generate.
    let transform_responses = transforms
        .iter()
        .zip(transform_stacks.iter())
        .map(|(transform, (_, stack))| {
            if is_url_to_generate(&transform.block) {
                generated_files.push((transform.block.clone(), lambda_stub(transform)));
            }

            response::validated::Transform {
                read_only: stack.iter().all(|lambda| lambda.is_readonly()),
            }
        })
        .collect();

    Ok(response::Validated {
        transforms: transform_responses,
        generated_files: generated_files.into_iter().collect(),
    })
}

const MIGRATION_STUB: &str = r#"
-- Use migrations to create or alter tables that your derivation will use.
-- Each migration is run only once, and new migrations will be applied as needed.
--
-- For example, create the join table below, and then use it across multiple lambdas:
--
-- A first lambda that updates indexed state:
--
--   INSERT INTO my_join_table (id, joined_value) VALUES ($id, $my::value)
--     ON CONFLICT REPLACE;
--
-- A second lambda that reads out and joins over the indexed state:
--
--    SELECT $id, $other$value, j.joined_value FROM my_join_table WHERE id = $id;

CREATE TABLE my_join_table (
    -- A common ID that's joined over.
    id           INTEGER PRIMARY KEY NOT NULL,
    -- A value that's updated by one lambda, and read by another.
    joined_value TEXT NOT NULL
);

"#;

fn lambda_stub(
    Transform {
        name: _,
        source,
        block: _,
        params,
    }: &Transform,
) -> String {
    use std::fmt::Write;
    let mut w = String::with_capacity(4096);

    let root = params.iter().find(|param| param.projection.ptr.is_empty());

    if let Some(root) = root {
        _ = write!(
            w,
            r#"
-- Example statement which passes-through source {source} documents without modification.
-- Use a WHERE clause to filter, for example: WHERE $my$column = 1234
SELECT JSON({});
"#,
            root.canonical_encoding
        );
    }

    _ = write!(
        w,
        r#"
-- Example statement demonstrating how to SELECT specific locations from documents of {source}.
-- This statement is effectively disabled by its WHERE FALSE clause and does not emit any documents.
--
-- You can rename a location by using the SQL "AS" syntax, for example:
--   SELECT $some$column AS "my_new_column_name;
--
-- You can also filter by using locations in a WHERE clause, for example:
--   SELECT $some$column WHERE $other$column = 1234;
SELECT
"#,
    );

    let comment = |p: &Param| {
        let mut w = String::new();

        if p.projection.is_primary_key {
            _ = write!(w, "Key ");
        } else if p.projection.is_partition_key {
            _ = write!(w, "Partitioned field ");
        } else {
            _ = write!(w, "Field ");
        };

        _ = write!(w, "{} at {}", p.projection.field, p.projection.ptr);

        if let Some(flow::Inference {
            title, description, ..
        }) = p.projection.inference.as_ref()
        {
            if !title.is_empty() {
                _ = write!(w, "; {}", title.replace("\n", " "));
            }
            if !description.is_empty() {
                _ = write!(w, "; {}", description.replace("\n", " "));
            }
        }

        w
    };

    let params: Vec<String> = params
        .iter()
        .filter_map(|p| {
            if p.projection.ptr.is_empty() {
                None // Skip projection of the document root.
            } else {
                Some(format!(
                    "    -- {}\n    {}",
                    comment(p),
                    p.canonical_encoding
                ))
            }
        })
        .collect();

    w.push_str(&params.join(",\n"));
    w.push_str(
        r#"
-- Disable this statement, so that it emits no documents.
WHERE FALSE;
"#,
    );

    w
}

#[cfg(test)]
mod test {
    use super::super::test_param;
    use super::do_validate;
    use crate::Transform;

    #[test]
    fn test_stub_generation() {
        let mut params = vec![
            test_param("id", "/id", false, false, false),
            test_param("part", "/part", false, false, false),
            test_param("flow_document", "", false, false, false),
            test_param("nested/int", "/nested/int", false, true, false),
            test_param("value", "/value", false, false, false),
        ];
        params[0].projection.is_primary_key = true;
        params[1].projection.is_partition_key = true;

        let migrations = vec![
            "file://path/to/migration.stub".to_string(),
            "CREATE TABLE foo (one TEXT, two INTEGER);".to_string(),
        ];

        let transforms = vec![Transform {
            block: "file://path/to/transform.stub.sql".to_string(),
            name: "fromFoobar".to_string(),
            source: "acmeCo/foo/bar".to_string(),
            params,
        }];

        let validated = do_validate(&migrations, &transforms).unwrap();

        insta::assert_json_snapshot!(validated);
    }
}
