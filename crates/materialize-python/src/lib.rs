use anyhow::Context;
use codegen::Binding;
use models::custom_connector::{Spec, task_dir};
use proto_flow::flow;
use proto_flow::materialize::{Request, Response, request, response};
use python_connector::{DOCUMENTATION_URL, ENTRY, Install, Project, ProjectConfig, Staged};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};

mod codegen;

pub fn run() -> anyhow::Result<()> {
    let mut requests = std::io::BufReader::new(std::io::stdin());
    let mut stdout = std::io::stdout();
    let mut line = String::new();

    // Handle Spec, Validate, and Apply requests, breaking upon an Open.
    let mut open = loop {
        line.clear();
        if requests.read_line(&mut line)? == 0 {
            return Ok(()); // Clean EOF.
        }
        let request: Request = serde_json::from_str(&line).context("parsing connector request")?;

        let response = match request.kind {
            Some(request::Kind::Spec(spec)) => {
                response::Kind::Spec(Box::new(spec_response(&spec.config_json)))
            }
            Some(request::Kind::Validate(validate)) => {
                response::Kind::Validated(validate_materialization(*validate)?)
            }
            Some(request::Kind::Apply(apply)) => {
                response::Kind::Applied(apply_materialization(*apply)?)
            }
            Some(request::Kind::Open(open)) => break open,
            kind => anyhow::bail!("unexpected request {kind:?}"),
        };
        stdout.write_all(
            &serde_json::to_vec(&Response {
                kind: Some(response),
                ..Default::default()
            })
            .unwrap(),
        )?;
        stdout.write_all("\n".as_bytes())?;
    };

    let materialization = open
        .materialization
        .as_ref()
        .context("Open request missing materialization")?;

    let bindings = spec_bindings(materialization)?;

    let project = project_of(&materialization.config_json)?;
    let staged = stage(
        &materialization.name,
        &project,
        &bindings,
        Install::of_request(false, project.has_lock()),
    )?;
    std::mem::drop(bindings);

    // User code sees only its own configuration, without the sentinel.
    strip_sentinels(open.materialization.as_mut());
    models::custom_connector::strip_sentinel(
        &mut open.sealed_config_json,
        python_connector::SENTINEL,
    );

    let open = Request {
        kind: Some(request::Kind::Open(open)),
        ..Default::default()
    };
    tracing::debug!(temp_dir = ?staged.root(), "starting Python materialization");
    staged.hand_off(&["python", ENTRY], &open, requests)
}

fn validate_materialization(
    mut validate: request::Validate,
) -> anyhow::Result<response::Validated> {
    let project = project_of(&validate.config_json)?;
    let project_root = validate.project_root.trim_end_matches('/').to_string();
    let bindings = validate_bindings(&validate, &project.spec)?;
    let defaults = default_validated(&bindings);
    let mut generated_files = python_connector::project_urls(
        &project_root,
        [generated_module(&validate.name, &project.spec, &bindings)?],
    );

    // A project having listed files which don't exist yet can't be run.
    // They're given starters, and the materialization has its defaults. Its
    // missing files are load errors, and it can't be published.
    if !project.missing().is_empty() {
        let dir = task_dir(&validate.name);
        generated_files.extend(python_connector::starter_files(
            &project,
            &project_root,
            dir,
            codegen::starter_py(&validate.name, &bindings),
            python_connector::starter_manifest(dir),
        ));
        return Ok(response::Validated {
            bindings: defaults,
            generated_files,
        });
    }

    let staged = stage(
        &validate.name,
        &project,
        &bindings,
        Install::of_request(true, project.has_lock()),
    )?;
    std::mem::drop(bindings);
    staged.check()?;

    // User code sees only its own configuration, without the sentinel.
    models::custom_connector::strip_sentinel(&mut validate.config_json, python_connector::SENTINEL);
    strip_sentinels(validate.last_materialization.as_mut());

    let input = serde_json::json!({
        "validate": validate,
        "validated": response::Validated {
            bindings: defaults.clone(),
            generated_files: BTreeMap::new(),
        },
    });
    let output = staged.run(
        &["python", ENTRY, "validate"],
        &serde_json::to_vec(&input).unwrap(),
    )?;
    let validated: response::Validated = serde_json::from_slice(&output)
        .context("parsing the Validated of `Materialization.validate`")?;

    check_validated(&defaults, &validated)?;

    Ok(response::Validated {
        bindings: validated.bindings,
        generated_files,
    })
}

fn apply_materialization(mut apply: request::Apply) -> anyhow::Result<response::Applied> {
    let materialization = apply
        .materialization
        .as_ref()
        .context("Apply request missing materialization")?;
    let project = project_of(&materialization.config_json)?;
    let bindings = spec_bindings(materialization)?;
    let staged = stage(
        &materialization.name,
        &project,
        &bindings,
        Install::of_request(false, project.has_lock()),
    )?;
    std::mem::drop(bindings);

    strip_sentinels(apply.materialization.as_mut());
    strip_sentinels(apply.last_materialization.as_mut());

    let output = staged.run(
        &["python", ENTRY, "apply"],
        &serde_json::to_vec(&apply).unwrap(),
    )?;
    serde_json::from_slice(&output).context("parsing the Applied of `Materialization.apply`")
}

/// Parse the Project of a materialization's configuration, whose declared
/// spec has its defaults resolved.
fn project_of(config_json: &[u8]) -> anyhow::Result<Project> {
    let ProjectConfig { mut project, .. } = serde_json::from_slice(config_json)
        .with_context(|| format!("invalid `{}` configuration", python_connector::SENTINEL))?;
    project.spec = resolve_spec(project.spec);
    Ok(project)
}

/// Remove the `_python` sentinel from the configuration of `materialization`,
/// before it's handed to user code.
fn strip_sentinels(materialization: Option<&mut flow::MaterializationSpec>) {
    if let Some(materialization) = materialization {
        models::custom_connector::strip_sentinel(
            &mut materialization.config_json,
            python_connector::SENTINEL,
        );
    }
}

/// Resolve the schemas which a materialization's declared `spec` omits to
/// this connector's defaults: `Spec::config_schema_or_default`, and a resource
/// of the common shape of SQL materializations.
fn resolve_spec(mut spec: Spec) -> Spec {
    let config_schema = spec.config_schema_or_default().to_string();
    spec.config_schema = Some(models::Schema::new(
        models::RawValue::from_string(config_schema).unwrap(),
    ));
    spec.resource_config_schema.get_or_insert_with(|| {
        models::Schema::new(models::RawValue::from_value(&serde_json::json!({
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "title": "Table",
                    "description": "Name of the endpoint resource of this binding.",
                    "x-collection-name": true,
                },
                "schema": {
                    "type": "string",
                    "title": "Alternative Schema",
                    "description": "Schema or namespace of the endpoint resource (optional).",
                    "x-schema-name": true,
                },
                "delta_updates": {
                    "type": "boolean",
                    "default": false,
                    "title": "Delta Updates",
                    "description": "Should updates of this resource be delta updates, which are never loaded?",
                    "x-delta-updates": true,
                },
            },
            "required": ["table"],
        })))
    });
    spec
}

/// Answer a Spec from the spec declared by a ProjectConfig, without staging
/// or running its code.
fn spec_response(config_json: &[u8]) -> response::Spec {
    let Spec {
        config_schema,
        resource_config_schema,
        oauth2,
        // Validation builds network ports. They aren't part of a Spec response.
        network_ports: _,
    } = resolve_spec(python_connector::declared_spec(config_json));

    let schema_json = |schema: Option<models::Schema>| {
        schema
            .expect("resolved by resolve_spec")
            .get()
            .to_string()
            .into()
    };

    response::Spec {
        protocol: python_connector::PROTOCOL,
        config_schema_json: schema_json(config_schema),
        resource_config_schema_json: schema_json(resource_config_schema),
        documentation_url: DOCUMENTATION_URL.to_string(),
        oauth2: oauth2.as_ref().map(python_connector::oauth2),
    }
}

/// Bindings of a Validate request. Each binding's resource path and
/// delta-updates mode are its resource configuration's, at the locations
/// annotated by the resolved `spec.resourceConfigSchema`.
fn validate_bindings<'a>(
    validate: &'a request::Validate,
    spec: &Spec,
) -> anyhow::Result<Vec<Binding<'a>>> {
    let schema = spec
        .resource_config_schema
        .as_ref()
        .expect("resolved by resolve_spec");
    let pointers = tables::utils::pointer_for_schema(schema.get())
        .context("invalid `resourceConfigSchema`")?;
    let (_, shape) =
        python_connector::config_shape(schema.get()).context("invalid `resourceConfigSchema`")?;

    let bindings: Vec<_> = validate
        .resolved_bindings()
        .enumerate()
        .map(|(index, (binding, resolved))| {
            let (collection, _) =
                resolved.with_context(|| format!("binding {index} is missing its collection"))?;
            let resource: serde_json::Value = serde_json::from_slice(&binding.resource_config_json)
                .with_context(|| {
                    format!("binding {index} has an invalid resource configuration")
                })?;
            let (resource_path, delta_updates) = resource_of(&pointers, &shape, &resource)
                .with_context(|| format!("binding {index} of collection {}", collection.name))?;

            Ok((
                resource_path,
                delta_updates,
                collection,
                binding.group_by.as_slice(),
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    Binding::of(bindings)
}

/// Resource path and delta-updates mode of a resource configuration: its
/// `x-schema-name` (if non-empty) followed by its `x-collection-name`,
/// and its `x-delta-updates` (or else false). Each is read as the generated
/// `ResourceConfig` reads it, so an omitted property is its schema default.
fn resource_of(
    pointers: &tables::utils::ResourceSpecPointers,
    shape: &doc::Shape,
    resource: &serde_json::Value,
) -> anyhow::Result<(Vec<String>, bool)> {
    let query = |ptr: &json::Pointer| python_connector::query_config(shape, ptr, resource);
    let string_at = |ptr: &json::Pointer| {
        query(ptr)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
    };
    let name = string_at(&pointers.x_collection_name).with_context(|| {
        format!(
            "its resource configuration has no `{}`, which is the name of its resource",
            pointers.x_collection_name
        )
    })?;
    let schema = pointers.x_schema_name.as_ref().and_then(string_at);

    let delta_updates = pointers
        .x_delta_updates
        .as_ref()
        .and_then(query)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_default();

    Ok((
        schema
            .into_iter()
            .chain([name])
            .map(str::to_string)
            .collect(),
        delta_updates,
    ))
}

/// Bindings of a built MaterializationSpec, which carry their validated
/// resource paths, delta-updates modes, and keys.
fn spec_bindings(materialization: &flow::MaterializationSpec) -> anyhow::Result<Vec<Binding<'_>>> {
    let bindings: Vec<_> = materialization
        .resolved_bindings()
        .enumerate()
        .map(|(index, (binding, resolved))| {
            let (collection, _) =
                resolved.with_context(|| format!("binding {index} is missing its collection"))?;
            let keys = binding
                .field_selection
                .as_ref()
                .map(|selection| selection.keys.as_slice())
                .unwrap_or_default();

            Ok((
                binding.resource_path.clone(),
                binding.delta_updates,
                collection,
                keys,
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    Binding::of(bindings)
}

/// Default Validated bindings, whose constraints require the root document,
/// allow the fields of the binding's key, and forbid every other field.
/// User code sees each binding's document and key, and nothing else.
fn default_validated(bindings: &[Binding]) -> Vec<response::validated::Binding> {
    use response::validated::{Constraint, ProjectionConstraint, constraint::Type};

    bindings
        .iter()
        .map(|binding| {
            let projection_constraints = binding
                .collection
                .projections
                .iter()
                .map(|projection| {
                    let (type_, reason) = if projection.ptr.is_empty() {
                        (
                            Type::LocationRequired,
                            "The root document is the `doc` of each stored document",
                        )
                    } else if binding.keys.contains(&projection.field) {
                        (
                            Type::FieldOptional,
                            "The field is a component of the binding's key",
                        )
                    } else {
                        (
                            Type::FieldForbidden,
                            "Only the root document and its key are materialized",
                        )
                    };
                    ProjectionConstraint {
                        field: projection.field.clone(),
                        constraint: Some(Constraint {
                            r#type: type_ as i32,
                            reason: reason.to_string(),
                            folded_field: String::new(),
                        }),
                    }
                })
                .collect();

            response::validated::Binding {
                resource_path: binding.resource_path.clone(),
                delta_updates: binding.delta_updates,
                projection_constraints,
                ..Default::default()
            }
        })
        .collect()
}

/// Check that `Materialization.validate` kept each binding of `defaults`,
/// with its resource path and delta-updates mode, which are its resource
/// configuration's.
fn check_validated(
    defaults: &[response::validated::Binding],
    validated: &response::Validated,
) -> anyhow::Result<()> {
    if validated.bindings.len() != defaults.len() {
        anyhow::bail!(
            "Materialization.validate left {} bindings, but the materialization has {}",
            validated.bindings.len(),
            defaults.len(),
        );
    }
    for (default, binding) in defaults.iter().zip(validated.bindings.iter()) {
        if binding.resource_path != default.resource_path {
            anyhow::bail!(
                "Materialization.validate changed the resource path of binding {:?} to {:?}: \
                 a binding's resource path is that of its resource configuration",
                default.resource_path,
                binding.resource_path,
            );
        }
        if binding.delta_updates != default.delta_updates {
            anyhow::bail!(
                "Materialization.validate changed the delta updates of binding {:?} to {}: \
                 a binding's delta updates are those of its resource configuration",
                default.resource_path,
                binding.delta_updates,
            );
        }
    }
    Ok(())
}

/// Stage the project of materialization `task` with its generated module and
/// entry point, and `install` its dependencies.
fn stage(
    task: &str,
    project: &Project,
    bindings: &[Binding],
    install: Install,
) -> anyhow::Result<Staged> {
    let generated = [
        generated_module(task, &project.spec, bindings)?,
        (
            ENTRY.to_string(),
            codegen::main_py(task, bindings, task_dir(task)),
        ),
    ];
    Staged::of_project(project, &generated, install)
}

/// File of the module generated for materialization `task`, relative to the project.
fn generated_module(
    task: &str,
    spec: &Spec,
    bindings: &[Binding],
) -> anyhow::Result<(String, String)> {
    let content = codegen::types_py(task, spec, bindings)
        .context("failed to generate the materialization's types")?;
    Ok((python_connector::module_path(task), content))
}

#[cfg(test)]
mod test {
    use super::*;

    /// A Validate of two bindings: a delta-updates binding having a schema,
    /// and a standard-updates binding of a collection having a composite key.
    fn fixture_validate(files: serde_json::Value, spec: serde_json::Value) -> request::Validate {
        let projection = |ptr: &str, field: &str, types: serde_json::Value, exists: &str| serde_json::json!({"ptr": ptr, "field": field, "inference": {"types": types, "exists": exists}});
        serde_json::from_value(serde_json::json!({
            "name": "acmeCo/materialize-acme",
            "connectorType": "IMAGE",
            "config": {"apiKey": "k", "_python": {"files": files, "spec": spec}},
            "bindings": [
                {
                    "resourceConfig": {"table": "Contacts", "schema": "crm", "delta_updates": true},
                    "collection": {
                        "name": "acmeCo/contacts",
                        "key": ["/id"],
                        "writeSchema": {
                            "type": "object",
                            "properties": {"id": {"type": "integer"}, "email": {"type": "string"}},
                            "required": ["id", "email"],
                        },
                        "projections": [
                            projection("", "flow_document", serde_json::json!(["object"]), "MUST"),
                            projection("/id", "id", serde_json::json!(["integer"]), "MUST"),
                            projection("/email", "email", serde_json::json!(["string"]), "MUST"),
                        ],
                    },
                    "groupBy": ["id"],
                },
                {
                    "resourceConfig": {"table": "page_views"},
                    "collection": {
                        "name": "acmeCo/page-views",
                        "key": ["/site", "/page"],
                        "writeSchema": {
                            "type": "object",
                            "properties": {
                                "site": {"type": "string"},
                                "page": {"type": ["string", "null"]},
                                "views": {"type": "integer"},
                            },
                            "required": ["site", "views"],
                        },
                        "projections": [
                            projection("", "flow_document", serde_json::json!(["object"]), "MUST"),
                            projection("/site", "site/name", serde_json::json!(["string"]), "MUST"),
                            projection("/page", "count", serde_json::json!(["null", "string"]), "MAY"),
                            projection("/views", "views", serde_json::json!(["integer"]), "MUST"),
                        ],
                    },
                    "groupBy": ["site/name", "count"],
                },
            ],
            "projectRoot": "file:///project/acmeCo/",
        }))
        .unwrap()
    }

    #[test]
    fn specs_are_answered_from_the_sentinel() {
        let declared = serde_json::json!({"_python": {"files": {}, "spec": {
            "configSchema": {"type": "object", "properties": {"apiKey": {"type": "string", "secret": true}}},
            "resourceConfigSchema": {"type": "object", "properties": {"object": {"type": "string", "x-collection-name": true}}},
            "oauth2": {"provider": "acme", "authUrlTemplate": "https://acme.example/authorize", "accessTokenUrlTemplate": "https://acme.example/token"},
        }}});

        // A spec which declares nothing, or a configuration which isn't a
        // ProjectConfig, has the defaults of the connector.
        let responses = [
            spec_response(declared.to_string().as_bytes()),
            spec_response(br#"{"_python": {}}"#),
            spec_response(b"{}"),
        ]
        .map(|spec| Response {
            kind: Some(response::Kind::Spec(Box::new(spec))),
            ..Default::default()
        });
        insta::assert_json_snapshot!(serde_json::to_value(responses).unwrap());
    }

    #[test]
    fn bindings_and_their_defaults() {
        let validate = fixture_validate(serde_json::json!({}), serde_json::json!({}));
        let project = project_of(&validate.config_json).unwrap();
        let bindings = validate_bindings(&validate, &project.spec).unwrap();

        insta::assert_debug_snapshot!((
            bindings
                .iter()
                .map(|b| (
                    &b.snake,
                    &b.pascal,
                    &b.resource_path,
                    b.delta_updates,
                    b.keys
                ))
                .collect::<Vec<_>>(),
            serde_json::to_value(default_validated(&bindings)).unwrap(),
        ));
    }

    #[test]
    fn resources_and_their_errors() {
        let schema = resolve_spec(Spec::default())
            .resource_config_schema
            .unwrap();
        let pointers = tables::utils::pointer_for_schema(schema.get()).unwrap();
        let (_, shape) = python_connector::config_shape(schema.get()).unwrap();
        let resource = |resource: serde_json::Value| {
            resource_of(&pointers, &shape, &resource).map_err(|err| format!("{err:#}"))
        };

        insta::assert_debug_snapshot!([
            resource(serde_json::json!({"table": "t"})),
            // An empty schema is no schema.
            resource(serde_json::json!({"table": "t", "schema": "", "delta_updates": true})),
            resource(serde_json::json!({"table": "t", "schema": "s", "delta_updates": false})),
            resource(serde_json::json!({"table": ""})),
            resource(serde_json::json!({"schema": "s"})),
        ]);

        // Omitted properties are their schema defaults, as in `ResourceConfig`.
        // A required property has no default, and nor does an invalid one.
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "table": {"type": "string", "x-collection-name": true, "default": "ignored"},
                "options": {
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string", "x-schema-name": true, "default": "crm"},
                        "delta": {"type": "boolean", "x-delta-updates": true, "default": true},
                    },
                    "default": {},
                },
            },
            "required": ["table"],
        })
        .to_string();
        let pointers = tables::utils::pointer_for_schema(&schema).unwrap();
        let (_, shape) = python_connector::config_shape(&schema).unwrap();
        let resource = |resource: serde_json::Value| {
            resource_of(&pointers, &shape, &resource).map_err(|err| format!("{err:#}"))
        };

        insta::assert_debug_snapshot!(
            "resources_and_their_defaults",
            [
                resource(serde_json::json!({"table": "t"})),
                resource(serde_json::json!({"table": "t", "options": {}})),
                resource(
                    serde_json::json!({"table": "t", "options": {"schema": "", "delta": false}})
                ),
                resource(serde_json::json!({})),
            ]
        );

        // A declared resource schema must annotate its resource's name.
        let validate = fixture_validate(
            serde_json::json!({}),
            serde_json::json!({"resourceConfigSchema": {"type": "object", "properties": {"table": {"type": "string"}}}}),
        );
        let project = project_of(&validate.config_json).unwrap();
        let err = validate_bindings(&validate, &project.spec).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"), @"invalid `resourceConfigSchema`: resource spec schema does not contain any location annotated with x-collection-name");
    }

    #[test]
    fn validated_bindings_keep_their_paths_and_delta_updates() {
        let defaults = vec![response::validated::Binding {
            resource_path: vec!["crm".to_string(), "contacts".to_string()],
            delta_updates: true,
            ..Default::default()
        }];
        let check = |bindings: Vec<response::validated::Binding>| {
            check_validated(
                &defaults,
                &response::Validated {
                    bindings,
                    ..Default::default()
                },
            )
            .map_err(|err| format!("{err:#}"))
        };
        let mut moved = defaults.clone();
        moved[0].resource_path.pop();
        let mut standard = defaults.clone();
        standard[0].delta_updates = false;
        let mut constrained = defaults.clone();
        constrained[0].case_insensitive_fields = true;

        insta::assert_debug_snapshot!([
            check(constrained),
            check(Vec::new()),
            check(moved),
            check(standard),
        ]);
    }

    #[test]
    fn missing_files_are_given_starters() {
        let validate = fixture_validate(
            serde_json::json!({
                "pyproject.toml": null,
                "materialize-acme/__init__.py": null,
                "materialize-acme/models.py": null,
                // A lock has no starter.
                "uv.lock": null,
            }),
            serde_json::json!({}),
        );
        let validated = validate_materialization(validate).unwrap();

        insta::assert_debug_snapshot!(
            "starter_files",
            validated.generated_files.keys().collect::<Vec<_>>()
        );
        insta::assert_snapshot!(
            "starter_py",
            validated.generated_files["file:///project/acmeCo/materialize-acme/__init__.py"]
        );
    }

    #[test]
    fn generated_module_and_entry() {
        let validate = fixture_validate(
            serde_json::json!({}),
            serde_json::json!({"configSchema": {
                "type": "object",
                "properties": {"apiKey": {"type": "string", "secret": true}},
                "required": ["apiKey"],
            }}),
        );
        let project = project_of(&validate.config_json).unwrap();
        let bindings = validate_bindings(&validate, &project.spec).unwrap();

        insta::assert_snapshot!(
            "types_py",
            codegen::types_py(&validate.name, &project.spec, &bindings).unwrap()
        );
        insta::assert_snapshot!(
            "main_py",
            codegen::main_py(&validate.name, &bindings, task_dir(&validate.name))
        );
    }

    #[test]
    fn sentinels_are_stripped() {
        let validate = fixture_validate(serde_json::json!({}), serde_json::json!({}));
        let mut materialization = flow::MaterializationSpec {
            name: validate.name.clone(),
            config_json: validate.config_json.clone(),
            ..Default::default()
        };
        strip_sentinels(Some(&mut materialization));

        assert_eq!(materialization.config_json, r#"{"apiKey":"k"}"#);
    }
}
