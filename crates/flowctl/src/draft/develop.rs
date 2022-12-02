use crate::api_exec;
use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Develop {
    /// Directory into which Flow catalog sources will be created.
    ///
    /// Flow catalog files are written into a directory hierarchy
    /// corresponding to their catalog name.
    /// The directory will be created if it doesn't exist.
    #[clap(long, default_value = ".")]
    root_dir: std::path::PathBuf,
    /// If enabled, generated JSON files instead of YAML.
    #[clap(long)]
    json: bool,
}

pub async fn do_develop(
    ctx: &mut crate::CliContext,
    Develop { root_dir, json }: &Develop,
) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        catalog_name: String,
        spec: Box<RawValue>,
        spec_type: String,
    }
    let rows: Vec<Row> = api_exec(
        ctx.controlplane_client()?
            .from("draft_specs")
            .select("catalog_name,spec,spec_type")
            .not("is", "spec_type", "null")
            .eq("draft_id", ctx.config().cur_draft()?),
    )
    .await?;

    let ext = if *json { "json" } else { "yaml" };
    let catalog_base = format!("flow.{ext}");

    std::fs::create_dir_all(&root_dir).context("couldn't create directory for draft")?;
    let root_dir =
        std::fs::canonicalize(root_dir).context("failed to canonicalize root directory")?;
    let root_catalog_path = root_dir.join(&catalog_base);
    let root_catalog_url = url::Url::from_file_path(&root_catalog_path).unwrap();

    // Index all catalog names to the catalog URL which will define them.
    // This is used to resolve imports which are necessary between catalog files.
    let name_to_catalog_url: BTreeMap<_, _> = rows
        .iter()
        .map(|r| {
            let m = root_catalog_url.join(&r.catalog_name).unwrap();
            (r.catalog_name.clone(), m.join(&catalog_base).unwrap())
        })
        .collect();

    let rows_len = rows.len();

    // This is a hairball, but is resistent to refactoring and I'm uncertain
    // that more indirection will really help with clarity.
    for (catalog_url, group) in rows
        .into_iter()
        .map(|r| (&name_to_catalog_url[&r.catalog_name], r))
        .sorted_by(|(l_url, _), (r_url, _)| l_url.cmp(r_url))
        .group_by(|(url, _row)| url.clone())
        .into_iter()
    {
        tracing::info!(%catalog_url, "processing catalog file");

        let catalog_path = catalog_url.to_file_path().unwrap();
        std::fs::create_dir_all(catalog_path.parent().unwrap())
            .context("couldn't create parent directory for catalog file")?;

        let mut catalog = models::Catalog::default();
        let mut seen_names = Vec::<models::Collection>::new();

        for (
            _module,
            Row {
                catalog_name,
                spec,
                spec_type,
            },
        ) in group
        {
            let base = catalog_name
                .rsplit_once("/")
                .expect("catalog names have at least one '/'")
                .1;

            tracing::info!(%catalog_name, %base, %spec_type, "processing specification");

            match spec_type.as_str() {
                "collection" => {
                    let mut spec: models::CollectionDef =
                        serde_json::from_str(spec.get()).context("parsing collection")?;

                    maybe_indirect_schema(
                        &catalog_url,
                        &catalog_url.join(&format!("{base}.schema.{ext}")).unwrap(),
                        &mut spec.schema,
                    )?;

                    if let Some(derivation) = &mut spec.derivation {
                        maybe_indirect_schema(
                            &catalog_url,
                            &catalog_url
                                .join(&format!("{base}.register.schema.{ext}"))
                                .unwrap(),
                            &mut derivation.register.schema,
                        )?;

                        for (transform, transform_def) in derivation.transform.iter_mut() {
                            if let Some(schema) = &mut transform_def.source.schema {
                                maybe_indirect_schema(
                                    &catalog_url,
                                    &catalog_url
                                        .join(&format!(
                                            "{base}.source.{transform}.schema.{ext}",
                                            transform = transform.as_str()
                                        ))
                                        .unwrap(),
                                    schema,
                                )?;
                            }
                            seen_names.push(transform_def.source.name.clone());
                        }

                        if let Some(typescript) = &mut derivation.typescript {
                            // Indirect the TypeScript module to a file, and track its reference.
                            let base = format!("{base}.ts");
                            let file_url = catalog_url.join(&base).unwrap();

                            std::fs::write(
                                file_url.to_file_path().unwrap(),
                                typescript.module.as_bytes(),
                            )?;
                            typescript.module = base;
                        }
                    }

                    catalog
                        .collections
                        .insert(models::Collection::new(catalog_name), spec);
                }
                "capture" => {
                    let mut spec: models::CaptureDef =
                        serde_json::from_str(spec.get()).context("parsing collection")?;

                    if let models::CaptureEndpoint::Connector(connector) = &mut spec.endpoint {
                        // Indirect the connector config to a file, and track its reference.
                        let base = format!("{base}.config.{ext}");
                        let file_url = catalog_url.join(&base).unwrap();

                        std::fs::write(
                            file_url.to_file_path().unwrap(),
                            &to_contents(&file_url, &connector.config)?,
                        )?;
                        connector.config =
                            RawValue::from_string(Value::String(base).to_string()).unwrap();
                    }

                    for binding in &spec.bindings {
                        seen_names.push(binding.target.clone());
                    }

                    catalog
                        .captures
                        .insert(models::Capture::new(catalog_name), spec);
                }
                "materialization" => {
                    let mut spec: models::MaterializationDef =
                        serde_json::from_str(spec.get()).context("parsing materialization")?;

                    if let models::MaterializationEndpoint::Connector(connector) =
                        &mut spec.endpoint
                    {
                        // Indirect the connector config to a file, and track its reference.
                        let base = format!("{base}.config.{ext}");
                        let file_url = catalog_url.join(&base).unwrap();

                        std::fs::write(
                            file_url.to_file_path().unwrap(),
                            &to_contents(&file_url, &connector.config)?,
                        )?;
                        connector.config =
                            RawValue::from_string(Value::String(base).to_string()).unwrap();
                    }

                    for binding in &spec.bindings {
                        seen_names.push(binding.source.clone());
                    }

                    catalog
                        .materializations
                        .insert(models::Materialization::new(catalog_name), spec);
                }
                "test" => {
                    let mut steps = serde_json::from_str::<Vec<models::TestStep>>(spec.get())
                        .context("parsing test steps")?;

                    for (ind, step) in steps.iter_mut().enumerate() {
                        let (collection, documents) = match step {
                            models::TestStep::Ingest(ingest) => {
                                (&ingest.collection, &mut ingest.documents)
                            }
                            models::TestStep::Verify(verify) => {
                                (&verify.collection, &mut verify.documents)
                            }
                        };

                        seen_names.push(collection.clone());

                        // Indirect documents above a threshold size into a referenced file.
                        let base = format!("{base}.{ind}.documents.json");
                        let file_url = catalog_url.join(&base).unwrap();
                        let contents = to_contents(&file_url, &documents)?;

                        if contents.len() > MAX_INLINE_SIZE {
                            std::fs::write(file_url.to_file_path().unwrap(), &contents)?;
                            *documents = models::TestDocuments::Url(models::RelativeUrl::new(base));
                        }
                    }

                    catalog.tests.insert(models::Test::new(catalog_name), steps);
                }
                _ => anyhow::bail!("invalid spec_type {spec_type}"),
            }
        }

        catalog.import = seen_names
            .into_iter()
            .filter_map(|n| name_to_catalog_url.get(n.as_str()).cloned())
            .sorted()
            .dedup()
            .filter_map(|target_url| {
                if target_url != *catalog_url {
                    Some(models::Import::Url(models::RelativeUrl::new(
                        make_relative(catalog_url, &target_url).unwrap(),
                    )))
                } else {
                    None
                }
            })
            .collect();
        std::fs::write(
            catalog_url.to_file_path().unwrap(),
            &to_contents(&catalog_url, &catalog)?,
        )?;
        tracing::info!(%catalog_url, "wrote catalog");
    }

    let root_catalog = models::Catalog {
        // Import all subordinate catalog file URLs through a relative path.
        import: name_to_catalog_url
            .into_values()
            .sorted()
            .dedup()
            .map(|catalog_url| {
                models::Import::Url(models::RelativeUrl::new(
                    make_relative(&root_catalog_url, &catalog_url).unwrap(),
                ))
            })
            .collect(),
        ..Default::default()
    };
    std::fs::write(
        &root_catalog_path,
        &to_contents(&root_catalog_url, &root_catalog)?,
    )?;
    tracing::info!(%root_catalog_url, "wrote root catalog");

    crate::typescript::do_generate(ctx, &root_catalog_path)
        .await
        .context("generating TypeScript project")?;

    println!("Wrote {rows_len} specifications under {root_catalog_url}.");
    Ok(())
}

fn maybe_indirect_schema(
    catalog_url: &url::Url,
    schema_url: &url::Url,
    schema: &mut models::Schema,
) -> anyhow::Result<()> {
    // Attempt to clean up the schema by removing a superfluous $id.
    match schema {
        models::Schema::Object(m) => {
            if m.contains_key("definitions") || m.contains_key("$defs") {
                // We can't touch $id, as it provides the canonical base against which
                // $ref is resolved to definitions.
            } else if let Some(true) = m
                .get("$id")
                .and_then(Value::as_str)
                .map(|s| s.starts_with("file://"))
            {
                m.remove("$id");
            }
        }
        _ => anyhow::bail!("expected object schema but got {schema:?}"),
    };

    let contents = to_contents(schema_url, &schema)?;

    if contents.len() <= MAX_INLINE_SIZE {
        // Leave small schema in-place.
        return Ok(());
    }

    std::fs::write(schema_url.to_file_path().unwrap(), &contents)?;
    *schema = models::Schema::Url(models::RelativeUrl::new(
        make_relative(catalog_url, schema_url).unwrap(),
    ));

    Ok(())
}

fn to_contents<S>(file_url: &url::Url, value: &S) -> anyhow::Result<Vec<u8>>
where
    S: Serialize,
{
    // Our models embed serde_json's RawValue, which can only be serialized by serde_json.
    // Do that first.
    let v = serde_json::to_vec_pretty(value).context("serializing JSON")?;

    if file_url.as_str().ends_with(".json") {
        Ok(v)
    } else if file_url.as_str().ends_with(".yaml") {
        // We want YAML, so transcode directly from serialized JSON to YAML.
        let mut v2 = Vec::new();
        let mut deser = serde_json::Deserializer::from_slice(&v);
        serde_transcode::transcode(&mut deser, &mut serde_yaml::Serializer::new(&mut v2))
            .context("serializing YAML")?;

        Ok(v2)
    } else {
        anyhow::bail!("unrecognized file extension in {file_url}")
    }
}

const MAX_INLINE_SIZE: usize = 512;

// This is a verbatim copy of Url::make_relative, with one fix added from this still-open PR:
// https://github.com/servo/rust-url/pull/754
pub fn make_relative(self_: &url::Url, url: &url::Url) -> Option<String> {
    if self_.cannot_be_a_base() {
        return None;
    }

    // Scheme, host and port need to be the same
    if self_.scheme() != url.scheme() || self_.host() != url.host() || self_.port() != url.port() {
        return None;
    }

    // We ignore username/password at this point

    // The path has to be transformed
    let mut relative = String::new();

    // Extract the filename of both URIs, these need to be handled separately
    fn extract_path_filename(s: &str) -> (&str, &str) {
        let last_slash_idx = s.rfind('/').unwrap_or(0);
        let (path, filename) = s.split_at(last_slash_idx);
        if filename.is_empty() {
            (path, "")
        } else {
            (path, &filename[1..])
        }
    }

    let (base_path, base_filename) = extract_path_filename(self_.path());
    let (url_path, url_filename) = extract_path_filename(url.path());

    let mut base_path = base_path.split('/').peekable();
    let mut url_path = url_path.split('/').peekable();

    // Skip over the common prefix
    while base_path.peek().is_some() && base_path.peek() == url_path.peek() {
        base_path.next();
        url_path.next();
    }

    // Add `..` segments for the remainder of the base path
    for base_path_segment in base_path {
        // Skip empty last segments
        if base_path_segment.is_empty() {
            break;
        }

        if !relative.is_empty() {
            relative.push('/');
        }

        relative.push_str("..");
    }

    // Append the remainder of the other URI
    for url_path_segment in url_path {
        if !relative.is_empty() {
            relative.push('/');
        }

        relative.push_str(url_path_segment);
    }

    // Add the filename if they are not the same
    if !relative.is_empty() || base_filename != url_filename {
        // If the URIs filename is empty this means that it was a directory
        // so we'll have to append a '/'.
        //
        // Otherwise append it directly as the new filename.
        if url_filename.is_empty() {
            relative.push('/');
        } else {
            if !relative.is_empty() {
                relative.push('/');
            }
            relative.push_str(url_filename);
        }
    }

    // Query and fragment are only taken from the other URI
    if let Some(query) = url.query() {
        relative.push('?');
        relative.push_str(query);
    }

    if let Some(fragment) = url.fragment() {
        relative.push('#');
        relative.push_str(fragment);
    }

    Some(relative)
}
