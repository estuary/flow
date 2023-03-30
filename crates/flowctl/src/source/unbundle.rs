use crate::source::Existing;
use anyhow::Context;
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::BTreeSet;
use std::convert::AsRef;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::{fs, io::AsyncReadExt};

/// Unbundle is the opposite of `bundle`. It takes a catalog and writes it out
/// as file(s) in the given `dir_path`. The input catalog is typically one that
/// is bundled, but this is not a requirement. If the given catalog contains
/// inlined resources, then they will be written out as separate files and the
/// spec will have them replaced with relative paths to them. Resources that
/// are _not_ inlined will be left unmodified. Note that `unbundle` does _not_
/// put things into nested subdirectories. The entire catalog will be written
/// directly within the given directory.
pub async fn unbundle(
    new_catalog: models::Catalog,
    dir_path: &Path,
    spec_filename: &str,
    json: bool,
    existing: Existing,
) -> anyhow::Result<()> {
    fs::create_dir_all(dir_path).await?;
    let flow_yaml_path = dir_path.join(spec_filename);

    let new_spec_names = new_catalog
        .all_spec_names()
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<String>>();
    let maybe_catalog = resolve_catalog_to_write(&flow_yaml_path, new_catalog, existing).await?;
    let Some(mut new_catalog) = maybe_catalog else {
        return Ok(());
    };

    let serializer = Serializer { json };
    let ext = if json { "json" } else { "yaml" };

    // First we'll write out any indirected parts of specs, like schemas, typescript modules, connector configs, etc.
    // We only do this for specs that are included in the new, bundled catalog. We don't do this for specs that only exist
    // in an existing catalog that has been merged with the new one, because those files may already be indirected.
    // See: https://github.com/estuary/flow/issues/924

    for (name, collection) in new_catalog.collections.iter_mut() {
        if !new_spec_names.contains(name.as_str()) {
            continue;
        }
        let base = base_name(name)?;

        // Potentially write out separate schema files for the collection
        // schema / write_schema / read_schema.
        for (schema, filename) in [
            (collection.schema.as_mut(), format!("{base}.schema.{ext}")),
            (
                collection.write_schema.as_mut(),
                format!("{base}.write.schema.{ext}"),
            ),
            (
                collection.read_schema.as_mut(),
                format!("{base}.read.schema.{ext}"),
            ),
        ] {
            let Some(schema) = schema else { continue };

            maybe_indirect_schema(
                dir_path,
                &filename,
                &serializer,
                schema,
                convert_merge_to_overwrite(existing),
            )
            .await?;
        }

        if let Some(derivation) = &mut collection.derivation {
            maybe_indirect_schema(
                dir_path,
                &format!("{base}.register.schema.{ext}"),
                &serializer,
                &mut derivation.register.schema,
                existing,
            )
            .await?;

            if let Some(typescript) = &mut derivation.typescript {
                // Indirect the TypeScript module to a file, and track its reference.
                let ts_filename = format!("{base}.ts");
                let file_path = dir_path.join(&ts_filename);

                write_file(
                    &file_path,
                    typescript.module.as_bytes(),
                    convert_merge_to_overwrite(existing),
                )
                .await?;
                typescript.module = ts_filename;
            }
        }
    }

    for (name, capture) in new_catalog.captures.iter_mut() {
        if !new_spec_names.contains(name.as_str()) {
            continue;
        }
        let base = base_name(name)?;

        if let models::CaptureEndpoint::Connector(connector) = &mut capture.endpoint {
            // Indirect the connector config to a file, and track its reference.
            let filename = format!("{base}.config.{ext}");
            indirect_endpoint_config(
                connector,
                dir_path,
                filename,
                &serializer,
                convert_merge_to_overwrite(existing),
            )
            .await?;
        }
    }
    for (name, materialization) in new_catalog.materializations.iter_mut() {
        if !new_spec_names.contains(name.as_str()) {
            continue;
        }
        let base = base_name(name)?;

        if let models::MaterializationEndpoint::Connector(connector) = &mut materialization.endpoint
        {
            // Indirect the connector config to a file, and track its reference.
            let filename = format!("{base}.config.{ext}");
            indirect_endpoint_config(
                connector,
                dir_path,
                filename,
                &serializer,
                convert_merge_to_overwrite(existing),
            )
            .await?;
        }
    }

    // finally, we can write out the catalog spec
    let spec_contents = serializer.to_vec(&new_catalog)?;
    write_file(&flow_yaml_path, &spec_contents, existing).await?;
    Ok(())
}

fn convert_merge_to_overwrite(existing: Existing) -> Existing {
    match existing {
        Existing::MergeSpec => Existing::Overwrite,
        other => other,
    }
}

/// Takes a `new_catalog` and determines how to handle its unbundling. Possible outcomes are:
/// - `Ok(None)`: Skip it because a spec already exists and --existing=skip
/// - `Ok(Some(new_catalog))`: just write the new catalog because no spec file exists for it already
/// - `Ok(Some(merged_catalog))`: the catalog was merged with an existing file and `merged_catalog` should be written out.
/// - `Err(_)`: we're gonna have to bail
async fn resolve_catalog_to_write(
    flow_yaml_path: &std::path::PathBuf,
    new_catalog: models::Catalog,
    existing: Existing,
) -> Result<Option<models::Catalog>, anyhow::Error> {
    let flow_yaml_meta = fs::metadata(&flow_yaml_path).await;
    let new_catalog = match (flow_yaml_meta, existing) {
        // We don't care if it's a regular file if we're skipping it.
        (Ok(_), Existing::Abort) => {
            anyhow::bail!("path: '{}' already exists", flow_yaml_path.display());
        }
        (Ok(_), Existing::Keep) => {
            tracing::info!(path = %flow_yaml_path.display(), "skipping file because it already exists and `--existing=keep`");
            return Ok(None);
        }
        (Ok(meta), other) if !meta.is_file() => {
            // error since this is not a regular file
            anyhow::bail!(
                "file: '{}' is not a regular file and cannot be handled by `--existing={}`",
                flow_yaml_path.display(),
                other
            );
        }

        (Ok(_), Existing::Overwrite) => {
            tracing::info!(path = %flow_yaml_path.display(), "removing file because it already exists and `--existing=overwrite`");
            fs::remove_file(flow_yaml_path)
                .await
                .context(format!("failed to remove: {}", flow_yaml_path.display()))?;
            new_catalog
        }
        (Ok(_), Existing::MergeSpec) => {
            tracing::info!(path = %flow_yaml_path.display(), "merging specs because it already exists and `--existing=merge-spec`");
            let mut existing_spec = parse_catalog_spec(flow_yaml_path).await.context(format!(
                "reading existing catalog file: {}",
                flow_yaml_path.display()
            ))?;
            merge_catalog(new_catalog, &mut existing_spec)?;
            existing_spec
        }
        (Err(err), _) if err.kind() == std::io::ErrorKind::NotFound => new_catalog,
        (Err(err), _) => {
            anyhow::bail!(
                "io error retrieving metadata for path: '{}': {}",
                flow_yaml_path.display(),
                err
            );
        }
    };
    Ok(Some(new_catalog))
}

struct Serializer {
    json: bool,
}

impl Serializer {
    pub fn to_vec<T: serde::Serialize>(&self, value: &T) -> anyhow::Result<Vec<u8>> {
        // Our models embed serde_json's RawValue, which can only be serialized by serde_json.
        // Do that first.
        let v = serde_json::to_vec_pretty(value).context("serializing JSON")?;
        if self.json {
            Ok(v)
        } else {
            // We want YAML, so transcode directly from serialized JSON to YAML.
            let mut v2 = Vec::new();
            let mut deser = serde_json::Deserializer::from_slice(&v);
            serde_transcode::transcode(&mut deser, &mut serde_yaml::Serializer::new(&mut v2))
                .context("serializing YAML")?;

            Ok(v2)
        }
    }
}

async fn try_write_file(path: &Path, contents: &[u8], existing: Existing) -> anyhow::Result<()> {
    // Is the file allowed to exist before we create it?
    let create_new = match existing {
        Existing::Abort | Existing::Keep => true,
        _ => false,
    };
    let result = fs::OpenOptions::new()
        .create_new(create_new)
        .truncate(existing == Existing::Overwrite)
        .create(true)
        .write(true)
        .open(path)
        .await;
    match result {
        Ok(mut file) => {
            file.write_all(contents).await?;
            Ok(())
        }
        Err(err)
            if err.kind() == std::io::ErrorKind::AlreadyExists && existing == Existing::Keep =>
        {
            tracing::info!(path = %path.display(), "skipping writing file because it already exists and --existing=skip");
            Ok(())
        }
        // This branch also handles the case of `Existing::Error` if the file already exists.
        Err(err) => Err(anyhow::anyhow!("unable to open file: {}", err)),
    }
}

async fn write_file(path: &Path, contents: &[u8], existing: Existing) -> anyhow::Result<()> {
    match try_write_file(path, contents, existing).await {
        Ok(()) => {
            tracing::info!(path = %path.display(), "wrote file");
            Ok(())
        }
        Err(err) => {
            tracing::error!(path = %path.display(), error = %err, "failed to write file");
            Err(err).context(format!("failed to write file: '{}'", path.display()))
        }
    }
}

async fn parse_catalog_spec(flow_yaml_path: &Path) -> Result<models::Catalog, anyhow::Error> {
    let mut existing = fs::File::open(flow_yaml_path).await?;
    let mut contents = Vec::with_capacity(16 * 1024);
    existing.read_to_end(&mut contents).await?;
    let cat = sources::parse_catalog_spec(&contents)?;
    Ok(cat)
}

/// Returns the last path component for a given catalog name, which must be a valid catalog name.
fn base_name(catalog_name: &impl AsRef<str>) -> anyhow::Result<&str> {
    catalog_name
        .as_ref()
        .rsplit_once("/")
        .map(|(_, base)| base)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "invalid catalog name: '{}', does not contain at least one '/'",
                catalog_name.as_ref()
            )
        })
}

/// Writes the endpoint config to a file and replaces it with a relative URL pointing to the file.
async fn indirect_endpoint_config(
    config: &mut models::ConnectorConfig,
    dir: &Path,
    filename: String,
    serializer: &Serializer,
    existing: Existing,
) -> anyhow::Result<()> {
    let file_path = dir.join(&filename);
    let contents = serializer.to_vec(&config.config)?;

    write_file(&file_path, &contents, existing).await?;
    config.config = RawValue::from_string(Value::String(filename).to_string()).unwrap();
    Ok(())
}

/// If the size of the schema is over the inlining threshold, the inlined schema will be written to a file and
/// replaced with a relative URL pointing to the file.
async fn maybe_indirect_schema(
    dir: &Path,
    filename: &str,
    serializer: &Serializer,
    schema: &mut models::Schema,
    existing: Existing,
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

    let contents = serializer.to_vec(&schema)?;

    if contents.len() <= MAX_INLINE_SIZE {
        // Leave small schema in-place.
        return Ok(());
    }

    let path = dir.join(filename);
    write_file(&path, &contents, existing).await?;
    *schema = models::Schema::Url(models::RelativeUrl::new(filename));

    Ok(())
}

fn merge_catalog(
    new_catalog: models::Catalog,
    existing: &mut models::Catalog,
) -> anyhow::Result<()> {
    let models::Catalog {
        import,
        resources,
        captures,
        collections,
        materializations,
        tests,
        storage_mappings,
        _schema,
    } = new_catalog;
    merge_imports(&import, &mut existing.import)?;

    existing.captures.extend(captures);
    existing.collections.extend(collections);
    existing.materializations.extend(materializations);
    existing.tests.extend(tests);
    existing.resources.extend(resources);
    existing.storage_mappings.extend(storage_mappings);

    Ok(())
}

fn merge_imports(
    new_imports: &[models::RelativeUrl],
    existing: &mut Vec<models::RelativeUrl>,
) -> anyhow::Result<()> {
    for new_import in new_imports {
        if let Some(existing_import) = existing.iter_mut().find(|i| i == new_import) {
            *existing_import = new_import.clone();
        } else {
            existing.push(new_import.clone());
        }
    }
    Ok(())
}

const MAX_INLINE_SIZE: usize = 512;
