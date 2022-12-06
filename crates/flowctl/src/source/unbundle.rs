use crate::source::Existing;
use anyhow::Context;
use serde_json::value::RawValue;
use serde_json::Value;
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
    let flow_yaml_path = dir_path.join(spec_filename);

    let maybe_catalog = resolve_catalog_to_write(&flow_yaml_path, new_catalog, existing).await?;
    let Some(mut new_catalog) = maybe_catalog else {
        return Ok(());
    };

    let serializer = Serializer { json };
    let ext = if json { "json" } else { "yaml" };

    for (name, collection) in new_catalog.collections.iter_mut() {
        let base = base_name(name);

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

            maybe_indirect_schema(dir_path, &filename, &serializer, schema, existing).await?;
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

                write_file(&file_path, typescript.module.as_bytes(), existing).await?;
                typescript.module = ts_filename;
            }
        }
    }

    for (name, capture) in new_catalog.captures.iter_mut() {
        let base = base_name(name);

        if let models::CaptureEndpoint::Connector(connector) = &mut capture.endpoint {
            // Indirect the connector config to a file, and track its reference.
            let filename = format!("{base}.config.{ext}");
            indirect_endpoint_config(connector, dir_path, filename, &serializer, existing).await?;
        }
    }
    for (name, materialization) in new_catalog.materializations.iter_mut() {
        let base = base_name(name);

        if let models::MaterializationEndpoint::Connector(connector) = &mut materialization.endpoint
        {
            // Indirect the connector config to a file, and track its reference.
            let filename = format!("{base}.config.{ext}");
            indirect_endpoint_config(connector, dir_path, filename, &serializer, existing).await?;
        }
    }

    // finally, we can write out the catalog spec
    let spec_contents = serializer.to_vec(&new_catalog)?;
    write_file(&flow_yaml_path, &spec_contents, existing).await?;
    Ok(())
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
        (Ok(meta), Existing::Keep) => {
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
            fs::remove_file(flow_yaml_path)
                .await
                .context(format!("failed to remove: {}", flow_yaml_path.display()))?;
            new_catalog
        }
        (Ok(_), Existing::MergeSpec) => {
            let mut existing = parse_catalog_spec(flow_yaml_path).await.context(format!(
                "reading existing catalog file: {}",
                flow_yaml_path.display()
            ))?;
            merge_catalog(new_catalog, &mut existing)?;
            existing
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
fn base_name(catalog_name: &impl AsRef<str>) -> &str {
    catalog_name
        .as_ref()
        .rsplit_once("/")
        .expect("catalog names have at least one '/'")
        .1
}

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
    new_imports: &[models::Import],
    existing: &mut Vec<models::Import>,
) -> anyhow::Result<()> {
    for new_import in new_imports {
        if let Some(existing_import) = existing
            .iter_mut()
            .find(|i| i.relative_url() == new_import.relative_url())
        {
            *existing_import = new_import.clone();
        } else {
            existing.push(new_import.clone());
        }
    }
    Ok(())
}

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

const MAX_INLINE_SIZE: usize = 512;
