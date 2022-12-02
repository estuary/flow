use json::schema::Keyword;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path;
use typescript::Mapper;

mod generators;
mod interface;

use interface::Interface;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to build JSON schema")]
    SchemaBuild(#[from] json::schema::BuildError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    IndexError(#[from] json::schema::index::Error),
}

pub enum WriteIntent {
    Always(String),
    IfNotExists(String),
    Never,
}

impl fmt::Debug for WriteIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteIntent::Always(body) => {
                f.write_str("Always:\n")?;
                f.write_str(&body)?;
            }
            WriteIntent::IfNotExists(body) => {
                f.write_str("IfNotExists:\n")?;
                f.write_str(&body)?;
            }
            WriteIntent::Never => {
                f.write_str("Never")?;
            }
        }
        Ok(())
    }
}

pub fn generate_npm_package<'a>(
    package_dir: &path::Path,
    collections: &'a [tables::Collection],
    derivations: &'a [tables::Derivation],
    imports: &'a [tables::Import],
    npm_dependencies: &'a [tables::NPMDependency],
    resources: &'a [tables::Resource],
    transforms: &'a [tables::Transform],
) -> Result<BTreeMap<String, WriteIntent>, Error> {
    let targets = Interface::extract_all(package_dir, collections, derivations, transforms);
    let compiled = tables::Resource::compile_all_json_schemas(resources)?;

    let mut files = BTreeMap::new();

    // TypeScript definitions, which are type-checked but don't produce compilation artifacts.
    for (collection, interface) in &targets {
        files.insert(
            format!(
                "flow_generated/types/{}.d.ts",
                collection.collection.as_str()
            ),
            WriteIntent::Always(generators::module_types(
                package_dir,
                &compiled,
                imports,
                *collection,
                interface.as_ref(),
            )),
        );
    }

    let interfaces = targets
        .iter()
        .filter_map(|(_, interface)| interface.as_ref());

    files.insert(
        "flow_generated/flow/routes.ts".to_string(),
        WriteIntent::Always(generators::routes_ts(package_dir, interfaces.clone())),
    );

    // Generate implementation stubs for required relative modules that don't exist.
    files.extend(
        generators::stubs_ts(package_dir, interfaces.clone())
            .into_iter()
            .map(|(k, v)| (k, WriteIntent::IfNotExists(v))),
    );

    // We use the literal files of this repository as ground-truth
    // templates which are packaged and scaffolded by built binaries.
    let template_files = vec![
        (".eslintrc.js", include_str!("../../../../.eslintrc.js")),
        (".prettierrc.js", include_str!("../../../../.prettierrc.js")),
        ("tsconfig.json", include_str!("../../../../tsconfig.json")),
        (
            "flow_generated/flow/main.ts",
            include_str!("../../../../flow_generated/flow/main.ts"),
        ),
        (
            "flow_generated/flow/server.ts",
            include_str!("../../../../flow_generated/flow/server.ts"),
        ),
    ];

    for (path, content) in template_files {
        files.insert(path.into(), WriteIntent::IfNotExists(content.into()));
    }

    // Write all external modules as relative to the local package_dir.
    for Interface {
        typescript_module,
        module_import_path,
        ..
    } in interfaces.filter(|i| !i.module_is_relative)
    {
        let content = &resources[resources
            .binary_search_by_key(typescript_module, |r| &r.resource)
            .unwrap()]
        .content;

        files.insert(
            module_import_path.clone(),
            WriteIntent::Always(String::from_utf8_lossy(content).to_string()),
        );
    }

    // Enumerate all project files into a TypeScript compiler config.
    files.insert(
        "flow_generated/tsconfig-files.json".to_string(),
        WriteIntent::Always(generators::tsconfig_files(
            files.keys().filter(|k| k.ends_with(".ts")),
        )),
    );

    // Patch "package.json".
    let file_path = package_dir.join("package.json");
    let content = if file_path.exists() {
        std::fs::read(&file_path)?
    } else {
        include_bytes!("../../../../package.json").to_vec()
    };
    files.insert(
        "package.json".to_string(),
        WriteIntent::Always(patch_package_dot_json(&content, npm_dependencies)?),
    );

    Ok(files)
}

pub fn write_npm_package<'a>(
    package_dir: &path::Path,
    files: BTreeMap<String, WriteIntent>,
) -> Result<(), std::io::Error> {
    for (path, intent) in files {
        let path = package_dir.join(path);

        match intent {
            WriteIntent::Always(contents) => {
                std::fs::create_dir_all(path.parent().unwrap())?;
                std::fs::write(&path, contents)?;
                tracing::info!("wrote {:?}", path);
            }
            WriteIntent::IfNotExists(contents) => {
                if matches!(std::fs::metadata(&path), Err(err) if err.kind() == std::io::ErrorKind::NotFound)
                {
                    std::fs::create_dir_all(path.parent().unwrap())?;
                    std::fs::write(&path, contents)?;
                    tracing::info!("wrote {:?}", path);
                } else {
                    tracing::debug!("skipping existing file {:?}", path);
                }
            }
            WriteIntent::Never => {}
        }
    }

    Ok(())
}

// Models the bits of the "package.json" file we care about patching,
// and passes through everything else.
#[derive(Serialize, Deserialize)]
struct PackageJson {
    #[serde(default)]
    dependencies: BTreeMap<String, String>,
    #[serde(default, rename = "bundledDependencies", alias = "bundleDependencies")]
    bundled_dependencies: BTreeSet<String>,
    #[serde(flatten)]
    rest: BTreeMap<String, serde_json::Value>,
}

fn patch_package_dot_json(
    content: &[u8],
    npm_dependencies: &[tables::NPMDependency],
) -> Result<String, serde_json::Error> {
    // Parse current package.json.
    let mut dom: PackageJson = serde_json::from_slice(content)?;

    // We require that all dependencies be declared within catalog specs,
    // and simply overwrite anything which may be manually added to
    // package.json.
    //
    // This is opinionated, maybe controversial, but the rationale is that
    // dependencies captured in catalog specs are scoped to that spec and
    // accessible to its external uses. One can point `flowctl build` at a
    // third-party catalog, or even a small part therein, and the tool is
    // able to produce a proper package with correct & minimal dependencies.
    //
    // The other important reason is that the runtime NPM package must be
    // fully self-contained, and it's *required* that packages are also
    // marked as bundled dependencies. This is easy to mess up and would
    // likely be a confusing constraint.

    dom.dependencies.clear();
    dom.bundled_dependencies.clear();

    for tables::NPMDependency {
        scope: _,
        derivation: _,
        package,
        version,
    } in npm_dependencies.iter()
    {
        dom.dependencies.insert(package.clone(), version.clone());
        dom.bundled_dependencies.insert(package.clone());
    }

    Ok(format!("{}\n", &serde_json::to_string_pretty(&dom)?))
}

fn camel_case(name: &str, mut upper: bool) -> String {
    let mut w = String::new();

    for c in name.chars() {
        if !c.is_alphanumeric() {
            upper = true
        } else if upper {
            w.extend(c.to_uppercase());
            upper = false;
        } else {
            w.push(c);
        }
    }

    w
}

fn relative_url(scope: &url::Url, package_dir: &path::Path) -> String {
    assert!(package_dir.is_absolute());

    package_dir
        .to_str()
        .filter(|_| scope.scheme() == "file")
        .and_then(|d| scope.path().strip_prefix(d))
        .map(|path| &path[1..]) // Trim leading '/', which remains after stripping directory.
        .map(|path| {
            let mut relative = path.to_string();

            // Re-attach trailing query & fragment components.
            if let Some(query) = scope.query() {
                relative.push('?');
                relative.push_str(query);
            }

            if let Some(fragment) = scope.fragment() {
                relative.push('#');
                relative.push_str(fragment);
            }
            relative
        })
        .unwrap_or_else(|| scope.to_string())
}

fn relative_path(from: &models::Collection, to: &models::Collection) -> String {
    let from = url::Url::parse(&format!("https://example/{}", from.as_str())).unwrap();
    let to = url::Url::parse(&format!("https://example/{}", to.as_str())).unwrap();
    make_relative(&from, &to).unwrap()
}

fn build_mapper<'a>(
    compiled: &'a [(url::Url, doc::Schema)],
    imports: &[tables::Import],
    schema: &'a url::Url,
    extract_anchors: bool,
) -> Mapper<'a> {
    let mut schema_no_fragment = schema.clone();
    schema_no_fragment.set_fragment(None);

    // Collect all dependencies of |schema|, with |schema| as the first item.
    let mut dependencies = tables::Import::transitive_imports(imports, &schema_no_fragment)
        .filter_map(|url| {
            compiled
                .binary_search_by_key(&url, |(resource, _)| resource)
                .ok()
                .and_then(|ind| compiled.get(ind).map(|c| &c.1))
        })
        .peekable();

    let mut index = doc::SchemaIndexBuilder::new();
    let mut top_level = BTreeMap::new();

    // A root |schema| reference (no fragment) by which the schema was fetched may
    // differ from the canonical URI under which it's indexed. Add an alias.
    if let (None, Some(compiled)) = (schema.fragment(), dependencies.peek()) {
        let _ = index.add_alias(compiled, schema);
    }

    for compiled in dependencies {
        let _ = index.add(compiled); // Best-effort.

        if !extract_anchors {
            continue;
        }

        let mut stack = vec![compiled];
        while let Some(schema) = stack.pop() {
            for kw in &schema.kw {
                match kw {
                    Keyword::Anchor(anchor_uri) => {
                        // Does this anchor meet our definition of a named schema?
                        if let Some((_, anchor)) = anchor_uri
                            .as_str()
                            .split_once('#')
                            .filter(|(_, s)| NAMED_SCHEMA_RE.is_match(s))
                        {
                            top_level.insert(anchor_uri, anchor.to_owned());
                        }
                    }
                    Keyword::Application(_, child) => {
                        stack.push(child);
                    }
                    _ => (),
                }
            }
        }
    }

    // We don't verify index references, as validation is handled
    // elsewhere and this is a best-effort attempt.

    Mapper {
        schema: schema.clone(),
        index: index.into_index(),
        top_level,
    }
}

lazy_static::lazy_static! {
    // The set of allowed characters in a schema `$anchor` is quite limited,
    // by Sec 8.2.3.
    //
    // To identify named schemas, we further restrict to anchors which start
    // with a capital letter and include only '_' as punctuation.
    // See: https://json-schema.org/draft/2019-09/json-schema-core.html#anchor
    static ref NAMED_SCHEMA_RE: regex::Regex = regex::Regex::new("^[A-Z][\\w_]+$").unwrap();
}

#[cfg(test)]
mod scenario_test;

// This is a verbatim copy of Url::make_relative, with one fix added from this still-open PR:
// https://github.com/servo/rust-url/pull/754
// This is also copied at: https://github.com/estuary/animated-carnival/blob/main/crates/flowctl/src/draft/develop.rs#L347-L349
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
