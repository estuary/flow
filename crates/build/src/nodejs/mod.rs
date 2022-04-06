use anyhow::Context;
use doc;
use schemalate::typescript::Mapper;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path;
use std::process::Command;
use url::Url;

mod generators;
mod interface;

use interface::{Interface, Module};

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

pub fn generate_package<'a>(
    package_dir: &path::Path,
    collections: &'a [tables::Collection],
    derivations: &'a [tables::Derivation],
    named_schemas: &'a [tables::NamedSchema],
    npm_dependencies: &'a [tables::NPMDependency],
    resources: &'a [tables::Resource],
    schema_docs: &'a [tables::SchemaDoc],
    transforms: &'a [tables::Transform],
) -> Result<BTreeMap<String, WriteIntent>, anyhow::Error> {
    // Compile and index all schemas. We assume that referential integrity
    // is checked elsewhere, and make only a best-effort attempt to index
    // and resolve all schemas.
    let compiled_schemas = tables::SchemaDoc::compile_all(schema_docs)?;

    let mut index = doc::SchemaIndexBuilder::new();
    for schema in compiled_schemas.iter() {
        let _ = index.add(schema);
    }
    let index = index.into_index();

    // Build mapper for mapping from schema URIs to TypeScript AST's.
    let mapper = Mapper {
        index: &index,
        top_level: named_schemas
            .iter()
            .map(|n| (&n.anchor, n.anchor_name.as_str()))
            .collect(),
    };
    let interfaces = Interface::extract_all(package_dir, derivations, transforms);

    let mut files = BTreeMap::new();

    // TypeScript definitions, which are type-checked but don't produce compilation artifacts.
    files.insert(
        "flow_generated/flow/anchors.d.ts".to_string(),
        WriteIntent::Always(generators::anchors_ts(package_dir, named_schemas, &mapper)),
    );
    files.insert(
        "flow_generated/flow/collections.d.ts".to_string(),
        WriteIntent::Always(generators::collections_ts(
            package_dir,
            collections,
            &mapper,
        )),
    );
    files.insert(
        "flow_generated/flow/registers.d.ts".to_string(),
        WriteIntent::Always(generators::registers_ts(package_dir, derivations, &mapper)),
    );
    files.insert(
        "flow_generated/flow/transforms.d.ts".to_string(),
        WriteIntent::Always(generators::transforms_ts(package_dir, &interfaces, &mapper)),
    );
    files.insert(
        "flow_generated/flow/interfaces.d.ts".to_string(),
        WriteIntent::Always(generators::interfaces_ts(package_dir, &interfaces)),
    );
    files.insert(
        "flow_generated/flow/routes.ts".to_string(),
        WriteIntent::Always(generators::routes_ts(package_dir, &interfaces)),
    );

    // Generate implementation stubs for required relative modules that don't exist.
    files.extend(
        generators::stubs_ts(package_dir, &interfaces)
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
            "flow_generated/flow/modules.d.ts",
            include_str!("../../../../flow_generated/flow/modules.d.ts"),
        ),
        (
            "flow_generated/flow/server.ts",
            include_str!("../../../../flow_generated/flow/server.ts"),
        ),
    ];

    for (path, content) in template_files {
        files.insert(path.into(), WriteIntent::IfNotExists(content.into()));
    }

    for tables::Resource {
        resource,
        content_type,
        content,
    } in resources.iter()
    {
        if !matches!(content_type, models::ContentType::TypescriptModule) {
            continue;
        }
        let module = Module::new(&resource, package_dir);

        let intent = if module.is_relative() {
            WriteIntent::Never
        } else {
            WriteIntent::Always(String::from_utf8_lossy(content).to_string())
        };

        files.insert(module.relative_path(), intent);
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

pub fn write_package<'a>(
    package_dir: &path::Path,
    files: BTreeMap<String, WriteIntent>,
) -> Result<(), anyhow::Error> {
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

pub fn compile_package(package_dir: &path::Path) -> Result<(), anyhow::Error> {
    if !package_dir.join("node_modules").exists() {
        npm_cmd(package_dir, &["install", "--no-audit", "--no-fund"])?;
    }
    npm_cmd(package_dir, &["run", "compile"])?;
    npm_cmd(package_dir, &["run", "lint"])?;
    Ok(())
}

pub fn pack_package(package_dir: &path::Path) -> Result<tables::Resources, anyhow::Error> {
    npm_cmd(package_dir, &["pack"])?;

    let pack = package_dir.join("catalog-js-transformer-0.0.0.tgz");
    let pack = std::fs::canonicalize(&pack)?;

    tracing::info!("built NodeJS pack {:?}", pack);

    let mut resources = tables::Resources::new();
    resources.insert_row(
        Url::from_file_path(&pack).unwrap(),
        models::ContentType::NpmPackage,
        bytes::Bytes::from(std::fs::read(&pack)?),
    );
    std::fs::remove_file(&pack)?;

    Ok(resources)
}

fn npm_cmd(package_dir: &path::Path, args: &[&str]) -> Result<(), anyhow::Error> {
    let mut cmd = Command::new("npm");

    for &arg in args.iter() {
        cmd.arg(arg);
    }
    cmd.current_dir(package_dir);

    tracing::info!(?package_dir, ?args, "invoking `npm`");

    let status = cmd
        .spawn()
        .and_then(|mut c| c.wait())
        .context("failed to spawn `npm` command")?;

    if !status.success() {
        anyhow::bail!(
            "npm command {:?}, in directory {:?}, failed with status {:?}",
            args,
            package_dir,
            status
        );
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
) -> Result<String, anyhow::Error> {
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

#[cfg(test)]
mod scenario_test;
