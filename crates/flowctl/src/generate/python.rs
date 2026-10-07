//! Synchronize the `files` of local Python projects with their directories.
//!
//! A capture's project is scaffolded by its connector, and the package
//! directory is its own: files found within it are adopted into its `files`.
//! A scaffolded capture which declares no `spec.configSchema` is given the
//! starter schema of the configuration its scaffolding reads.
//! A derivation has no directory of its own (its module sits beside those of
//! sibling derivations), so its files are never adopted.

use crate::local_specs;
use std::collections::BTreeSet;
use std::path::Path;

/// Sync Python projects of the specifications at `source`, returning
/// whether any specification was re-written.
pub async fn sync_projects(source: &url::Url) -> anyhow::Result<bool> {
    let mut draft = local_specs::load(source).await;
    let mut changed = false;

    for row in draft.captures.iter_mut() {
        let Some(models::CaptureDef {
            endpoint: models::CaptureEndpoint::Python(python),
            ..
        }) = &mut row.model
        else {
            continue;
        };
        let Ok(root) = validation::builtin_project_root(&row.scope).to_file_path() else {
            continue; // Not a local project.
        };
        let listed: BTreeSet<String> = match &python.files {
            models::ProjectFiles::Indirect(paths) => paths.iter().cloned().collect(),
            // Inline files are the user's to maintain.
            models::ProjectFiles::Inline(files) if !files.is_empty() => continue,
            models::ProjectFiles::Inline(_) => BTreeSet::new(),
        };

        if listed.is_empty() && python.spec.config_schema.is_none() {
            python.spec.config_schema = Some(models::Schema::new(models::RawValue::from_value(
                &capture_python::starter_config_schema(),
            )));
            tracing::info!(capture = %row.capture, "adding the starter `spec.configSchema`");
            changed = true;
        }
        create_missing(&root, &listed)?;

        let package = validation::python_package(&row.capture);
        let mut paths = listed.clone();

        if root.join("pyproject.toml").is_file() {
            paths.insert("pyproject.toml".to_string());
        }
        adopt(&root, &root.join(&package), &mut paths)?;

        if paths != listed {
            tracing::info!(
                capture = %row.capture,
                adopted = ?paths.difference(&listed).collect::<Vec<_>>(),
                "adding project files to the capture's `files`",
            );
            python.files = models::ProjectFiles::Indirect(paths.into_iter().collect());
            changed = true;
        }
    }

    for row in draft.collections.iter() {
        let Some(models::DeriveUsing::Python(models::DeriveUsingPython {
            files: models::ProjectFiles::Indirect(paths),
            ..
        })) = row
            .model
            .as_ref()
            .and_then(|model| model.derive.as_ref())
            .map(|derive| &derive.using)
        else {
            continue;
        };
        let Ok(root) = validation::builtin_project_root(&row.scope).to_file_path() else {
            continue;
        };
        create_missing(&root, &paths.iter().cloned().collect())?;
    }

    if changed {
        // Write back only the specifications, and not the (inlined) contents
        // of the files which they list.
        draft
            .resources
            .retain(|resource| resource.content_type != proto_flow::flow::ContentType::Text);
        local_specs::write_resources(draft)?;
    }
    Ok(changed)
}

/// Create each listed file which doesn't exist as an empty file,
/// as a starting point for the user.
fn create_missing(root: &Path, listed: &BTreeSet<String>) -> anyhow::Result<()> {
    for path in listed {
        let path = root.join(path);
        if path.exists() {
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, "")?;
        tracing::info!(path = %path.display(), "created empty project file");
    }
    Ok(())
}

/// Add the files of `dir` (recursively) to `paths`, relative to `root`.
fn adopt(root: &Path, dir: &Path, paths: &mut BTreeSet<String>) -> anyhow::Result<()> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(()); // The directory doesn't exist (yet).
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        // Skip caches, virtual environments, and editor or tool droppings.
        if name.starts_with('.') || name == "__pycache__" || name.ends_with(".pyc") {
            continue;
        }
        if entry.file_type()?.is_dir() {
            adopt(root, &path, paths)?;
        } else if let Ok(relative) = path.strip_prefix(root) {
            paths.insert(relative.to_string_lossy().to_string());
        }
    }
    Ok(())
}
