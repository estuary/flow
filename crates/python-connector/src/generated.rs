use crate::GENERATED_PREFIX;

/// Imports of a generated module whose `source` is given: `imports`, followed
/// by the standard modules which generated types use (under private aliases,
/// so that a generated field named `datetime` or `typing` doesn't shadow
/// them), each only if it's used.
pub fn imports_py(source: &str, imports: &[&str]) -> String {
    let aliased = [
        ("_datetime.", "import datetime as _datetime"),
        ("_typing.", "import typing as _typing"),
        ("_pydantic.", "import pydantic as _pydantic"),
    ];
    imports
        .iter()
        .copied()
        .chain(
            aliased
                .iter()
                .filter(|(used, _)| source.contains(used))
                .map(|(_, import)| *import),
        )
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generated entry point of a project, which its connector runs. It's within
/// the project's generated files, so that the user's own paths are their own.
pub const ENTRY: &str = "flow_generated/main.py";

/// Fixed import name of a task's directory, which is loaded as a package of
/// this name. It's not a valid distribution name, and can't collide with
/// a dependency of the project.
pub const TASK_PACKAGE: &str = "__flow_task__";

/// Python source which loads the task's directory `dir` by its path, as the
/// package `TASK_PACKAGE`, and binds it to `task`. The directory may have any
/// name (such as `source-acme`), and its modules import one another relatively.
/// It uses the `importlib.util`, `pathlib`, `sys`, and `typing` modules, and
/// must be run from `ENTRY`.
pub fn load_task_py(dir: &str) -> String {
    format!(
        r#"def load_task() -> typing.Any:
    """Load the task's directory, its `__init__.py`, as package `{TASK_PACKAGE}`."""
    directory = pathlib.Path(__file__).resolve().parent.parent / {dir}
    spec = importlib.util.spec_from_file_location(
        "{TASK_PACKAGE}",
        directory / "__init__.py",
        submodule_search_locations=[str(directory)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


task: typing.Any = load_task()
"#,
        dir = crate::pydantic::python_literal(&serde_json::json!(dir)),
    )
}

/// Components of the Python module generated for a catalog `name`, such as
/// a derived collection or a capture: each `/`-separated component of the
/// name, sanitized into a Python identifier.
pub fn module_parts(name: &str) -> Vec<String> {
    name.split('/')
        .map(crate::pydantic::sanitize_python_identifier)
        .collect()
}

/// Files of the module generated for a catalog `name`, as paths relative
/// to the project root: an empty `__init__.py` of each parent package,
/// and the module's own `__init__.py` having `content`.
///
/// For example, `acmeCo/sources/source-acme` has module
/// `acmeCo.sources.source_acme`, importable through the PYTHONPATH.
pub fn module_files(name: &str, content: String) -> Vec<(String, String)> {
    let parts = module_parts(name);

    let mut files: Vec<(String, String)> = (1..parts.len())
        .map(|i| {
            (
                format!("{GENERATED_PREFIX}/{}/__init__.py", parts[..i].join("/")),
                String::new(),
            )
        })
        .collect();

    files.push((
        format!("{GENERATED_PREFIX}/{}/__init__.py", parts.join("/")),
        content,
    ));
    files
}

#[cfg(test)]
mod test {
    #[test]
    fn module_files_of_names() {
        let cases: Vec<_> = [
            "patterns/sums",
            "a/b/c/d",
            "simple",
            "acmeCo/sources/source-acme",
        ]
        .into_iter()
        .map(|name| {
            (
                name,
                super::module_parts(name).join("."),
                super::module_files(name, "content".to_string()),
            )
        })
        .collect();

        insta::assert_debug_snapshot!(cases);
    }
}
