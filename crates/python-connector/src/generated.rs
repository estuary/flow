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
