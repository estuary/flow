/// Resolve a test file path that works in both Cargo and Bazel contexts.
///
/// This macro provides a consistent way to reference test resource files
/// regardless of whether tests are run via `cargo test` or `bazel test`.
///
/// # Arguments
///
/// * `rel` - A relative path from the crate's root directory (as a string literal or expression)
///
/// # Returns
///
/// A PathBuf pointing to the test resource file.
///
/// # Examples
///
/// ```ignore
/// use test_support::test_resource_path;
///
/// // In a test:
/// let fixture_path = test_resource_path!("tests/fixtures/example.yaml");
/// let content = std::fs::read_to_string(fixture_path).unwrap();
/// ```
///
/// # How it works
///
/// - **Cargo context**: Uses `CARGO_MANIFEST_DIR` to locate files relative to the crate root
/// - **Bazel context**: Uses `TEST_SRCDIR` and `TEST_WORKSPACE` environment variables to
///   reconstruct paths within Bazel's execution environment
#[macro_export]
macro_rules! test_resource_path {
    ($rel:expr) => {{
        let cargo_manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let cargo_workspace = cargo_manifest.parent().unwrap().parent().unwrap();

        // Bazel: detect test execution context and compute relative path.
        if let (Ok(bazel_srcdir), Ok(bazel_ws)) = (
            std::env::var("TEST_SRCDIR"),
            std::env::var("TEST_WORKSPACE"),
        ) {
            std::path::PathBuf::from(bazel_srcdir)
                .join(bazel_ws)
                .join(cargo_manifest.strip_prefix(cargo_workspace).unwrap())
                .join($rel)
        } else {
            cargo_manifest.join($rel)
        }
    }};
}
