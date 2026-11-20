//! Local CLI behavior for sandboxed and ordinary tasks in the same catalog.

use std::path::Path;

const PLATFORM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../connector-vmm-tests/fixtures/platform"
);
const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const NUMBERS: &str = "acmeCo/vmm-python/numbers";
const CAPTURE: &str = "acmeCo/vmm-python/source-hello";
const MATERIALIZATION: &str = "acmeCo/vmm-python/materialize-api";
/// An access token, unsigned, which expires in 2100.
const TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQxMDI0NDQ4MDB9.c2lnbmF0dXJl";

/// Invalid VMM settings catch any dependency on reactor configuration.
fn flowctl(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> (bool, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_flowctl"))
        .args(args)
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("FLOWCTL_PROFILE", dir.file_name().unwrap())
        .env("CONNECTOR_VMM_STATE_DIR", "relative/state")
        .env("CONNECTOR_VMM_DISK_MIB", "invalid")
        .env("CONNECTOR_VMM_MEMORY_OVERHEAD_MIB", "invalid")
        .env_remove("FLOW_AUTH_TOKEN")
        .envs(env.iter().copied())
        .stdin(std::process::Stdio::null())
        .output()
        .expect("flowctl runs");
    let text = |bytes: Vec<u8>| String::from_utf8(bytes).unwrap();
    (
        output.status.success(),
        text(output.stdout),
        text(output.stderr),
    )
}

/// A VMM capture whose config, and an ordinary SQLite derivation whose
/// lambda, `generate` would write.
fn generate_catalog(dir: &Path) -> String {
    let path = dir.join("generate.flow.yaml");
    std::fs::write(
        &path,
        format!(
            r#"import: ["{REPO}/examples/derive-patterns/inputs.flow.yaml"]
captures:
  acmeCo/vmm-source:
    endpoint: {{ python: {{ files: [pyproject.toml, vmm-source/__init__.py], config: source.config.yaml }} }}
    vmm: true
    egress: {{}}
    bindings: []
    shards: {{ flags: {{ enable-runtime-v2: "true" }} }}
collections:
  patterns/generated:
    schema: {{ type: object, properties: {{ Key: {{ type: string }} }}, required: [Key] }}
    key: [/Key]
    derive:
      using: {{ sqlite: {{}} }}
      transforms:
        - {{ name: fromInts, source: patterns/ints, shuffle: any, lambda: generated.sql }}
"#
        ),
    )
    .unwrap();
    path.to_string_lossy().into_owned()
}

#[test]
fn a_selected_vmm_task_is_refused_before_any_call_or_write() {
    let dir = tempfile::tempdir().unwrap();
    let generate = generate_catalog(dir.path());
    let (numbers, capture) = (
        format!("{PLATFORM}/numbers.flow.yaml"),
        format!("{PLATFORM}/capture/flow.yaml"),
    );
    let materialize = format!("{PLATFORM}/materialize/flow.yaml");
    let fixture = format!("{REPO}/tests/preview/ints-strings-fixture.ndjson");
    let (none, token): (&[(&str, &str)], &[(&str, &str)]) = (&[], &[("FLOW_AUTH_TOKEN", TOKEN)]);
    let (numbers, capture, materialize) =
        (numbers.as_str(), capture.as_str(), materialize.as_str());

    for (task, args, env) in [
        (
            NUMBERS,
            vec![
                "preview",
                "--source",
                numbers,
                "--task",
                NUMBERS,
                "--fixture",
                fixture.as_str(),
            ],
            none,
        ),
        (
            CAPTURE,
            vec!["raw", "preview-next", "--source", capture],
            none,
        ),
        (
            "acmeCo/vmm-source",
            vec![
                "generate",
                "--source",
                generate.as_str(),
                "--task",
                "acmeCo/vmm-source",
            ],
            none,
        ),
        (
            MATERIALIZATION,
            vec![
                "raw",
                "spec",
                "--source",
                materialize,
                "--task",
                MATERIALIZATION,
            ],
            none,
        ),
        (CAPTURE, vec!["raw", "discover", "--source", capture], none),
        (
            CAPTURE,
            vec![
                "raw",
                "oauth",
                "--source",
                capture,
                "--client-id",
                "an-id",
                "--client-secret",
                "a-secret",
                "--endpoint-config",
                "{}",
            ],
            token,
        ),
    ] {
        let (ok, _, stderr) = flowctl(dir.path(), &args, env);
        let refusal = format!(r#"Task "{task}" requests VMM sandboxing"#);
        assert!(!ok && stderr.contains(&refusal), "{args:?}:\n{stderr}");
    }
    assert!(!dir.path().join("source.config.yaml").exists());
    assert!(!dir.path().join("generated.sql").exists());

    let platform = format!("{PLATFORM}/flow.yaml");
    let (ok, _, stderr) = flowctl(
        dir.path(),
        &["preview", "--source", platform.as_str()],
        none,
    );
    assert!(
        !ok && stderr.contains("contain multiple tasks. Use --task"),
        "{stderr}"
    );
}

#[test]
fn an_ordinary_task_beside_vmm_tasks_previews_and_generates() {
    let dir = tempfile::tempdir().unwrap();
    let mixed = dir.path().join("mixed.flow.yaml");
    std::fs::write(
        &mixed,
        format!(
            "import:\n  - \"{REPO}/examples/derive-patterns/join-outer.flow.yaml\"\n  - \"{PLATFORM}/capture/flow.yaml\"\n"
        ),
    )
    .unwrap();
    let (mixed, fixture) = (
        mixed.to_string_lossy().into_owned(),
        format!("{REPO}/tests/preview/ints-strings-fixture.ndjson"),
    );
    let (ok, stdout, stderr) = flowctl(
        dir.path(),
        &[
            "preview",
            "--source",
            mixed.as_str(),
            "--task",
            "patterns/outer-join-sql",
            "--fixture",
            fixture.as_str(),
        ],
        &[],
    );
    assert!(ok, "{stderr}");
    assert!(
        !stdout.is_empty()
            && stdout
                .lines()
                .all(|line| line.contains("patterns/outer-join-sql")),
        "{stdout}"
    );

    let nested = dir.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    generate_catalog(&nested);
    let root = dir.path().join("flow.yaml");
    std::fs::write(&root, "import: [nested/generate.flow.yaml]\n").unwrap();
    let root = root.to_string_lossy().into_owned();
    let (ok, _, stderr) = flowctl(
        dir.path(),
        &[
            "generate",
            "--source",
            root.as_str(),
            "--task",
            "patterns/generated",
        ],
        &[],
    );
    assert!(ok, "{stderr}");
    assert!(nested.join("generated.sql").exists());
    assert!(!dir.path().join("generated.sql").exists());
    for at in [dir.path(), nested.as_path()] {
        assert!(
            !at.join("source.config.yaml").exists(),
            "the VMM task's config is not stubbed"
        );
    }
}

/// `raw build` and `raw test` refuse each VMM validation they dispatch, and
/// keep their ordinary results. `raw combine` prepares a VMM derivation's
/// collection without connectors.
#[test]
fn whole_catalog_commands_refuse_only_dispatched_vmm_validations() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("build.flow.yaml");
    std::fs::write(
        &source,
        format!(
            r#"import: ["{PLATFORM}/numbers.flow.yaml"]
collections:
  acmeCo/vmm-python/echo:
    schema: {{ type: object, properties: {{ ts: {{ type: string }}, message: {{ type: string }} }}, required: [ts] }}
    key: [/ts]
    derive:
      using: {{ sqlite: {{}} }}
      transforms:
        - name: fromGreetings
          source: acmeCo/vmm-python/greetings
          shuffle: any
          lambda: select $ts, $message;
"#
        ),
    )
    .unwrap();
    let source = source.to_string_lossy().into_owned();
    let db = dir.path().join("build.db").to_string_lossy().into_owned();
    let refusal = "this data plane does not support VMM execution";

    let (ok, _, stderr) = flowctl(
        dir.path(),
        &[
            "raw",
            "build",
            "--db-path",
            db.as_str(),
            "--build-id",
            "11:22:33:44:55:66:77:88",
            "--source",
            source.as_str(),
        ],
        &[],
    );
    assert!(ok, "{stderr}");
    let (mut errors, mut built) = (tables::Errors::new(), tables::BuiltCollections::new());
    let mut loaded: [&mut dyn tables::SqlTableObj; 2] = [&mut errors, &mut built];
    tables::load_tables(&rusqlite::Connection::open(&db).unwrap(), &mut loaded).unwrap();
    let errors: Vec<String> = errors
        .iter()
        .map(|row| format!("{:#}", row.error))
        .collect();
    assert!(
        errors.len() == 1 && errors[0].contains(refusal),
        "{errors:?}"
    );
    assert!(built.iter().any(|row| {
        row.collection.as_str() == "acmeCo/vmm-python/echo"
            && row
                .spec
                .as_ref()
                .is_some_and(|spec| spec.derivation.is_some())
    }));

    let (ok, _, stderr) = flowctl(
        dir.path(),
        &["raw", "test", "--source", source.as_str()],
        &[],
    );
    assert!(
        !ok && stderr.contains(refusal) && stderr.contains("catalog build failed with 1 error(s)"),
        "{stderr}"
    );

    // Tests never run a capture, so a VMM capture doesn't stop them.
    let capture = format!("{PLATFORM}/capture/flow.yaml");
    let (ok, _, stderr) = flowctl(
        dir.path(),
        &["raw", "test", "--source", capture.as_str()],
        &[],
    );
    assert!(ok, "{stderr}");
    let (ok, _, stderr) = flowctl(
        dir.path(),
        &[
            "raw",
            "combine",
            "--source",
            source.as_str(),
            "--collection",
            NUMBERS,
        ],
        &[],
    );
    assert!(ok, "{stderr}");
}

#[test]
fn preparation_finds_plaintext_secrets_by_declared_schemas() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("secret")).unwrap();
    std::fs::write(dir.path().join("secret/__init__.py"), "").unwrap();
    std::fs::write(
        dir.path().join("pyproject.toml"),
        "[project]\nname = \"acme\"\n",
    )
    .unwrap();
    let python = "{ files: [pyproject.toml, secret/__init__.py], config: { api_key: hunter2 }, \
        spec: { configSchema: { type: object, properties: { api_key: { type: string, secret: true } } } } }";
    let v2 = r#"flags: { enable-runtime-v2: "true" }"#;
    let schema = "{ type: object, properties: { id: { type: string } }, required: [id] }";
    let source = dir.path().join("flow.yaml");
    std::fs::write(
        &source,
        format!(
            "collections:
  acmeCo/source: {{ schema: {schema}, key: [/id] }}
  acmeCo/derive/secret:
    schema: {schema}
    key: [/id]
    derive:
      using: {{ python: {python} }}
      shards: {{ {v2} }}
      transforms: [{{ name: fromSource, source: acmeCo/source, shuffle: any }}]
captures:
  acmeCo/capture/secret: {{ endpoint: {{ python: {python} }}, bindings: [], shards: {{ {v2} }} }}
  acmeCo/disabled/secret: {{ endpoint: {{ python: {python} }}, bindings: [], shards: {{ disable: true, {v2} }} }}
materializations:
  acmeCo/materialize/secret: {{ endpoint: {{ python: {python} }}, bindings: [], shards: {{ {v2} }} }}
"
        ),
    )
    .unwrap();

    let source = source.to_string_lossy().into_owned();
    let (ok, _, stderr) = flowctl(
        dir.path(),
        &[
            "raw",
            "combine",
            "--source",
            source.as_str(),
            "--collection",
            "acmeCo/source",
        ],
        &[],
    );
    assert!(!ok, "{stderr}");
    for task in [
        "capture acmeCo/capture/secret",
        "capture acmeCo/disabled/secret",
        "derivation acmeCo/derive/secret",
        "materialization acmeCo/materialize/secret",
    ] {
        assert!(
            stderr.contains(&format!("{task} endpoint configuration location")),
            "{task}:\n{stderr}"
        );
    }
}
