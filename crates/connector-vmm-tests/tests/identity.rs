//! Exercise build identity with tracked, untracked, ignored and submodule
//! changes. Inherited identity must not survive failed Git identification.

use std::path::Path;

const LIB: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/build/identity-lib.sh"
);

/// git in `dir`, isolated from the user's and system's configuration.
fn git(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "acmeCo")
        .env("GIT_AUTHOR_EMAIL", "dev@acmeco.example")
        .env("GIT_COMMITTER_NAME", "acmeCo")
        .env("GIT_COMMITTER_EMAIL", "dev@acmeco.example")
        .output()
        .expect("running git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// What `export_flow_build_identity` of `dir` exports, having inherited a
/// clean identity from another tree, and the status of `flow_build_identity`.
fn identity(dir: &Path) -> String {
    let output = std::process::Command::new("bash")
        .args([
            "-c",
            r#"source "$1"
flow_build_identity "$2" >/dev/null && status=0 || status=$?
export_flow_build_identity "$2"
printf '%q, status %s\n' "${FLOW_BUILD_IDENTITY}" "${status}""#,
            "identity",
            LIB,
        ])
        .arg(dir)
        .env(
            "FLOW_BUILD_IDENTITY",
            "0123456789abcdef0123456789abcdef01234567",
        )
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("running bash");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

#[test]
fn identity_follows_every_file_git_would_commit() {
    let tmp = std::env::temp_dir().join(format!(
        "connector-vmm-identity-{}",
        connector_vmm_tests::run::random_hex()
    ));
    let repo = tmp.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let mut table = String::new();
    let mut step = |name: &str, heads: &[String]| {
        let mut seen = identity(&repo);
        for (n, head) in heads.iter().enumerate() {
            seen = seen.replace(head, &format!("<commit {}>", n + 1));
        }
        table.push_str(&format!("{name}: {seen}\n"));
    };
    let write = |path: &str, content: &str| std::fs::write(repo.join(path), content).unwrap();

    step("not a repository", &[]);
    git(&repo, &["init", "--quiet"]);
    write(".gitignore", "ignored/\n");
    write("lib.rs", "fn main() {}\n");
    step("before the first commit", &[]);

    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "first"]);
    let mut heads = vec![git(&repo, &["rev-parse", "HEAD"])];
    step("committed", &heads);
    write("lib.rs", "fn main() { println!(); }\n");
    step("a tracked file modified", &heads);
    git(&repo, &["checkout", "--quiet", "--", "lib.rs"]);
    step("restored", &heads);
    write("new.rs", "pub fn added() {}\n");
    step("new source, untracked", &heads);
    git(&repo, &["add", "new.rs"]);
    step("new source, staged", &heads);
    git(&repo, &["commit", "--quiet", "-m", "second"]);
    heads.push(git(&repo, &["rev-parse", "HEAD"]));
    step("committed again", &heads);
    std::fs::create_dir(repo.join("ignored")).unwrap();
    write("ignored/output.log", "built\n");
    step("an ignored file", &heads);
    std::fs::remove_file(repo.join("new.rs")).unwrap();
    step("a tracked file removed", &heads);
    git(&repo, &["checkout", "--quiet", "--", "new.rs"]);
    step("restored", &heads);

    // Display and ignore preferences hide nothing.
    git(&repo, &["config", "status.showUntrackedFiles", "no"]);
    write("hidden.rs", "pub fn hidden() {}\n");
    step("new source, untracked files not shown", &heads);
    std::fs::remove_file(repo.join("hidden.rs")).unwrap();

    let sub = tmp.join("sub");
    std::fs::create_dir(&sub).unwrap();
    git(&sub, &["init", "--quiet"]);
    std::fs::write(sub.join("schema.json"), "{}\n").unwrap();
    git(&sub, &["add", "."]);
    git(&sub, &["commit", "--quiet", "-m", "sub"]);
    let sub = sub.to_str().unwrap();
    git(
        &repo,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "--quiet",
            sub,
            "sub",
        ],
    );
    git(&repo, &["commit", "--quiet", "-m", "third"]);
    heads.push(git(&repo, &["rev-parse", "HEAD"]));
    step("a submodule committed", &heads);
    git(&repo, &["config", "diff.ignoreSubmodules", "all"]);
    git(&repo, &["config", "submodule.sub.ignore", "all"]);
    write("sub/schema.json", "{\"type\": \"object\"}\n");
    step("its content modified, submodules ignored", &heads);

    std::fs::remove_dir_all(&tmp).unwrap();
    insta::assert_snapshot!(table, @r"
    not a repository: '', status 128
    before the first commit: '', status 1
    committed: <commit 1>, status 0
    a tracked file modified: <commit 1>-dirty, status 0
    restored: <commit 1>, status 0
    new source, untracked: <commit 1>-dirty, status 0
    new source, staged: <commit 1>-dirty, status 0
    committed again: <commit 2>, status 0
    an ignored file: <commit 2>, status 0
    a tracked file removed: <commit 2>-dirty, status 0
    restored: <commit 2>, status 0
    new source, untracked files not shown: <commit 2>-dirty, status 0
    a submodule committed: <commit 3>, status 0
    its content modified, submodules ignored: <commit 3>-dirty, status 0
    ");
}
