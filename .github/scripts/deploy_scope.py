#!/usr/bin/env python3
"""Decide whether a set of changed files reaches a deployable artifact.

The `Deploy agent-api` workflow deploys a prebuilt `control-plane-agent` image
to the `agent-api` Cloud Run service. That image contains the `agent` binary
plus a few sidecar binaries, so a change is *in the release* exactly when it
touches something the image is built from:

  * a crate in the transitive path-dependency closure of `crates/agent`,
  * a workspace-level file that affects how that binary is built,
  * the Dockerfile, packaging task, or entrypoint script for the image.

Anything else -- `site/`, other services' crates, other Dockerfiles' binaries --
cannot be shipped by this workflow.

Within the closure we additionally mark changes that are compiled but cannot
alter behavior (tests, snapshots, benches, docs), so snapshot churn alone
does not count as shipping.

Usage:
    deploy_scope.py --files changed.txt [--target agent]

`changed.txt` is one repo-relative path per line. Exit code is 0 when the
change ships, 1 when it does not, so the workflow can branch on it. Any other
exit code is a real error.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tomllib
from dataclasses import dataclass, field

# Per-artifact build inputs. Each target names a root crate, plus the files
# outside any crate that still change what is built into that artifact:
#
#   workspace_files    exact paths (lockfiles, toolchain pins).
#   input_prefixes     path prefixes baked into the artifact itself.
#
# agent: the control-plane-agent image. go.mod / go.sum pin the Go module
# graph for the flowctl-go and gazette binaries the image carries; the two
# ops-catalog bundles are include_str!'d by control-plane-api, which links
# into the agent binary.
#
# flowctl: the released CLI binary, built by flowctl-release.yaml with plain
# cargo (no mise, no Go, no Dockerfile). It include_str!'s the ops-task
# bundle, which is deliberately absent from the agent target: the Rust
# flowctl binary is not in the agent image.
TARGETS = {
    "agent": {
        "workspace_files": {
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            "mise.toml",
            ".cargo/config.toml",
            "go.mod",
            "go.sum",
        },
        "input_prefixes": (
            ".github/workflows/platform-build.yaml",  # builds and packages the image
            ".sqlx/",  # offline query metadata; gnu-opt builds with SQLX_OFFLINE=true
            "docker/control-plane-agent.Dockerfile",
            "mise/tasks/ci/package",
            "mise/tasks/ci/docker-images",
            "mise/tasks/ci/gnu-opt",  # the `cargo build --release` that produces `agent`
            "mise/tasks/build/",
            "go/",  # flowctl-go and gazette are copied into the image
            "ops-catalog/data-plane-template.bundle.json",
            "ops-catalog/reporting-L2-template.bundle.json",
        ),
    },
    "flowctl": {
        "workspace_files": {
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo/config.toml",
        },
        "input_prefixes": (
            ".github/workflows/flowctl-release.yaml",
            ".sqlx/",  # offline query metadata; the release builds with SQLX_OFFLINE=1
            "ops-catalog/ops-task-template.bundle.json",
        ),
    },
}

# Within a shipping crate, these paths are compiled but cannot change behavior.
# _test.go files are excluded from production Go binaries by the toolchain.
INERT_SUFFIXES = (".snap", ".md", "_test.go")
INERT_PATH_PARTS = (
    "/tests/",
    "/benches/",
    "/snapshots/",
    "/integration_tests/",
    "/testdata/",
)


@dataclass
class Verdict:
    ships: bool = False
    payload: list[str] = field(default_factory=list)
    inert: list[str] = field(default_factory=list)
    excluded: list[str] = field(default_factory=list)
    crates_touched: set[str] = field(default_factory=set)


def crate_dir_by_name(repo: pathlib.Path) -> dict[str, str]:
    """Map crate name -> directory name under `crates/`."""
    out: dict[str, str] = {}
    for manifest in sorted((repo / "crates").glob("*/Cargo.toml")):
        data = tomllib.loads(manifest.read_text())
        name = data.get("package", {}).get("name")
        if name:
            out[name] = manifest.parent.name
    return out


def path_deps(manifest: pathlib.Path) -> set[str]:
    """Path dependencies of a manifest, excluding dev-dependencies.

    dev-dependencies are deliberately excluded: they are linked only into test
    binaries, never into the shipped one. build-dependencies and
    target-conditional dependencies are included.
    """
    data = tomllib.loads(manifest.read_text())
    tables: list[dict] = []
    for key in ("dependencies", "build-dependencies"):
        tables.append(data.get(key, {}))
    for cfg in data.get("target", {}).values():
        for key in ("dependencies", "build-dependencies"):
            tables.append(cfg.get(key, {}))

    deps: set[str] = set()
    for table in tables:
        for name, spec in table.items():
            if isinstance(spec, dict) and "path" in spec:
                deps.add(spec.get("package", name))
    return deps


def closure(repo: pathlib.Path, root_crate: str) -> set[str]:
    """Directory names of every in-repo crate linked into `root_crate`."""
    dirs = crate_dir_by_name(repo)
    if root_crate not in dirs:
        raise SystemExit(f"error: no crate named {root_crate!r} under crates/")

    seen: set[str] = set()
    queue = [root_crate]
    while queue:
        name = queue.pop()
        if name in seen:
            continue
        seen.add(name)
        crate_dir = dirs.get(name)
        if crate_dir is None:
            # A path dep outside `crates/` (e.g. a vendored fork). Record the
            # name so it is not silently dropped, but there is no dir to match.
            continue
        for dep in path_deps(repo / "crates" / crate_dir / "Cargo.toml"):
            if dep not in seen:
                queue.append(dep)

    return {dirs[n] for n in seen if n in dirs}


def is_inert(path: str) -> bool:
    return path.endswith(INERT_SUFFIXES) or any(
        part in f"/{path}" for part in INERT_PATH_PARTS
    )


def classify(paths: list[str], shipping_dirs: set[str], target: str = "agent") -> Verdict:
    spec = TARGETS[target]
    v = Verdict()

    for path in paths:
        in_artifact = False

        if path in spec["workspace_files"] or path.startswith(spec["input_prefixes"]):
            in_artifact = True
        elif path.startswith("crates/"):
            parts = path.split("/")
            if len(parts) > 2 and parts[1] in shipping_dirs:
                in_artifact = True
                v.crates_touched.add(parts[1])

        if not in_artifact:
            v.excluded.append(path)
        elif is_inert(path):
            v.inert.append(path)
        else:
            v.payload.append(path)

    v.ships = bool(v.payload)
    return v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", required=True, help="file with one changed path per line")
    ap.add_argument(
        "--target", default="agent", choices=sorted(TARGETS),
        help="artifact to classify against; also names the root crate",
    )
    ap.add_argument("--repo", default=".", help="repository root")
    args = ap.parse_args()

    repo = pathlib.Path(args.repo).resolve()
    paths = [
        line.strip()
        for line in pathlib.Path(args.files).read_text().splitlines()
        if line.strip()
    ]

    verdict = classify(paths, closure(repo, args.target), args.target)

    print(
        f"{'ships' if verdict.ships else 'does not ship'}: "
        f"{len(verdict.payload)} payload, {len(verdict.inert)} inert, "
        f"{len(verdict.excluded)} excluded",
        file=sys.stderr,
    )
    return 0 if verdict.ships else 1


if __name__ == "__main__":
    raise SystemExit(main())
