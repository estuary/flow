#!/usr/bin/env python3
"""Reconcile pending-deploy labels against what is actually deployed.

Desired state, per label: a merged PR carries the label exactly when its
changes are compiled into the control-plane-agent image (see deploy_scope.py)
and the named deployment does not include its merge commit yet.

  pending:agent-api  the `agent-api` Cloud Run service. Its baseline is read
                     from the logs of the newest successful `Deploy agent-api`
                     run: the deployed image tag ends in the git-describe
                     suffix g<short-sha> of the commit it was built from.
  pending:agent      the `flow-agent` Kubernetes Deployment (the worker tier).
                     Its baseline is the image tag pinned in estuary/ops
                     env/estuary/combustable-cronut/main.jsonnet. Reading that
                     private repo from CI requires a token with contents:read
                     on estuary/ops in the GH_TOKEN_OPS environment variable.
  pending:flowctl    the released flowctl CLI. Its baseline is the tag of the
                     latest published (non-prerelease) GitHub release.

The script recomputes each full label set and applies the difference — adding
and removing labels — so missed events, rollbacks, and manual label edits all
converge on the next run.

Requires: a full-history checkout (git log <deployed>..HEAD), the `gh` CLI
with a token holding actions:read and pull-requests:write, and Python 3.11+.

Usage:
    reconcile_deploy_labels.py [--deploy-run ID] [--dry-run]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from deploy_scope import classify, closure

LABEL_COLOR = "1D76DB"
DEPLOY_WORKFLOW = "deploy-agent-api.yaml"
IMAGE_NAME = "control-plane-agent"
OPS_REPO = "estuary/ops"
OPS_PIN_PATH = "env/estuary/combustable-cronut/main.jsonnet"


def run(*argv: str, env: dict | None = None) -> str:
    merged_env = {**os.environ, **env} if env else None
    return subprocess.run(
        argv, check=True, capture_output=True, text=True, env=merged_env
    ).stdout


def tag_to_commit(repo: str, tag: str) -> str:
    """Full sha of the commit an image tag was built from."""
    suffix = re.search(r"(?:^|-)g([0-9a-f]{7,40})$", tag)
    ref = suffix.group(1) if suffix else tag
    return run("gh", "api", f"repos/{repo}/commits/{ref}", "--jq", ".sha").strip()


def agent_api_commit(repo: str, run_id: str | None) -> str:
    """Baseline of the agent-api Cloud Run service, from deploy run logs."""
    if not run_id:
        # The runs listing does not reliably return newest-first (observed
        # windows anchored months back), so page through every successful run
        # and select the newest ourselves. This is a low-volume, manually
        # dispatched workflow; the full listing is a few pages.
        lines = run(
            "gh", "api", "--paginate",
            f"repos/{repo}/actions/workflows/{DEPLOY_WORKFLOW}/runs"
            "?status=success&per_page=100",
            "--jq", r'.workflow_runs[] | "\(.created_at) \(.id)"',
        ).splitlines()
        if not lines:
            raise SystemExit(f"error: no successful {DEPLOY_WORKFLOW} run found")
        run_id = max(lines).split()[1]

    log = run("gh", "run", "view", run_id, "--repo", repo, "--log")
    m = re.search(rf"{IMAGE_NAME}:([A-Za-z0-9._-]+)", log)
    if not m:
        raise SystemExit(f"error: no {IMAGE_NAME} image tag in logs of run {run_id}")
    return tag_to_commit(repo, m.group(1))


def flowctl_release_commit(repo: str) -> str:
    """Baseline of the released flowctl CLI: the latest published release.

    /releases/latest excludes prereleases, so the continuously-updated
    dev-next prerelease does not move this baseline.
    """
    tag = run("gh", "api", f"repos/{repo}/releases/latest", "--jq", ".tag_name").strip()
    return run("gh", "api", f"repos/{repo}/commits/{tag}", "--jq", ".sha").strip()


def worker_commit(repo: str) -> str:
    """Baseline of the flow-agent k8s Deployment, from the estuary/ops pin."""
    env = None
    if token := os.environ.get("GH_TOKEN_OPS"):
        env = {"GH_TOKEN": token}
    try:
        content = run(
            "gh", "api", f"repos/{OPS_REPO}/contents/{OPS_PIN_PATH}",
            "--jq", ".content", env=env,
        )
    except subprocess.CalledProcessError as err:
        raise SystemExit(
            f"error: cannot read {OPS_REPO}/{OPS_PIN_PATH}: {err.stderr.strip()}\n"
            "In CI this needs GH_TOKEN_OPS, a token with contents:read on "
            f"{OPS_REPO}."
        )
    jsonnet = base64.b64decode(content).decode()
    m = re.search(rf"{IMAGE_NAME}:([A-Za-z0-9._-]+)'", jsonnet)
    if not m:
        raise SystemExit(f"error: no {IMAGE_NAME} image pin in {OPS_PIN_PATH}")
    return tag_to_commit(repo, m.group(1))


def merged_prs_since(
    repo: str, repo_dir: str, deployed: str, end_ref: str
) -> dict[int, list[str]]:
    """PR number -> changed files, for merged PRs not included in the deploy.

    Commits are resolved to PRs via associatedPullRequests: the repository
    mixes squash and non-squash merges, so subject parsing undercounts.
    """
    shas = run(
        "git", "-C", repo_dir, "log", "--format=%H", f"{deployed}..{end_ref}"
    ).split()

    prs: dict[int, dict] = {}
    for start in range(0, len(shas), 25):
        chunk = shas[start : start + 25]
        aliases = [
            f'c{i}: object(oid: "{sha}") {{ ... on Commit {{'
            " associatedPullRequests(first: 1) { nodes {"
            " number merged files(first: 100) { totalCount nodes { path } }"
            " } } } }"
            for i, sha in enumerate(chunk)
        ]
        owner, name = repo.split("/")
        query = (
            f'query {{ repository(owner: "{owner}", name: "{name}") {{ '
            + " ".join(aliases)
            + " } }"
        )
        with tempfile.NamedTemporaryFile("w", suffix=".graphql", delete=False) as f:
            f.write(query)
        data = json.loads(run("gh", "api", "graphql", "-F", f"query=@{f.name}"))
        os.unlink(f.name)

        for obj in data["data"]["repository"].values():
            if obj is None:
                continue
            for pr in obj["associatedPullRequests"]["nodes"]:
                if pr["merged"]:
                    prs[pr["number"]] = pr["files"]

    out: dict[int, list[str]] = {}
    for number, files in prs.items():
        if files["totalCount"] > 100:
            out[number] = run(
                "gh", "api", "--paginate",
                f"repos/{repo}/pulls/{number}/files", "--jq", ".[].filename",
            ).split()
        else:
            out[number] = [f["path"] for f in files["nodes"]]
    return out


def currently_labeled(repo: str, label: str) -> set[int]:
    out = run(
        "gh", "pr", "list", "--repo", repo,
        "--state", "merged", "--label", label, "--limit", "100",
        "--json", "number", "--jq", ".[].number",
    )
    return {int(n) for n in out.split()}


def reconcile(
    repo: str, repo_dir: str, target: str, shipping_dirs: set[str],
    label: str, description: str, deployed: str, end_ref: str, apply: bool,
) -> None:
    print(f"{label}: deployed commit {deployed}")
    desired = {
        number
        for number, files in merged_prs_since(repo, repo_dir, deployed, end_ref).items()
        if classify(files, shipping_dirs, target).ships
    }
    current = currently_labeled(repo, label)

    to_add = sorted(desired - current)
    to_remove = sorted(current - desired)
    print(f"{label}: desired {sorted(desired)}; add {to_add}; remove {to_remove}")

    if not apply:
        return

    if to_add:
        subprocess.run(
            ["gh", "label", "create", label, "--repo", repo,
             "--color", LABEL_COLOR, "--description", description],
            check=False, capture_output=True,
        )
    for number in to_add:
        run("gh", "pr", "edit", str(number), "--repo", repo, "--add-label", label)
    for number in to_remove:
        run("gh", "pr", "edit", str(number), "--repo", repo, "--remove-label", label)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "estuary/flow"))
    ap.add_argument("--repo-dir", default=".", help="full-history checkout")
    ap.add_argument("--deploy-run", help="deploy run id; defaults to the latest successful run")
    ap.add_argument(
        "--end-ref", default="HEAD",
        help="merged-through ref; pass origin/master when running from a branch checkout",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = pathlib.Path(args.repo_dir).resolve()
    agent_dirs = closure(repo_root, "agent")
    flowctl_dirs = closure(repo_root, "flowctl")

    reconcile(
        args.repo, args.repo_dir, "agent", agent_dirs,
        "pending:agent-api",
        "Merged, ships via Deploy agent-api, and not yet deployed",
        agent_api_commit(args.repo, args.deploy_run),
        args.end_ref,
        apply=not args.dry_run,
    )
    reconcile(
        args.repo, args.repo_dir, "agent", agent_dirs,
        "pending:agent",
        "Merged, in the control-plane-agent image, and not yet rolled to flow-agent",
        worker_commit(args.repo),
        args.end_ref,
        apply=not args.dry_run,
    )
    reconcile(
        args.repo, args.repo_dir, "flowctl", flowctl_dirs,
        "pending:flowctl",
        "Merged, changes the flowctl binary, and not in a published release",
        flowctl_release_commit(args.repo),
        args.end_ref,
        apply=not args.dry_run,
    )


if __name__ == "__main__":
    main()
