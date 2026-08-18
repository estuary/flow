#!/usr/bin/env python3
"""Reconcile `pending:agent-api` labels against what is actually deployed.

Desired state: a merged PR carries the label exactly when its changes are
compiled into the deployed image (see deploy_scope.py) and the most recently
deployed image does not include its merge commit. This script recomputes that
full set and applies the difference — adding and removing labels — so missed
events, rollbacks, and manual label edits all converge on the next run.

The deployed commit is read from the logs of a successful `Deploy agent-api`
run: the image tag ends in the git-describe suffix g<short-sha> of the commit
the image was built from (bare, e.g. g71fbcda6, or a full describe string,
e.g. v0.6.12-24-gc8fbf5ce5ce).

Requires: a full-history checkout (git log <deployed>..HEAD), the `gh` CLI
with a token holding actions:read and pull-requests:write, and Python 3.11+.

Usage:
    reconcile_deploy_labels.py [--deploy-run ID] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from deploy_scope import classify, closure

LABEL = "pending:agent-api"
LABEL_COLOR = "1D76DB"
LABEL_DESCRIPTION = "Merged, ships via Deploy agent-api, and not yet deployed"
DEPLOY_WORKFLOW = "deploy-agent-api.yaml"
IMAGE_NAME = "control-plane-agent"


def run(*argv: str) -> str:
    return subprocess.run(argv, check=True, capture_output=True, text=True).stdout


def deployed_commit(repo: str, run_id: str | None) -> str:
    """Full sha of the commit the deployed image was built from."""
    if not run_id:
        # `gh run list` does not return a guaranteed order, so sort here.
        runs = json.loads(run(
            "gh", "run", "list", "--repo", repo,
            "--workflow", DEPLOY_WORKFLOW, "--status", "success", "--limit", "50",
            "--json", "databaseId,createdAt",
        ))
        if not runs:
            raise SystemExit(f"error: no successful {DEPLOY_WORKFLOW} run found")
        run_id = str(max(runs, key=lambda r: r["createdAt"])["databaseId"])

    log = run("gh", "run", "view", run_id, "--repo", repo, "--log")
    m = re.search(rf"{IMAGE_NAME}:([A-Za-z0-9._-]+)", log)
    if not m:
        raise SystemExit(f"error: no {IMAGE_NAME} image tag in logs of run {run_id}")
    tag = m.group(1)

    suffix = re.search(r"(?:^|-)g([0-9a-f]{7,40})$", tag)
    ref = suffix.group(1) if suffix else tag
    return run("gh", "api", f"repos/{repo}/commits/{ref}", "--jq", ".sha").strip()


def merged_prs_since(repo: str, repo_dir: str, deployed: str) -> dict[int, list[str]]:
    """PR number -> changed files, for merged PRs not included in the deploy.

    Commits are resolved to PRs via associatedPullRequests: the repository
    mixes squash and non-squash merges, so subject parsing undercounts.
    """
    shas = run("git", "-C", repo_dir, "log", "--format=%H", f"{deployed}..HEAD").split()

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


def currently_labeled(repo: str) -> set[int]:
    out = run(
        "gh", "pr", "list", "--repo", repo,
        "--state", "merged", "--label", LABEL, "--limit", "100",
        "--json", "number", "--jq", ".[].number",
    )
    return {int(n) for n in out.split()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "estuary/flow"))
    ap.add_argument("--repo-dir", default=".", help="full-history checkout")
    ap.add_argument("--deploy-run", help="deploy run id; defaults to the latest successful run")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    deployed = deployed_commit(args.repo, args.deploy_run)
    print(f"deployed commit: {deployed}")

    shipping_dirs = closure(pathlib.Path(args.repo_dir).resolve(), "agent")
    desired = {
        number
        for number, files in merged_prs_since(args.repo, args.repo_dir, deployed).items()
        if classify(files, shipping_dirs).ships
    }
    current = currently_labeled(args.repo)

    to_add = sorted(desired - current)
    to_remove = sorted(current - desired)
    print(f"desired {sorted(desired)}; add {to_add}; remove {to_remove}")

    if args.dry_run:
        return

    if to_add:
        subprocess.run(
            ["gh", "label", "create", LABEL, "--repo", args.repo,
             "--color", LABEL_COLOR, "--description", LABEL_DESCRIPTION],
            check=False, capture_output=True,
        )
    for number in to_add:
        run("gh", "pr", "edit", str(number), "--repo", args.repo, "--add-label", LABEL)
    for number in to_remove:
        run("gh", "pr", "edit", str(number), "--repo", args.repo, "--remove-label", LABEL)


if __name__ == "__main__":
    main()
