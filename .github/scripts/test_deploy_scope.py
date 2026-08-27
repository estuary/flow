"""Tests for deploy_scope.py, run against the real workspace manifests.

Run from the repository root or from this directory:

    python3 .github/scripts/test_deploy_scope.py
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from deploy_scope import classify, closure

REPO = pathlib.Path(__file__).resolve().parents[2]

# Computed once: the closure walks every crate manifest under crates/.
CLOSURE = closure(REPO, "agent")
FLOWCTL_CLOSURE = closure(REPO, "flowctl")


class TestClosure(unittest.TestCase):
    def test_expected_members(self):
        # Crates the verdicts below depend on. `connector-init` is the
        # counterintuitive member: it is a path dependency of v1 `runtime`,
        # so its code links into the `agent` binary even though its behavior
        # executes in the connector sidecar.
        for crate in (
            "agent",
            "connector-init",
            "control-plane-api",
            "models",
            "notifications",
            "proto-flow",
            "runtime",
            "sources",
            "validation",
        ):
            self.assertIn(crate, CLOSURE)

    def test_expected_non_members(self):
        # `runtime-next` and `shuffle` are not linked into `agent` at all;
        # only v1 `runtime` is. A regression here would silently mislabel
        # every runtime-next PR as shipping.
        for crate in ("runtime-next", "shuffle", "data-plane-controller"):
            self.assertNotIn(crate, CLOSURE)

    def test_membership_not_count(self):
        # 39 in-repo crates at 5221eb6. Assert a floor, not equality, so the
        # test does not break each time a crate is added to the workspace.
        self.assertGreaterEqual(len(CLOSURE), 30)


class TestClassify(unittest.TestCase):
    def verdict(self, *paths):
        return classify(list(paths), CLOSURE)

    def test_control_plane_api_ships(self):
        v = self.verdict("crates/control-plane-api/src/graphql/storage_mappings.rs")
        self.assertTrue(v.ships)
        self.assertEqual(v.crates_touched, {"control-plane-api"})

    def test_notifications_ships(self):
        v = self.verdict("crates/notifications/src/shard_failed.rs")
        self.assertTrue(v.ships)
        self.assertEqual(v.crates_touched, {"notifications"})

    def test_runtime_next_does_not_ship(self):
        v = self.verdict("crates/runtime-next/src/leader/capture/fsm.rs")
        self.assertFalse(v.ships)
        self.assertEqual(len(v.excluded), 1)

    def test_data_plane_controller_does_not_ship(self):
        v = self.verdict("crates/data-plane-controller/src/stack.rs")
        self.assertFalse(v.ships)
        self.assertEqual(len(v.excluded), 1)

    def test_connector_init_ships(self):
        v = self.verdict("crates/connector-init/src/codec.rs")
        self.assertTrue(v.ships)
        self.assertEqual(v.crates_touched, {"connector-init"})

    def test_snapshot_is_inert(self):
        v = self.verdict(
            "crates/sources/tests/snapshots/"
            "schema_generation__catalog_schema_snapshot.snap"
        )
        self.assertFalse(v.ships)
        self.assertEqual(len(v.inert), 1)

    def test_docs_do_not_ship(self):
        v = self.verdict("site/docs/reference/materialization.md")
        self.assertFalse(v.ships)
        self.assertEqual(len(v.excluded), 1)

    def test_mise_toml_ships(self):
        v = self.verdict("mise.toml")
        self.assertTrue(v.ships)
        self.assertEqual(v.payload, ["mise.toml"])

    def test_go_test_files_are_inert(self):
        # The Go toolchain excludes _test.go files from production binaries,
        # so a test-only Go PR does not ship.
        v = self.verdict("go/flow/converge_test.go")
        self.assertFalse(v.ships)
        self.assertEqual(len(v.inert), 1)

    def test_non_test_go_files_ship(self):
        v = self.verdict("go/flow/converge.go")
        self.assertTrue(v.ships)

    def test_go_module_files_ship(self):
        # go.mod / go.sum pin the module graph of the flowctl-go and gazette
        # binaries copied into the image.
        for path in ("go.mod", "go.sum"):
            self.assertTrue(self.verdict(path).ships)

    def test_release_build_task_ships(self):
        # mise/tasks/ci/gnu-opt is the cargo invocation that produces the
        # `agent` binary; its flags and env are build inputs.
        self.assertTrue(self.verdict("mise/tasks/ci/gnu-opt").ships)

    def test_embedded_ops_catalog_bundles_ship(self):
        # Both are include_str!'d by control-plane-api, in the closure.
        for path in (
            "ops-catalog/data-plane-template.bundle.json",
            "ops-catalog/reporting-L2-template.bundle.json",
        ):
            self.assertTrue(self.verdict(path).ships)

    def test_flowctl_ops_bundle_does_not_ship(self):
        # flowctl embeds this bundle, and the Rust flowctl binary is not in
        # the image. Other ops-catalog sources do not ship either.
        for path in (
            "ops-catalog/ops-task-template.bundle.json",
            "ops-catalog/catalog-stats.ts",
            "ops-catalog/data-plane-template.flow.yaml",
        ):
            v = self.verdict(path)
            self.assertFalse(v.ships)
            self.assertEqual(len(v.excluded), 1)


class TestFlowctlTarget(unittest.TestCase):
    def verdict(self, *paths):
        return classify(list(paths), FLOWCTL_CLOSURE, "flowctl")

    def test_flowctl_sources_ship(self):
        self.assertTrue(self.verdict("crates/flowctl/src/ops.rs").ships)
        # flowctl has moved to flow-client-next; its predecessor flow-client
        # is still used by dekaf (only), so it is not in flowctl's closure.
        # When dekaf adopts flow-client-next, closures follow the manifests
        # automatically and only this pin needs a fresh look.
        self.assertTrue(self.verdict("crates/flow-client-next/src/lib.rs").ships)
        self.assertFalse(self.verdict("crates/flow-client/src/lib.rs").ships)

    def test_runtime_next_ships_for_flowctl(self):
        # The inverse of the agent target: flowctl links runtime-next and
        # shuffle (for preview-next), so those PRs ship via flowctl releases.
        self.assertTrue(self.verdict("crates/runtime-next/src/logger.rs").ships)
        self.assertTrue(self.verdict("crates/shuffle/src/lib.rs").ships)

    def test_ops_task_bundle_ships_for_flowctl(self):
        # The inverse of the agent target: flowctl include_str!'s this bundle,
        # and the two bundles embedded by control-plane-api do not apply.
        self.assertTrue(self.verdict("ops-catalog/ops-task-template.bundle.json").ships)
        self.assertFalse(
            self.verdict("ops-catalog/data-plane-template.bundle.json").ships
        )

    def test_agent_image_inputs_do_not_apply(self):
        # flowctl-release builds with plain cargo: no mise, no Go, no image.
        for path in (
            "mise.toml",
            "go.mod",
            "go/flow/converge.go",
            "docker/control-plane-agent.Dockerfile",
            "crates/agent/src/main.rs",
        ):
            self.assertFalse(self.verdict(path).ships)

    def test_release_workflow_ships(self):
        self.assertTrue(self.verdict(".github/workflows/flowctl-release.yaml").ships)


if __name__ == "__main__":
    unittest.main()
