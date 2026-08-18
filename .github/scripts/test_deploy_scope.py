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


if __name__ == "__main__":
    unittest.main()
