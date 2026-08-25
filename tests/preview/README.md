# `flowctl raw preview-next` snapshot tests

Python snapshot tests of `flowctl` connector sessions, driven through
`flowctl raw preview-next` (the runtime-next preview implementation).

Each `test_*.py` shells out to `flowctl raw preview-next`, parses the NDJSON
`["collection/name",<doc>]` lines it writes to stdout, and compares them against
a checked-in snapshot under `snapshots/`. Sources are either a sibling
`test_*.flow.yaml` or an example catalog from `examples/`; derivation and
materialization tests feed transactions from a `*-fixture.ndjson` — documents
separated by `{"commit": true}` markers — in place of live journal reads.

Snapshot files are named by `pytest-insta` from the test's node ID
(`preview_test_<module>__<test-fn>__<name>`), so renaming a directory, file, or
test function orphans its snapshot.

To run them:
* Install [Poetry](https://python-poetry.org/) for python.
* Run `poetry install` to setup an environment with test dependencies.
* Build `flowctl` and put it on `$PATH` — the tests invoke it by bare name.
* Run `poetry run pytest tests/preview/` to run all tests.
* If the snapshot is updated, add `--insta review` to interactively review and update differences.
