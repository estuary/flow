# `flowctl` snapshot tests

This directory includes an experimental approach for Python-based snapshot tests of `flowctl` connector sessions. To run them:
* Install [Poetry](https://python-poetry.org/) for python.
* Run `poetry install` to setup an environment with test dependencies.
* Run `poetry run pytest tests/` to run all tests.
* If the snapshot is updated, add `--insta review` to interactively review and update differences.
