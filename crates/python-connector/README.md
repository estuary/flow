# python-connector

Shared machinery of connectors which run user-authored Python projects:
`capture-python`, `derive-python`, and (in time) materialize-python.

## Key types and entry points

- [`Project`](src/project.rs) — a project staged into a temporary directory.
  `Project::install` takes an [`Install`](src/project.rs) mode chosen by
  `Install::of_request`: Validate resolves a fresh `uv.lock` (`Resolve`) or
  checks the project's own (`Check`), and installs the `dev` group so that the
  project's own pyright runs, while a running task installs exactly its baked
  lock without the `dev` group (`Frozen`). A request other than Validate which
  has no lock (a Discover, or the Open of a legacy derivation) resolves one,
  without the `dev` group. `Project::run` executes within the
  project's environment and rewrites paths of errors to be relative to the
  project.
- [`split_sentinel` et al.](src/config.rs) — the `_python` sentinel which
  validation composes into a built connector configuration, and which is removed
  before a configuration reaches user code.
- [`Spec`](src/spec.rs) — the sentinel's `spec`, declared by the task model with
  defaults applied by validation. Shims answer Spec from it without staging.
- [`config_types_py`](src/config_types.rs) — generated `EndpointConfig` and
  `ResourceConfig` of a `Spec`. A capture's `ResourceConfig` extends the CDK's
  `BaseResourceConfig`, with a `path()` of its `x-schema-name` (if any) and
  `x-collection-name` properties.
- [`pydantic`](src/pydantic/mod.rs) — maps JSON schemas into Pydantic models.
  `Mapper::for_config` maps configurations, whose literal `default`s become
  field defaults, whose `duration` / `date-time` strings become `timedelta`
  / `datetime`, and whose fields never shadow `BaseModel` attributes (such as
  `schema` or `model_config`, which are aliased). `Mapper::new` maps documents,
  which keep their wire types and names.
- Generated types use `datetime`, `typing`, and `pydantic` under private
  aliases (`_datetime`, …), which `imports_py` imports as used, so that a field
  named `typing` doesn't shadow its module. User text (descriptions, enum
  values, aliases) is rendered through escaping literal renderers.
- `default_pyproject` — the project of a derivation which doesn't have one.

## Non-obvious details

- **The cooldown is project configuration**, not a CLI flag: the default
  `pyproject.toml` sets `[tool.uv] exclude-newer = "7 days"`, and a project
  with its own `pyproject.toml` decides its own. A lock records its
  `exclude-newer` (as `exclude-newer-span`), and `uv lock --check` without the
  same setting reports the lock as stale, so no flag may override the project.
- **An explicit `uv.lock` in `files` is used verbatim**, after `uv lock --check`
  confirms it's current. `uv sync --frozen` with a stale lock *succeeds* and
  silently omits newly added dependencies, hence the check.
- A sync which follows a fresh or checked lock uses `--frozen` rather than
  `--locked` (which would re-resolve).
- Tools such as pyright come from a project's `dev` dependency group, and are
  run only if it has them.
- **Generated types are the garden path.** User code may parse into its own
  types instead; the platform doesn't check that they match the declared schemas.
- **Schemas are user-authored**, so mapping them returns errors rather than
  panicking.
