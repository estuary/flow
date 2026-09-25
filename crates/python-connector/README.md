# python-connector

Shared machinery of connectors which run user-authored Python projects:
`derive-python`, and (in time) capture-python and materialize-python.

## Key types and entry points

- [`pydantic`](src/pydantic/mod.rs) — maps JSON schemas into Pydantic models.
