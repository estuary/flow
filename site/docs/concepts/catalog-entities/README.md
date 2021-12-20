# Catalog Specification

A [catalog specification](../#specifications) (or "catalog spec") defines the behavior of your catalog:
The entities it contains, like collections and captures, and their specific behaviors and configuration.

Catalog specifications are often maintained as YAML or JSON file(s) in a collaborative Git repository,
along with other source files Flow uses such as TypeScript modules, JSON schemas, or test fixtures.

These files use the extension `*.flow.yaml` or simply `flow.yaml` by convention.
As a practical benefit, using this extension will activate Flow's VSCode integration and auto-complete.