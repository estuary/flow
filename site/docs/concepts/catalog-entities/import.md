# Imports

An `import` stanza imports other catalog specifications via a relative or absolute URL,
including their contents into the current catalog.

During the catalog build process Flow ensures that all collections you reference
are resolvable through an import path.

```yaml
import:
    - ../path/to/flow.yaml
    - http://example/path/to/catalog.flow.yaml
```