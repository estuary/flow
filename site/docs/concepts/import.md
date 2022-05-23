---
sidebar_position: 6
---
# Imports

The YAML files that comprise a catalog specification may include an `import` section.
This is what allows you to organize your catalog spec across multiple
interlinked files, and in some cases, import other resources.
When a catalog is deployed, the imported resources are treated as part of the file
into which they are imported.

The `import` section is structured as a list of partial or absolute URLs,
which Flow always evaluates relative to the base directory of the current source file.
For example, these are possible imports within a collection:

```yaml
# Suppose we're in file "/path/dir/flow.yaml"
import:
  - sub/directory/flow.yaml        # Resolves to "file:///path/dir/sub/directory/flow.yaml".
  - ../sibling/directory/flow.yaml # Resolves to "file:///path/sibling/directory/flow.yaml".
  - https://example/path/flow.yaml # Uses the absolute url.
```

The import rule is flexible; a collection doesn’t have to do anything special
to be imported by another,
and [`flowctl`](flowctl.md) can even directly build remote sources:

```bash
# Test an example from the flow-template repository.
$ flowctl draft test --source https://raw.githubusercontent.com/estuary/flow-template/main/word-counts.flow.yaml
```

## Fetch behavior

Flow resolves, fetches, and validates all imports during the catalog build process,
and then includes their fetched contents within the built catalog.
The built catalog is thus a self-contained snapshot of all resources
_as they were_ at the time the catalog was built.

This means it's both safe and recommended to directly reference
an authoritative source of a resource, such as a third-party JSON schema.
It will be fetched and verified only at catalog build time,
and thereafter that fetched version will be used for execution,
regardless of whether the authority URL itself later changes or errors.

## Import types

Almost always, the `import` stanza is used to import other Flow
catalog source files.
This is the default when given a string path:

```yaml
import:
 - path/to/source/catalog.flow.yaml
```

A long-form variant also accepts a content type of the imported resource:

```yaml
import:
 - url: path/to/source/catalog.flow.yaml
   contentType: CATALOG
```

Other permitted content types include `JSON_SCHEMA` and `TYPESCRIPT_MODULE`,
but these are not typically used and are needed only for advanced use cases.

## JSON Schema `$ref`

Certain catalog entities, like collections, commonly reference JSON schemas.
It's not necessary to explicitly add these to the `import` section;
they are automatically resolved and treated as an import.
You can think of this as an analog to the JSON Schema `$ref` keyword,
which is used to reference a schema that may
be contained in another file.

The one exception is schemas that use the `$id` keyword
at their root to define an alternative canonical URL.
In this case, the schema must be referenced through its canonical URL,
and then explicitly added to the `import` section
with `JSON_SCHEMA` content type.

## TypeScript modules

Certain entities in your catalog spec — typically derivations — may use
TypeScript lambda definitions.
These lambdas are conventionally defined in TypeScript modules
that accompany the specific catalog spec file.
Flow looks for and automatically imports TypeScript modules
which live alongside a Flow catalog spec file.

Given a Flow catalog spec at `/path/to/my.flow.yaml`,
Flow automatically imports the TypeScript module `/path/to/my.flow.ts`.
This is conventionally the module which implements all TypeScript lambdas
related to catalog entities defined in `my.flow.yaml`.
You do not need to add `my.flow.ts` to the `import` stanza.

However, Flow must know of all additional TypeScript modules that
are part of the catalog.
If other modules are needed, they must be added as a to the `import` section
with `TYPESCRIPT_MODULE` content type.

## NPM dependencies

Your TypeScript modules may depend on other
[NPM packages](https://www.npmjs.com/),
which can be be imported through the `npmDependencies`
stanza of a Flow catalog spec.
For example, [moment](https://momentjs.com/) is a common library
for working with times:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="catalog.flow.yaml" default>

```yaml
npmDependencies:
  moment: "^2.24"

collections: { ... }
```

</TabItem>
<TabItem value="catalog.flow.ts" default>

```typescript
import * as moment from 'moment';

// ... use `moment` as per usual.
```

</TabItem>
</Tabs>

Use any version string understood by `package.json`,
which can include local packages, GitHub repository commits, and more.
See [package.json documentation](https://docs.npmjs.com/cli/v8/configuring-npm/package-json#dependencies).

During the catalog build process, Flow gathers NPM dependencies
across all catalog source files and patches them into the catalog's
managed `package.json`.
Flow organizes its generated TypeScript project structure
for a seamless editing experience out of the box with VS Code
and other common editors.

## Import paths

import Mermaid from '@theme/Mermaid';

If a catalog source file `foo.flow.yaml` references a collection in `bar.flow.yaml`,
for example as a target of a capture,
there must be an _import path_ where either `foo.flow.yaml`
imports `bar.flow.yaml` or vice versa.

Import paths can be direct:

<Mermaid chart={`
	graph LR;
		foo.flow.yaml-->bar.flow.yaml;
`}/>

Or they can be indirect:

<Mermaid chart={`
	graph LR;
		bar.flow.yaml-->other.flow.yaml;
        other.flow.yaml-->foo.flow.yaml;
`}/>

The sources must still have an import path
even if referenced from a common parent.
The following would **not** work:

<Mermaid chart={`
	graph LR;
		parent.flow.yaml-->foo.flow.yaml;
		parent.flow.yaml-->bar.flow.yaml;
`}/>

These rules make your catalog sources more self-contained
and less brittle to refactoring and reorganization.
Consider what might otherwise happen if `foo.flow.yaml`
were imported in another project without `bar.flow.yaml`.