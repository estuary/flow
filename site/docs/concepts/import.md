# Imports

The `import` section is a list of partial or absolute URLs
that are always evaluated relative to the base directory of the current source file.
For example, these are possible imports within a collection:

```yaml
# Suppose we're in file "/path/dir/flow.yaml"
import:
  - sub/directory/flow.yaml        # Resolves to "file:///path/dir/sub/directory/flow.yaml".
  - ../sibling/directory/flow.yaml # Resolves to "file:///path/sibling/directory/flow.yaml".
  - https://example/path/flow.yaml # Uses the absolute url.
```

The import rules are designed so that a collection doesn’t have to do anything special
in order to be imported by another,
and [`flowctl`](flowctl.md) can even directly build remote sources:

```bash
# Test an example from the flow-template repository.
$ flowctl test --source https://raw.githubusercontent.com/estuary/flow-template/main/word-counts.flow.yaml
```

## Fetch Behavior

Flow resolves, fetches, and validates all imports during the catalog build process,
and then includes their fetched contents within the built catalog.
The built catalog is thus a self-contained snapshot of all resources
_as they were_ at the time the catalog was built.

An implication is that it's both safe and recommended to directly reference
an authoritative source of a resource, such as a third-party JSON schema.
It will be fetched and verified only at catalog build time,
and thereafter that fetched version will be used for execution,
regardless of whether the authority URL itself later changes or errors.

## Import Types

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

JSON Schema has a `$ref` keyword for referencing a schema which may
be contained in another file.
Similarly, various catalog source entities like collections also
accept schema URLs.
These schema references are implicitly resolved
and treated as an import of the built catalog.
It's not required to further list them in the `import` stanza.

The one caveat are schemas which use the `$id` keyword
at their root to define an alternative canonical URL.
In this case the schema must be referenced through its canonical URL,
and then explicitly added as a catalog import
with `JSON_SCHEMA` content type.

## TypeScript Modules

You may declare entities in catalog source files that use
TypeScript lambda definitions, such as derivations.
These lambdas are conventionally defined in TypeScript modules
which accompany the specific catalog source.
Flow looks for and automatically imports TypeScript modules
which live alongside a Flow catalog source file.

Given a Flow catalog source at `/path/to/my.flow.yaml`,
Flow will automatically import the TypeScript module `/path/to/my.flow.ts`.
This is conventionally the module which implements all TypeScript lambdas
related to catalog entities defined in `my.flow.yaml`,
and you do **not** need to also add `my.flow.ts` to the `import` stanza.

However, Flow must know of all transitive TypeScript modules which
are part of the catalog.
If additional modules are needed which live outside of these implicit
modules, they must be added as a catalog import
with `TYPESCRIPT_MODULE` content type.

## NPM Dependencies

Your TypeScript modules may depend on other
[NPM packages](https://www.npmjs.com/),
which can be be imported through the `npmDependencies`
stanza of a Flow catalog source.
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
across all catalog sources and patches them into the catalog's
managed `package.json`.
Flow organizes its generated TypeScript project structure
for a seamless editing experience out of the box with VSCode
and other common editors.

## Import Paths

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

However the sources must still have an import path
even if referenced from a common parent.
The following would **not** work:

<Mermaid chart={`
	graph LR;
		parent.flow.yaml-->foo.flow.yaml;
		parent.flow.yaml-->bar.flow.yaml;
`}/>

These rules make your catalog sources more self contained
and less brittle to refactoring and reorganization.
Consider what might otherwise happen if `foo.flow.yaml`
were imported in another project without `bar.flow.yaml`.