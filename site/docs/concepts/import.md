---
sidebar_position: 7
---
# Imports

When you work on a draft Data Flow [using `flowctl draft`](../guides/flowctl/edit-draft-from-webapp.md),
your Flow specifications may be spread across multiple files.
For example, you may have multiple **materializations** that read from collections defined in separate files,
or you could store a **derivation** separately from its **tests**.
You might also reference specifications that aren't in your local draft.
For example, you might create a derivation with a source collection that is not in your local draft.

When you publish your draft, Flow automatically resolves references to specifications across the entirety of the [catalog](./catalogs.md).
This is possible because every entity in Flow has a globally unique name.

Alternatively, you can explicitly add other local specification files to the Data Flow's build process by including an `import` section
in the Flow specification file you'll publish.
When the draft is published, the imported specifications are treated as part of the file
into which they are imported.
All entities in the draft will be used to overwrite any existing version of those entities in the global catalog.

Explicit imports are useful when you need to update multiple components of a data flow at the same time,
but they're in separate files.
For example, when you update a derivation, you must also update its test(s) at the same time to prevent failures.
You could import `test.yaml` into `my-derivation.yaml` and then publish `my-derivation.yaml` to update both entities in the catalog.

A common pattern for a given draft is to have a single top-level specification
file which explicitly imports all the others.
Flow automatically generates such a top-level file for your draft when you begin a local work session
using `flowctl draft develop`.

## Specification

The `import` section is structured as a list of partial or absolute URIs,
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
# Test an example from a GitHub repository.
$ flowctl draft test --source https://raw.githubusercontent.com/estuary/flow-template/main/word-counts.flow.yaml
```

## Fetch behavior

Flow resolves, fetches, and validates all imports in your local environment during the catalog build process,
and then includes their fetched contents within the published catalog on the Estuary servers.
The resulting catalog entities are thus self-contained snapshots of all resources
_as they were_ at the time of publication.

This means it's both safe and recommended to directly reference
an authoritative source of a resource, such as a third-party JSON schema, as well as resources within your private network.
It will be fetched and verified locally at build time,
and thereafter that fetched version will be used for execution,
regardless of whether the authority URL itself later changes or errors.

## Import types

Almost always, the `import` stanza is used to import other Flow
specification files.
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

Other permitted content types include `JSON_SCHEMA`,
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

## Importing derivation resources

In many cases, [derivations](./derivations.md) in your catalog will need to import resources.
Usually, these are TypeScript modules that define the lambda functions of a transformation,
and, in certain cases, the NPM dependencies of that TypeScript module.

These imports are specified in the derivation specification, _not_ in the `import` section of the specification file.

For more information, see [Derivation specification](./derivations.md#specification) and [creating TypeScript modules](./derivations.md#creating-typescript-modules).

## Import paths

import Mermaid from '@theme/Mermaid';

If a catalog source file `foo.flow.yaml` references a collection in `bar.flow.yaml`,
for example as a target of a capture,
there must be an _import path_ where either `foo.flow.yaml`
imports `bar.flow.yaml` or vice versa.

When you omit the `import` section, Flow chooses an import path for you.
When you explicitly include the `import` section, you have more control over the import path.

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