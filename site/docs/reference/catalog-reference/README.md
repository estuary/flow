---
description: Principles of catalog design and catalog specification sections
---

# Catalog design and specification

Flow allows you to organize schemas, captures, collections, transformations, tests, and materializations into a single directory: a **catalog**. The **catalog spec** is the main YAML file(s) that defines catalog structure. Most Flow entities can either be defined in-line in the catalog spec or referenced as files, in which case they can be re-used at your discretion

The concept is flexible by design: you can — and should — structure your sources in ways that make sense for your projects, teams, and organization.&#x20;

### Creating the catalog specification

The default catalog spec name is `flow.yaml`. You're encouraged to follow this convention by using prefixes to differentiate separate catalog specs you may have, as in `mypipeline.flow.yaml`.&#x20;

You can either create the catalog spec manually in the Flow devcontainer environment, or use `flowctl discover` to auto-generate a catalog spec, as detailed below.

Within the catalog spec, you work in several sections to define and reference various types of top-level Flow entities. Each section is explained in depth below. For a conceptual overview of the catalog entity types, see the [Catalogs ](../../concepts/catalog-entities/)concept page.&#x20;

:::info
You can use the command `flowctl json-schema`to get a complete view of Flow entities including requirements and options. However, it provides a lot of output including some internal entities, and this reference section provides the same information in a more digestible format.
:::

### Using `flowctl discover`

When you know the endpoint and you'll be capturing from and the URL of the required [connector](captures/endpoint-configurations.md), you can use `flowctl discover` to generate a catalog spec with a capture and a collection. Then, you can add additional sections and make changes manually.&#x20;

For a full sample workflow, see the [Hello Flow](../../getting-started/flow-tutorials/hello-flow.md) tutorial.

### Catalog spec sections

#### `import` section

The `import` section is a list of partial or absolute URLs that are always evaluated relative to the base directory of the current source. For example, these are possible imports within a collection:

```yaml
# Suppose we're in file "/path/dir/flow.yaml"
import:
  - sub/directory/flow.yaml        # Resolves to "file:///path/dir/sub/directory/flow.yaml".
  - ../sibling/directory/flow.yaml # Resolves to "file:///path/sibling/directory/flow.yaml".
  - https://example/path/flow.yaml # Uses the absolute url.
```

The import rules are designed so that a collection doesn’t have to do anything special in order to be imported by another, and [`flowctl`](../../concepts/flowctl.md) can even directly build remote sources:

```bash
# Test an example from the flow-template repository.
$ flowctl test --source https://raw.githubusercontent.com/estuary/flow-template/main/word-counts.flow.yaml
```

JSON schemas have a `$ref` keyword, by which local and external schema URLs may be referenced. Flow uses these same import rules for resolving JSON schemas, and it’s recommended to directly reference the authoritative source of an external schema. Using a hypothetical Citi-bike schema URL, this would look like the following:

```yaml
    schema:
      type: object
      properties:
        bike_id: { type: integer }
        station: { $ref: https://citibike.com/stationSchema }
      required: [bike_id, station]
```

`flowctl` fetches and resolves all catalog and JSON Schema sources at build time, resulting in a self-contained snapshot of these resources _as they were_ at the time the catalog was built.

#### `captures` section

The captures in a catalog each bind a target within an external endpoint to one of your defined collections. Data is continuously captured from the target to the collection. A given collection can have multiple captures, but only one capture can exist for a given endpoint and target.

Detailed reference information on captures can be found [here](../../concepts/catalog-entities/captures.md).

#### `collections` section

The `collections` section is a list of collection definitions within a catalog source. A collection must be defined before it may be used as a source within another collection.

Derived collections may also reference collections defined in other catalog sources, but must first import them either directly or indirectly.&#x20;

Detailed reference information on collections can be found [here](collections.md).

#### `materializations` section

A materialization binds a Flow collection with an external system and a target, such as a SQL table, into which the collection is to be continuously materialized. Detailed reference documentation on materializations can be found [here](materialization/).

#### **`tests` section**

Flow catalogs can also define functional **contract tests,** which verify the integrated end-to-end behaviors of one or more collections. You’ll see examples of these tests throughout this documentation. Detailed reference documentation on testing can be found [here](tests.md).

### Schemas

Flow catalogs rely on JSON schema to validate documents and ensure the integrity of your pipeline. Each collection requires a schema, but they should be stored in a separate YAML file from the catalog spec so that they can be re-used. Learn more [here](schemas-and-data-reductions.md).

