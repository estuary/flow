---
description: How to create and implement schemas
---

# Schemas

A [Flow schema ](../../concepts/catalog-entities/schemas-and-data-reductions.md)is a [draft 2019-09 JSON Schema](https://json-schema.org/draft/2019-09/release-notes.html) that validates a Flow collection. Schemas also provide annotations at document locations, such as reduction strategies for combining one document into another.

It is recommended to store schemas in a file separate from the catalog spec. This allows them to be re-used. Within a [collection definition](collections.md) in the catalog spec, you give the schema as a relative or absolute URI. URIs can optionally include a JSON fragment pointer that locates a specific sub-schema therein.

For example, `"schemas/marketing.yaml#/$defs/campaign"` would reference the schema at location `{"$defs": {"campaign": ...}}` within `./schemas/marketing.yaml`. &#x20;

Here are several valid example schemas that illustrate their available fields for usage:

```yaml
# Full URIs that point to a valid schema can be used.
schema: 'http://example/schema#/$defs/subPath'

# Also, relative paths
schema: 'path/to/schema.json'

schema:
# Or define them in-line, if necessary. 
  properties:
    bar:
      const: 42
    foo:
      type: integer
  type: object

schema:
  properties:
    foo_count:
      reduce:
      # Or use reduction strategies
        strategy: sum
      type: integer
  reduce:
    strategy: merge
  type: object
```

## Reductions

Flow implements a number of reduction strategies for use within schemas, which tell Flow how two instances of a document can be meaningfully combined together. Learn more about [reduction strategies](../reduction-strategies/).

