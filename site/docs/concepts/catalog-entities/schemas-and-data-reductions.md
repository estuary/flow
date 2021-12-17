---
description: How Flow uses JSON schemas to model data structure and constraints
---

# Schemas

You must provide Flow with a JSON schema for each [collection](collections.md) in your catalog to control data quality.&#x20;

Flow makes heavy use of [JSON Schema](https://json-schema.org/understanding-json-schema/) to describe the expected structure and semantics of JSON documents. JSON Schema is an expressive standard for defining JSON: it goes well beyond basic type information and can model [tagged unions](https://en.wikipedia.org/wiki/Tagged\_union), recursion, and other complex, real-world composite types. Schemas can also define rich data validations like minimum & maximum values, regular expressions, date/time/email, and other formats.

Together, these features let schemas represent structure _as well as_ expectations and constraints that are evaluated and must hold true for every collection document _before_ it’s added to the collection. They’re a powerful tool for ensuring end-to-end data quality: for catching data errors and mistakes early, before they can cause damage.

### Creating schemas

Your schema can be quite permissive or as strict as you wish. There are a few restrictions, however.

* The top-level type must be `object`. Flow adds a bit of metadata to each of your documents, which can only be done with a container type.&#x20;
* Any fields that are part of the collection's `key` must provably exist in any document that validates against the schema. Put another way, every document within a collection must include all of the fields of the collection's key, and the schema must guarantee that.
* It is highly recommended that schemas be stored in separate files so that they can be re-used.

For example, the following collection schema would be invalid because the `id` field which is used as its key is not `required`, so it might not actually exist in all documents.

{% tabs %}
{% tab title="flow.yaml" %}
```yaml
collections:
  invalid/key-not-required:
    schema: schemas.yaml#/$defs/mySchema1
    key: [/id]
```
{% endtab %}

{% tab title="schemas.yaml" %}
```yaml
$defs:
  mySchema1:
    type: object
    properties:
      id: {type: integer}
      value: {type: string}
    required: [value]
```
{% endtab %}
{% endtabs %}

To fix the above schema, change `required` to `[id, value]`.

### How schemas work

A central design tenant of Flow is that users need only provide a model of their data _one time_, as a JSON schema. Having done that, Flow leverages static inference over the schema to provide translations into other schema flavors:

* Most [**projections**](other-entities.md) of a collection are automatically inferred from its schema, and inference is used to map to appropriate SQL types and constraints.
* Inference powers many of the error checks Flow performs, such as ensuring that the collection key must exist and is of an appropriate type.
* Flow generates TypeScript definitions from schemas to provide compile-time type checks of user lambda functions. These checks are immensely helpful for surfacing mismatched expectations around, for example, whether a field must exist, which otherwise usually cause issues in production.

### Reduction annotations

Flow uses standard JSON Schema with the addition of one additional component: reduction annotations. These help aggregate data by defining how separate documents will be combined.

[Learn more about data reductions](reductions.md).
