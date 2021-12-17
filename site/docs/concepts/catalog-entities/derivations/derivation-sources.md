---
description: >-
  Existing collections and their schemas provide the data source for
  derivations.
---

# Derivation sources

The source is the upstream collection that the derived collection consumes.&#x20;

Using our example introduced in the main [Derivations](./) page, our source is a captured Flow collection,`sourceOneName`.

```yaml
derivation:
      transform:
        transformOnesName:
          source:
            name: sourceOneName
            schema: "remote.schema.yaml#/$defs/withRequired"
          publish: { lambda: typescript }
```

Sources can be either captured or derived collection; however, a derived collection cannot directly or indirectly source from itself. In other words, collections must represent a directed acyclic graph (not having any loops), such that document processing will always [halt](https://en.wikipedia.org/wiki/Halting\_problem). Of course, that doesn’t stop you from integrating a service that adds a cycle, if that’s your thing.

### **Source schemas**

Every collection must have a [schema](../schemas-and-data-reductions.md), which is used not only to write data to that collection, but also when a derivation reads from it. This is because collection schemas may evolve over time; documents read from the source are re-validated against the latest collection schema to ensure they are still valid. A schema error will halt the execution of the derivation until the mismatch can be corrected. Sources like this one can optionally point to a file containing an alternative source schema to use:&#x20;

```yaml
remote.schema.yaml#/$defs/withRequired.
```

In this case, the captured collection uses a permissive schema that ensures documents are never lost, and the derived collection can then assert a stricter source schema. In the event that source documents violate that schema, the derivation will halt with an error allowing you to update your schema and transformations, and continue processing from where the derivation left off without writing bad data.
