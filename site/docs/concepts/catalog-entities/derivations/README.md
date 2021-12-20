---
description: >-
  Derivations are the catalog entity used to transform and join data in
  collections.
---

# Derivations

In Flow, you can perform transformations and joins on existing [collections](../collections.md). This process is known as a **derivation**, and the results are stored in a derived collection.&#x20;

A given derivation can contain one or more transformations, where each transformation reads a **source** collection and re-structures its documents using mapping **lambda functions**.

![](<derivations.svg>)

Transformations rely on **registers** to keep track of states and apply functions to a given data point when a particular event occurs. Registers are key-based JSON documents that transformations can read and update.&#x20;

The process that keys the register is called a data **shuffle**, and is defined by each transformation using information extracted from its source documents.

Let's look at an example of a derivation with a simple transformation:

```yaml
    derivation:
      transform:
        transformOnesName:
          source:
            name: sourceOneName
            schema: "remote.schema.yaml#/$defs/withRequired"
          publish: { lambda: typescript }
```

Subsequent pages in this section will explore important elements of a derivation in-depth using this example.

* [Derivation sources](derivation-sources.md)
* [Lambdas](lambdas.md)
* [Registers and shuffles](registers-and-shuffles.md)
* [Transformations](transforms.md)
* [Other derivation concepts](other-derivation-concepts.md)

