---
description: How to transform and join data in Flow
---

# Derivations

{% hint style="info" %}
This documentation covers most use cases of derivations. You can find additional information about specific features not discussed here by using `flowctl json-schema` on the command line.
{% endhint %}

****[Derivations](../../../concepts/catalog-entities/derivations/) collectively refer to any transformation or join that you can do in Flow aside from schema-based [reduction annotations](../../reduction-strategies/). They define how documents are derived from other collections. A [collection](../collections.md) without a derivation is referred to as a **captured collection**. &#x20;

Derivations in Flow are objects that use the following entities:

```yaml
# To be nested within a collection definition in a catalog spec.
derivation:

    # A register holds the internal states of a derivation, which can be read and updated by
    # all of its transforms. Any stateful transformation (joins, for example) must use a register
    # to store its state.
    # When reading source documents, each document is mapped to a single register using the
    # shuffle key, or the key of the source collection if a shuffle key is not explicitly defined.
    # "Update" lambdas of the transformation produce updates which are reduced into the register,
    # and a "publish" lambda reads the current (and previous, if updated) register value.
    # Optional, type: object
    register:

        # A register always has an associated JSON schema, which may also use reduction annotations.
        # Schemas may be specified either as URIs or inline, just like for collection schemas.
        # However, it is recommended to use URIs as a best practice and store schemas seperately.
        # Details can be found on the "Schemas" page.
        # Required, type: object | string
        schema: {type: [integer, 'null']}

        # Registers allow an initial value to be passed in for a document that has never been updated.
        # default: null
        initial: null

    # Defines the set of transformations that produce the documents in this collection.
    # Each transformation reads and shuffles documents of a single source collection, and processes each document
    # through either one or both of a register "update" lambda and a derived document "publish" lambda.
    transform:

        # Name of the transformation, which is used in determining the name of associated typescript
        # functions.
        title:

            # Update lambda that maps a source document to register updates.
            # Optional, type: object
            update: {lambda: typescript}

            # Publish lambda  that maps a source document and registers into derived documents of the
            # collection.
            # Optional, type: object
            publish: {lambda: typescript}

            # Source collection read by this transformation.
            # Required, type: object
            source:
                # name of the source collection used by this transformation.
                name: example/collection

            # Shuffle by which source documents are mapped to registers.  Composite key of JSON pointers.
            # default: key of source collection.
            shuffle: [/fieldA, /field/B]

            # Delay applied to documents processed by this transformation.
            # Delays are applied as an adjustment to the UUID clock encoded within each document, which
            # is then used to impose a relative ordering of all documents read by this derivation. This
            # means that read delays are applied in a consistent way, even when back-filling over
            # historical documents. When caught up and tailing the source collection, delays also "gate"
            # documents such that they aren't processed until the current wall-time reflects the delay.
            # default: null, pattern: ^\\d+(s|m|h)$
            readDelay: "48h"

            # When all transforms are of equal priority, Flow processes documents according to their
            # associated publishing time, as encoded in the document UUID.
            # However, when one transform has a higher priority than others, then *all* ready documents
            # are processed through the transform before *any* documents of other transforms are processed.
            # default: null, integer => 0
            priority: 0
```

A TypeScript lambda is referenced by `update` and `publish`, which references a TypeScript file. Learn more on the [lambdas page](lambdas.md).

Let's take a look at a simple real-life derivation that uses all of the above concepts. This example is part of a catalog that works with Citi-bike data and calls an API to create a list of bikes that haven't moved in two days.

```yaml
    derivation:
      register:
        # Store the most-recent ride timestamp for each bike_id in a register,
        # and default to null if the bike hasn't ridden before.
        schema: { type: [string, "null"] }
        initial: null

      transform:
        liveRides:
          source:
            name: examples/citi-bike/rides
            # This is an arbitrary pointer to an example collection in our
            # Flow git repository.
          shuffle: { key: [/bike_id] }
          update: { lambda: typescript }

        delayedRides:
          source:
            name: examples/citi-bike/rides
          shuffle: { key:  [/bike_id] }
          # Use a 2-day read delay, relative to the document's ingestion.
          # To see read delays in action within a short-lived
          # testing contexts, try using a smaller value (e.g., 2m).
          readDelay: "48h"
          publish: { lambda: typescript }
```

The above derivation makes use of all available entities except for `priority`. &#x20;

### Registers

Under the hood, registers are backed by replicated, embedded RocksDB instances which co-locate one-to-one with the lambda execution contexts that Flow manages. As contexts are assigned and re-assigned, their DBs travel with them.

If any single RocksDB instance becomes too large, Flow can perform an online **split,** which subdivides its contents into two new databases — and paired execution contexts — which are re-assigned to other machines.
