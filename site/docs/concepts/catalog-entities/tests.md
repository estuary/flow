---
description: Understanding the essential components of a good Flow test
---

# Tests

As Flow catalogs grow in breadth and scope, and as requirements change or new contributors get involved,
tests are invaluable for ensuring the correctness of your data products.

Flow tests verify the end-to-end behavior of your catalog schemas and derivations.
At their most basic, you feed example documents into a collection,
and then verify that documents coming out of a derived collection meet your test's expectation:

```yaml
tests:
  acmeCo/tests/greetings:
    - ingest:
        description: Add people to greet.
        collection: acmeCo/people
        documents:
          - { userId: 1, name: "Zelda" }
          - { userId: 2, name: "Link" }

    - verify:
        description: Ensure people were greeted.
        collection: acmeCo/greetings
        documents:
          - { userId: 1, greeting: "Hello Zelda" }
          - { userId: 2, greeting: "Hello Link" }
```

A test is a sequence of one or more sequential steps, each either an `ingest` or a `verify`.
  * `ingest` steps add one or more documents to a collection.
  * `verify` steps make assertions about the current contents of a collection.

All steps must complete successfully in order for a test to pass.

## Ingest

`ingest` steps add documents to a named collection.
All documents must validate against the collection's
[schema](schemas-and-data-reductions.md),
or a catalog build error will be reported.

All documents from a _single_ `ingest` step are added in one transaction.
This means that multiple documents with a common key will be combined _prior_
to their being appended to the collection. Suppose `acmeCo/people` had key `[/id]`:

```yaml
tests:
  acmeCo/tests/greetings:
    - ingest:
        description: Zeldas are combined to one added document.
        collection: acmeCo/people
        documents:
          - { userId: 1, name: "Zelda One" }
          - { userId: 1, name: "Zelda Two" }

    - verify:
        description: Only one Zelda is greeted.
        collection: acmeCo/greetings
        documents:
          - { userId: 1, greeting: "Hello Zelda Two" }
```

## Verify

`verify` steps assert that the current contents of a collection match the provided document fixtures.
Verified documents are fully reduced, with one document for each unique key, ordered under the key's natural order.

You can verify the contents of both derivations and captured collections.
Documents given in `verify` steps do _not_ need to be comprehensive:
it is not an error if the actual document has additional locations not present in the document to verify,
so long as all matched document locations are equal.
Verified documents also do not need to validate against the collection's schema.
They do, however, need to include all fields that are part of the collection's key.

```yaml
tests:
  acmeCo/tests/greetings:
    - ingest:
        collection: acmeCo/people
        documents:
          - { userId: 1, name: "Zelda" }
          - { userId: 2, name: "Link" }
    - ingest:
        collection: acmeCo/people
        documents:
          - { userId: 1, name: "Zelda Again" }
          - { userId: 3, name: "Pikachu" }

    - verify:
        collection: acmeCo/greetings
        documents:
          # greetings are keyed on /userId, and the second greeting is kept.
          - { userId: 1, greeting: "Hello Zelda Again" }
          # `greeting` is "Hello Link", but is not asserted here.
          - { userId: 2 }
          - { userId: 3, greeting: "Hello Pikachu" }
```

### Partition Selectors

Verify steps may include a partition selector to
verify only documents of a specific partition:

```yaml
tests:
  acmeCo/tests/greetings:
    - verify:
        collection: acmeCo/greetings
        description: Verify only documents which greet Nintendo characters.
        documents:
          - { userId: 1, greeting: "Hello Zelda" }
          - { userId: 3, greeting: "Hello Pikachu" }
        partitions:
          include:
            platform: [Nintendo]
```

[Learn more about partition selectors](projections.md#partition-selectors).

## Tips

The following tips can aid in testing large or complex derivations.

### Testing Reductions

Reduction annotations are expressive and powerful, and their use should thus be tested thoroughly. An easy way to test reduction annotations on captured collections is to write a two-step test that ingests multiple documents with the same key and then verifies the result. For example, the following test might be used to verify the behavior of a simple `sum` reduction:

```yaml
tests:
  acmeCo/tests/sum-reductions:
    - ingest:
        description: Ingest documents to be summed.
        collection: acmeCo/collection
        documents:
          - {id: 1, value: 5}
          - {id: 1, value: 4}
          - {id: 1, value: -3}
    - verify:
        description: Verify value was correctly summed.
        collection: acmeCo/collection
        documents:
          - {id: 1, value: 6}
```

### Reusing Common Fixtures

When you write a lot of tests, it can be tedious to repeat documents that are used multiple times. YAML supports [anchors and references](https://blog.daemonl.com/2016/02/yaml.html), which you can implement to re-use common documents throughout your tests. One nice pattern is to define anchors for common ingest steps in the first test, which can be re-used by subsequent tests. For example:

```yaml
tests:
  acmeCo/tests/one:
    - ingest: &mySetup
        collection: acmeCo/collection
        documents:
          - {id: 1, ...}
          - {id: 2, ...}
          ...
    - verify: ...

  acmeCo/tests/two:
    - ingest: *mySetup
    - verify: ...
```

This allows all the subsequent tests to re-use the documents from the first `ingest` step without having to duplicate them.