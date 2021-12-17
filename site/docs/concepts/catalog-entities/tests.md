---
description: Understanding the essential components of a good Flow test
---

# Tests

Flow catalogs can express an incredible breadth of data products, created using powerful tools like reductions and stateful transformations. With so much expressive power, testing is _absolutely necessary_ in order to ensure the quality of your data products.

{% hint style="info" %}
#### Testing is _absolutely necessary_ to ensure the quality of your data products.
{% endhint %}

This article will explain the concepts used in testing. Let's start with a simple example:

```yaml
tests:
  'test greetings':
    # Add documents to the people collection
    - ingest:
        collection: people
        documents:
          - { userId: 1, name: "Zelda" }
          - { userId: 2, name: "Link" }

    # Assert that the proper greetings have been derived
    - verify:
        collection: greetings
        documents:
          - { userId: 1, greeting: "Hello Zelda" }
          - { userId: 2, greeting: "Hello Link" }
```

There are a few things worth pointing out about this:

* The name `test greetings` is the name of the test. This is what will be printed when a test passes or fails. It will benefit you to make these descriptive and human-readable.
* An individual test is composed of a sequence of one or more steps, which are executed in the order given. Each step can be either an `ingest` or a `verify` step.
  * `ingest` steps add one or more documents to a collection.
  * `verify` steps make assertions about the contents of a collection.
* All steps must complete successfully in order for a test to pass.&#x20;

### Ingest steps

`ingest` steps add the given documents to the given collection. This is pretty straightforward, but there are a few details worth being aware of.

* All documents must validate against the collection's [schema](schemas-and-data-reductions.md), or they will be rejected and the test will fail.
* You can only ingest into captured collections, not derivated collections.
* All documents from a _single_ `ingest` step are added in a single transaction.
* This means that multiple documents with the same key will be reduced _prior_ to being appended to the collection. For example, say you have a collection with a key of `[/id]`, and an `ingest` step like:

```yaml
- ingest:
    collection: myCollection
    documents:
      - {id: 1, value: "foo"}
      - {id: 1, value: "bar"}
```

Assuming the default reduction strategy of `lastWriteWins`, this would result in only the document `{id: 1, value: "bar"}` being appended.

### Verify steps

`verify` steps assert that the complete and fully reduced contents of a collection match the provided example documents. Since the documents are fully reduced, it means that there will be at most a single document per unique key. It also means that the values provided in `verify` steps will exactly match what you'd see if you had materialized the collection to a database like PostgreSQL.

Before `flowctl` executes a `verify` step, it first ensures that all [derivations](derivations/) have fully processed all the documents from all prior `ingest` steps. This eliminates the possibility of flaky tests due to race conditions.

* `verify` steps assert that the given collection contains exactly the given set of documents. Put another way, you must always specify the complete collection contents in a verify step.&#x20;
* You can verify the contents of both derivations and captured collections.
* Documents given in `verify` steps do _not_ need to validate against the collection's schema. They do, however, need to include all fields that are part of the collection's key.
* The documents must be provided in lexicographical order by key. This ensures that there is exactly one way to list any given set of documents, and contributes to more readable tests.
* Any non-key fields that are missing from a document in a verify step will simply not be checked.

### Testing tips

The following tips can aid in testing large or complex derivations.

#### **Testing reductions of captured collections**

Reduction annotations are expressive and powerful, and their use should thus be tested thoroughly. An easy way to test reduction annotations on captured collections is to write a two-step test that ingests multiple documents with the same key and then verifies the result. For example, the following test might be used to verify the behavior of a simple `sum` reduction:

```yaml
tests:
  'my sum reduction works':
    - ingest:
        collection: myCollection
        documents:
          - {id: 1, value: 5}
          - {id: 1, value: 4}
          - {id: 1, value: -3}
    - verify:
        collection: myCollection
        documents:
          - {id: 1, value: 6}
```

#### **Reusing common example documents**

When you write a lot of tests, it can be tedious to repeat documents that are used multiple times. YAML supports [anchors and references](https://blog.daemonl.com/2016/02/yaml.html), which you can implement to re-use common documents throughout your tests. One nice pattern is to define anchors for common ingest steps in the first test, which can be re-used by subsequent tests. For example:

```yaml
tests:
  'first test':
    - ingest: &mySetup
        collection: myCollection
        documents:
          - {id: 1, ...}
          - {id: 2, ...}
          ...
    - verify: ...

  'subsequent test':
    - ingest: *mySetup
    - verify: ...
```

This allows all the subsequent tests to re-use the documents from the first `ingest` step without having to duplicate them.

To learn more about test programming elements, see the [tests reference documentation](../../reference/catalog-reference/tests.md).
