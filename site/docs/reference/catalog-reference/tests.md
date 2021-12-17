---
description: How to test your catalog's behavior
---

# Tests

[Tests](../../concepts/catalog-entities/tests.md) verify the expected behavior of a catalog by allowing you to ingest sample documents and ensure that the expected output is produced.

```yaml
# An object with tests to run, where each key is the name of a test, and the value is an array of
# test steps to execute.
tests:
    # Test names can be anything.
    'any old test name you want':
          # Tests are defined as an array of steps, which will be executed in the order given.
          # Each step may either ingest or verify. A test may include any number of steps with any
          # combination of ingest and verify steps.

          # An ingestion test step ingests document fixtures into the named collection.
        - ingest:

              # Name of the collection into which the test will ingest.  Collection names consist of
              # Unicode letters, numbers and symbols. Spaces and other special characters are disallowed.
              collection: example/collection

              # The documents to ingest. Each document must conform to the collection's schema.
              # Required, type: array
              documents:
                - foo: bar
                  baz: 1

          # A verification step verifies that the contents of a named collection match the expected
          # features, after fully processing all preceding ingestion steps.
        - verify:

              # The name of the collection to verify
              collection: example/collection

              # Documents to verify. Each document may contain only a portion of the matched documents
              # properties, and any properties present in the actual document but not in the document
              # fixture will be ignored. All other values must match or the test will fail.
              # Documents here must be provided in lexicographical order by key.
              # Required, type: array
              documents:
                - foo: bar
                  baz: 1

              # Selector over partitions to verify.
              # type: object
              partitions:

                  # Partition field names and corresponding values which must be matched from the source
                  # collection. Only documents having one of the specified values across all specified
                  # partition names will be matched. For example, source: [App, Web] region: [APAC] would
                  # mean only documents of ''App'' or ''Web'' source and also occurring in the ''APAC''
                  # region will be processed.
                  # type: object
                  include:
                      a_partition: ["A", "B"]
                  # Partition field names and values which are excluded from the source collection. Any
                  # documents matching *any one* of the partition values will be excluded.
                  exclude:
                      another_partition: [32, 64]
```

An example test section can be found below:

```yaml
tests:
    examples:
    - ingest:
        collection: a/collection
        documents:
        - example: document
        - another: document
    - verify:
        collection: a/collection
        documents:
        - expected: document
            partitions:
                include:
                  another_partition:
                      - A
                      - B
                exclude:
                  a_partition:
                      - 32
                      - 64
```



