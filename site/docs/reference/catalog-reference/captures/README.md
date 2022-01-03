---
description: >-
  How to bind a Flow collection to an external data source and extract data from
  it
---

# Captures

A [capture](../../../concepts/catalog-entities/captures.md) binds a Flow collection to an external data source, such as a cloud storage bucket or a real-time data stream, from which the collection is continuously populated. Captures are defined in the catalog spec.

Captures are objects utilizing the following entities:

```yaml
# A set of captures to include in the catalog.
# Optional, type: object
captures:

  # The name of the capture, formatted as {tenant}/{capturename}, where {tenant} is your cloud storage bucket.
  example/name:

    # Bindings define how collections are populated from the data source.  A
    # single capture may populate many collections, each defined as a separate
    # binding.
    # Required, type: array
    bindings:

      - # The target is the name of a collection to populate. This
        # must be defined somewhere within the catalog spec, but it may be
        # in a separate file that is imported by, or imports, this file.
        # Required, type: string
        target: example/collection/name

        # The resource includes any additional configuration required to
        # extract data from the endpoint and map it into the collection.
        # This is freeform configuration based on the endpoint type and connector.
        # Required, type: object
        resource: {}

    # Endpoints define how to connect to the source of the capture.
    # Required, type: object
    endpoint:

      # Each endpoint uses a specific connector.
      s3:

        # Each connector has its own set of configuration values specific to
        # that system.
        bucket: exampleS3Bucket
        prefix: filePrefix

```

The [Endpoint configurations page](endpoint-configurations.md) provides additional detail on supported endpoint types and connector configurations.
