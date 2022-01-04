---
description: >-
  How to bind a Flow collection to an external system and that system keep it up
  to date
---

# Materializations

A [materialization](../../../concepts/catalog-entities/materialization.md) binds a Flow collection with an external system, such as a database, and a target, such as a SQL table, into which the collection is to be continuously written.&#x20;

Materializations are objects utilizing the below entities:

```yaml
# A set of materializations to include in the catalog.
# Optional, type: object
materializations:

  # The name of the materialization.
  example/name:

    # Bindings define how collections are included in the materialization.
    # A single materialization may pull from many collections, each
    # defined as a separate binding.
    # Required, type: object
    bindings:

      # The source is the name of a collection to materialize. This
      # must be defined somewhere within the catalog spec, but it may be
      # in a separate file that is imported by, or imports, this file.
      # Required, type: string
      - source: example/collection
      
        # The resource is the a freeform set of configuration values used
        # by the specific endpoint type. Each endpoint type will require its
        # own set of configuration values.
        # Required, type: object 
        resource:
          # In this example, the `sqlite` endpoint type expects a `table` key
          # to specify the table the data will be materialized into.
          table: example_table

        # You may optionally materialize only a subset of the partitions in a partitioned
        # collection by defining a partition selector here. Both the include and exclude fields here
        # accept objects where the keys are the names of the fields on which the collection is
        # logically partitioned, and the values are an array of values that name specific partitions.
        # Optional, type: object
        partitions:

          # For each key in this object, only include a partition if the field value matches one
          # of the values in the array. For example, here we would only materialize partitions
          # where `myPartitionField1` exactly matches one of the given values.
          include: { "myPartitionField1": [ "value1", "value2" ]}

          # For each key in this object, exclude a partition if the field value matches one of the
          # values in the array. For example, here we would materialize all the `myPartitionField2`
          # partitions, except those matching one of the given values.
          exclude: { "myPartitionField2": [ 1, 5, 7] }

        # Selected projections for this materialization. Flow will select reasonable defaults depending
        # on the type of system being materialized into. But you may control exactly which fields are
        # included in a materialization in your fields object.
        # Optional, type: object
        fields:

          # Whether or not to include all of the fields that are recommended for the endpoint.
          # For databases, this means all scalar fields that have a single possible type.
          # Required, default: true, type: boolean
          recommended: true

          # Array of projections to exclude.
          # default: [], type: array
          exclude: [myField, otherField]

          # Fields to include.  This supplements any recommended fields, where enabled.
          # Values are passed through to the driver, e.x. for customization of the driver's
          # schema generation or runtime behavior with respect to the field.  Flow automatically
          # adds most fields, so this is an advanced Flow feature.
          # default: {}, type: object
          include:  {goodField: {}, greatField: {}}

    # Endpoints define how to connect to the destination of the materialization.
    endpoint:

      # An endpoint has a specific connector type.
      sqlite:

        # Each type of endpoint has its own set of configuration values specific to
        # that system.
        path: db/example.db

```

The [Endpoint configurations page](endpoints.md) provides additional detail on supported endpoint types.

