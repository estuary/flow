---
description: How to define collections in the catalog spec
---

# Collections

A [**collection**](../../concepts/catalog-entities/collections.md) is a set of related documents, where each adheres to a common schema and grouping key. Collections are append-only: once a document is added to a collection, it is never removed. However, it may be replaced or updated — either as a whole or in part — by a future document sharing its key.&#x20;

Each new document of a given key is **reduced** into existing documents of the key. By default, Flow executes such a reduction by completely replacing the previous document, but you can specify much richer reduction behaviors by using [annotated reduction strategies](../reduction-strategies/) in the collection schema.

### `collections` section

The `collections` section is a list of collection definitions within a catalog spec file. A collection must be defined before it can be used as a source or destination for a capture or materialization.

Derived collections may reference collections defined in other catalog sources, but are required to first [import](./#import-section) them, either directly or indirectly. Flow collections are objects that use the following entities:

```yaml
# An object of collections to include in the catalog, where the key is the name of the collection,
# and the value is an object with its definition. You may define any number of collections.
collections:
    # The user-defined name of the collection. Flow collections exist conceptually in a global
    # namespace, so every collection must have a unique name. By convention, slashes are used to
    # fully qualify collection names using path components. Collection names may not be changed.
    # Names may include only unicode letters, numbers and symbols - no spaces or other special characters.
    myOrg/myDomain/collectionName:

        # The key of the collection defines the fields (that must exist) within each document that
        # uniquely identify the entity to which it pertains. Each field that is part of the key must
        # be guaranteed by the schema to always exist, and to have a single possible scalar type.
        # The fields are specified each as a JSON Pointer.
        # Required, type: array
        key: [/json/ptr]

        # Schema against which collection documents are validated and reduced.
        # This should be a URI that points to a YAML or JSON file with the schema;
        # defining the schema inline is discouraged. See below for more details.
        # Required, type: string | object
        schema: mySchemas.yaml#/$defs/myCollectionSchema

        # Projections and logical partitions for this collection.
        # See below for details.
        # Optional, type: object
        projections:

        # Derivation that builds this collection from others through transformations.  This defines
        # how documents are derived from other collections.  A collection without a derivation is
        # referred to as a "captured collection".
        # See below for details.
        # Optional, type: object
        derivation:
        
```

### Projections

Projections are named locations within a collection document that may be used for logical partitioning or directly exposed to databases into which collections are materialized. Projections are objects that use the following entity structure:

```yaml
    a_field: "/json/ptr"
    # JSON Pointer that identifies a location in a document.
    # string, pattern: ^(/[^/]+)*
    a_partition: 
    # type: object
    
    # Entity that defines a partition.
        location: "/json/ptr"
        # type: string, pattern: ^(/[^/]+)*
        
        # Location of this projection
        partition: true
        # type: boolean
        
        # Is this projection a logical partition?     
    
```

You can learn more about projections in their [conceptual documentation](../../concepts/catalog-entities/other-entities.md).

Details on the following sub-entities can be found on their pages:

* [Schema](schemas-and-data-reductions.md)
* [Derivations](derivations/)

The below is a simple example collection that can be defined in Flow. To show the complete example, the schema is shown inline, although in practice it is recommended to store schemas separately and use a URI.&#x20;

```yaml
collections: 
  examples/citi-bike/last-seen: 
  key: [/bike_id] 
  schema: 
    type: object 
    properties: 
      bike_id:
        type: integer
      last: 
        type: string
    required: [bike_id, last]
```



