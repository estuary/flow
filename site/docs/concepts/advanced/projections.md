# Projections

:::tip
Projections are an advanced concept.
You can use Estuary without knowing the details of projections,
but this section may help you better understand how Estuary works.
:::

Estuary's documents are arbitrary JSON, and may contain multiple levels of hierarchy and nesting.
However, systems that Estuary integrates with often model flat tables of rows and columns, without hierarchy.
Others are somewhere in between.

**Projections** are the means by which Estuary translates between the documents
of a collection and a table representation.
A projection defines a mapping between a structured document location,
given as a [JSON-Pointer](https://tools.ietf.org/html/rfc6901),
and a corresponding **field** name used as, for example, a CSV file header or SQL table column.

Many projections are inferred automatically from a collection’s JSON schema,
using a field that is simply the JSON Pointer with its leading slash removed.
For example, a schema scalar with pointer `/myScalar` will generate a projection with field `myScalar`.

You can supplement by providing additional collection projections,
and a document location can have more than one projection field that references it:

```yaml
collections:
  acmeCo/user-sessions:
    schema: session.schema.yaml
    key: [/user/id, /timestamp]
    projections:
      # A "user/id" projection field is automatically inferred.
      # Add an additional field that doesn't have a slash.
      user_id: /user/id
      # Partly decompose a nested array of requests into a handful of named projections.
      "first request": /requests/0
      "second request": /requests/1
      "third request": /requests/2
```

## Logical partitions

Projections can also be used to logically partition a collection,
specified as a longer-form variant of a projection definition:

```yaml
collections:
  acmeCo/user-sessions:
    schema: session.schema.yaml
    key: [/user/id, /timestamp]
    projections:
      country:
        location: /country
        partition: true
      device:
        location: /agent/type
        partition: true
      network:
        location: /agent/network
        partition: true
```

Logical partitions isolate the storage of documents
by their differing values for partitioned fields.
Estuary extracts partitioned fields from each document,
and every unique combination of partitioned fields
is a separate logical partition.

Every logical partition has one or more **physical partitions**
into which their documents are written,
which in turn controls
how files are arranged within cloud storage.

For example, a document of "acmeCo/user-sessions" like:

```json
{"country": "CA", "agent": {"type": "iPhone", "network": "LTE"}, ...}
```

Might produce files in cloud storage like:

```
s3://bucket/example/sessions/country=CA/device=iPhone/network=LTE/pivot=00/utc_date=2020-11-04/utc_hour=16/<name>.gz
```

:::info
`country`, `device`, and `network` together identify a _logical partition_,
while `pivot` identifies a _physical partition_.
`utc_date` and `utc_hour` is the time at which the journal fragment was created.
:::

[Learn more about physical partitions](journals.md#physical-partitions).

### Partition selectors

When reading from a collection, Estuary catalog entities like derivations, materializations,
and tests can provide a **partition selector**, which identifies the subset
of partitions that should be read from a source collection:

```yaml
# Partition selectors are included as part of a larger entity,
# such as a derivation or materialization.
partitions:
  # `include` selects partitioned fields and corresponding values that
  # must be matched in order for a partition to be processed.
  # All of the included fields must be matched.
  # Default: All partitions are included. type: object
  include:
    # Include partitions from North America.
    country: [US, CA]
    # AND where the device is a mobile phone.
    device: [iPhone, Android]

  # `exclude` selects partitioned fields and corresponding values which,
  # if matched, exclude the partition from being processed.
  # A match of any of the excluded fields will exclude the partition.
  # Default: No partitions are excluded. type: object
  exclude:
    # Skip sessions which were over a 3G network.
    network: ["3G"]
```

Partition selectors are efficient as they allow Estuary to altogether
avoid reading documents that aren’t needed.
