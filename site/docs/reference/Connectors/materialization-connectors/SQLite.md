
# SQLite

This connector materializes Estuary collections into an ephemeral SQLite database.
Note that this connector is for demo and sandbox purposes, and if the connector is restarted for any
reason, the data materialized up to that point will be lost in the
materialization (your collection will stay in-tact and your other
materializations will have the data as normal).

It is available for use in the Estuary web application. For local development or
open-source workflows,
[`ghcr.io/estuary/materialize-sqlite:dev`](https://ghcr.io/estuary/materialize-sqlite:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## Prerequisites

To use this connector, you'll need:

* At least one Estuary collection

## Configuration

This materialization requires no configuration, and all you need to do is choose
a collection to materialize.

### Properties

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/table`** | Table | Table name to materialize to. It will be created by the connector, unless the connector has previously created it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-sqlite:dev
        config: {}
    bindings:
      - resource:
          table: ${TABLE_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta updates

This connector does not support delta updates at the moment.

## Reserved words

SQLite has a list of reserved words that must be quoted in order to be used as an identifier.
Estuary considers all the reserved words that are marked as "reserved" in any of
the columns in the official [SQlite documentation](https://www.sqlite.org/lang_keywords.html).

These reserve words are listed in the table below. Estuary automatically quotes fields that are in this list.

| Reserved words    |              |
|-------------------|--------------|
| abort             | intersect    |
| action            | into         |
| add               | is           |
| after             | isnull       |
| all               | join         |
| alter             | key          |
| always            | last         |
| analyze           | left         |
| and               | like         |
| as                | limit        |
| asc               | match        |
| attach            | materialized |
| autoincrement     | natural      |
| before            | no           |
| begin             | not          |
| between           | nothing      |
| by                | notnull      |
| cascade           | null         |
| case              | nulls        |
| cast              | of           |
| check             | offset       |
| collate           | on           |
| column            | or           |
| commit            | order        |
| conflict          | others       |
| constraint        | outer        |
| create            | over         |
| cross             | partition    |
| current           | plan         |
| current_date      | pragma       |
| current_time      | preceding    |
| current_timestamp | primary      |
| database          | query        |
| default           | raise        |
| deferrable        | range        |
| deferred          | recursive    |
| delete            | references   |
| desc              | regexp       |
| detach            | reindex      |
| distinct          | release      |
| do                | rename       |
| drop              | replace      |
| each              | restrict     |
| else              | returning    |
| end               | right        |
| escape            | rollback     |
| except            | row          |
| exclude           | rows         |
| exclusive         | savepoint    |
| exists            | select       |
| explain           | set          |
| fail              | table        |
| filter            | temp         |
| first             | temporary    |
| following         | then         |
| for               | ties         |
| foreign           | to           |
| from              | transaction  |
| full              | trigger      |
| generated         | unbounded    |
| glob              | union        |
| group             | unique       |
| groups            | update       |
| having            | using        |
| if                | vacuum       |
| ignore            | values       |
| immediate         | view         |
| in                | virtual      |
| index             | when         |
| indexed           | where        |
| initially         | window       |
| inner             | with         |
| insert            | without      |
| instead           ||
