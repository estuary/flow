---
sidebar_position: 1
---

# Time Travel

Time travel offers the functionality to limit your materialization data to a specific date range. Applying a time travel to an existing materialization will not remove existing documents, but rather it will provide a filtered view of the materialization within a certain time range. 


## How to configure time travel

In the Flow web app, either navigate to an existing materialization or create a new one. Under **Source Collections** scroll to the bottom of a **Resource Configuration** for a specific collection. If you are working with a new materialization, you must link a collection to the materialization before continuing.

There are two optional `date-time` fields for implementing time travel: `notBefore` and `notAfter`. Clicking on either field will pop up a date/time picker used to populate the row. It is not required to select a value for both fields in order for the time travel to be applied, however selecting a value for both fields will only materialize data which satisfies both limits. For instance, all new data would have to occur before `notAfter` **and** after `notBefore`.

### Specification

Alternatively, both fields can be defined in the Flow specification file with the following format:

```yaml
materializations:
  # The name of the materialization.
  acmeCo/example/database-views:
  	# Endpoint defines how to connect to the destination of the materialization.
    # Required, type: object
    endpoint:
      # This endpoint uses a connector provided as a Docker image.
      connector:
        # Docker image that implements the materialization connector.
        image: ghcr.io/estuary/materialize-mysql:dev
        # File that provides the connector's required configuration.
        # Configuration may also be presented inline.
        config: path/to//connector-config.yaml
    bindings:
      - # Source collection read by this binding.
        # Required, type: object or string
        source:
          # Name of the collection to be read.
          # Required.
          name: acmeCo/example/collection
          # Lower bound date-time for documents which should be processed. 
          # Source collection documents published before this date-time are filtered.
          # `notBefore` is *only* a filter. Updating its value will not cause Flow
          # to re-process documents that have already been read.
          # Optional. Default is to process all documents.
          notBefore: 2023-01-23T01:00:00Z
          # Upper bound date-time for documents which should be processed.
          # Source collection documents published after this date-time are filtered.
          # Like `notBefore`, `notAfter` is *only* a filter. Updating its value will
          # not cause Flow to re-process documents that have already been read.
          # Optional. Default is to process all documents.
          notAfter: 2023-01-23T02:00:00Z
```


## Properties

|-----------------------------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| Property                    | Title                  | Description                                                                                                                                                                                                                                                                                                                                                                                | Type   | Required/Default |
-----------------------------------------------------------------------------------------------------------------------------|--------|------------------|
| **`/notBefore`**             | Not Before               | Only include date before this time                                                                                                                                                                                                                                                                                                                                          | date-time |          |
| **`/notAfter`**              | Not After                | Only include data after this time                                                                                                                                                                                                                                                                                                 | date-time |          |
