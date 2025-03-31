---
sidebar_position: 1
---

# Time Travel

Time travel functionality allows you to restrict the data materialization process to a specific date range. When applying time travel to an existing materialization, it's important to note that it won't remove any existing documents. Instead, it will only materialize new data that falls within the specified date and time window. New data will not be included in your materialization destination unless it conforms to the specified date range criteria. Consequently, setting a lower boundary in the future date will delay the materialization of data until that future date is reached.


## How to configure time travel

In the Flow web app, either navigate to an existing materialization or create a new one. Under **Source Collections** scroll to the bottom of a **Resource Configuration** for a specific collection. If you are working with a new materialization, you must link a collection to the materialization before continuing.

You'll find two optional date-time fields for implementing time travel: `notBefore` and `notAfter`. Click on either field to open a date/time picker that you can use to set the values. It's not mandatory to select values for both fields for time travel to take effect. However, selecting values for both fields will ensure that only data meeting both criteria is materialized. In other words, new data must fall before the `notAfter` date and after the `notBefore` date to be included in the materialization.

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
| Property | Title | Description | Type |
|---|---|---|---|
| **`/notBefore`**| Not Before | Only include data after this time | date-time |
| **`/notAfter`** | Not After  | Only include data before this time | date-time |
