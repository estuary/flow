---
description: How to ingest data with Flow Ingester and the WebSocket API
---

# WebSocket API

The WebSocket API provides an alternative to the REST API for ingesting data with Flow Ingester. It's especially useful when you don’t know how much data there is ahead of time, or when you don’t need precise control over transaction boundaries.&#x20;

When ingesting over a WebSocket, Flow Ingester automatically divides the data into periodic transactions to provide optimal performance. The WebSocket API is also more flexible in the data formats that it can accept; it can ingest comma-separated (CSV) and tab-separated (TSV) data directly, in addition to JSON. However, its key limitation is that WebSocket API can only ingest into a single collection per WebSocket connection.

The collection for WebSocket ingestions is given in the path of the URL, following the format `/ingest/<collection-name>`. For example, to ingest into the `examples/citi-bike/rides` collection, you’d use `ws://localhost:8081/ingest/examples/citi-bike/rides`.

When you initiate a WebSocket connection, you must always set the [Sec-Websocket-Protocol](https://tools.ietf.org/html/rfc6455) header. The value must be one of the following:

* `json/v1`
* `csv/v1`
* `tsv/v1`

If you’re using the [websocat CLI](https://github.com/vi/websocat), then you can simply use the `--protocol` option.

#### Ingesting JSON over WebSocket

When ingesting JSON, Flow Ingester accepts data over the WebSocket in JSON-newline, or [JSON Lines](https://jsonlines.org), format. Objects should not be enclosed within an array or have any separator characters between them except for whitespace. For example, to ingest a few rides into the `examples/citi-bike/rides` collection, let's start with the documents in JSON Lines format in the file `rides.jsonl`:

```
{"bike_id":7,"begin":{"timestamp":"2020-08-27 09:30:01","station":{"id":66,"name":"North 4th St"}},"end":{"timestamp":"2020-08-27 10:00:02","station":{"id":23,"name":"High St"}}}
{"bike_id":26,"begin":{"timestamp":"2020-08-27 09:32:01","station":{"id":91,"name":"Grant Ave"}},"end":{"timestamp":"2020-08-27 09:50:12","station":{"id":23,"name":"High St"}}}
```

Given the above content in a file named `rides.jsonl`, we could ingest it using `websocat` like so:

```bash
cat rides.jsonl | websocat --protocol json/v1 'ws://localhost:8080/ingest/examples/citi-bike/rides'
```

This adds the data to the collection named `examples/citi-bike/rides`.

#### Ingesting CSV/TSV over Websocket

Flow Ingester can ingest a few different character-separated formats. Currently, it supports CSV and TSV formats, using the csv/v1 and tsv/v1 protocols, respectively.&#x20;

Flow collections always store all data in JSON documents that validate against the collection’s schema, so the tabular data in character-separated files must be converted to JSON before being written. Flow Ingester converts these for you based on the headers in the data and the projections for the Flow collection. Each header in a character-separated ingestion must have the same name as a [projection](../../concepts/catalog-entities/other-entities.md) of the collection. The projection will be used to map the field named by the header to the JSON pointer, which is used to construct the JSON document. For example, the `examples/citi-bike/rides` collection looks like this:

{% code title="examples/citi-bike/rides.flow.yaml" %}
```yaml
collections:
  examples/citi-bike/rides:
    key: [/bike_id, /begin/timestamp]
    schema: https://raw.githubusercontent.com/estuary/docs/developer-docs/examples/citi-bike/ride.schema.yaml
    # Define projections for each CSV header name used in the source dataset.
    projections:
      bikeid: /bike_id
      birth year: /birth_year
      end station id: /end/station/id
      end station latitude: /end/station/geo/latitude
      end station longitude: /end/station/geo/longitude
      end station name: /end/station/name
      gender: /gender
      start station id: /begin/station/id
      start station latitude: /begin/station/geo/latitude
      start station longitude: /begin/station/geo/longitude
      start station name: /begin/station/name
      starttime: /begin/timestamp
      stoptime: /end/timestamp
      tripduration: /duration_seconds
      usertype: /user_type
```
{% endcode %}

Given this, we could ingest a CSV file that looks like this:

```
bikeid,starttime,"start station id","start station name",stoptime,"end station id","end station name"
7,"2020-08-27 09:30:01",66,"North 4th St","2020-08-27 10:00:02",23,"High St"
26,"2020-08-27 09:32:01",91,"Grant Ave","2020-08-27 09:50:12",23,"High St"
```

Assuming this was the content of `rides.csv`, you could ingest it using:

```bash
cat rides.csv | websocat --protocol csv/v1 'ws://localhost:8080/ingest/examples/citi-bike/rides'
```

The actual JSON documents that would be written to the collection are:

```bash
{"bike_id":7,"begin":{"timestamp":"2020-08-27 09:30:01","station":{"id":66,"name":"North 4th St"}},"end":{"timestamp":"2020-08-27 10:00:02","station":{"id":23,"name":"High St"}}}
{"bike_id":26,"begin":{"timestamp":"2020-08-27 09:32:01","station":{"id":91,"name":"Grant Ave"}},"end":{"timestamp":"2020-08-27 09:50:12","station":{"id":23,"name":"High St"}}}
```

For example, the projection `bikeid: /bike_id` means that, for each row in the CSV, the value of the `bikeid` column was used to populate the `bike_id` property of the final document. Flow uses the collection’s JSON schema to determine the required type of each property. Additionally, each document that’s constructed is validated against the collection’s schema prior to it being written.

#### Null, empty, and missing values

In JSON documents, there’s a difference between an explicit `null` value and an undefined value. When Flow Ingester parses a character-separated row, it also differentiates between `null`, empty string, and undefined values. Empty values being ingested are always interpreted as explicit `null` values as long as the schema location allows for `null` values (for example, `type: ["integer", "null"]`). If the schema does not allow `null` as an acceptable type, but it does allow `string`, then the value will be interpreted as an empty string. A row may also have fewer values than exist in the header row. If it does, any unspecified column values will be undefined in the final document.&#x20;

In the following example, let’s assume that the schema allows the types in each column’s name.

```
id,string,stringOrNull,integerOrNull
1,"","",""
2,,,
3,
4
```

Assuming simple direct projections, this would result in the following JSON documents being ingested:

```bash
{"id":1,"string":"","stringOrNull":null,"integerOrNull":null}
{"id":2,"string":"","stringOrNull":null,"integerOrNull":null}
{"id":3,"string":""}
{"id":4}
```

Note how in rows `1` and `2`, empty `stringOrNull` values are mapped to `null`, regardless of the presence of quotes. In row `3`, the trailing comma indicates that the row has two values, and that the second value is empty (`""`), but the remainder are undefined. In row `4`, all values besides `id` are undefined.

#### Websocket responses

Regardless of which format you ingest, all WebSocket ingestions return responses similar to the following:

```bash
{"Offsets":{"examples/citi-bike/rides/pivot=00":545},"Etcd":{"cluster_id":14841639068965178418,"member_id":10276657743932975437,"revision":28,"raft_term":2},"Processed":2}
```

The response shows the offsets of the transaction boundaries in the journals. If you ingest larger amounts of data, you will receive many such responses. In addition to the journal offsets, each response also includes the `Processed` property, which indicates the number of WebSocket frames that have been successfully ingested. This can be used to allow clients to resume where they left off in the event that a WebSocket ingestion fails partway through. For example, if you sent one JSON object per WebSocket frame, then you would know from the `Processed` field how many documents had been successfully ingested prior to the failure:`Processed` times the number of documents per frame.
