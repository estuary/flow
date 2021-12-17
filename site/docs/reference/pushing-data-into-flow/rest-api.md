---
description: How to ingest data with Flow Ingester and the REST API
---

# REST API

The REST API makes it easy to add data to one or more Flow collections transactionally using Flow Ingester.&#x20;

The endpoint is available at `/ingest` (for example, `http://localhost:8080/ingest`). This endpoint responds only to PUT and POST requests with a `Content-Type: application/json`. Any other method or content type will result in a 404 error response. The request body should be a JSON object where the keys are names of Flow collections, and the values are arrays of documents for that collection. For example:

```bash
curl -H 'Content-Type: application/json' --data @- 'http://localhost:8081/ingest' <<EOF
{
    "examples/citi-bike/rides": [
        {
            "bike_id": 7,
            "begin": {
                "timestamp": "2020-08-27 09:30:01.2",
                "station": {
                    "id": 3,
                    "name": "Start Name"
                }
            },
            "end": {
                "timestamp": "2020-08-27 10:00:02.3",
                "station": {
                    "id": 4,
                    "name": "End Name"
                }
            }
        }
    ]
}
EOF
```

Running the above will result in output similar to the following:

```bash
{"Offsets":{"examples/citi-bike/rides/pivot=00":305},"Etcd":{"cluster_id":14841639068965178418,"member_id":10276657743932975437,"revision":28,"raft_term":2}}
```

In this example, we are ingesting a single document (beginning with `{ "bike_id": 7,...`) into the collection `examples/citi-bike/rides`. You may ingest any number of documents into any number of Flow collections in a single request body, and they will be added in a single transaction. The response `Offsets` includes all of the journals where the data was written, along with the new “head” of the journal. This capability allows applications to read data directly from or cloud storage if desired.

#### REST transactional semantics

Flow Ingester ingests data using a single transaction per REST request. This process can be basically summarized as follows:

* If the HTTP response indicates success, the documents are guaranteed to be written to the brokers and replicated.
* If the HTTP response indicates an error, the transaction isn't committed and no derivations observe any of the documents.

For more details on transactions, see the [Gazette Transactional Append docs](https://gazette.readthedocs.io/en/latest/architecture-transactional-appends.html).&#x20;
