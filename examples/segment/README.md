# Example: User Segmentation

A common problem in the marketing domain is user segmentation. Many companies
maintain segments of their users to better understand behavior and address cohorts.
A company may have many segmentation events coming in continuously, each of which
represents an add or remove of a user to a segment.

These granular events must be transformed into current understandings of:

-   What users are in a given segment?
-   For a given user, what segments are they a member of?
-   Did an event actually change the status of a user & segment? Was it a repeated add or remove?

## Generating Data

We've built a synthetic [data generator](generate.go) for this example which can be run as:

```console
$ go run examples/segment/generate.go | head -n 100
{"event":"49ee9dde-e31d-d67c-1eae-fb28527794f6","timestamp":"2021-03-19T13:32:26-04:00","user":"usr-01a268","segment":{"vendor":10,"name":"seg-9"}}
{"event":"ad47f7ac-d428-95c3-71fd-4aeab9f0b142","timestamp":"2021-03-19T13:32:26-04:00","user":"usr-008913","segment":{"vendor":11,"name":"seg-2611"}}
{"event":"96fb45e3-238b-3fe5-b383-71f17a906eaa","timestamp":"2021-03-19T13:32:26-04:00","user":"usr-02ac91","segment":{"vendor":2,"name":"seg-10"},"remove":true}
```

The generator models some of the real-world properties commonly seen in this problem class:

-   Segments tend to be heavily skewed, with a few highly active segments and a long tail of less active ones.
-   Observed users are also skewed, though not usually to the degree of segments, BUT!
-   Some users are bots, not humans, and these are an altogether different and _highly_ skewed distribution (_Not implemented yet_).
-   Events are often restatements of other events -- for example, due to a browser page reload or back / forward navigation.

See [generate.go](generate.go) for details on how it models these properties. It produces newline JSON with a schema defined in [event.schema.yaml](event.schema.yaml).

## Test and Ingest

Collections are implemented and explained in [flow.yaml](flow.yaml).

They're also tested in [tests.flow.yaml](tests.flow.yaml). Flow puts a lot of emphasis on making integrated, contract-based testing of derivations really easy. Let's run `flowctl test` to confirm the behavior of our derivations:

```console
$ flowctl test --source examples/segment/flow.yaml

Running  3  tests...
Segment membership updates with segmentation events: PASSED
Segment toggles reflect events that are novel updates of a user / segment status: PASSED
User profiles update with segmentation events: PASSED

Ran 3 tests, 3 passed, 0 failed
```

Now start Flow in development mode:

```console
$ flowctl develop --source examples/segment/flow.yaml --port 8080
```

In another tab, begin ingesting generated segment events into Flow.
[`pv`](https://packages.debian.org/sid/utils/pv) is a tool for rate-limiting,
and [`websocat`](https://github.com/vi/websocat) is a CLI tool for talking to
Flow's WebSocket ingestion API:

```console
$ go run examples/segment/generate.go | \
    pv --rate-limit 1000 --line-mode --quiet | \
    websocat --protocol json/v1 ws://localhost:8080/ingest/examples/segment/events
```

## Laking of segment events

Segment events are written into collection `examples/segment/events` and, from there,
persisted to the configured fragment store -- which for these examples is a local
file system stand-in for cloud storage. Within the created `flowctl-develop` runtime
directory you'll see fragments being written every minute or so:

```console
$ find flowctl-develop/fragments/examples/segment/events -type f
flowctl-develop/fragments/examples/segment/events/vendor=1/pivot=00/utc_date=2021-03-19/utc_hour=19/0000000000000000-00000000000000cb-b4e33489b66bcca2aa02a3465e427cee73522d21.gz
flowctl-develop/fragments/examples/segment/events/vendor=2/pivot=00/utc_date=2021-03-19/utc_hour=19/0000000000000000-00000000000000cc-fd1d8f293c2aa161ec08242b789c14b1d589baf0.gz
flowctl-develop/fragments/examples/segment/events/vendor=3/pivot=00/utc_date=2021-03-19/utc_hour=19/0000000000000000-00000000000000cb-3e26c095227bd0ce15169aea62724f7c4e7f7423.gz
flowctl-develop/fragments/examples/segment/events/vendor=4/pivot=00/utc_date=2021-03-19/utc_hour=19/0000000000000000-00000000000000cd-2dab2dcb76e001e85610ef98a61a44a83e11a06e.gz
```

The listing reflects the collection's logical partitioning on `/vendor`, as well
as additional Hive-format partition labels Flow uses to organize the data lake
and support push-down query predicates. Fragment files hold raw events as
newline JSON:

```json
{
    "_meta": {
        "uuid": "d2bc4fb3-88ec-11eb-b001-739cc92fe335"
    },
    "event": "12b0aa8a-4013-bce8-6459-788b037cb55a",
    "remove": true,
    "segment": {
        "name": "seg-627",
        "vendor": 1
    },
    "timestamp": "2021-03-19T15:53:49-04:00",
    "user": "usr-021b42"
}
```

## User Profiles

We can range-read over user profiles from our SQLite key/value store stand-in:

```console
$ sqlite3 examples/examples.db 'SELECT flow_document FROM segment_profiles limit 5;' |
    jq -c '{user: .user, segments: [.segments[] | select (.member) | .segment.name ] }'
{"user":"usr-000000","segments":["seg-0","seg-11B1","seg-178F","seg-3","seg-55","seg-65","seg-7E"]}
{"user":"usr-000001","segments":["seg-0","seg-111","seg-19","seg-275","seg-2A","seg-3","seg-331","seg-35E","seg-8","seg-B","seg-F8E"]}
{"user":"usr-000002","segments":["seg-12","seg-17","seg-1D","seg-1F7C","seg-39","seg-3B","seg-3D9","seg-4D","seg-689","seg-691","seg-6E","seg-9DF"]}
{"user":"usr-000004","segments":["seg-0","seg-4A2","seg-6","seg-8B"]}
{"user":"usr-000006","segments":["seg-106","seg-15","seg-1D8","seg-22","seg-2A","seg-55","seg-8","seg-C5C"]}
```

Or do a point lookup of a specific user:

```console
$ sqlite3 examples/examples.db 'SELECT flow_document FROM segment_profiles WHERE user = "usr-0003df"' | \
    jq  '.segments'
[
  {
    "first": "2021-03-19T17:46:15-04:00",
    "last": "2021-03-19T18:00:55-04:00",
    "member": true,
    "segment": {
      "name": "seg-0",
      "vendor": 1
    }
  },
  {
    "first": "2021-03-19T17:55:17-04:00",
    "last": "2021-03-19T17:55:42-04:00",
    "member": true,
    "segment": {
      "name": "seg-1",
      "vendor": 2
    }
  },
  {
    "last": "2021-03-19T18:06:27-04:00",
    "member": false,
    "segment": {
      "name": "seg-116",
      "vendor": 9
    }
  }
]
```

## Membership Lists

We can range-read over members currently in a segment:

```console
$ sqlite3 examples/examples.db 'SELECT vendor, segment_name, user, first, last FROM segment_memberships WHERE member LIMIT 10;'
1|seg-0|usr-000000|2021-03-19T15:55:31-04:00|2021-03-19T18:00:00-04:00
1|seg-0|usr-000004|2021-03-19T15:53:58-04:00|2021-03-19T18:04:26-04:00
1|seg-0|usr-000010|2021-03-19T15:54:51-04:00|2021-03-19T17:23:19-04:00
1|seg-0|usr-000012|2021-03-19T15:56:34-04:00|2021-03-19T17:25:02-04:00
1|seg-0|usr-000013|2021-03-19T15:54:34-04:00|2021-03-19T17:45:07-04:00
1|seg-0|usr-000026|2021-03-19T15:56:31-04:00|2021-03-19T17:59:46-04:00
1|seg-0|usr-000028|2021-03-19T15:55:35-04:00|2021-03-19T17:47:33-04:00
1|seg-0|usr-00002e|2021-03-19T15:54:46-04:00|2021-03-19T17:23:14-04:00
1|seg-0|usr-000033|2021-03-19T17:25:37-04:00|2021-03-19T18:00:08-04:00
1|seg-0|usr-000036|2021-03-19T15:55:01-04:00|2021-03-19T17:58:35-04:00
```

## Interlude: to "pull" or "push"?

The profiles and membership views we've used so far are "pull" use cases.
They materialize into a store (SQLite here, but as easily another stateful
SQL or key/value DB) which provides storage for fully-reduced document
instances, and which is queried on demand.

As such, the Flow derivations are largely stateless: during their processing
transactions they roll up source documents in-memory, and at transaction close
they commit combined documents to collection journals. Each combined document
precisely reflects only the source documents read during the transaction.
Aside from transaction checkpoints, there's no other state Flow maintains.
Only by _materializing_ the derivation are all of these partial roll-ups
reduced into complete documents.

The other major class of use case is "push", where there's a down-stream
action I want to take driven by a signal I've gleamed from source data.
For example, perhaps I'll send an email or SMS the first time a user is added to
a particular segment, and I'll do that by sending a Webhook or writing to a
pub/sub topic -- types of materializations which are state*less*, since
Flow can't ask a Webhook for the prior version of a document.

That's a problem, because it wouldn't do to send an SMS every time the user
reloads their browser. State has to be tracked _somewhere_, and so for this
class of problem Flow provides registers.

## Segment Toggles

This view filters events to those which change the user's status within
a segment: events that transition a user from "added" to "removed" and
vice versa. This behavior is mostly in service of making an interesting
example and reducing data volume enough to be able to follow it.

Stop `flowctl` and `generate.go`, and start
[`demo-webhook-api.js`](../demo-webhook-api.js) in another tab:

```console
$ node examples/demo-webhook-api.js
```

This is a toy NodeJS web server which prints POST bodies to stdout. It also
inserts a configured response delay (e.x. 500ms) to model slow APIs and
back-pressure. Try altering the configured delay: Flow dynamically compensates
by doing more (or less) reduction work per-transaction to match the throughput
characteristics of the API.

Uncomment / enable a materialization of toggles to the webhook in [flow.yaml](flow.yaml),
and then start `flowctl develop` again:

```yaml
materializations:
    - endpoint:
          name: example/webhook-api
      source:
          name: examples/segment/toggles
```

If you had previously let the generator run for a while, after a moment
you'll see a very large batch of documents POST-ed to the API as the new
materialization back-fills over historical data in the collection.
Now start `generate.go` again. You'll see rapid, ongoing output from the
webhook server.

## Cleanup

Stop `flowctl`, remove the `flowctl-develop` runtime directory, and remove
`examples/examples.db` to complete a development session and restore
to a pristine state. Without doing this, Flow will (currently) remember previous
data and applied derivations and materializations, even if they've
since been commented or removed from your catalog sources -- which can be
confusing if you're not expecting it.

## Extras #1: "push" profiles

Some services would prefer to consume a complete set of segments with
each update of a given user, rather than an incremental delta of changes.
The profiles derivation can be updated to do this. See comments in
[flow.yaml](flow.yaml) and [flow.ts](flow.ts) to try it out.

## Extras #2: Turn up the heat.

We've been running 1K events per second, which any reasonable hardware should
be able to comfortably manage. Keep in mind, for every event Flow is:

-   Validating and ingesting via Websocket into collection `examples/segment/events`.
-   Deriving `profiles`, `memberships`, and `toggles`: running TypeScript lambdas,
    combining and reducing outputs, and validating the schema of every read or written
    document along the way.
-   Indexing registers via RocksDB, managing transaction checkpoints, and maintaining recovery
    logs to ensure that transaction semantics are maintained and that registers are durable.
-   Writing and compressing data fragments for `events`, `profiles`, and `memberships`.
-   Performing precise, incremental materializations of the `profiles` and `memberships` SQLite tables.

In a production setting these tasks would be assigned across a fleet of hardware.
`flowctl develop` is essentially the production Flow runtime, shrunk down into a single process.
It's quite early -- we haven't begun performance optimization in earnest -- but on the author's
hardware (AMD Ryzen 9 3900X), Flow currently manages 10K events per second with 5-700% CPU
utilization.
