Brokers: A Tutorial Introduction
================================

Gazette brokers serve **journals**, a resource resembling a file. Journals are
append-only: bytes that have previously been written to a journal are immutable and
cannot be changed. Like files, journals may be read from any byte offset, and readers
which have caught up to the next offset to be written (called the **write head**) may
optionally block and have future appended content streamed to them. In this way, read
operations over journals resemble `tail -c ${my_offset} -f` operations over Linux files.

*Unlike* files, journals are frequently written to by distributed systems having many
concurrent writers, and a key function of the broker is to provide global ordering
of how raced append requests are sequenced into a journal. Crucially, writers are
assured that their entire span of appended bytes lands together (commits) in the
journal, or none of it does. Following from this, it's never possible for two appended
spans to interleave. Readers are similarly guaranteed they'll read only committed
journal content, and will never see a partial append that is later rolled back.

Journals are replicated for durability across multiple brokers and availability zones,
and a broker **cluster** is a set of broker processes which collectively balance and
provide fault-tolerant serving of many journals.

First Steps
-----------

For this introduction we'll start a stand-alone broker using its 'demo' mode,
which instructs the broker to use an embedded Etcd store and to create an
`example/journal` for our use. Copy and paste its `export BROKER_ADDRESS`
to your terminal.
```bash
$ go install go.gazette.dev/core/cmd/gazette && ~/go/bin/gazette demo 
INFO[0000] starting demo broker                          buildDate=unknown config="{ ... }" version=development

        The broker is now running in stand-alone demonstration mode, and is ready for clients.
        A journal "example/journal" has also been created. Have fun!

        Broker is listening at:
        export BROKER_ADDRESS=http://127.0.0.1:41789
```

Now let's issue our first append request.
```bash
$ curl -X PUT --data-binary @- ${BROKER_ADDRESS}/example/journal << EOF
{"Msg": "Hello, Gazette!"}
{"Msg": "See you later alligator"}
EOF
```

We can read our written content.
```bash
$ curl ${BROKER_ADDRESS}/example/journal
{"Msg": "Hello, Gazette!"}
{"Msg": "See you later alligator"}
```

Read requests take an offset (which defaults to 0).
```bash
$ curl "${BROKER_ADDRESS}/example/journal?offset=16"
Gazette!"}
{"Msg": "See you later alligator"}
```

Wait a tick, that's _not_ valid JSON. What happened?

Well, journals are *byte oriented*, which means that even though we happened to
write tidy JSON payloads, the brokers see journals as simply a sequence of bytes.
Thus offsets are always byte offsets. A key take-away is that message formatting
and representation is a concern of the client, and not of the broker. The broker
doesn't care if journals contain lines of text, streaming video, binary digits of PI,
`/dev/urandom`, or anything else.

That's not to say that journals aren't eminently suited to JSON, protobuf, or
other delimited formats, however! The rule of thumb is that, so long as clients
produce properly delimited sequences of serialized messages, the journal byte-stream
in its entirety will be a well-formed stream of messages (because individual appends
are atomic, and the broker will never interleave them).

Concurrent Appends
------------------

Let's verify the broker properly handles concurrent appends by issuing a bunch of
raced requests (`&` tells the shell to start each command in the background).
```bash
$ for i in {1..20}
do
  DATA='{"Msg": "Race!", "N": '${i}$'}\n'
	curl -X PUT --data-binary "$DATA" ${BROKER_ADDRESS}/example/journal &
done && wait
[1] 9858
[2] 9859
[3] 9860
[1]   Done                    curl -X PUT --data-binary "$DATA" ${BROKER_ADDRESS}/example/journal
[2]   Done                    curl -X PUT --data-binary "$DATA" ${BROKER_ADDRESS}/example/journal
[4]   Done                    curl -X PUT --data-binary "$DATA" ${BROKER_ADDRESS}/example/journal
```

We expect that our raced messages landed in the journal intact. Let's verify by
piping to `jq`, which will error if it encounters invalid JSON. We definitely see
that our appends were sequenced into the journal in arbitrary order.
```bash
$ curl -s ${BROKER_ADDRESS}/example/journal | jq -c '.'
{"Msg":"Hello, Gazette!"}
{"Msg":"See you later alligator"}
{"Msg":"Race!","N":2}
{"Msg":"Race!","N":1}
{"Msg":"Race!","N":13}
{"Msg":"Race!","N":8}
{"Msg":"Race!","N":7}
{"Msg":"Race!","N":5}
{... etc ...}
```

Of course, this is all running off of a stand-alone broker. How do we ensure this total
ordering in the general case, where we have lots of brokers handling requests from lots
of clients?

Briefly, at any time a given journal has exactly one broker which is coordinating every
append to that journal. The choice of *which* broker is determined via a distributed
assignment algorithm running atop Etcd. Other brokers in the cluster will proxy append
requests to the current primary on the client's behalf.

One implication is that *every* append to a journal must pass through an assigned broker
(and usually multiple such brokers, spanning availability zones, which together make up
the journal's replication peerset). Collectively, a distributed system cannot append to
a journal faster than those brokers can handle, no matter how many other brokers may
exist. Journals are thus the *unit of scaling* for Gazette, and higher write volumes are
accommodated by balancing across larger numbers of journals *as well as* brokers. We'll
talk about how to do this a bit later.

Don't worry, though: journals are still plenty fast. For storage efficiency it's
usually a good idea to have Gazette compress journals on your behalf, and in practice
the bottleneck of appending to a journal tends to be how quickly Snappy or Gzip can run.

Streaming Reads
---------------

We can also use *blocking* reads to have journal content streamed to us as it commits.
Here we use `offset=-1` to tell the broker we want to begin reading from the current
write head. Note that `curl` and `jq` will run until we Ctrl-C them.
```bash
$ curl -sN "${BROKER_ADDRESS}/example/journal?block=true&offset=-1" | jq -c '.'
```

Try appending to the journal. Notice how our `curl` updates with each journal write:
the broker is pushing new content to us over a singled long-lived HTTP response.

Brokers have no notion of subscriptions, consumer queues, or other state aside from 
that which serves an active read stream. It's on readers to track the offset they've
read through and, when their stream must eventually be restarted, to supply that offset
to the broker. While this may appear tedious, it's important for the construction of
correct, stateful readers with exactly-once processing semantics that they "own" their
consumption offsets. Plus, Gazette has client-side facilities to help manage this
tracking for you.

gRPC API
--------

As we've seen, brokers present journals over an HTTP API using familiar GET and PUT
verbs. One callout is that journals are _natively_ presented over a gRPC service, and
what we're actually interacting with here is an HTTP gateway that brokers offer, wrapping
the [gRPC Journal service](../broker/protocol/protocol.proto#L535).

The HTTP gateway is handy for building simple clients or reading journals from a
web browser, but at high volumes in production a native gRPC client should be used
instead (such as the [Gazette Go client](https://godoc.org/github.com/gazette/core/broker/client)).

Gazette also offers a fully-featured tool `gazctl` which can often make quick work of
efficiently integrating legacy or Gazette-unaware applications.

Gazctl: Gazette's Soup-to-Nuts CLI Tool
---------------------------------------

Gazctl is a command-line tool for interacting with a Gazette cluster. Most anything
you can do with Gazette, you can do from gazctl. It's the go-to tool for:

 * Fetching, adding, removing, and updating journals served by the cluster.
 * Integrating non-native applications or batch processing pipelines.
 * Inspecting and administering a Gazette broker or consumer cluster.

Gazctl can be directly `go install`'d. Run it without arguments, or run any sub-command
with the `--help` flag for detailed documentation on the tool's capabilities and usage.
```bash
$ go install go.gazette.dev/core/cmd/gazctl && ~/go/bin/gazctl
```

We'll use gazctl going forward for the rest of this tutorial. Gazctl understands the
`BROKER_ADDRESS` environment variable, or we can create an optional configuration file
at `$HOME/.config/gazette/gazctl.ini`.
```bash
$ mkdir -p ~/.config/gazette/ && cat > ~/.config/gazette/gazctl.ini << EOF
[journals.Broker]
Address = ${BROKER_ADDRESS}
EOF
```

We can append to our journal and stream its content from gazctl.
```bash
$ gazctl journals append -l name=example/journal << EOF
{"Msg": "Hello, Gazctl!"}
EOF
$ gazctl journals read -l name=example/journal --block
{"Msg": "Hello, Gazctl!"}
```

The simple examples in this tutorial belie how powerful and expressive the `read`,
`append`, and other sub-commands really are. Be sure to look over their documentation.

Fragments
---------

As setup for this section, let's use gazctl to write a message with the current date every second.
```bash
$ while true; do sleep 1 && echo '{"Msg": "'$(date)'"}' ; done | \
	gazctl journals append -l name=example/journal --framing=lines
```

Now poke at read internals a bit by enabling debug logging. We see:
* That our `--tail` offset of -1 was resolved to an explicit offset 41172,
* That offsets increment with each chunk of read content, and
* Each chunk references a "fragment" that its offset falls within.
```bash
$ gazctl journals read -l name=example/journal --tail --block --log.level=debug
INFO[0000] read started                                  journal=example/journal offset=0
DEBU[0000] read is ready                                 fragment.Begin=14 fragment.End=41215 fragment.URL= journal=example/journal offset=41172
{"Msg": "Mon 29 Jul 2019 11:34:29 PM EDT"}
DEBU[0001] read is ready                                 fragment.Begin=14 fragment.End=41258 fragment.URL= journal=example/journal offset=41215
{"Msg": "Mon 29 Jul 2019 11:34:30 PM EDT"}
DEBU[0002] read is ready                                 fragment.Begin=14 fragment.End=41301 fragment.URL= journal=example/journal offset=41258
{"Msg": "Mon 29 Jul 2019 11:34:31 PM EDT"}
```

A key property of journals is that, once an offset range of a journal is written,
it can never change. Gazette uses the term **fragment** to describe these byte-ranges of a
journal, formally defined by `(journal-name, begin-offset, end-offset, and SHA1-sum)`. A
constraint of fragments is that their `[begin, end)` byte spans never subdivide a run of
bytes appended by a client. Put differently, fragments contain only whole client appends,
and if those appends each consist of properly delimited messages, then so does the
fragment.

A **fragment file** is a file of raw journal content, written under a naming scheme which
incorporates the fragment definition itself. These files have some interesting properties:
like journals, they're immutable. They're content-addressed. With a little care in their
naming scheme we can ensure that filename order corresponds to relative offset order,
such that a directory listing is itself a sorted index over offsets.

A strategy begins to emerge: BLOB services like Amazon S3, Google Cloud Storage, Azure,
etc are *very* good at inexpensively storing and serving huge sets of files that are
written once and never change. They're familiar, easy to operate, and offer a total
read throughput which scales linearly with the number of files stored. For most
organizations, "read all of my data in S3, all at once" is a perfectly sane thing to do. 

Gazette brokers make good use of BLOB stores by offloading all but the very most recent
content of a journal. Everything else is persisted to a configured store as soon as is
feasible, optionally after compression, and brokers then use file listings to maintain
an index of journal offsets and covering fragment files. A requested read into historical
portions of the journal is satisfied by opening its covering fragment file, seeking to the
desired offset, and proxying data to the client.

But we can do better. A mildly intelligent client can be told of the _existence_ of a
fragment file that satisfies its requested offset, and then go read it directly from
the store. S3, Google Cloud Storage, and Azure Blob Store even support signed URLs, which
enable the broker to authorize the bearer to a specific file in a time-bound manner. The
client need not even posses general access rights to the storage bucket.

This offloading of client reads is an especially powerful property. When you consider
that brokers are responsible for the recording of critical writes in a system--writes
you cannot afford to lose--you *really* don't want those same brokers and disks to also
be serving large-scale replays of historical events. This is a recipe for an outage.
A client supporting read offload, by comparison, need only occasionally ask the broker
where to find the next chunk of data and, once caught up to the near-present, it then
seamlessly transitions to streaming from the broker itself. With read offload even
massive-scale replays contribute negligible load on broker clusters, and can be done
with impunity.

Even better, cloud pricing structures mean that offloaded reads have cost efficiencies:
* They're (nearly) free from *any* zone of their region, imposing no inter-zone transfer cost.
* They throw in effectively unbounded read IOPs capacity. No need to provision & pay for
persistent disks with suitable IOPs to keep reading applications fed.
* They can even perform decompression on your behalf, to save CPU cycles.

Picking up the thread of the tutorial, gazctl has a `fragments` command for listing
fragments of our journal.
```bash
$ gazctl journals fragments -l name=example/journal 
+-----------------+--------+---------+---------------+-----------------+-------------+
|     JOURNAL     | OFFSET | LENGTH  |   PERSISTED   |      SHA1       | COMPRESSION |
+-----------------+--------+---------+---------------+-----------------+-------------+
| example/journal |      0 | 43 B    | 8 minutes ago | 92a7ee0e4be7... | SNAPPY      |
| example/journal |     43 | 2.3 KiB | 7 minutes ago | e3c86a45d870... | SNAPPY      |
| example/journal |   2365 | 2.5 KiB | 6 minutes ago | c06eb3b317c0... | SNAPPY      |
| example/journal |   4902 | 2.5 KiB | 5 minutes ago | 6c651e79c7fe... | SNAPPY      |
| example/journal |   7482 | 2.5 KiB | 4 minutes ago | 1eceb1b39740... | SNAPPY      |
| example/journal |  10062 | 2.5 KiB | 3 minutes ago | 579e03e6202f... | SNAPPY      |
| example/journal |  12599 | 2.5 KiB | 2 minutes ago | f65f0b59f423... | SNAPPY      |
| example/journal |  15179 | 2.5 KiB | 1 minute ago  | 49b43a078397... | SNAPPY      |
| example/journal |  17759 | 2.5 KiB |               | fd560d3b9033... | SNAPPY      |
| example/journal |  20296 | 1.9 KiB |               | 6882ce2d56fd... | SNAPPY      |
+-----------------+--------+---------+---------------+-----------------+-------------+
```

When running in "demo" mode, the broker creates a local `demo-fragment-store` directory
into which fragments are persisted and which we can inspect. In a real deployment a BLOB
store or mounted NAS array would be used instead (and we would also configure for much
larger fragments). Fragments are named by their offsets and SHA1 sum using zero-padding
and hex-encoding, which preserves the relative offset ordering of file names. Notice how
the latest `6882ce` fragment from our above listing doesn't exist yet: it's actively being
appended to by the broker. We see all others have been persisted.
```bash
$ ls -lR demo-fragment-store/
demo-fragment-store/example/journal:
total 36
-rw------- 1 johnny johnny  61 Jul 30 12:42 0000000000000000-000000000000002b-92a7ee0e4be7a03fd1a3224055a9d6b7bbd6125e.sz
-rw------- 1 johnny johnny 339 Jul 30 12:43 000000000000002b-000000000000093d-e3c86a45d87051716caa2b6b5dcc7be77d4e21bb.sz
-rw------- 1 johnny johnny 365 Jul 30 12:44 000000000000093d-0000000000001326-c06eb3b317c0e42696e2dd2bc2e07a589b5c4bf7.sz
-rw------- 1 johnny johnny 370 Jul 30 12:45 0000000000001326-0000000000001d3a-6c651e79c7fe8847c41264e90efaea8c28cacf59.sz
-rw------- 1 johnny johnny 370 Jul 30 12:46 0000000000001d3a-000000000000274e-1eceb1b39740fd0accb1de8d4654fafa2f20db24.sz
-rw------- 1 johnny johnny 365 Jul 30 12:47 000000000000274e-0000000000003137-579e03e6202f1fe7ae7c9eaeaa6342b4cfb1483e.sz
-rw------- 1 johnny johnny 370 Jul 30 12:48 0000000000003137-0000000000003b4b-f65f0b59f423266775e4d8ba075e56adba296b1f.sz
-rw------- 1 johnny johnny 370 Jul 30 12:49 0000000000003b4b-000000000000455f-49b43a0783974daee3ff4265b1e418097de1472a.sz
-rw------- 1 johnny johnny 365 Jul 30 12:50 000000000000455f-0000000000004f48-fd560d3b90331733704959f1c0608b4c7c690537.sz
```

A couple of final comments on fragments:

Gazette can be trivially integrated with existing batch processing workflows through
fragment files. After all, they have no special encoding and hold only raw journal
content. They're written to the BLOB store in a regularized, ascending order prefixed
by journal name. A Map/Reduce, Spark Streaming, or other traditional batch workflow need
only know where to pick up new files. The gazctl `fragments` sub-command can further help
with this; see its documentation for more discussion.

From an architecture perspective, fragments and their stores are at the heart of how
brokers themselves are able to stay ephemeral, disposable, and fast to scale. A broker can
begin serving journal reads as soon as it completes a fragment store file listing. Or a new
broker can be integrated into a journal's replication peerset by having that peerset close
its current fragment and "roll" to a new & empty one at the current write head. No data
migrations are ever required to "catch up" a broker. Nor must we ever wait for a faulted
broker to restart and re-join the peerset, potentially gating new appends until it does: as
soon as a broker has faulted, it's immediately and permanently replaced. The broker's _one_
cardinal responsibility is to ensure that all fragments it previously replicated are
promptly persisted to backing stores. Other than this, they can come and go freely.
Brokers are cattle, not pets.

JournalSpecs
------------

So far we've worked with a single journal, but an active production cluster will often
serve hundreds of journals, thousands, or more. The `list` sub-command is used to list
journals of the cluster and their current assigned brokers. Right now we have just one
journal. Let's fix that. But first, we'll talk about working with journal representations.
```bash
$ gazctl journals list --primary
+-----------------+-------------+
|      NAME       |   PRIMARY   |
+-----------------+-------------+
| example/journal | demo-broker |
+-----------------+-------------+
```

As mentioned, Gazette relies on Etcd for consensus over distributed state of the system,
such as current broker-to-journal assignments and even the set of
[JournalSpecs](../broker/protocol/protocol.proto#L117).
Specs like JournalSpec define the existence and desired behavior of entities in Gazette. If
you come from Kubernetes, this will feel familiar and indeed Gazette uses specs in
analogous ways. We can use gazctl to fetch our single JournalSpec in YAML form.
[examples.journalspace.yaml](../test/examples.journalspace.yaml)
has an extended writeup of the YAML format and individual fields, so we won't cover it here.
```bash
$ gazctl journals list --format yaml
name: example/journal
replication: 1
labels:
- name: app.gazette.dev/message-type
  value: TestMessage
- name: app.gazette.dev/region
  value: local
- name: app.gazette.dev/tag
  value: demo
- name: content-type
  value: application/x-ndjson
fragment:
  length: 131072
  compression_codec: SNAPPY
  stores:
  - file:///
  refresh_interval: 1m0s
  retention: 1h0m0s
  flush_interval: 1m0s
revision: 3
```


Gazctl has an `apply` sub-command for modifying JournalSpecs. Here we modify the above output
to switch from `SNAPPY` to `GZIP` compression.
```bash
$ gazctl journals apply << EOF
name: example/journal
replication: 1
labels:
- name: app.gazette.dev/message-type
  value: TestMessage
- name: app.gazette.dev/region
  value: local
- name: app.gazette.dev/tag
  value: demo
- name: content-type
  value: application/x-ndjson
fragment:
  length: 131073
  compression_codec: GZIP
  stores:
  - file:///
  refresh_interval: 1m0s
  retention: 1h0m0s
  flush_interval: 1m0s
revision: 3
EOF
INFO[0000] successfully applied                          revision=5
```

Gazctl also has an `edit` sub-command which will be familiar to `kubectl` users,
and is convenient shorthand for this common "list, modify, then apply" workflow.
```bash
$ gazctl journals edit -l name=example/journal
```

Finally, let's use `apply` to create some new journals.
```bash
$ gazctl journals apply << EOF
name: foobar/
replication: 1
labels:
- name: content-type
  value: application/x-ndjson
- name: my-label
fragment:
  length: 4096
  compression_codec: GZIP
  stores:
  - file:///
  refresh_interval: 1m0s
  flush_interval: 1m0s
children:
  - name: foobar/part-000
  - name: foobar/part-001
  - name: foobar/part-002
EOF
INFO[0000] successfully applied                          revision=7
```

Our new journals now appear in `list`, assigned to our demo broker.
```bash
$ gazctl journals list --primary
+-----------------+-------------+
|      NAME       |   PRIMARY   |
+-----------------+-------------+
| example/journal | demo-broker |
| foobar/part-000 | demo-broker |
| foobar/part-001 | demo-broker |
| foobar/part-002 | demo-broker |
+-----------------+-------------+
```

Labels and Selectors
--------------------

Since journals are the *unit of scale* for brokers, you'll sometimes want to spread a
collection of like records across many journals. This is commonly called a "topic",
where individual journals serve as partitions of the topic. Indeed, topics and
partitioning are an essential strategy for building highly-scaled systems.

However, you'll find that brokers have no APIs for managing topics. Nor is it a field of
JournalSpecs. We arguably defined a grouping above by using a common `foobar/` prefix,
but this is purely convention: journal names are a flat key-space and the `/` has no
special meaning. In fact, topics have no formal definition _anywhere_ in the Gazette
codebase. What gives?

A key insight is that a topic, and the data which is referred to by that topic, *is really
in the eye of the beholder*. By way of example, we might have a collection of `QueryLog`
events that we want to model as a topic. Suppose these are generated from serving in various
regions, like `us-east-1` or `eu-west-1`. Further suppose we have distinct web and mobile
apps which both generate this event type. It becomes a bit messy to define what the
topic(s) of `QueryLogs` should be. Is it all of them? Segregated by serving region? Or by
whether it came from the web vs mobile app? Both? What about the query sub-type? It's hard
(or impossible!) to define precise topics ahead of time, without perfect knowledge of how
they'll ultimately be used. Fortunately we don't have to.

Gazette uses a concept of **labels** to capture metadata of a journal, such as its message
type, serving region, or anything else, and **selectors** for querying sets of journals by
their labels. If you're familiar with Kubernetes
[Labels and Selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/),
their implementation in Gazette works almost identically. When creating or editing a journal,
best practice is to also populate labels for that journal. The choice of labels and values
is arbitrary and teams can evolve their own meanings over time, but Gazette does
[provide some conventions](../labels/labels.go).

Having done this, it turns out that label selectors become an excellent way to define
"topics" on a ex post facto basis. Each application that consumes `QueryLogs` can define
for itself what dimensions are desired for its use-case, and by crafting an appropriate
selector, then be assured of processing the set of partitions that exist now or in the
future.

Gazette labels have one deviation from the Kubernetes implementation worth calling out,
which is that labels are a multi-map: a label can be repeated with distinct values. A
selector selects on any matched included value, and disallows a match on any excluded
value.

We've actually been using label selectors this whole time via the `-l` flag. Every journal
has two labels which are implicitly defined: `name`, which is the exact journal name, and
`prefix`, which matches any prefix of the journal name that ends in `/`. Let's close out
this tutorial by trying out some examples.
```bash
$ gazctl journals list -l prefix=example/
+-----------------+
|      NAME       |
+-----------------+
| example/journal |
+-----------------+
$ gazctl journals list -l prefix=foobar/
+-----------------+
|      NAME       |
+-----------------+
| foobar/part-000 |
| foobar/part-001 |
| foobar/part-002 |
+-----------------+
$ gazctl journals list -l app.gazette.dev/message-type=TestMessage
+-----------------+
|      NAME       |
+-----------------+
| example/journal |
+-----------------+
$ gazctl journals list -l my-label
+-----------------+
|      NAME       |
+-----------------+
| foobar/part-000 |
| foobar/part-001 |
| foobar/part-002 |
+-----------------+
$ gazctl journals list -l "name in (example/journal, foobar/part-001)"
+-----------------+
|      NAME       |
+-----------------+
| example/journal |
| foobar/part-001 |
+-----------------+
$ gazctl journals list -l "prefix=foobar/, name not in (foobar/part-001)"
+-----------------+
|      NAME       |
+-----------------+
| foobar/part-000 |
| foobar/part-002 |
+-----------------+
```
