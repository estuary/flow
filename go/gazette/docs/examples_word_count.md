Serving up a Real-Time Language Model
-------------------------------------

This example application implements a simple language model of N-grams which are
continuously extracted and accumulated from published source documents, all using
the Gazette
[consumer](https://godoc.org/go.gazette.dev/core/consumer) framework.

The complete implementation can be found in package
[go.gazette.dev/core/examples/word-count](https://godoc.org/go.gazette.dev/core/examples/word-count).

Objectives
----------

Our goal is to deploy an application which presents a real-time language model service.
The model we'll use is very basic, as a simple collection of bi-gram counts.

It should offer a gRPC API for publishing new documents to be incorporated into the
language model, and an API for fetching the current counts of a given bi-gram or
prefix. Clients interact with it over its gRPC API -- the fact that it's built as a
Gazette consumer could be considered an implementation detail.

Finally, we'll include a command-line tool `wordcountctl` for interacting with the service.

> This example has a dependency on RocksDB, which means it cannot be directly
> `go install`'d without having appropriate development libraries available.
> Use the `gazette/examples` docker image to run pre-built binaries.

Deploy the Example to Kubernetes
--------------------------------

Deploy the example to the `word-count` namespace of a test Kubernetes cluster,
such as Minikube or Docker for Desktop:

```bash
$ kubectl apply -k  ./kustomize/test/deploy-word-count/
namespace/word-count created
serviceaccount/minio created
configmap/etcd-scripts-24t872gm88 created
configmap/example-journal-specs created
configmap/gazette-zonemap created
configmap/minio-create-bucket-mm6f469cbt created
configmap/postgres-init created
configmap/publish-docs-word-count-gkhd5f8hdf created
configmap/shard-specs-word-count-5dgc7bg848 created
secret/gazette-aws-credentials created
secret/minio-credentials-fgdm8fkm5m created
service/consumer-word-count created
service/etcd created
service/gazette created
service/minio created
deployment.apps/consumer-word-count created
deployment.apps/gazette created
deployment.apps/minio created
statefulset.apps/etcd created
job.batch/apply-journal-specs created
job.batch/apply-shard-specs-word-count created
job.batch/minio-create-bucket created
job.batch/publish-docs-word-count created
```

The Kustomize manifest starts the `consumer-word-count` deployment, its
dependencies, jobs to
[create specifications](../kustomize/bases/example-word-count/shard_specs.yaml),
and a job `publish-docs-word-count` which
[publishes a small set of documents](../kustomize/bases/example-word-count/publish_docs.sh)
to the application.

Query for Counts
----------------

Port-forward to the consumer in a new tab:
```bash
$ kubectl -nword-count port-forward svc/consumer-word-count 32190:8080
```

Then use docker to run `wordcountctl` and query for a specific bi-gram.
The consumer process will proxy the request to the appropriate shard:
```bash
$ docker run --network host --env CONSUMER_ADDRESS=http://localhost:32190 --rm -it gazette/examples wordcountctl query --prefix="the city"
```

We can also query a specific shard for all of its bi-grams having a prefix:
```bash
$ docker run --network host --env CONSUMER_ADDRESS=http://localhost:32190 --rm -it gazette/examples wordcountctl query --prefix="the" --shard=shard-000
```


