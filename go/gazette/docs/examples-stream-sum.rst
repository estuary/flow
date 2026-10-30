Summing over Multiplexed File Chunks
====================================

This example application implements a SHA-summing consumer which incrementally
sums file "chunks" and publishes their final sum. It's implemented using
the Gazette consumer_ framework.

The complete implementation can be found in package stream-sum_.

.. _consumer: https://godoc.org/go.gazette.dev/core/consumer
.. _stream-sum: https://godoc.org/go.gazette.dev/core/examples/stream-sum

Objectives
----------

A number of "chunker" jobs will randomly create and publish "chunks" of number of files.
Each chunker will publish incremental chunks of many simultaneous files. A "summer"
consumer must accumulate the running SHA-sum of each file and, when the file is
completed, it must emit a final SHA-sum of its content. Each chunker job will
independently verify that a final and correct SHA-sum is published in a timely fashion.

This is a fully self-contained example, and also serves as a *soak test* for Gazette.
Chunkers and summers run continuously, and each verifies expected guarantees of
Gazette brokers and consumers: that all messages are delivered exactly one time,
within a bounded SLA. If any of these guarantees are violated, the summer or chunker
process will crash with an error.

These guarantees are further tested by a suite of `crash tests`_ which crash and partition
components of the application, like Etcd, gazette brokers, and consumer processes.

.. _`crash tests`:  https://github.com/gazette/core/blob/master/kustomize/test/bases/crash-tester

Deploy the Example to Kubernetes
--------------------------------

Deploy the example to the ``stream-sum`` namespace of a test Kubernetes cluster,
such as Minikube or Docker for Desktop:

.. code-block:: console

   $ kubectl apply -k  ./kustomize/test/deploy-stream-sum/
   namespace/stream-sum created
   serviceaccount/minio created
   configmap/etcd-scripts-24t872gm88 created
   configmap/example-journal-specs created
   configmap/gazette-zonemap created
   configmap/generate-shards-stream-sum-9k96chk9cg created
   configmap/minio-create-bucket-mm6f469cbt created
   configmap/postgres-init created
   secret/gazette-aws-credentials created
   secret/minio-credentials-fgdm8fkm5m created
   service/consumer-stream-sum created
   service/etcd created
   service/gazette created
   service/minio created
   deployment.apps/consumer-stream-sum created
   deployment.apps/gazette created
   deployment.apps/minio created
   statefulset.apps/etcd created
   job.batch/apply-journal-specs created
   job.batch/apply-shard-specs-stream-sum created
   job.batch/chunker-stream-sum created
   job.batch/minio-create-bucket created
