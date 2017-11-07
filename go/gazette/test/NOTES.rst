Notes on setting up and testing via Minikube
============================================

Bootstrap an Etcd cluster::

  helm install stable/etcd-operator --name etcd-operator --set image.tag=v0.6.1

Use the following `etcd-cluster.yaml`::

  apiVersion: "etcd.database.coreos.com/v1beta2"
  kind: "EtcdCluster"
  metadata:
    name: "etcd-cluster"
  spec:
    size: 3
    version: "3.1.8"

Apply via::

  kubectl create -f etcd-cluster.yaml

Start `minio` to provide an S3-compatible cloud filesystem. Use the following values::

  defaultBucket:
    enabled: true
    ## If enabled, must be a string with length > 0
    name: examples
    ## Can be one of none|download|upload|public
    policy: public

Install via::

  helm install stable/minio --name minio -f minio-values.yaml

This will setup the `examples` bucket, but we also need an additional
`recovery-logs` bucket. Port-forward to the minio service, and use the web
UI and the configured access & secret key (typically the defaults of
`AKIAIOSFODNN7EXAMPLE` and `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
to create a `recovery-logs` bucket with R/W access::

  alias minio_pod="kubectl get pod -l release=minio -o jsonpath={.items[0].metadata.name}"
  kubectl port-forward $(minio_pod) 9000:9000
  browse http://localhost:9000

Start a Gazette cluster::

  helm install charts/gazette --name gazette

Start the `word-count` application::

  helm install charts/examples/word-count --name word-count

Determine the Gazette service cluster IP::

  export GAZETTE=$(kubectl get svc gazette-gazette -o jsonpath={.spec.clusterIP}):8081

On first run, create the word-count journals::

  curl -v -X POST http://$GAZETTE/examples/word-count/sentences
  curl -v -X POST http://$GAZETTE/examples/word-count/counts

Begin a long-lived read of output counts::

  curl -L -v "http://$GAZETTE/examples/word-count/counts?block=true&offset=-1"

Load a bunch of data into the input sentences journal. Note that depending on
the broker the request is routed to, you may get a "401 Gone" response, which is
informing you that another broker is currently responsible for the journal. You
may wish to update URL below to the specific broker indicated by the
returned `Location:` header::

  curl -v -X PUT http://$GAZETTE/examples/word-count/sentences --data-binary @a_tale_of_two_citites.txt

