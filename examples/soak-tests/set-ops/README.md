# Set-ops soak test

This is a soak test that's intended to exercise the system end to end with arbitrarily large data
volumes. The choice of using set reductions is significant because it requires that documents are
processed in the right order in order to arrive at a correct result. This is not true for
commutative operations like sum.

The basic gist of running these tests is to first get Flow running the catalog in this directory.
Then you run `setops_test.go`, which runs indefinitely until it detects a failure.
The soak tests require postgres, and don't work with sqlite.

## Running locally with flowctl-go develop

- `docker run --name testpg --rm -it -p 5432:5432 -e POSTGRES_PASSWORD=admin postgres:latest`
- `flowctl-go develop --source examples/local-postgres.flow.yaml`
- `go test -v ./examples/soak-tests/set-ops -streams 20 -ops-per-second -verify-period 15s`


## Running in a local kubernetes environment

- `flowctl-go apply --source examples/local-k8s.flow.yaml`
- `./examples/soak-tests/set-ops/run-k8s.sh`
- `kubectl logs job.batch/soak-test-set-ops -f`

The `run-k8s.sh` script creates a kubernetes Job that runs the test within the cluster.
If you want to increase the load on the system, you can modify the Job to increase the parallelism.
Each Pod will ingest documents and verify the results related to those documents.
