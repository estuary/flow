# Ops Catalog

The primary objective of `ops-catalog` is to constantly drive the published set of `ops/` specs to
the desired state. These specs represent the tasks needed to support the Flow reporting
infrastructure for aggregated stats and logs for all tenant catalogs on the platform.

The specs are generated from template files, with the source data being the list of all tenants.
Currently, `ops-catalog` periodically polls the `tenants` table for the complete list of tenants.
In the future it may be optimal to establish a trigger mechanism to run the convergence when a new
tenant is added to the tenants table.

### Multi-Tier Architecture

The reporting system consists of two levels of derivations and a single materialization. The first
level of derivations source data from all tenant logs and stats collections. One level 1 derivation
reads from many of these collections, and computes aggregated documents corresponding to granular
periods of time. Second level derivations source from the level 1 derivations and simply collect all
documents into a single collection, passing through from source to destination unchanged. The single
reporting materialization materializes all of these documents into a single table.

There is currently a single level 2 derivation sourcing from all level 1 derivations. As the number
of tenants grows very large it may become necessary to introduce a third tier of derivations to
allow for more level 2 derivations to exist.

### Local Development and Testing

`ops-catalog` uses a working directory to render templates and publish them using `flowctl`. When
running the local stack, these templates can be inspected by ascertaining the location of the
working directory from `ops-catalog` logs and inspecting the rendered templates directly.

For development and modification templates, a `generate` command is available. This allows an
arbitrary list of tenants to be fed into the template engine as jsonl over stdin, and the rendered
templates will be output to a specified directory. These templates will be generated with the
"tests" flag set to `true` within the template context. The current shape of the templates will
cause Flow test specs to be generated for each provided tenant, and these tests can be run with
`flowctl-go` to verify the correctness of the derivations.

Example command to generate templates with `generate`:
```bash
echo '{"tenant": "tenant1/", "l1_stat_rollup": 0}\n{"tenant": "tenant2/", "l1_stat_rollup": 0}{"tenant": "tenant3/", "l1_stat_rollup": 1}' | cargo run generate --output-dir generated
```

Running tests on the generated templates:
```bash
flowctl-go test --source generated/flow.yaml --directory=generated/test
```