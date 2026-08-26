# examples/

This folder holds a variety of examples and proof-of-concepts using Estuary.

## Using these examples

These examples are written as Data Flow YAML specifications, and demonstrate how to
manage Estuary's data pipelines using files checked into a git repository. You would
typically use `flowctl catalog test` and `flowctl catalog publish` in order to
test and publish YAML specifications. But you would need to change the names
to use a prefix that you have `admin` access to in order to actually run these
yourself.

Note that these examples are tested with `flowctl raw test`, which runs a
catalog's tests entirely locally. That is distinct from `flowctl catalog test`,
which runs tests on the server side and performs authorization checks (those
would fail here, because users don't have access to these prefixes in the Estuary
control-plane). The specs themselves are the same either way.

## Running All Tests

Estuary makes it easy to write tests that verify the end-to-end behaviors of
catalog collections. We recommend that _every_ Estuary catalog include tests,
and these examples are no exception:

```console
$ flowctl raw test --source examples/flow.yaml
```

You can also directly test catalog sources which are hosted remotely:

```console
$ flowctl raw test --source https://raw.githubusercontent.com/estuary/flow/master/examples/all.flow.yaml
```

## Examples

- [bank/](../site/docs/concepts/bank/) is the example from the [Derivations Concepts documentation](https://docs.estuary.dev/concepts/derivations/)
- [citi-bike/](citi-bike/) is a comprehensive example using Citi Bike system data.
- [stock-stats/](stock-stats/) models per-day market security statistics that update with ticks.
- [temp-sensors/](temp-sensors/) shows how to do some basic aggregations, like min, max, and average.

## Reference

- [derive-patterns/](derive-patterns/) demonstrates common patterns and approaches in building derivations.
- [reduction-types/](reduction-types/) discusses reduction annotations available in Estuary.
