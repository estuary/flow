# examples/

This folder holds a variety of examples and proof-of-concepts using Flow.

## Running All Tests

Flow makes it easy to write tests that verify the end-to-end behaviors of
catalog collections. We recommend that _every_ Flow catalog include tests,
and these examples are no exception:

```console
$ flowctl-go test --source examples/all.flow.yaml
```

You can also directly test catalog sources which are hosted remotely:

```console
$ flowctl-go test --source https://raw.githubusercontent.com/estuary/flow/master/examples/all.flow.yaml
```

## Examples

-   [citi-bike/](citi-bike/) is a comprehensive example using Citi Bike system data.
-   [net-trace/](net-trace/) works with packet data to materialize service status in a network.
-   [re-key/](re-key/) models the problem of marrying events captured _before_ a user sign-up,
    when the user is keyed on a temporary ID, with events _after_ sign-up where a stable user ID is now known.
-   [segment/](segment/) is a comprehensive example drawn from the marketing segmentation domain.
-   [shopping/](shopping/) models a shopping cart and purchase interactions.
-   [stock-stats/](stock-stats/) models per-day market security statistics that update with ticks.
-   [wiki/](wiki/) rolls up Wikipedia page edits. It's based on an old Druid example, that we'll be updating to MediaWiki's live API.

## Reference

-   [derive-patterns/](derive-patterns/) demonstrates common patterns and approaches in building derivations.
-   [reduction-types/](reduction-types/) discusses reduction annotations available in Flow.
