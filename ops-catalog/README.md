# Control plane ops catalog

This is the Flow catalog that manages the Flow tasks that we use to materialize stats to the control
plane database, so that they are made available via REST endpoints.

Whenever a new tenant signs up, they need to be added to `derivations.flow.yaml`,
`derivations.flow.ts`, and `ops-collections.flow.yaml`. The intent is to automate that as part of new
tenant sign up, but it's not done yet.

`local.flow.yaml` exists to allow testing things locally.

The tables that are materialized into are created as part of the sql migration `10_stats.sql`. They
are not created automatically by the materialization connector so that the migration can define row
level security policies without requiring that the materialization has already been applied.

