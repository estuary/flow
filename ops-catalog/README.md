# Control-plane Ops Catalog Template

This is the Flow catalog template that is used to provision new tenants within
the control plane.

Whenever a new tenant is provisioned, the `TENANT` placeholder string is
replaced with the actual tenant, and the specifications of the catalog are then
applied.

To generate a bundled version of the template, suitable for use within the
control-plane agent, run:

```bash
flowctl raw bundle --source ops-catalog/template-local.flow.yaml
```

The template expects that a new partition of table `catalog_stats`, defined in SQL
migration `11_stats.sql`, is explicitly created by the agent prior to the
materialization being applied.
