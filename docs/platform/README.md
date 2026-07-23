# Estuary Platform

Estuary is a real-time data platform. Users interact with a **control plane** to manage a **catalog** of captures, collections, derivations, materializations, and tests, and the platform runs those tasks and serves their data in one or more **data planes**.

This tree is the platform's **porcelain** description — its concepts, operational components, and ubiquitous language, as implemented today. It is organized by concept, not by code; follow the breadcrumbs at each leaf to reach the crates that implement it. Decision history is not here — it lives in git, issues, and specs.

> Maintained by the `platform-docs` skill. Working within a trunk below? Read its `## Glossary` before naming anything.

## Glossary

**Catalog**:
The complete set of captures, collections, derivations, materializations, and tests a user manages. See [catalog/](./catalog/).

**Spec**:
The declarative (JSON/YAML) model of a collection or task. See [catalog/](./catalog/) for the model spec versus the built spec.

**Task**:
A capture, derivation, or materialization — a process the platform runs. See [tasks/](./tasks/).
_Avoid_: job, pipeline

**Collection**:
An append-only set of JSON documents with an enforced schema. See [collections/](./collections/).
_Avoid_: table, topic, stream

**Control plane**:
The user-facing surface for managing the catalog. See [control-plane/](./control-plane/).

**Data plane**:
The runtime surface that executes tasks and serves collection data. See [data-plane/](./data-plane/).

**Connector**:
An OCI image integrating an external system. See [connectors/](./connectors/).
_Avoid_: driver, plugin, integration

**Tenant**:
A top-level namespace prefix homing an organization. See [namespace/](./namespace/).
_Avoid_: account, org, customer

## Concept map

- [catalog/](./catalog/) — the declarative model users manage and its lifecycle: specs, drafts, publications, built specs, activation.
- [namespace/](./namespace/) — the hierarchical `/`-delimited namespace: prefixes as roles, tenants, grants, and capabilities.
- [collections/](./collections/) — data with enforced JSON schema, the journals that back it, and storage mappings.
- [tasks/](./tasks/) — shared task concepts, then a node each for [captures/](./tasks/captures/), [derivations/](./tasks/derivations/), [materializations/](./tasks/materializations/), and [tests/](./tasks/tests/).
- [connectors/](./connectors/) — the integration surface: OCI images and the capture/materialize protocols run as sidecars.
- [control-plane/](./control-plane/) — the management surface: agent, data-plane controller, and config store.
- [data-plane/](./data-plane/) — the runtime surface: Gazette brokers, reactors, and Etcd.
