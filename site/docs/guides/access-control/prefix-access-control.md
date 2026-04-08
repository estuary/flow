---
sidebar_position: 2
slug: /guides/prefix-access-control/
---

# Prefix-based access control

Estuary's authorization model is built around **prefixes**. By default, your organization gets a single top-level prefix (e.g. `acmeCo/`) and all users and tasks operate within it with broad access. For organizations that need stronger isolation — between environments, regions, or teams — you can configure access at sub-prefix granularity.

This guide explains the four layers that control access, and walks through three common configurations.

## The four layers

Full isolation between sub-prefixes requires configuring all four layers:

| Layer | What it controls | Configured via |
|---|---|---|
| **User grants** | Which prefixes a user can create, modify, or view tasks in | Web app (Admin > Access Grants > Organization Membership) or CLI |
| **Role grants** | Which collections a task can read from or write to | Web app (Admin > Access Grants > Data Sharing) or CLI |
| **Storage mappings** | Where collection data is physically stored | Web app (Admin > Storage Mappings) |
| **Data plane access** | Which data planes tasks can run on | Estuary support (no self-service UI) |

### A note on grants

User and role grants are **additive** — a more specific grant cannot restrict a broader one. If `acmeCo/` has write access to `acmeCo/`, then a user or task in `acmeCo/staging/` inherits that access regardless of what grants you add at the sub-prefix level. To restrict access, you must **delete** the broader grant and replace it with narrower ones.

Storage mappings and data plane access work differently — they use longest-prefix-match, so sub-prefix entries do take precedence over parent entries for those layers.

---

## Scenario 1: Default (single prefix)

Every new organization starts with this setup. All users and tasks share a single prefix with no isolation.

| Layer | Behaviour | Default configuration |
|---|---|---|
| User grants | All users can create and modify tasks anywhere in the prefix | Users are granted admin to `acmeCo/` |
| Role grants | All tasks can read and write any collection in the prefix | `acmeCo/` → `acmeCo/` (write)<br/>`acmeCo/` → `ops/acmeCo/` (read, for task logs) |
| Storage mappings | All collections stored in the same location | Single mapping at `acmeCo/` |
| Data plane access | All tasks run on the same data plane | Single data plane at `acmeCo/` |

This works well for small teams where isolation is not needed.

---

## Scenario 2: Environment isolation with cross-read access

Two sub-prefixes — `acmeCo/dev/` and `acmeCo/prod/` — where dev tasks can read from prod collections (e.g. for testing pipelines against real data), but dev users cannot create or modify prod tasks.

| Layer | Behaviour | Configuration |
|---|---|---|
| User grants | Dev users can only create or modify tasks in `acmeCo/dev/`; prod users are scoped to `acmeCo/prod/` | Dev team: admin to `acmeCo/dev/`<br/>Prod team: admin to `acmeCo/prod/` |
| Role grants | Dev tasks can read prod collections but cannot write to them; prod tasks are fully isolated from dev | Delete `acmeCo/ → acmeCo/` (write)<br/>Add `acmeCo/dev/ → acmeCo/dev/` (write)<br/>Add `acmeCo/dev/ → acmeCo/prod/` (read)<br/>Add `acmeCo/prod/ → acmeCo/prod/` (write) |
| Storage mappings | Dev and prod collection data is stored separately | Create mappings for `acmeCo/dev/` and `acmeCo/prod/`; parent mapping can be kept as fallback or removed |
| Data plane access | Both environments share the same compute resources | No changes needed if sharing a single data plane — tasks inherit via longest-prefix-match |

---

## Scenario 3: Full isolation between sub-prefixes

Two completely separate sub-prefixes — `acmeCo/EU/` and `acmeCo/US/` — with separate storage and data planes. User A can administer both; User B can only administer `acmeCo/US/`.

| Layer | Behaviour | Configuration |
|---|---|---|
| User grants | User A can administer both regions; User B can only administer US | User A: admin to `acmeCo/EU/` and `acmeCo/US/`<br/>User B: admin to `acmeCo/US/` only |
| Role grants | EU and US tasks cannot access each other's collections | Delete `acmeCo/ → acmeCo/` (write)<br/>Add `acmeCo/EU/ → acmeCo/EU/` (write)<br/>Add `acmeCo/US/ → acmeCo/US/` (write) |
| Storage mappings | Collection data is physically separated by region | Create `acmeCo/EU/` → EU bucket<br/>Create `acmeCo/US/` → US bucket<br/>Remove parent `acmeCo/` mapping to prevent cross-region fallback |
| Data plane access | Tasks run on region-specific compute | Contact Estuary support to create per-sub-prefix data plane grants and remove the parent `acmeCo/` data plane grant |

:::note
Data plane visibility for sub-prefix-only users is currently limited in some parts of the UI due to an ongoing migration. If a sub-prefix user cannot see their assigned data plane, contact Estuary support to verify the grant is in place.
:::

---

## Limitations

- **Private links cannot be restricted to sub-prefixes.** Preventing `acmeCo/prod/` and `acmeCo/dev/` tasks from connecting to the same private database requires separate data planes — there is no per-prefix private link scoping.
- **Data plane grants have no self-service UI** — contact Estuary support to configure them.
