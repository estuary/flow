# Namespace

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Prefix**:
A `/`-delimited path segment that acts as a role and is the unit of authorization.

**Tenant**:
A top-level prefix homing an organization.
_Avoid_: account, org, customer

**Grant**:
A capability conferred from a user or role to a role.

**Capability**:
The level of access a grant confers over a role.

## Overview

Collections and tasks live in a unified, hierarchical namespace. `/`-delimited prefixes act as roles and are the unit of AuthZ: users hold capabilities to roles, and roles hold capabilities to other roles.

## Where this lives

- `supabase/` — `user_grants` and `role_grants` tables and authorization rules
- `crates/iam-auth`, `crates/tokens` — authentication and capability tokens
