# Control Plane

_Stub — deepen via `/platform-docs` as this concept is built out._

## Glossary

**Control plane**:
The user-facing surface for managing the catalog.

**Agent**:
The service that runs control-plane APIs and background automation.

**Data-plane controller**:
The component that provisions data planes.

## Overview

The control plane is the user-facing management surface. It stores catalog and platform config in Supabase, serves APIs and runs automation through the agent, and provisions data planes via the controller.

## Where this lives

- `crates/agent`, `crates/automations`, `crates/control-plane-api` — APIs and automation
- `crates/data-plane-controller` — data-plane provisioning
- `supabase/` — catalog and platform config store
