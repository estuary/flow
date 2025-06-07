# Migrate

Data plane migration library for Flow.

## What it does

This crate migrates Flow specifications (captures, collections, derivations, materializations) from one data plane to another. Migrations enable load balancing, maintenance operations, scaling, and disaster recovery by seamlessly moving workloads between data plane environments while preserving data consistency.

## How it fits in

The migrate crate bridges Flow's control plane and data planes:

- **Control plane integration**: Reads specification metadata from Supabase/PostgreSQL
- **Data plane operations**: Communicates directly with Gazette brokers and Reactor shards via gRPC
- **Automation framework**: Provides scheduled migration capabilities through the `automations` crate
- **Activation system**: Uses the `activate` crate to manage journal and shard configurations

Migrations are driven by inserts into table `data_plane_migrations`:
A database trigger on table inserts creates and initializes an `internal.tasks` row,
and `MigrationExecutor` (running with the `agent`) performs the migration.

## Essential types

### Core API

- `migrate_data_planes(pg_pool, src_data_plane, tgt_data_plane, catalog_prefix)` - Main migration function that orchestrates the three-phase migration process

### Automation

- `MigrationExecutor` - Implements `automations::Executor` for automated migrations triggered by database events
- `MigrationTaskState` - Tracks migration progress (stores `migration_id`)
- `MigrationTaskMessage::Initialize` - Message to start an automated migration task

### Internal types

- `DataPlane` - Represents a data plane with its clients (`gazette::journal::Client`, `gazette::shard::Client`) and ops collection templates
- `SpecMigration` - Represents a specification being migrated (catalog name, live spec ID, built specification)

## Migration workflow

Migrations follow a three-phase process designed to minimize downtime and ensure data consistency:

### Phase 1: Cordon and Copy
1. Identify specs matching the catalog prefix in the source data plane
2. Apply cordon labels to source shards/journals (redirects future connector-networking traffic to target)
3. Suspend source journals to prevent new writes
4. Copy shard and journal configurations to the target data plane
5. Merge with any existing target configurations (for migration rollbacks)

### Phase 2: Database Update
1. Update `live_specs.data_plane_id` to point specs to the target data plane

### Phase 3: Remove Cordon
1. Remove cordon labels from target data plane resources
2. Fully activate specs in the target environment
