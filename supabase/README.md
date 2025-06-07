# Supabase Control Plane

This directory contains the Supabase-based control plane for Flow, which manages the catalog, user accounts, billing, and platform configuration.

## Architecture

The control plane consists of:
- **PostgreSQL database**: Core catalog storage and platform state
- **Edge functions**: Serverless functions for OAuth, billing, and alerts
- **Authentication**: User management and JWT-based auth

## Structure

### Database Schema (`migrations/`)
SQL migrations that define the control plane database schema:
- `00_polyfill.sql` - Initial database polyfills
- `20241012000000_compacted.sql` - Main schema compaction from production
- Timestamped migrations for incremental schema changes

Key schemas:
- `public` - Main Flow catalog tables (collections, captures, materializations, etc.)
- `internal` - Internal platform tables
- `auth` - Supabase auth tables

#### Schema Compaction
To produce a new compaction of the production database:

```bash
pg_dump ${DATABASE} \
  --exclude-table=public.flow_checkpoints_v1 \
  --exclude-table=public.flow_materializations_v2 \
  --schema internal \
  --schema public \
  --schema-only \
| grep -v "CREATE SCHEMA public;" \
| grep -v "ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin"
```

### Edge Functions (`functions/`)
TypeScript serverless functions:
- `alerts/` - Email notification system for platform alerts
- `billing/` - Stripe integration for payment processing
- `oauth/` - OAuth2 flows for connector authorization
- `azure-dpc-oauth/` - Azure-specific OAuth for data plane controllers
- `_shared/` - Common utilities and CORS handling

### Tests (`tests/`)
pgTAP-based SQL tests covering:
- Database schemas and constraints
- Business logic functions
- Performance characteristics
- Authentication and authorization

### Configuration Files
- `config.toml` - Supabase local development configuration
- `seed.sql` - Development seed data (users, tenants, grants)
- `oauth_seed.sql` - OAuth provider configurations
- `secret.yaml` - Encrypted secrets for functions
- `supabase-prod-ca-2021.crt` - Production TLS certificate

## Development

### Local Database
A local PostgreSQL instance runs at `postgresql://postgres:postgres@127.0.0.1:5432/postgres`.

Reset and apply migrations:
```bash
supabase db reset
```

### Running Tests
Execute the full SQL test suite:
```bash
./run_sql_tests.sh
```

### Edge Functions
Functions are deployed automatically but can be tested locally with the Supabase CLI.

## Foundational Tables

### Core Catalog Management
- **`live_specs`** - Active catalog specifications (captures, collections, derivations, materializations). The single most important table in the system.
- **`draft_specs`** - Proposed specification changes being developed by users
- **`drafts`** - Change-sets grouping related specification modifications
- **`publications`** - Operations that validate and publish drafts to live specs
- **`publication_specs`** - Historical audit trail of all specification publications

### User Management & Authorization
- **`tenants`** - Top-level organizational units with quotas and billing configuration
- **`user_grants`** - User permissions for catalog prefixes (read, write, admin)
- **`role_grants`** - Role-based authorization model for scalable permission management

### Connector Infrastructure
- **`connectors`** - Registry of available connector images implementing Flow protocols
- **`connector_tags`** - Connector versions with schemas and configuration
- **`discovers`** - User-initiated discovery operations for auto-generating captures

### Data Flow Management
- **`live_spec_flows`** - Directed dependencies between specifications (data lineage)
- **`evolutions`** - Schema evolution operations maintaining backward compatibility
- **`storage_mappings`** - Storage configuration applied to published specifications

### Operations & Monitoring
- **`data_planes`** - Infrastructure configuration and status for task execution
- **`catalog_stats`** - Usage metrics partitioned by time grain (hourly/daily/monthly)
- **`inferred_schemas`** - Auto-detected schemas from observed collection data

## Usage

The control plane DB is accessed by:
- **Agent**: Control plane service that validates and processes catalog changes
- **Data plane controllers**: Infrastructure management services
- **Web UI**: User-facing dashboard at dashboard.estuary.dev, through PostgREST
- **flowctl CLI**: Command-line interface for catalog management, through PostgREST