---
sidebar_position: 3
---

# Agent Skills

Set up and operate Estuary pipelines through your AI assistant. Agent skills are step-by-step playbooks that give your assistant everything it needs to run the right commands, build the right specs, and explain the gotchas — no docs to stitch together, no flowctl commands to memorize.

Ask your assistant to "capture my Postgres into Snowflake" or "why is this materialization failing?", and the relevant skill guides it through the work.

Skills are distributed from the open-source [`estuary/agent-skills`](https://github.com/estuary/agent-skills) repository and work with [Claude Code, GitHub Copilot, Cursor, OpenAI Codex, Gemini CLI, and 30+ other tools](https://agentskills.io) via the open [SKILL.md](https://agentskills.io) standard.

:::tip
Pair skills with the [MCP integration](./mcp-integration.md) so your assistant's explanations stay grounded in current Estuary documentation while the skills drive the work. See [Using coding agents with Estuary](./using-coding-agents.md) for the full setup.
:::

## What's included

### Captures (sources)

Capture data from databases, APIs, and webhooks into Estuary collections.

| Skill | Description |
|-------|-------------|
| `capture-postgres-create` | PostgreSQL CDC (vanilla, RDS, Aurora, Cloud SQL, Supabase, Neon) |
| `capture-mysql-create` | MySQL CDC via binlog replication (RDS, Aurora, Cloud SQL, Azure) |
| `capture-mongodb-create` | MongoDB CDC (Atlas, DocumentDB, self-hosted) |
| `capture-sqlserver-create` | SQL Server CDC (RDS, Azure SQL, Cloud SQL) |
| `capture-http-ingest-create` | HTTP webhook capture (GitHub, Shopify, Stripe, or any JSON source) |
| `capture-generic-create` | Any of 148+ source connectors via dynamic schema discovery |

### Materializations (destinations)

Stream Estuary collections into downstream databases and warehouses.

| Skill | Description |
|-------|-------------|
| `materialize-postgres-create` | PostgreSQL destination |
| `materialize-snowflake-create` | Snowflake destination (JWT auth) |
| `materialize-bigquery-create` | BigQuery destination (GCS staging) |
| `materialize-redshift-create` | Amazon Redshift destination (S3 staging) |
| `materialize-databricks-create` | Databricks destination (Unity Catalog) |
| `materialize-generic-create` | Any destination connector via dynamic schema discovery |

### Operations

Manage and troubleshoot running pipelines.

| Skill | Description |
|-------|-------------|
| `estuary-flowctl-setup` | Install, authenticate, and update the flowctl CLI |
| `estuary-task-health` | End-to-end health check for a task: status, data flow, errors, and history |
| `estuary-catalog-status` | Check control-plane status of a task with `flowctl catalog status` |
| `estuary-task-stats` | Inspect data volume, document counts, and hourly throughput for a task |
| `estuary-catalog-history` | Review publication history and recent spec changes |
| `estuary-logs` | Search and analyze task logs with jq filtering |
| `estuary-connector-restart` | Pause and restart connectors via shard management |
| `estuary-ssh-tunnels` | Diagnose and fix SSH tunnel connection issues |

## Prerequisites

- An [Estuary account](https://dashboard.estuary.dev/register)
- The [flowctl](../guides/get-started-with-flowctl.md) CLI — the `estuary-flowctl-setup` skill walks you through installation and authentication

## Installation

### Skills CLI

Install all skills at once, in any supported tool:

```bash
npx skills add estuary/agent-skills
```

### Claude Code

Add the Estuary marketplace:

```bash
/plugin marketplace add estuary/agent-skills
```

Then install by group:

```bash
/plugin install estuary-captures@estuary
/plugin install estuary-materializations@estuary
/plugin install estuary-operations@estuary
```

Or run `/plugin` to browse from the Discover tab. Installed skills auto-update when the marketplace refreshes.

### Manual installation

Clone the repository and copy the skill folders into your AI tool's skill directory:

```bash
git clone https://github.com/estuary/agent-skills.git
cp -r agent-skills/skills/* your-project/.claude/skills/
```

Common paths by tool:

| Tool | Path |
|------|------|
| Claude Code | `.claude/skills/` |
| Cursor | `.cursor/skills/` |
| GitHub Copilot / VS Code | `.github/skills/` |
| OpenCode | `.opencode/skills/` |
| Codex | `.codex/skills/` |

## Usage

Once installed, ask your AI assistant in plain English:

> "Capture my Postgres database into Estuary."
>
> "Materialize my collections into Snowflake."
>
> "Capture from MySQL and materialize into Redshift."
>
> "Why is my materialization failing?"

Your assistant picks the matching skill and walks through the commands, spec, and common pitfalls.

## Related pages

- [Using coding agents with Estuary](./using-coding-agents.md) — end-to-end setup for flowctl, MCP, and skills
- [MCP integration](./mcp-integration.md) — connect Estuary's documentation to your assistant
- [flowctl setup](../guides/get-started-with-flowctl.md) — install and authenticate the CLI
