---
sidebar_position: 4
title: Agent Skills
description: Set up Estuary Agent Skills for AI assistants to create captures and materializations, troubleshoot tasks, and manage pipelines with flowctl.
---

# Estuary Agent Skills

Estuary Agent Skills let you create and operate Estuary pipelines from an AI coding assistant. Each skill is a small instruction set that teaches your assistant how to run a specific `flowctl` workflow, such as creating a Postgres capture, materializing into Snowflake, or diagnosing a failing task.

Skills are distributed from the open-source [`estuary/agent-skills`](https://github.com/estuary/agent-skills) repository and follow the open [SKILL.md](https://agentskills.io) standard. They work with Claude Code, Cursor, OpenAI Codex, GitHub Copilot, Gemini CLI, and other compatible tools.

## Prerequisites

Before installing the skills, make sure you have:

- An [Estuary account](https://dashboard.estuary.dev/register).
- The `flowctl` CLI installed and authenticated. See the [flowctl installation guide](./get-started-with-flowctl.md). The `estuary-flowctl-setup` skill can also guide your assistant through installation and authentication on demand.
- A supported AI coding assistant, such as [Claude Code](https://www.anthropic.com/claude-code), [OpenAI Codex](https://openai.com/codex/), or [Cursor](https://cursor.com).

## What's included

Skills are grouped into plugins by workflow area, including operations, captures, derivations, materializations, and schema.

:::info
New skills are added to `agent-skills` over time, so the summaries below may not be exhaustive. For the current list, browse the [`estuary/agent-skills`](https://github.com/estuary/agent-skills) repository, or run `/plugin` in Claude Code and open the **Discover** tab.
:::

### Operations

Manage and troubleshoot running pipelines: check task health, review logs, inspect data volume and throughput, look at publication history, restart connectors, and diagnose SSH tunnel issues. Install this one first — it's useful on every task, regardless of source or destination.

### Captures (sources)

Capture data from databases, APIs, and webhooks into Estuary collections. Dedicated skills cover Postgres, MySQL, MongoDB, and SQL Server CDC, plus HTTP webhook ingestion. A generic skill configures any of the 150+ [source connectors](https://estuary.dev/integrations/) via dynamic schema discovery.

### Derivations (transformations)

Transform, aggregate, and reshape collections in streaming SQL, TypeScript, or Python: filtering and field selection, aggregations like running totals and min/max, joins across collections, array flattening, stateful logic such as balances or approval workflows, and time-based windowing.

### Materializations (destinations)

Stream Estuary collections into downstream databases and warehouses. Dedicated skills cover Postgres, Snowflake, BigQuery, Redshift, and Databricks. A generic skill handles any other destination connector.

### Schema

Shape collection and materialization schemas after the fact: rename or remap fields, control which fields materialize, override column types, redact or hash sensitive fields, set defaults for missing values, and add fields pruned by the schema complexity limit.

## Installation

Installation steps depend on which assistant you use. After installing, restart your assistant if it doesn't pick up the new skills automatically.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>

<TabItem value="claude-code" label="Claude Code">

Claude Code includes a plugin marketplace. Add the Estuary marketplace:

```bash
/plugin marketplace add estuary/agent-skills
```

Then install the plugins you want:

```bash
/plugin install estuary-operations@estuary
/plugin install estuary-captures@estuary
/plugin install estuary-derivations@estuary
/plugin install estuary-materializations@estuary
/plugin install estuary-schema@estuary
```

You can also run `/plugin` and browse skills from the **Discover** tab. Installed skills update automatically when the marketplace refreshes.

To verify, ask Claude: *"What Estuary skills do you have?"* and confirm the relevant skills are listed.

</TabItem>

<TabItem value="codex" label="OpenAI Codex">

Codex reads skills from a `.codex/skills/` directory in your project, or globally from `~/.codex/skills/`. Clone the repo and copy the skills:

```bash
git clone https://github.com/estuary/agent-skills.git
mkdir -p .codex/skills
cp -r agent-skills/skills/* .codex/skills/
```

You can also install everything with the cross-tool [Skills CLI](https://agentskills.io):

```bash
npx skills add estuary/agent-skills
```

Start a new Codex session and ask: *"capture Postgres into Estuary"*. Codex should load the matching skill.

</TabItem>

<TabItem value="cursor" label="Cursor">

Cursor reads skills from a `.cursor/skills/` directory in your workspace. Clone the repo and copy the skills:

```bash
git clone https://github.com/estuary/agent-skills.git
mkdir -p .cursor/skills
cp -r agent-skills/skills/* .cursor/skills/
```

Or use the Skills CLI:

```bash
npx skills add estuary/agent-skills
```

Reload Cursor and ask: *"set up an Estuary capture from MySQL"* to confirm the skills are wired up.

</TabItem>

<TabItem value="other" label="Other tools">

The skills are plain `SKILL.md` files. Any tool that supports the open [SKILL.md](https://agentskills.io) standard can use them. Common directory conventions:

| Tool | Skills directory |
|------|------------------|
| Claude Code | `.claude/skills/` |
| Cursor | `.cursor/skills/` |
| GitHub Copilot / VS Code | `.github/skills/` |
| OpenCode | `.opencode/skills/` |
| Codex | `.codex/skills/` |

Install across any of them with the Skills CLI:

```bash
npx skills add estuary/agent-skills
```

Or clone the [`estuary/agent-skills`](https://github.com/estuary/agent-skills) repo and copy `skills/*` into your tool's skills directory.

</TabItem>

</Tabs>

## Walkthrough: your first pipeline

Once a skill group is installed and `flowctl` is authenticated, you can drive Estuary from natural-language prompts. A typical first run looks like this.

### 1. Confirm flowctl is ready

Ask your assistant:

> "Check that flowctl is installed and I'm authenticated to Estuary."

This invokes the `estuary-flowctl-setup` skill, which checks your CLI version, refreshes your auth token, and points you at the [flowctl installation docs](./get-started-with-flowctl.md) if anything is missing.

### 2. Create a capture

Describe the source:

> "Capture my Postgres database into Estuary. The host is `db.example.com`, database `analytics`, and I want all tables in the `public` schema."

The matching `capture-*-create` skill discovers your schema, drafts a capture spec, guides you through replication slot setup, and publishes the capture with `flowctl`.

### 3. Materialize into a destination

> "Materialize the collections I just captured into Snowflake."

The `materialize-snowflake-create` skill collects your warehouse, account, and role, generates the materialization spec, and publishes it. The same pattern works for BigQuery, Redshift, Databricks, Postgres, or any other destination through `materialize-generic-create`.

### 4. Troubleshoot and operate

When something goes wrong, ask:

> "Why is my materialization to Snowflake failing?"
>
> "Is data flowing through my Postgres capture?"
>
> "Show me what changed on this task in the last week."

These prompts route to the operations skills (`estuary-task-health`, `estuary-task-stats`, `estuary-logs`, `estuary-catalog-history`), which pull data from `flowctl` and summarize the results.

If a skill itself misbehaves, file an [issue on the agent-skills repo](https://github.com/estuary/agent-skills/issues) or reach out in the [Estuary Slack community](https://go.estuary.dev/slack).

## Example prompts

Use the skills for any task you would normally run by hand:

- *"Capture from MySQL and materialize into Redshift."*
- *"Set up a webhook capture for Shopify orders."*
- *"Run a health check on my `acme/postgres-prod` capture."*
- *"My capture is failing with an SSH tunnel error. Help me debug it."*
- *"Restart the materialization to BigQuery."*

For docs-aware Q&A from the same assistants (without the action skills), see [MCP Integration](../features/mcp-integration.md).
