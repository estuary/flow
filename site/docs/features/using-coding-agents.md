---
sidebar_position: 1
---

# Using Coding Agents with Estuary

Build and operate Estuary pipelines from your AI coding assistant — Claude Code, Cursor, GitHub Copilot, OpenAI Codex, Gemini CLI, and others. Instead of stitching together docs and remembering flowctl commands, you can ask in plain English: "capture my Postgres into Snowflake" or "why is this materialization failing?"

Three pieces work together:

1. **[flowctl](../guides/get-started-with-flowctl.md)** — Estuary's CLI. This is what your agent actually runs to discover sources, build specs, publish, and inspect tasks.
2. **[MCP integration](./mcp-integration.md)** — connects your assistant to Estuary's documentation so its answers are backed by current docs, without leaving your editor.
3. **[Agent skills](../guides/agent-skills.md)** — step-by-step playbooks that tell your assistant the exact commands, spec shapes, and gotchas for each connector and operation.

You can use any one of these on its own, but they're most useful together: skills drive flowctl to do the work, and MCP keeps the assistant's explanations accurate.

## Setup

1. **[Install and authenticate flowctl](../guides/get-started-with-flowctl.md)** — the CLI your agent drives.
2. **[Connect the MCP server](./mcp-integration.md)** — so your assistant can answer documentation questions in context.
3. **[Install the agent skills](../guides/agent-skills.md)** — so your assistant knows the right commands and spec shapes for each connector.

## What you can do

Once set up, ask your assistant in plain English. For example:

- "Capture my PostgreSQL database into Estuary."
- "Materialize my collections into Snowflake."
- "Capture from MySQL and materialize into Redshift."
- "Why is my materialization failing?"
- "What's the difference between captures and materializations?"

The skills tell your assistant which flowctl commands to run and how to build the specs; the MCP integration keeps its explanations grounded in Estuary's documentation.
