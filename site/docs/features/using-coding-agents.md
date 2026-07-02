---
sidebar_position: 1
---

# Using Coding Agents with Estuary

Build and operate Estuary pipelines from your AI coding assistant — Claude Code, Cursor, GitHub Copilot, OpenAI Codex, Gemini CLI, and others. Instead of stitching together docs and remembering flowctl commands, you can ask in plain English: "capture my Postgres into Snowflake" or "why is this materialization failing?"

Three pieces work together:

1. **[flowctl](../guides/get-started-with-flowctl.md)** — Estuary's CLI. This is what your agent actually runs to discover sources, build specs, publish, and inspect tasks.
2. **[MCP integration](./mcp-integration.md)** — connects your assistant to Estuary's documentation so its answers are backed by current docs, without leaving your editor.
3. **[Agent skills](./agent-skills.md)** — step-by-step playbooks that tell your assistant the exact commands, spec shapes, and gotchas for each connector and operation.

You can use any one of these on its own, but they're most useful together: skills drive flowctl to do the work, and MCP keeps the assistant's explanations accurate.

## Setup

### 1. Install and authenticate flowctl

flowctl is the CLI your agent drives. Install it:

```bash
# Mac (Homebrew)
brew tap estuary/flowctl
brew install flowctl
```

For Linux, direct download, or Windows (WSL), see the [flowctl setup guide](../guides/get-started-with-flowctl.md).

Then authenticate:

```bash
flowctl auth login
```

This opens the Estuary dashboard's CLI-API tab. Copy the access token and paste it back into the terminal. For CI/CD and other non-interactive environments, use a refresh token via `FLOW_AUTH_TOKEN` — see the [flowctl setup guide](../guides/get-started-with-flowctl.md).

### 2. Connect the MCP server

Point your assistant at Estuary's MCP server so it can answer documentation questions in context:

```bash
# Claude Code
claude mcp add --transport http estuary https://estuary.mcp.kapa.ai
```

Setup differs per tool (Cursor, VS Code, ChatGPT Desktop, Claude Desktop, and others). See [MCP integration](./mcp-integration.md) for the full instructions and authentication steps.

### 3. Install the agent skills

Add the [Estuary agent skills](./agent-skills.md) so your assistant knows the right commands and spec shapes for each connector:

```bash
# Skills CLI (works across tools)
npx skills add estuary/agent-skills
```

```bash
# Claude Code (plugin marketplace)
/plugin marketplace add estuary/agent-skills
```

See [Agent skills](./agent-skills.md) for the Claude Code plugin groups, manual installation, and per-tool skill directories.

## What you can do

Once set up, ask your assistant in plain English. For example:

- "Capture my PostgreSQL database into Estuary."
- "Materialize my collections into Snowflake."
- "Capture from MySQL and materialize into Redshift."
- "Why is my materialization failing?"
- "What's the difference between captures and materializations?"

The skills tell your assistant which flowctl commands to run and how to build the specs; the MCP integration keeps its explanations grounded in Estuary's documentation.

## Next steps

- [flowctl setup](../guides/get-started-with-flowctl.md) — install, authenticate, and update the CLI
- [MCP integration](./mcp-integration.md) — connect Estuary's docs to your assistant
- [Agent skills](./agent-skills.md) — the full catalog of capture, materialization, and operations skills
