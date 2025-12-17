---
sidebar_position: 1
---

# MCP Integration

Connect to Estuary Flow's documentation directly from your IDE or AI tool using the Model Context Protocol (MCP).

## Overview

The Model Context Protocol (MCP) allows AI assistants in your development environment to access Estuary Flow's documentation contextually. This means you can ask questions about Estuary Flow directly in your IDE and get accurate, documentation-backed answers without leaving your workflow.

## Setup

**Server URL:** `https://estuary.mcp.kapa.ai`

:::info Authentication Required
When you first connect to the Estuary MCP server, you'll be prompted to sign in with Google. This is a minimal authentication (OpenID Connect) that doesn't access your email, name, or personal data - it's only used to prevent abuse.
:::

Setup steps vary depending on which AI assistant you're using.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>

<TabItem value="cursor" label="Cursor">

Add the following to your `.cursor/mcp.json` file:

```json
{
  "mcpServers": {
    "estuary": {
      "type": "http",
      "url": "https://estuary.mcp.kapa.ai"
    }
  }
}
```

For more information, see the [Cursor MCP documentation](https://docs.cursor.com/context/model-context-protocol).

</TabItem>

<TabItem value="vscode" label="VS Code">

**Prerequisites:** VS Code 1.102+ with GitHub Copilot enabled.

Create an `mcp.json` file in your workspace `.vscode` folder:

```json
{
  "servers": {
    "estuary": {
      "type": "http",
      "url": "https://estuary.mcp.kapa.ai"
    }
  }
}
```

For more details, see the [VS Code MCP documentation](https://code.visualstudio.com/docs/copilot/customization/mcp-servers).

</TabItem>

<TabItem value="claudecode" label="Claude Code">

Run the following command in your terminal:

```bash
claude mcp add estuary https://estuary.mcp.kapa.ai
```

Then run the `/mcp` command in Claude Code and follow the steps in your browser to authenticate.

For more information, see the [Claude Code MCP documentation](https://docs.anthropic.com/en/docs/claude-code/mcp).

</TabItem>

<TabItem value="chatgpt" label="ChatGPT Desktop">

ChatGPT Desktop supports MCP servers in developer mode:

1. Open ChatGPT Desktop.
2. Go to **Settings** > **Features**.
3. Enable **Developer mode**.
4. Navigate to **Settings** > **MCP Servers**.
5. Click **Add Server** and enter:
   - **Name**: `estuary`
   - **URL**: `https://estuary.mcp.kapa.ai`

For more information, see the [ChatGPT Desktop MCP documentation](https://platform.openai.com/docs/guides/developer-mode).

</TabItem>

<TabItem value="claude" label="Claude Desktop">

Add to your Claude Desktop config file:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "estuary": {
      "type": "http",
      "url": "https://estuary.mcp.kapa.ai"
    }
  }
}
```

Restart Claude Desktop for changes to take effect.

For more details, see the [Claude Desktop documentation](https://support.anthropic.com/en/articles/9487310-desktop-app).

</TabItem>

<TabItem value="other" label="Other">

MCP is an open protocol supported by many clients. Use the server URL `https://estuary.mcp.kapa.ai` and refer to your client's documentation for setup instructions.

Most clients accept the standard MCP JSON configuration format:

```json
{
  "mcpServers": {
    "estuary": {
      "url": "https://estuary.mcp.kapa.ai"
    }
  }
}
```

</TabItem>

</Tabs>

## What you can do

Once connected, you can ask context-aware questions about Estuary Flow from within your editor. For example:

- "How do I set up a PostgreSQL CDC capture?"
- "What's the difference between captures and materializations?"
- "How do I create a derivation in TypeScript?"
- "What connectors are available for data warehouses?"
- "How do I configure schema evolution settings?"

The AI assistant will use Estuary's documentation to provide accurate, up-to-date answers while you code.

