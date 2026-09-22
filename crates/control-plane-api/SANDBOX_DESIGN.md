# Sandbox API

The sandbox API gives users and their agents a personal Linux environment for running commands and working with files, backed by Fly.io Sprites.

## Technical notes:

**Personal workspaces.** Sandboxes belong to and are only reachable by the creating user (or an agent acting with that user's identity).
**Catalog authorization.** Creation requires the `CreateSandbox` capability bit on the sandbox's `catalogName` - included in the `Admin` bundle today. This leaves open the possibilty of sharing a sandbox with multiple users.
**Initialization** GitHub is allowed during flowctl installation, then access is restricted to Estuary services. `sandboxReset` returns the filesystem to this state (and wipes memory and running processes).

## Controversy, seeking scrutiny

These constraints:

- include sandboxes in the public gql api
- support simple clients for now (poll for output, maybe websocket streaming later)
- explicitly exclude command output from the control plane database

...lead to these choices:

**History lives in the sandbox** Command metadata and results are written to the sandbox filesystem where they're executed. Everything disappears on sandbox reset/delete.
**Exec, then poll** The exec mutation returns once the exec has started. The return value includes file paths for stdout and stderr files, allowing the client to poll the `sandboxFileRead` query. Sprites
**Shell-based execution.** Some of the gql queries and mutations execute their own command inside the sprite to satisfy the client request (e.g. sandboxFileRead runs `dd` in the sandbox to read files)

There are native sprite APIs that we're not using

## open questions:

Proxy the sprites websocket API to support interactives sessions
should we allow arbitrary package installation and external endpoints?
