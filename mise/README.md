# Development Infrastructure

Flow development uses [mise](https://mise.jdx.dev) as a unified tool manager, environment manager, and task runner. Development happens inside VMs to ensure consistency across developers, CI, and production environments.

## Why VMs?

All developers should use a VM for development:

- **Consistency** - VMs match the CI environment (Ubuntu 24.04 LTS, systemd), eliminating "works on my machine" issues
- **Isolation** - Bootstrap a complete environment without impacting your host machine
- **Security** - Production credentials on your host stay outside development environments

## Quick Start

### 1. Install mise on your host

```bash
curl https://mise.run | sh
```

Add mise to your shell (bash example):
```bash
echo 'eval "$(~/.local/bin/mise activate bash)"' >> ~/.bashrc
source ~/.bashrc
```

### 2. Create a VM

Choose based on your needs:

**[Lima](https://lima-vm.io/) VM** (local, uses host resources):
```bash
# Share a parent directory of your checkout (NOT your home directory)
mise run vm:create-lima tiger ~/work
```

**GCP VM** (remote, more hardware available):
```bash
mise run vm:create-gcp panther
# Optionally specify machine type for more resources:
mise run vm:create-gcp panther --machine-type c4-highcpu-16
```

Lima is good for local-first development on capable hardware. GCP allows you to bring more resources than your laptop may have.

GCP VMs have an idle timer that automatically shuts down the instance when no SSH connection is maintained. The VM will auto-start on your next SSH connection.

### 3. Connect to the VM

**Lima:**
```bash
cd ~/work/path/to/shared/repo
limactl shell tiger

# Or use SSH directly. Note the lima- prefix when using `ssh`.
ssh lima-tiger
```

Note: Lima VMs use `/<vm-name>` as the home directory (e.g., `/tiger`) to avoid ambiguity with the host share, which is typically under `/home/<user>` or `/Users/<user>`.

**GCP:**
```bash
ssh dev-<you>-panther
cd ~/estuary/flow
```

## Inside the VM

### Run a Local Stack

Start the full control plane and a data plane:

```bash
mise run local:stack
```

This starts:
- Supabase (database, auth, edge functions)
- Control plane agent and config-encryption service
- A `local-cluster` data plane with 4 gazette brokers and 1 reactor
- Links the data plane to the control plane

Check service status:
```bash
systemctl --user list-dependencies flow-control-plane.target
systemctl --user list-dependencies flow-plane@local-cluster.target
```

View logs:
```bash
journalctl --user -u flow-control-agent -f
journalctl --user -u flow-gazette@local-cluster-8000 -f
```

### Run CI Tests Locally

Run the full platform test suite (mirrors GitHub Actions):

```bash
mise run ci:platform-test
```

This runs format checks, builds, and all test suites in the same order as CI.

### Stop Everything

```bash
mise run local:stop
```

This stops all services and cleans up generated state. Advanced users can stop individual components via systemd (e.g., `systemctl --user stop flow-plane@local-cluster.target`).

### Deleting VMs

**Lima:**
```bash
limactl stop tiger
limactl delete tiger
```

**GCP:**

Delete the instance in the [GCP Console](https://console.cloud.google.com/compute/instances?project=estuary-theatre), then remove the SSH config:
```bash
rm ~/.ssh/gcp-vms/dev-<you>-panther.config
```

## Host Integration

### Port Forwarding for UI Development

The [Estuary UI](https://github.com/estuary/ui) runs on your host and connects to services in the VM. Forward the required ports:

```bash
# From your HOST (not the VM)
mise run vm:port-forward lima-tiger         # Lima VM (note: SSH uses lima- prefix)
mise run vm:port-forward dev-<you>-panther  # GCP instance
```

This forwards:
- Supabase (PostgREST, Postgres, Studio, Mailpit) on ports 5431-5434
- Control plane agent (8675) and config-encryption (8765)
- Data plane brokers (8000-8003) and reactors (8098-8099)
- Cockpit system UI (9090)

Leave the port-forward running while developing with the UI.

### Claude Code in VM

To use Claude Code inside a VM, first copy your access token from the host:

```bash
# From your HOST (not the VM)
mise run vm:claude lima-tiger         # Lima VM (note: SSH uses lima- prefix)
mise run vm:claude dev-<you>-panther  # GCP instance
```

Then inside the VM, run `claude` normally.

This copies your Claude Code access token (not refresh token) into the VM. Since refresh tokens are single-use, refreshes happen on the host - run a fresh session on your host if the VM token expires.


### Cockpit Web UI

VMs are provisioned with the [Cockpit](https://cockpit-project.org/) tool for remote web management.
Cockpit is a general application for system management, and includes monitoring of systemd units
and logs.

Access Cockpit by using the `vm:port-forward` task and connecting to [http://localhost:9090](http://localhost:9090).
The login is your username with password `admin`.

To focus on components of a local stack:
 * Navigate to "Services"
 * Toggle to "User" (vs "System")
 * Enter `flow-` into the filter box.

### SOPS

For development we use an [age](https://github.com/FiloSottile/age) key for
encryption, defined in `mise/tasks/local/reactor`.  To decrypt you will need to
provide the key to sops:
```
export SOPS_AGE_KEY=AGE-SECRET-KEY-1AHW9QTMUTGWDZAC6JDXWC796K0NNDZDKLN8CXPYZM67F2DQVVTHQT3PCD4
```

## IDE Setup

### VS Code with Remote SSH (recommended)

Use VS Code's [Remote - SSH](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-ssh) extension to develop inside the VM. VM provisioning automatically configures VS Code server settings via `bootstrap:ide-settings`, which sets up:

- **rust-analyzer** - Correct environment for RocksDB, jemalloc, cargo target directory
- **Go extension** - Proper GOROOT and CGO flags for cgo builds

To connect:
1. Install the Remote - SSH extension in VS Code
2. Open the command palette and select "Remote-SSH: Connect to Host..."
3. Enter your VM hostname (e.g., `lima-tiger` for Lima, `dev-<you>-panther` for GCP)
4. Open the Flow repository folder

### Vim / Neovim

SSH into the VM and use your preferred terminal editor. The mise environment is automatically activated in your shell, so LSP servers and build tools will have correct paths and flags.

```bash
ssh lima-tiger
cd /tiger/work/flow
nvim .
```

### Zed

Zed editor support is TODO - requires updating `bootstrap:ide-settings` to write Zed configuration.

## Task Reference

List all available tasks:
```bash
mise tasks
```

### Bootstrap Tasks
| Task | Description |
|------|-------------|
| `bootstrap:apt-packages-ci-base` | Install packages matching GitHub Actions runners |
| `bootstrap:apt-packages-ci-extra` | Install additional required packages |
| `bootstrap:ide-settings` | Configure VS Code Remote SSH settings |

### Build Tasks
| Task | Description |
|------|-------------|
| `build:rocksdb` | Compile and install RocksDB static library |
| `build:go-protobufs` | Generate Go protobuf bindings |
| `build:rust-protobufs` | Generate Rust protobuf bindings |
| `build:gazette` | Build gazette binaries |
| `build:flowctl-go` | Build flowctl-go binary |

### Local Stack Tasks
| Task | Description |
|------|-------------|
| `local:stack` | Start full control plane + data plane |
| `local:control-plane` | Start control plane only |
| `local:data-plane <name> <port>` | Start a data plane |
| `local:supabase` | Start Supabase only |
| `local:stop` | Stop all services and clean up |

### CI Tasks
| Task | Description |
|------|-------------|
| `ci:platform-test` | Run full test suite (mirrors CI) |
| `ci:platform-build` | Run full build suite (mirrors CI) |
| `ci:sql-tap` | Run pgTAP SQL tests |
| `ci:nextest-run` | Run Rust tests via nextest |
| `ci:gotest` | Run Go tests |

### VM Tasks
| Task | Description |
|------|-------------|
| `vm:create-lima <name> <share_dir>` | Create a Lima VM |
| `vm:create-gcp <project>` | Create a GCP VM |
| `vm:port-forward <hostname>` | Forward ports from VM to host |
| `vm:claude` | Copy Claude Code token into VM |
