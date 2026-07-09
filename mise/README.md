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

### 1a. Install pre-reqs on your host (MacOS)

Enable Git LFS if you haven't already

```bash
brew install git-lfs
git lfs install
```

Lima/Mise

```bash
brew install lima
brew install mise
```

Add mise to your shell (zsh example):

```bash
echo 'eval "$(mise activate zsh)"' >> ~/.zshrc
source ~/.zshrc
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
- A `local-<stack>-cluster` data plane with 4 gazette brokers and 1 reactor
- Links the data plane to the control plane

Add `--dekaf` to also start the Dekaf Kafka shim (it is off by default). It ends
by printing a stack card; run `mise run local:stack-info` any time for this
stack's full port map, units, and ready-to-paste commands.

Each checkout (the primary clone or a linked git worktree) gets its own fully
isolated **stack** with its own ports, database, and build artifacts, so several
can run at once. Every checkout follows identical rules — there is no
special/canonical stack. The primary clone is stack `flow`; a worktree's name is
its directory basename, and each allocates the lowest free index. See
[`local/README.md`](../local/README.md) for the stack model, the
`base(i) = 10000 + 1000·index` port rule, and the registry. RAM bounds
concurrency to roughly two full stacks per 16 GiB.

Check service status (`mise run local:stack-info` prints the exact names; stack
`flow`, index 0, shown):

```bash
systemctl --user list-dependencies flow-control-plane@flow.target
systemctl --user list-dependencies flow-plane@local-flow-cluster.target
```

View logs:

```bash
journalctl --user -u flow-control-agent@flow -f
journalctl --user -u flow-gazette@local-flow-cluster-10200 -f
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

This stops **this stack's** services and cleans up its generated state, leaving
any other stacks (and the shared unit templates and TLS material) untouched. The
registry slot and build cache are retained across stop/start, so restarting is
cheap. A slot is released only by **deleting its worktree**, after which
`local:stack-prune` reclaims it (automatically run on stack start/stop).
Advanced users can stop individual components via systemd
(e.g., `systemctl --user stop flow-plane@local-flow-cluster.target`).

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
mise run vm:port-forward lima-tiger              # sole stack on the host (no arg)
mise run vm:port-forward dev-<you>-panther       # sole stack on the GCP instance
mise run vm:port-forward dev-<you>-panther myfix # a specific stack named 'myfix'
```

The optional second argument selects which stack to forward; if the host has
exactly one stack you can omit it. The task reads the remote registry to resolve
the stack's index (hence its ports) and forwards in **two modes**:

- **Remap** — the classic laptop ports map to this stack's real ports, for
  fixed-address clients (saved psql/Studio/Mailpit bookmarks, the UI dev-server
  `.env`): laptop `5431→api`, `5432→db`, `5433→studio`, `5434→mailpit`,
  `8675→agent`, `8765→config-enc`. This belongs to one stack at a time.
- **Identity** — this stack's real ports forward to themselves, for
  advertisement-following clients (`flowctl` over `*.flow.localhost`): api, db,
  agent, config-encryption, plane-0 brokers/reactors/sidecar-admin/dekaf, and
  cockpit 9090.

Pass `--no-remap` to forward a second stack alongside (identity sets never
collide). Leave the port-forward running while developing with the UI.

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

- Navigate to "Services"
- Toggle to "User" (vs "System")
- Enter `flow-` into the filter box.

### SOPS

For development we use an [age](https://github.com/FiloSottile/age) key for
encryption, defined in `mise/tasks/local/reactor`. To decrypt you will need to
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

VM provisioning automatically writes Zed settings via `bootstrap:ide-settings`, configuring `gopls` and `rust-analyzer` with the same environment used for VS Code. Use Zed's [Remote Development](https://zed.dev/docs/remote-development) to connect to your VM hostname (e.g., `lima-tiger` or `dev-<you>-panther`) and open the Flow repository.

### JetBrains RustRover

1. In the JetBrains toolbox, click "Local" and select "SSH".
2. Click "New SSH Connection".
3. Enter your VM hostname (e.g., `lima-tiger` for Lima, `dev-<you>-panther` for GCP).
4. Click "Create".
5. Once connected, click "Install a tool".
6. Find "RustRover" and click "Install".

## Task Reference

List all available tasks:

```bash
mise tasks
```

### Bootstrap Tasks

| Task                              | Description                                      |
| --------------------------------- | ------------------------------------------------ |
| `bootstrap:apt-packages-ci-base`  | Install packages matching GitHub Actions runners |
| `bootstrap:apt-packages-ci-extra` | Install additional required packages             |
| `bootstrap:ide-settings`          | Configure VS Code Remote SSH and Zed settings    |

### Build Tasks

| Task                   | Description                                |
| ---------------------- | ------------------------------------------ |
| `build:rocksdb`        | Compile and install RocksDB static library |
| `build:go-protobufs`   | Generate Go protobuf bindings              |
| `build:rust-protobufs` | Generate Rust protobuf bindings            |
| `build:gazette`        | Build gazette binaries                     |
| `build:flowctl-go`     | Build flowctl-go binary                    |

### Local Stack Tasks

| Task                             | Description                                                 |
| -------------------------------- | ----------------------------------------------------------- |
| `local:stack [--dekaf]`          | Start full control plane + data plane (Dekaf opt-in)        |
| `local:stack-env`                | Print this checkout's stack env (index, name, ports, paths) |
| `local:stack-prune`              | Reclaim slots of deleted checkouts (auto-run at stack joints) |
| `local:control-plane`            | Start control plane only                                    |
| `local:data-plane <name> <port>` | Start a data plane                                          |
| `local:data-plane-controller`    | Start data-plane-controller (service + job) in dry-run mode |
| `local:runtime-sidecar`          | Start runtime-v2 sidecar for a local data plane             |
| `local:seed-controller-job`      | Seed a controller job to trigger data plane converge        |
| `local:supabase`                 | Start Supabase only                                         |
| `local:bigtable`                 | Start BigTable emulator only                                |
| `local:stop`                     | Stop this stack's services and clean up its state           |
| `local:dekaf-kafka`              | Start local Kafka for Dekaf testing                         |

### CI Tasks

| Task                | Description                       |
| ------------------- | --------------------------------- |
| `ci:platform-test`  | Run full test suite (mirrors CI)  |
| `ci:platform-build` | Run full build suite (mirrors CI) |
| `ci:sql-tap`        | Run pgTAP SQL tests               |
| `ci:nextest-run`    | Run Rust tests via nextest        |
| `ci:gotest`         | Run Go tests                      |
| `ci:dekaf-e2e`      | Run Dekaf E2E tests               |

### VM Tasks

| Task                                   | Description                                             |
| -------------------------------------- | ------------------------------------------------------- |
| `vm:create-lima <name> <share_dir>`    | Create a Lima VM                                        |
| `vm:create-gcp <project>`              | Create a GCP VM                                         |
| `vm:port-forward <hostname>`           | Forward ports from VM to host                           |
| `vm:claude <vm_name>`                  | Copy Claude Code token into VM                          |
| `vm:copy-gcloud-credentials <vm_name>` | Copy gcloud CLI and credentials for sops/KMS encryption |
| `vm:copy-ssh-credentials <vm_name>`    | Copy SSH credentials for git repository access          |
