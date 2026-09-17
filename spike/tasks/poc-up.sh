#!/usr/bin/env bash
# POC setup: put the stack's reactor behind the libkrun sandbox switch, and
# publish the catalog whose shards this POC watches.
#
# Six pieces of state are involved and the stack's own teardown knows about
# none of them, which is why this is a script and not a runbook. `poc-down.sh`
# reverses every step; `poc.sh` asserts the result.
#
# Run under mise, with the stack already up:
#   mise run local:stack
#   mise exec -- spike/tasks/poc-up.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/poc-common.sh"

step "agent: raise the connector timeout"
# A cold derive-python Validate is an image pull, a `uv` resolve of pandas and
# pandas-stubs, and a force-installed pyright in strict mode. The agent's proxy
# gives up after 300s between responses (CONNECTOR_TIMEOUT in
# crates/control-plane-api/src/proxy_connectors.rs) and the spike never measured
# that wall time. local:stop removes this drop-in with the rest of the stack's.
mkdir -p "$(dirname "$POC_AGENT_DROPIN")"
cat > "$POC_AGENT_DROPIN" <<'UNIT'
[Service]
Environment=FLOW_CONNECTOR_TIMEOUT=15m
UNIT
systemctl --user daemon-reload
systemctl --user restart "$POC_AGENT_UNIT"
echo "restarted $POC_AGENT_UNIT with FLOW_CONNECTOR_TIMEOUT=15m"

step "control plane: disable the stack's own ops tasks"
poc_ops_disable true

step "control plane: tenant and connector registration"
# Skipped when already provisioned: the onboarding API rejects a user that holds
# any grants, so a second run for alice would fail rather than no-op.
if [ -f "$POC_TENANT_ENV" ]; then
    echo "tenant $POC_TENANT already provisioned ($POC_TENANT_ENV)"
else
    mise run local:test-tenant --tenant "$POC_TENANT" --user alice@example.com
fi
# Required by check_connector_image (publications/specs.rs), which fails the
# publish with "Forbidden connector image" when no `connectors` row matches.
# derive-python needs none: DeriveUsing::Python makes connector_image() None.
"$REPO_DIR/local/install-connector.sh" ghcr.io/estuary/source-hello-world dev

step "reactor: policy and sandbox environment"
sudo install -m 0644 "$SPIKE_DIR/catalog/$POC_POLICY" "$SPIKE_REACTOR_DIR/"
cat > "$POC_SANDBOX_ENV" <<ENV
# Root's podman is the store production's reactor drives, and the launch line
# uses podman-only flags (--mount type=image), so this cannot be docker. The
# reactor runs as root, so there is no sudo wrapper.
DOCKER_CLI=podman
# Every connector this reactor launches, sandboxed or not, lands here.
FLOW_NETWORK=$SPIKE_NET_CONNECTORS
# The switch. Helper image, reactor dir and sizes take spike defaults:
# localhost/flow-sandbox-helper:spike, $SPIKE_REACTOR_DIR,
# 1024 MiB + 256 overhead, 2 vcpus, 4096 MiB scratch.
FLOW_SANDBOX_SPIKE_POLICY=$SPIKE_REACTOR_DIR/$POC_POLICY
ENV
echo "wrote $POC_SANDBOX_ENV"

step "reactor: swap the user unit for a root one"
# The reactor must be root: libkrun creates <id>/sock/init.sock as root and
# connecting to a unix socket needs write permission on it, and the runtime's
# cleanup SIGKILLs the container it launched, which an unprivileged process
# cannot do to a root one. See POC.md section 0.
#
# The unit deliberately drops the stack's two ExecStartPre build steps: cargo
# and go as root would leave root-owned artifacts in $CARGO_TARGET_DIR and
# ~/go/pkg/mod and break every later build as this user. Restart=no keeps a
# failure visible instead of churning containers.
systemctl --user stop "$POC_USER_UNIT" "$POC_SIDECAR_USER_UNIT" || true
systemctl --user mask "$POC_USER_UNIT" "$POC_SIDECAR_USER_UNIT"

# The sidecar moves with the reactor: they share the 0700 shuffle tempdir a V2
# derivation creates, so a uid split kills every derivation. See poc-common.sh.
sudo tee "$POC_SIDECAR_ROOT_UNIT_PATH" >/dev/null <<UNIT
[Unit]
Description=Flow Runtime Sidecar (libkrun sandbox POC, ${POC_DP})
After=network.target

[Service]
Type=simple
TimeoutStartSec=600
Environment=HOME=/root
EnvironmentFile=$POC_SIDECAR_ENV
ExecStart=/bin/sh -c 'exec \${CARGO_TARGET_DIR}/debug/runtime-sidecar'
Restart=no
UNIT

sudo tee "$POC_ROOT_UNIT_PATH" >/dev/null <<UNIT
[Unit]
Description=Flow Reactor (libkrun sandbox POC, ${POC_DP}-${POC_REACTOR_PORT})
After=network.target $POC_SIDECAR_ROOT_UNIT

[Service]
Type=simple
TimeoutStartSec=600
Environment=HOME=/root
EnvironmentFile=$POC_REACTOR_ENV
EnvironmentFile=$POC_SANDBOX_ENV
ExecStart=/bin/sh -c 'exec \${GOBIN}/flowctl-go serve consumer \\
    --consumer.allow-origin \${FLOW_DASHBOARD_ORIGIN} \\
    --consumer.allow-origin http://localhost:\${FLOW_PORT_DASHBOARD}'
Restart=no
UNIT
sudo systemctl daemon-reload
sudo systemctl start "$POC_SIDECAR_ROOT_UNIT" "$POC_ROOT_UNIT"

# Read from the running process, not from `systemctl show -p Environment`, which
# reports only `Environment=` directives and never `EnvironmentFile=` contents:
# it would show HOME and nothing else. This is also the stronger check, since it
# proves the sandbox file was read second and won on FLOW_NETWORK.
reactor_pid="$(sudo systemctl show "$POC_ROOT_UNIT" -p MainPID --value)"
reactor_env="$(sudo sh -c "tr '\0' '\n' < /proc/${reactor_pid}/environ")"
for want in DOCKER_CLI=podman "FLOW_NETWORK=$SPIKE_NET_CONNECTORS" \
    "FLOW_SANDBOX_SPIKE_POLICY=$SPIKE_REACTOR_DIR/$POC_POLICY"; do
    if grep -qxF -- "$want" <<<"$reactor_env"; then
        echo "  $want"
    else
        echo "reactor environment is missing $want" >&2
        exit 1
    fi
done

if ! poc_wait 120 bash -c "ss -ltn 2>/dev/null | grep -q ':${POC_REACTOR_PORT}\b'"; then
    echo "reactor did not listen on ${POC_REACTOR_PORT}; see" >&2
    echo "  sudo journalctl -u $POC_ROOT_UNIT -n 200" >&2
    exit 1
fi
echo "reactor listening on ${POC_REACTOR_PORT} as root"

step "catalog: stage and publish"
# Derived from the spike fixtures rather than edited in place: the fixtures are
# what experiments 1 to 4 ran and stay untouched. Two changes. `acmeCo/events`
# is dropped from the derivation spec because the capture already declares it,
# and the derivation gains a shards block, which hangs off `derive` and not off
# the collection (CollectionDef is deny_unknown_fields).
mkdir -p "$POC_WORK_DIR"
cp "$SPIKE_DIR/catalog/derive-pandas.flow.py" "$POC_WORK_DIR/"
python3 - "$SPIKE_DIR/catalog" "$POC_WORK_DIR" "$POC_CAPTURE" "$POC_DERIVATION" <<'PY'
import pathlib, sys, yaml

src, dst, capture, derivation = (pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]),
                                 sys.argv[3], sys.argv[4])

cap = yaml.safe_load((src / 'capture-hello-world.flow.yaml').read_text())
# Written out explicitly so G6 has something to flip.
cap['captures'][capture]['shards'] = {'disable': False}

der = yaml.safe_load((src / 'derive-pandas.flow.yaml').read_text())
del der['collections']['acmeCo/events']
# enable-runtime-v2 selects the runtime-next path, which is where the switch
# lives. derive-image-tag pins the frozen V1 image experiment 4 ran and the
# module is written against; without it a V2 task resolves `:stable`, which is
# not in root's store and has never been booted as a guest.
der['collections'][derivation]['derive']['shards'] = {
    'flags': {'enable-runtime-v2': 'true', 'derive-image-tag': 'dev'}}

yaml.safe_dump(cap, open(dst / 'capture-hello-world.flow.yaml', 'w'), sort_keys=False)
yaml.safe_dump(der, open(dst / 'derive-pandas.flow.yaml', 'w'), sort_keys=False)
(dst / 'flow.yaml').write_text(
    'import:\n  - capture-hello-world.flow.yaml\n  - derive-pandas.flow.yaml\n')
PY
echo "staged $POC_WORK_DIR"

poc_publish

step "waiting for both guests"
for task in "$POC_CAPTURE" "$POC_DERIVATION"; do
    if poc_wait 300 poc_has_container "$task"; then
        echo "  $task -> $(poc_container "$task")"
    else
        echo "no container for $task after 300s; see" >&2
        echo "  sudo journalctl -u $POC_ROOT_UNIT -f" >&2
        exit 1
    fi
done

echo
echo "poc-up.sh: ok. Now: mise exec -- spike/tasks/poc.sh"
