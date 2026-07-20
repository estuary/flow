#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) shared GCP dev-VM helpers, not runnable"
#
# Sourced by mise/tasks/vm/create-gcp and mise/tasks/vm/move-gcp via
# `source "$(dirname "$0")/gcp-lib.sh"`. Not executable, so mise does not
# offer it as a task.

# Matches GCP capacity-exhaustion errors and ONLY those. gcloud prints the
# human-readable message rather than the ZONE_RESOURCE_POOL_EXHAUSTED code,
# so we match the documented message variants too:
#   "The zone '...' does not have enough resources available to fulfill
#    the request. Try a different zone, or try again later."
#   "A <type> VM instance ... is currently unavailable in the <zone> zone."
# Callers grep case-insensitively against combined stdout+stderr, and fall
# through to another zone only on a match. Quota errors and not-found errors
# deliberately do NOT match: quota is project-wide (zone-hopping won't help)
# and not-found is a typo (every retry would fail identically).
# shellcheck disable=SC2034 # consumed by sourcing scripts
GCP_CAPACITY_ERROR_RE='ZONE_RESOURCE_POOL_EXHAUSTED|does not have enough resources available to fulfill the request|is currently unavailable in the [a-z0-9-]+ zone'

# gcp_instance_zone <project> <instance>
# Prints the zone basename of every instance with this name, one per line.
# More than one line means the name exists in multiple zones.
gcp_instance_zone() {
    gcloud compute instances list \
        --project="$1" \
        --filter="name=($2)" \
        --format="value(zone.basename())"
}

# gcp_write_ssh_config <project> <instance> <username>
# Writes ~/.ssh/gcp-vms/<instance>.config and ensures ~/.ssh/config includes
# the gcp-vms directory. Idempotent, so move-gcp calls it to heal older,
# zone-pinned configs.
#
# The ProxyCommand discovers the instance's zone at connect time instead of
# baking it in, so the config survives zone moves. %h/%p are ssh tokens
# substituted at connect time; the \$-escapes below keep zone discovery out
# of this (unquoted) heredoc's write-time expansion.
gcp_write_ssh_config() {
    local project="$1"
    local instance="$2"
    local username="$3"

    # Ensure the Include exists, prepending because ssh_config is sensitive
    # to Host/Match directive ordering. -s: a missing ~/.ssh/config is simply
    # "no match". We use a temp file for portability across macOS and Linux.
    if ! grep -qs gcp-vms ~/.ssh/config; then
        local tmp_config
        tmp_config=$(mktemp)
        cat > "${tmp_config}" <<EOF
# Include GCP VM SSH configurations.
Include ~/.ssh/gcp-vms/*.config

EOF
        cat ~/.ssh/config >> "${tmp_config}" 2>/dev/null || true
        mkdir -p ~/.ssh
        mv "${tmp_config}" ~/.ssh/config
    fi

    mkdir -p ~/.ssh/gcp-vms/
    cat > ~/.ssh/gcp-vms/"${instance}".config <<EOF
Host ${instance}
  ControlMaster auto
  ControlPath ~/.ssh/mux-${instance}.sock
  ControlPersist 20m
  ForwardAgent true
  HostName ${instance}
  ServerAliveCountMax 3
  ServerAliveInterval 30
  User ${username}
  UserKnownHostsFile ~/.ssh/gcp-vms/hosts
  StrictHostKeyChecking accept-new
  CheckHostIP no
  ProxyCommand bash -c 'zone=\$(gcloud compute instances list --project=${project} --filter="name=(%h)" --format="value(zone.basename())"); [ -n "\$zone" ] || { echo "instance %h not found in project ${project}" >&2; exit 1; }; gcloud compute instances start %h --project=${project} --zone="\$zone" 2>/dev/null; exec gcloud compute start-iap-tunnel %h %p --listen-on-stdin --project=${project} --zone="\$zone"'
EOF
}

# gcp_forget_host_key <instance>
# Drops the instance's entry from the known-hosts file used by the generated
# SSH configs. Cloud-init regenerates host keys whenever the instance-id
# changes (zone moves, or recreating a VM under a reused name), and the
# configs use strict checking, so the stale key must go before the first
# connection. `accept-new` then records the fresh key.
gcp_forget_host_key() {
    if [ -f ~/.ssh/gcp-vms/hosts ]; then
        ssh-keygen -R "$1" -f ~/.ssh/gcp-vms/hosts >/dev/null 2>&1 || true
    fi
}

# gcp_wait_for_ssh <instance> [max_attempts]
# Polls until an SSH connection to the instance succeeds. Attempts are 5s
# apart; the default 36 gives the VM 3 minutes to boot.
gcp_wait_for_ssh() {
    local instance="$1"
    local max_attempts="${2:-36}"
    local attempt=0

    while [ "${attempt}" -lt "${max_attempts}" ]; do
        attempt=$((attempt + 1))
        echo "Connecting to ${instance} (attempt ${attempt}/${max_attempts})..."

        if ssh -o BatchMode=yes "${instance}" exit; then
            return 0
        fi
        sleep 5
    done

    echo "ERROR: Failed to connect to ${instance} after ${max_attempts} attempts" >&2
    return 1
}
