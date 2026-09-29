#!/usr/bin/env bash
# One-time host setup: delegate the cpuset and io cgroup controllers (as well
# as systemd's default cpu, memory, and pids) to the invoking user, so that
# runtime-lab runs can set `cpuset.cpus`, `io.max`, and friends.
#
# `setup-cgroups.sh --check` only verifies, changing nothing.
#
# Run as your user (not root); it uses sudo for the privileged steps:
#   1. A drop-in for user@.service makes the delegation durable across reboots
#      and user-manager restarts.
#   2. Controllers are enabled now, down the running user@$UID.service
#      hierarchy, so no logout or user-manager restart is needed.
#   3. The user manager is re-executed (processes keep running), so that it
#      enables the controllers on the scopes it creates for runs.
set -euo pipefail

if [[ $EUID -eq 0 ]]; then
  echo "run this as your user, not root: it uses sudo where needed" >&2
  exit 1
fi
UID_=$(id -u)
CONTROLLERS="cpu cpuset io memory pids"
DROPIN=/etc/systemd/system/user@.service.d/runtime-lab-delegate.conf

# Verify from within a delegated user scope, as runtime-lab runs.
check() {
  local got
  got=$(systemd-run --user --scope --quiet --collect -p Delegate=yes -- \
    bash -c 'cat /sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup)/cgroup.controllers')
  echo "controllers available to runtime-lab scopes: $got"
  for c in $CONTROLLERS; do
    if ! grep -qw "$c" <<< "$got"; then
      echo "controller $c is unavailable: run $0 (without --check)" >&2
      return 1
    fi
  done
}
if [[ "${1:-}" == "--check" ]]; then
  check
  exit $?
fi

sudo mkdir -p "$(dirname $DROPIN)"
printf '[Service]\nDelegate=%s\n' "$CONTROLLERS" | sudo tee $DROPIN >/dev/null
sudo systemctl daemon-reload

enable() {
  local node=$1 want=""
  for c in $CONTROLLERS; do
    grep -qw "$c" "$node/cgroup.controllers" && want="$want +$c"
  done
  [[ -n "$want" ]] && echo $want | sudo tee "$node/cgroup.subtree_control" >/dev/null
}
enable /sys/fs/cgroup
enable /sys/fs/cgroup/user.slice
enable /sys/fs/cgroup/user.slice/user-$UID_.slice
enable /sys/fs/cgroup/user.slice/user-$UID_.slice/user@$UID_.service

systemctl --user daemon-reexec

check
