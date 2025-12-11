#!/usr/bin/env bash
# Idle shutdown script for GCP development VMs.
# Shuts down after consecutive checks with no active SSH sessions.

set -euo pipefail

# Redirect stderr to syslog to capture any unexpected errors.
LOG_TAG="idle-shutdown"
exec 2> >(logger -t "$LOG_TAG" -p user.err)

# Log on any error with line number and failed command.
trap 'logger -t "$LOG_TAG" -p user.err "ERROR at line $LINENO: $BASH_COMMAND (exit $?)"' ERR

# Configuration (can be overridden via /etc/idle-shutdown.conf)
MIN_CONSECUTIVE_IDLE=${MIN_CONSECUTIVE_IDLE:-3}
MIN_UPTIME_MINUTES=${MIN_UPTIME_MINUTES:-10}
STATE_FILE=${STATE_FILE:-/var/lib/idle-shutdown/idle_count}

log() {
    logger -t "$LOG_TAG" "$1"
}

# Load config file if present.
if [[ -f /etc/idle-shutdown.conf ]]; then
    source /etc/idle-shutdown.conf
fi

# Ensure state directory exists.
mkdir -p "$(dirname "$STATE_FILE")"

# Check minimum uptime - don't shutdown during initial setup.
UPTIME_MINUTES=$(awk '{print int($1/60)}' /proc/uptime)
if [[ $UPTIME_MINUTES -lt $MIN_UPTIME_MINUTES ]]; then
    log "Uptime ${UPTIME_MINUTES}m < ${MIN_UPTIME_MINUTES}m minimum; skipping check"
    echo 0 > "$STATE_FILE"
    exit 0
fi

# Check for active SSH sessions via multiple methods.
has_ssh_sessions() {
    if who | grep -qE 'pts/'; then
        return 0
    fi

    # Check for sshd child processes (any connection, regardless of port).
    # The listener process contains "[listener]", connection processes don't.
    # This catches non-PTY connections like VS Code Remote SSH.
    if pgrep -a sshd | grep -qv '\[listener\]'; then
        return 0
    fi

    return 1
}

# Main logic.
if has_ssh_sessions; then
    log "Active SSH session detected; resetting idle counter"
    echo 0 > "$STATE_FILE"
    exit 0
fi

# No SSH sessions - increment idle counter.
IDLE_COUNT=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
IDLE_COUNT=$((IDLE_COUNT + 1))
echo "$IDLE_COUNT" > "$STATE_FILE"

log "No SSH sessions (${IDLE_COUNT}/${MIN_CONSECUTIVE_IDLE})"

# Shutdown if idle long enough.
if [[ $IDLE_COUNT -ge $MIN_CONSECUTIVE_IDLE ]]; then
    log "Reached ${MIN_CONSECUTIVE_IDLE} consecutive idle checks; shutting down"
    /usr/bin/systemctl poweroff
fi
