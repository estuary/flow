#!/bin/bash

# This script can be used to start a complete local Flow instance. This includes:
#
# - ui (via npm start, so it auto-rebuilds)
# - control-plane (supabase, including postgres)
# - agent
# - temp-data-plane (includes etcd, gazette broker, and flow reactor)
# - data-plane-gateway
# - oauth edge function
#
# It does this by launching a tmux session with a window for each of the above components.
# If you're not familiar with tmux, I recommend
# [this intro+cheatsheet](https://www.hostinger.com/tutorials/tmux-beginners-guide-and-cheat-sheet/)
#
# You need to have all of these repositories checked out locally in order for this to work:
# flow, animated-carnival, ui, data-plane-gateway

# This script will start the tmux session and create a window for each component. The starting of
# each component is handled by start-component.sh. That script needs to find where you have those
# repositories checked out. If they're all checked out under their default names and in the same
# parent directory, then everything should "just work". If not, then see start_component.sh for how
# to deal with that.
set -e

SESSION="flow-dev"

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

function log() {
    echo -e "$@" 1>&2
}

function bail() {
    log "$@"
    exit 1
}

# verify requirements here, before we get into the tmux session.
command -v nc || bail "netcat must be installed (nc command was not found)"

function start_component() {
    local component="$1"
    tmux new-window -d -t "$SESSION" -n "$component"
    tmux send-keys -t "=${SESSION}:=${component}" "cd $SCRIPT_DIR" Enter
    tmux send-keys -t "=${SESSION}:=${component}" "./start-component.sh $component" Enter
    log "started: $component"
}

# Prevent running from within an existing tmux session since I honestly don't know what would happen
# and don't have time to figure it out.
if [[ -n "$TMUX" ]]; then
    bail "Cannot call from within an existing tmux session (yet. help appreciated)"
fi

# If there's already an existing session with our session name, then we'll attach to it and then
# exit after the user disconnects from it.
if tmux has -t "$SESSION"; then
    tmux attach-session -t "$SESSION"
    exit 0
fi

tmux new-session -d -s "$SESSION" -n 'terminal'
log "Created new tmux session called '$SESSION'"

# tmux seems to always create a window automatically
tmux send-keys -t "=${SESSION}:=terminal" 'echo Use this terminal for whatever you want' Enter

flow_components=("temp-data-plane" "control-plane" "ui" "control-plane-agent" "data-plane-gateway" "config-encryption" "oauth-edge")
for component in ${flow_components[@]}; do
    start_component "$component"
done

tmux attach-session -t "$SESSION"

