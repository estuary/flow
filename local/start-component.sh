#!/bin/bash

# Starts a single component of Flow for local development. Takes the name of the component as the
# only argument. Argument must be one of: ui, temp-data-plane data-plane-gateway, control-plane, or control-plane-agent.
# This script will expect that the git repositories for all components are checked out in the same
# parent directory as this repo. You can configure a different checkout location for any repo(s) by
# setting an environment varaible named like "$component_repo", where $component is one of the
# components listed above, but with any dashes replaced with underscores.
set -e

BROKER_PORT=8080
CONSUMER_PORT=9000
export BROKER_ADDRESS=http://localhost:$BROKER_PORT
export CONSUMER_ADDRESS=http://localhost:$CONSUMER_PORT

function log() {
    echo -e "$@" 1>&2
}

function bail() {
    log "$@"
    exit 1
}

function must_run() {
    log "Running: " $@
    "$@" || bail "Command failed: '$@', exit code $?"
}

function wait_until_listening() {
    local port="$1"
    local desc="$2"
    log Waiting for $desc to be listening on port $port
    while ! nc -z localhost $port; do
        sleep 1
    done
    log $desc is now listening on port $port
}

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

# The parent directory of this repository, which is used to resolve the paths of all repositories
# unless there's a specific env varaible for it.
ANIMATED_CARNIVAL_PARENT_DIR="$(realpath "$SCRIPT_DIR/../../")"

function project_dir() {
    local project="$1"

    # Look for an environment variable for this specific repo. If `project` is 'animated-carnival',
    # then the expected variable name would be `animated_carnival_repo` (lowercase).
    local env_var_name="${project//-/_}_repo"
    if [[ -n "${!env_var_name}" ]]; then
        echo "${!env_var_name}"
    else
        echo "$ANIMATED_CARNIVAL_PARENT_DIR/$project"
    fi
}

function start_ui() {
    cd "$(project_dir 'ui')"
    must_run npm install
    must_run npm start
}

function start_data_plane() {
    cd "$(project_dir 'flow')"
    must_run make package
    must_run ./.build/package/bin/flowctl temp-data-plane --log.level=info
}

function start_data_plane_gateway() {
    cd "$(project_dir 'data-plane-gateway')"
    wait_until_listening $BROKER_PORT 'Gazette broker'
    wait_until_listening $CONSUMER_PORT 'Flow reactor'
    must_run go run .
}

function start_control_plane() {
    cd "$(project_dir 'animated-carnival')"
    must_run supabase start
}

function start_control_plane_agent() {
    cd "$(project_dir 'animated-carnival')"
    # Start building immediately, since it could take a while
    must_run cargo build -p agent

    # Now wait until the temp-data-plane is running. We need this in order to determine the builds
    # root when the agent starts. If it's not running, the agent will fail immediately.
    wait_until_listening $BROKER_PORT 'Gazette broker'
    wait_until_listening $CONSUMER_PORT 'Flow reactor'

    # Now we're finally ready to run this thing. 
    # Use the resolved flow project directory to set the --bin-dir argument.
    # We're counting on `make package` to have completed successfully at this point, which should be
    # the case if the temp-data-plane is running.
    local flow_bin_dir="$(project_dir 'flow')/.build/package/bin"
    export RUST_LOG=info
    must_run cargo run -p agent -- --bin-dir "$flow_bin_dir"
}

case "$1" in
    ui)
        start_ui
        ;;
    temp-data-plane)
        start_data_plane
        ;;
    data-plane-gateway)
        start_data_plane_gateway
        ;;
    control-plane)
        start_control_plane
        ;;
    control-plane-agent)
        start_control_plane_agent
        ;;
    *)
        bail "Invalid argument: '$1'"
        ;;
esac
