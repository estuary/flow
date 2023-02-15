#!/bin/bash

# Starts a single component of Flow for local development. Takes the name of the component as the
# only argument. Argument must be one of: ui, temp-data-plane data-plane-gateway, control-plane, or control-plane-agent.
# This script will expect that the git repositories for all components are checked out in the same
# parent directory as this repo. You can configure a different checkout location for any repo(s) by
# setting an environment varaible named like "$component_repo", where $component is one of the
# components listed above, but with any dashes replaced with underscores.
set -e
set -E
set -o pipefail

BROKER_PORT=8080
CONSUMER_PORT=9000
INFERENCE_PORT=9090
export BROKER_ADDRESS=http://localhost:$BROKER_PORT
export CONSUMER_ADDRESS=http://localhost:$CONSUMER_PORT
export INFERENCE_ADDRESS=localhost:$INFERENCE_PORT

# The kms key used by the local config-encryption. All of estuary engineering should have access to this key.
TEST_KMS_KEY=projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/testing

function log() {
    echo -e "$@" 1>&2
}

function bail() {
    log "$@"
    exit 1
}

function must_run() {
    log "Running: " "$@"
    "$@" || bail "Command failed: '$*', exit code $?"
}

function wait_until_listening() {
    local port="$1"
    local desc="$2"
    log Waiting for "$desc" to be listening on port "$port"
    while ! nc -z localhost "$port"; do
        sleep 1
    done
    log "$desc" is now listening on port "$port"
}

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

# The parent directory of this repository, which is used to resolve the paths of all repositories
# unless there's a specific env varaible for it.
REPO_PARENT_DIR="$(realpath "$SCRIPT_DIR/../../")"

function project_dir() {
    local project="$1"

    # Look for an environment variable for this specific repo. If `project` is 'flow',
    # then the expected variable name would be `flow_repo` (lowercase).
    local env_var_name="${project//-/_}_repo"
    if [[ -n "${!env_var_name}" ]]; then
        echo "${!env_var_name}"
    else
        echo "$REPO_PARENT_DIR/$project"
    fi
}

function start_config_encryption() {
    cd "$(project_dir 'config-encryption')"

    # This is part of a hack to allow the oauth edge function to call the config-encryption service locally.
    # The _other_ part of the hack is down in `start_oauth_edge`.
    # This container exists to do nothing other than to attach to the supabase docker network and expose port 8765, which
    # is what config-encryption listens on. The pause container exists for just these kinds of shennanigans.
    # Per: https://stackoverflow.com/a/44739847 the `docker start` will return 0 if the container is already running
    docker start config_encryption_hack_proxy || \
         must_run docker run --rm --name config_encryption_hack_proxy -p 8765 --network supabase_network_flow --detach google/pause:latest
    must_run cargo run -- --gcp-kms "$TEST_KMS_KEY"
}

function start_ui() {
    cd "$(project_dir 'ui')"
    must_run npm install
    must_run npm start
}

function start_data_plane() {
    cd "$(project_dir 'flow')"
    must_run make package
    must_run ./.build/package/bin/flowctl-go temp-data-plane --log.level=info --network=supabase_network_flow
}

function start_data_plane_gateway() {
    cd "$(project_dir 'data-plane-gateway')"

    command -v openssl || bail "This script requires the openssl binary, which was not found on the PATH"
    local cert_path=local-tls-cert.pem
    local key_path=local-tls-private-key.pem

    if [[ ! -f "${cert_path}" ]] || [[ ! -f "${key_path}" ]]; then
        # Just in case only one of the files got deleted
        rm -rf "${cert_path}" "${key_path}"
        openssl req -x509 -nodes -days 365 \
            -subj  "/C=CA/ST=QC/O=Estuary/CN=localhost:28318" \
            -newkey rsa:2048 -keyout "${key_path}" \
            -out "${cert_path}"
    fi

    wait_until_listening $BROKER_PORT 'Gazette broker'
    wait_until_listening $CONSUMER_PORT 'Flow reactor'
    wait_until_listening $INFERENCE_PORT 'Schema inference'
    must_run go run . --tls-certificate "${cert_path}" --tls-private-key "${key_path}" --log.level debug --inference-address "${INFERENCE_ADDRESS}"
}

function start_control_plane() {
    cd "$(project_dir 'flow')"
    must_run supabase start
}

function start_control_plane_agent() {
    local flow_bin_dir="$(project_dir 'flow')/.build/package/bin"

    cd "$(project_dir 'flow')"
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
    export RUST_LOG=info
    must_run cargo run -p agent -- --bin-dir "$flow_bin_dir" --connector-network=supabase_network_flow --dataplane local
}

function start_oauth_edge() {
    cd "$(project_dir 'flow')"
    # We need to do some weird crap to allow the oauth edge function to connect to the config-encryption
    # service running on localhost (outside of docker). The hostname that's used for config-encyrption
    # will be set to the gateway IP of the docker network. A dummy container, which is attached to that network
    # and listening on port 8765, ensures that port 8765 will be exposed on the host at that address.
    # Determine the gateway IP of the supabase docker network:
    local gateway_ip="$(docker network inspect supabase_network_flow -f '{{ (index .IPAM.Config 0).Gateway }}' )"
    # lol I guess this is a way to trim whitespace from a bash variable: https://stackoverflow.com/a/12973694
    gateway_ip="$(echo "$gateway_ip" | xargs echo )"
    if [[ -z "$gateway_ip" ]]; then
        bail "unable to determine docker network gateway ip"
    fi
    # put this file in /var/tmp/ because macs have issues mounting other files into a docker container, which is
    # what I _think_ supabase functions serve is doing?
    echo "CONFIG_ENCRYPTION_URL=http://${gateway_ip}:8765/v1/encrypt-config" > /var/tmp/config-encryption-hack-proxy-addr
    must_run supabase functions serve oauth --env-file /var/tmp/config-encryption-hack-proxy-addr

}

function start_schema_inference() {
    cd "$(project_dir 'flow')"
    # Start building immediately, since it could take a while
    must_run cargo build -p schema-inference

    wait_until_listening $BROKER_PORT 'Gazette broker'
    must_run cargo run -p schema-inference -- serve --broker-url=$BROKER_ADDRESS --port=$INFERENCE_PORT
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
    config-encryption)
        start_config_encryption
        ;;
    oauth-edge)
        start_oauth_edge
        ;;
    schema-inference)
        start_schema_inference
        ;;
    *)
        bail "Invalid argument: '$1'"
        ;;
esac
