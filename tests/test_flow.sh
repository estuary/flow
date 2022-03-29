#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

API=http://localhost:3000
args=(-s -H "Authorization: Bearer ${TOKEN}")

# Helper which prints new logs of a $token since $offset,
# and returns the next offset to use.
function print_logs() {
    local token=$1
    local offset=$2

    # Select out logs of the |token| from |offset|, and pretty-print them with `jq`.
    # Capture the number of lines which were printed into |delta|.
    local delta=$(
    curl "${args[@]}" "${API}/rpc/view_logs?bearer_token=${token}&offset=${offset}" \
        | jq  -r '.[] | .logged_at[5:19] + "|" + .stream + "> " + .line' \
        | tee /dev/stderr | wc --lines
    )

    # Return the next offset to use.
    echo $(($offset + $delta))
}

# Helper which polls a record until its state is no longer queued, and then bails if not "success".
# In the meantime it prints new logs available under the record's logs_token.
function poll_while_queued() {
    local thing=$1
    local token=$(curl "${args[@]}" "${thing}&select=logs_token" | jq -r '.[0].logs_token')
    local offset=0

    echo "Waiting for ${thing} (logs ${token})..."

    # Wait until $thing is no longer queued, printing new logs with each iteration.
    while [ ! -z "$(curl "${args[@]}" "${thing}&select=id&state->>type=eq.queued" | jq -r '.[]')" ]
    do
        sleep 1
        offset=$(print_logs $token $offset)
    done

    local state=$(curl "${args[@]}" "${thing}&select=state->>type" | jq -r '.[0].type')
    [[ "$state" != "success" ]] && echo "${thing} failed with state ${state}" && false

    echo "... ${thing} completed sucessfully"
}


# Name of the capture to create.
CAPTURE_NAME=acmeCo/nested/anvils
# Test connector image and configuration to use for discovery.
CONNECTOR=ghcr.io/estuary/source-hello-world
# Configuration of the connector.
CONNECTOR_CONFIG=$(jq -c '.' <<END
{
    "greetings": 100
}
END
)

# Print out our identity, as understood by the API server.
echo "Your resolved identity:"
curl -s "${args[@]}" ${API}/rpc/auth_id | jq

# Ensure the user has a session, and fetch their current account ID.
ACCOUNT=$(curl "${args[@]}" -X POST ${API}/rpc/auth_session | jq '.account_id')
if [ $ACCOUNT == "null" ];
then
    echo "Your TOKEN appears invalid. Refresh it?"
    exit 1
fi

# Fetch the most-recent connector image for soure-hello-world.
CONNECTOR_IMAGE=$(curl "${args[@]}" "${API}/connectors?select=connector_images(id)&connector_images.order=updated_at.asc&image=eq.${CONNECTOR}" | jq '.[0].connector_images[0].id')

# Create a discovery and grab its id.
DISCOVERY=$(curl "${args[@]}" \
    "${API}/discovers?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"account_id\":${ACCOUNT},\"capture_name\":\"${CAPTURE_NAME}\",\"endpoint_config\":${CONNECTOR_CONFIG},\"image_id\":${CONNECTOR_IMAGE}}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/discovers?id=eq.${DISCOVERY}"

# Retrieve the discovered specification, and pretty print it.
DISCOVER_SPEC=$(curl "${args[@]}" "${API}/discovers?id=eq.${DISCOVERY}&select=state->>spec" | jq -r '.[0].spec')
echo $DISCOVER_SPEC | jq '.'

# Create a build and grab its id.
BUILD=$(curl "${args[@]}" \
    "${API}/builds?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"account_id\":${ACCOUNT},\"spec\":${DISCOVER_SPEC}}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/builds?id=eq.${BUILD}"

# View our completed build rolled up with our account
curl "${args[@]}" "${API}/builds?id=eq.${BUILD}&select=created_at,updated_at,state,accounts(id,credentials(verified_email,display_name))" | jq '.'

# Create another build from a canned catalog specification.
# This one has a failing test, which is reported and causes us to bail out :(
BUILD=$(curl "${args[@]}" \
    "${API}/builds?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"account_id\":${ACCOUNT},\"spec\":$(jq -c '.' $(dirname "$0")/test_catalog.json)}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/builds?id=eq.${BUILD}"

