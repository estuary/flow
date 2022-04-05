#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# This script requires that you have an access token from Supabase.
# It's a little awkward to get at the moment, though we can expose it in our dashboard UI.

# Estuary supabase endpoint.
API=https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1
# Estuary public API key (this is okay to share).
APIKEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco

# REST calls require both the public apikey, and a signed user token.
args=(-s -H "apikey: ${APIKEY}" -H "Authorization: Bearer ${TOKEN}")

# Helper which prints new logs of a $token since $offset,
# and returns the next offset to use.
function print_logs() {
    local token=$1
    local offset=$2

    # Select out logs of the |token| from |offset|, and pretty-print them with `jq`.
    # Capture the number of lines which were printed into |delta|.
    local delta=$(
    curl "${args[@]}" "${API}/rpc/view_logs?bearer_token=${token}&offset=${offset}" \
        | jq  -r '.[] | .logged_at[5:19] + "|" + .stream + "> " + .log_line' \
        | tee /dev/stderr | wc --lines
    )

    # Return the next offset to use.
    echo $(($offset + $delta))
}

# Helper which polls a record until its job_status is no longer queued, and then bails if not "success".
# In the meantime it prints new logs available under the record's logs_token.
function poll_while_queued() {
    local thing=$1
    local token=$(curl "${args[@]}" "${thing}&select=logs_token" | jq -r '.[0].logs_token')
    local offset=0

    echo "Waiting for ${thing} (logs ${token})..."

    # Wait until $thing is no longer queued, printing new logs with each iteration.
    while [ ! -z "$(curl "${args[@]}" "${thing}&select=id&job_status->>type=eq.queued" | jq -r '.[]')" ]
    do
        sleep 1
        offset=$(print_logs $token $offset)
    done

    local status=$(curl "${args[@]}" "${thing}&select=job_status->>type" | jq -r '.[0].type')
    [[ "$status" != "success" ]] && echo "${thing} failed with status ${status}" && false

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

# Ensure the user has a session, and fetch their current account ID.
ACCOUNT=$(curl "${args[@]}" ${API}/rpc/auth_uid)
echo "Your account: ${ACCOUNT}"
if [ $ACCOUNT == "null" ];
then
    echo "Your TOKEN appears invalid. Refresh it?"
    exit 1
fi

# Fetch the most-recent connector image for soure-hello-world.
CONNECTOR_TAG=$(curl "${args[@]}" "${API}/connectors?select=connector_tags(id)&connector_tags.order=updated_at.asc&image_name=eq.${CONNECTOR}" | jq '.[0].connector_tags[0].id')
echo "Tagged connector image: ${CONNECTOR_TAG}"

# Create a discovery and grab its id.
DISCOVERY=$(curl "${args[@]}" \
    "${API}/discovers?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"capture_name\":\"${CAPTURE_NAME}\",\"endpoint_config\":${CONNECTOR_CONFIG},\"connector_tag_id\":${CONNECTOR_TAG}}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/discovers?id=eq.${DISCOVERY}"

# Retrieve the discovered specification, and pretty print it.
DISCOVER_SPEC=$(curl "${args[@]}" "${API}/discovers?id=eq.${DISCOVERY}&select=catalog_spec" | jq -r '.[0].catalog_spec')
echo $DISCOVER_SPEC | jq '.'

# Create a draft and grab its id.
DRAFT=$(curl "${args[@]}" \
    "${API}/drafts?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"catalog_spec\":${DISCOVER_SPEC}}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/drafts?id=eq.${DRAFT}"

# View our completed draft.
curl "${args[@]}" "${API}/drafts?id=eq.${DRAFT}&select=created_at,updated_at,job_status" | jq '.'

# Create another draft from a canned catalog specification.
# This one has a failing test, which is reported and causes us to bail out :(
DRAFT=$(curl "${args[@]}" \
    "${API}/drafts?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"catalog_spec\":$(jq -c '.' $(dirname "$0")/test_catalog.json)}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/drafts?id=eq.${DRAFT}"

