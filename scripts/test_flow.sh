#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
#set -o xtrace

# Running this script against our production endpoint requires that you have an access token from Supabase.
# It's a little awkward to get at the moment, though we can expose it in our dashboard UI.
#
#API=https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1
# Estuary public API key (this is okay to share).
#APIKEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco

# Configuration for local development:
API=http://localhost:5431/rest/v1
APIKEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs

# The local development secret is 'super-secret-jwt-token-with-at-least-32-characters-long'.
# See: https://github.com/supabase/supabase-js/issues/25#issuecomment-1019935888
#
# This TOKEN is bob@example.com, created using https://jwt.io, and is good for 20 years.
TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoyMjgwMDY3NTAwLCJzdWIiOiIyMjIyMjIyMi0yMjIyLTIyMjItMjIyMi0yMjIyMjIyMjIyMjIiLCJlbWFpbCI6ImJvYkBleGFtcGxlLmNvbSIsInJvbGUiOiJhdXRoZW50aWNhdGVkIn0.7BJJJI17d24Hb7ZImlGYDRBCMDHkqU1ppVTTfqD5l8I

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


function test_discover_then_publish() {
# Name of the capture to create.
CAPTURE_NAME=acmeCo/nested/anvils
# Test connector image and configuration to use for discovery.
CONNECTOR=ghcr.io/estuary/source-test
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

# Create an empty draft.
DRAFT=$(curl "${args[@]}" \
    "${API}/drafts?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{}" \
    | jq -r '.[0].id')

# Fetch the most-recent connector image for soure-test.
CONNECTOR_TAG=$(curl "${args[@]}" "${API}/connectors?select=connector_tags(id)&connector_tags.order=updated_at.asc&image_name=eq.${CONNECTOR}" | jq '.[0].connector_tags[0].id')
echo "Tagged connector image: ${CONNECTOR_TAG}"

# Create a discovery and grab its id.
DISCOVERY=$(curl "${args[@]}" \
    "${API}/discovers?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"capture_name\":\"${CAPTURE_NAME}\",\"endpoint_config\":${CONNECTOR_CONFIG},\"connector_tag_id\":${CONNECTOR_TAG},\"draft_id\":\"${DRAFT}\"}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/discovers?id=eq.${DISCOVERY}"

# Retrieve the discovered specification, and pretty print it.
curl "${args[@]}" "${API}/draft_specs?draft_id=eq.${DRAFT}&select=catalog_name,spec_type,spec" | jq '.'

# Create a publication and grab its id.
PUBLISH=$(curl "${args[@]}" \
    "${API}/publications?select=id" \
    -H "Content-Type: application/json" \
    -H "Prefer: return=representation" \
    -d "{\"draft_id\":\"${DRAFT}\",\"dry_run\":true}" \
    | jq -r '.[0].id')
poll_while_queued "${API}/publications?id=eq.${PUBLISH}"

# View our completed draft.
curl "${args[@]}" "${API}/publications?id=eq.${PUBLISH}&select=created_at,updated_at,job_status" | jq '.'
}

test_discover_then_publish
