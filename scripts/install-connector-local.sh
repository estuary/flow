#!/bin/bash
set -e

USAGE="${0} <connector-image> [<tag>]

Adds the given connector the local postgres database.

example: ./install-connector-local.sh ghcr.io/estuary/source-gcs

The first argument is the image name and it is always required.
The second argument is optional and names the image tag. If not provided,
the tag will default to ':local', which is treated specially by the agent
to disable pulling the image."

function bail() {
    echo "$@" 1>&2
    exit 1
}

if [[ -z "$1" || "$1" == "-h" ]]; then
    bail "$USAGE"
fi

CONNECTOR="$1"
shift 1
if [[ -z "$CONNECTOR" ]]; then
    bail "must supply connector image name as first argument"
fi

TAG="${1:-local}"

echo "Adding connector image_name: '${CONNECTOR}', image_tag: '${TAG}'" 1>&2

psql 'postgres://postgres:postgres@localhost:5432/postgres' <<EOF
    begin;

    insert into connectors (image_name, external_url) values ('${CONNECTOR}', 'https://estuary.dev/') on conflict do nothing;

    insert into connector_tags (connector_id, image_tag)
        values (
            (select id from connectors where image_name = '${CONNECTOR}'),
            ':${TAG}'
        )
        on conflict (connector_id, image_tag) do update set job_status = '{"type": "queued"}';

    commit;
EOF

