#!/bin/bash

curl -H 'Content-Type: application/json' -d @- 'http://localhost:8081/ingest' <<EOF
{
    "examples/shopping/users": [
        {
            "id": 6,
            "name": "Donkey Kong",
            "email": "bigguy@dk.com"
        },
        {
            "id": 7,
            "name": "Echo",
            "email": "explorer@ocean.net"
        },
        {
            "id": 8,
            "name": "Gordon Freeman",
            "email": "mfreeman@apeture.com"
        }
    ]
}
EOF