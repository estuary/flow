#!/bin/bash

curl -H 'Content-Type: application/json' -d @- 'http://localhost:8081/ingest' <<EOF
{
    "examples/shopping/users": [
        {
            "id": 8,
            "name": "Gordon Freeman",
            "email": "gordo@retiredlife.org"
        }
    ]
}
EOF
