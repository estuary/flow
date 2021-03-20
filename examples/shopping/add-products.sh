#!/bin/bash -e

cd "$( dirname "${BASH_SOURCE[0]}" )"

cat products.csv | websocat --protocol csv/v1 'ws://localhost:8081/ingest/examples/shopping/products'
