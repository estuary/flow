#!/bin/bash

wget --no-clobber https://storage.googleapis.com/estuaryflowexamples/network-flows.csv.gz

# Pipe CSV rows into Flow's CSV WebSocket ingestion API.
gzip  -cd  network-flows.csv.gz \
      | pv --line-mode --quiet --rate-limit 5000 \
      | websocat --protocol csv/v1 ws://localhost:8080/ingest/examples/net-trace/pairs