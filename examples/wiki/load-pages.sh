#!/bin/bash
wget --no-clobber https://github.com/apache/druid/raw/master/examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz

# Pipe documents into Flow's JSON WebSocket ingestion API.
gzip -cd wikiticker-2015-09-12-sampled.json.gz \
      | pv --line-mode --quiet --rate-limit 500 \
      | websocat --protocol json/v1 ws://localhost:8081/ingest/examples/wiki/edits