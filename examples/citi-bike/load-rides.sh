#!/bin/bash
wget --no-clobber https://s3.amazonaws.com/tripdata/202009-citibike-tripdata.csv.zip

# Pipe CSV rows into Flow's CSV WebSocket ingestion API.
unzip -p 202009-citibike-tripdata.csv.zip \
      | pv --line-mode --quiet --rate-limit 500 \
      | websocat --protocol csv/v1 ws://localhost:8080/ingest/examples/citi-bike/rides