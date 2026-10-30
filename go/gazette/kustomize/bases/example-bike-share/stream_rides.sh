#!/bin/sh

readonly DATASET=${DATASET:-201909-citibike-tripdata.csv}
readonly RATE=${RATE:-300}

curl -o ${DATASET}.zip https://s3.amazonaws.com/tripdata/${DATASET}.zip

# Run a pipeline that unpacks CSV records, prefixes each with a UUID,
# partitions on Bike ID, rate-limited to $RATE, and appends each
# record to a modulo-mapped journal.
unzip -p ${DATASET}.zip \
  | gazctl attach-uuids --skip-header \
  | awk -F "," '{print $13}{print}' \
  | pv --line-mode --quiet --rate-limit ${RATE} \
  | gazctl journals append -l app.gazette.dev/message-type=bike_share.Ride --framing=lines --mapping=modulo
