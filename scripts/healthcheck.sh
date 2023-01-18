#!/usr/bin/env bash

paths=(
  /usr/local/bin/agent 
  /usr/local/bin/flowctl-go 
  /usr/bin/gsutil 
  /usr/local/bin/sops 
  /usr/bin/jq)

for i in "${paths[@]}"
do
  if [ -f "$i" ] ;
  then
  echo "$i: OK"
  else
  echo "$i is missing"
  exit 2
  fi
done