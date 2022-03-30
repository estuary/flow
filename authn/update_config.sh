#!/bin/bash

sops --decrypt --output-type=json config.sops.yaml | jq -c | flyctl secrets set AUTHN_CONFIG=-
