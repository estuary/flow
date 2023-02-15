#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

ENVIRONMENT="$1";
if [[ -z "$ENVIRONMENT" ]]; then
	echo "missing required positional argument of 'prod' or 'local'" 1>&2
	exit 1
fi

DATAPLANE="$2";
if [[ -z "$DATAPLANE" ]]; then
	echo "missing required positional argument for dataplane" 1>&2
	exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
INPUT_FILENAME="${SCRIPT_DIR}/template-${ENVIRONMENT}.flow.yaml"

flowctl raw bundle --source "$INPUT_FILENAME" | sed "s/DATAPLANE/${DATAPLANE}/g"
