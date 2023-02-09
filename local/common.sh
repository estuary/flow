#!/bin/bash

set -e

function copy_local_flowctl_config() {
    local profile="$1"

    local src="${SCRIPT_DIR}/local-flowctl-config.json"
    local target="${HOME}/.config/flowctl/${profile}.json"
    if [[ "$(uname)" == 'Darwin' ]]; then
        target="${HOME}/Library/Application Support/flowctl/${profile}.json"
    fi

    if [[ ! -f "$target" ]]; then
        log "copying local flowctl config from '${src}' to '${target}'"
        cp "$src" "$target"
    fi
}
