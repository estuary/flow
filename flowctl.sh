#!/bin/bash

# Make sure we have docker
DOCKER_EXEC=$(which docker)
if [ ! -x "$DOCKER_EXEC" ] ; then
    echo "flowctl.sh requires docker in order to operate."
    exit 254
fi

# Make sure we know where the docker sock is
DOCKER_SOCK="/var/run/docker.sock"
if [ ! -e "$DOCKER_SOCK" ] ; then
    echo "Could not find the docker socket."
    exit 254
fi

# Default values
FLOWCTL_DIRECTORY=$(pwd)
FLOWCTL_CONTAINAER_DIRECTORY="/home/flow/project"
FLOWCTL_PORT="8080"

# parse_option (option name, assign to variable, $@) (relies on global argpos for current position in args)
function parse_option {
    name="$1"
    shift
    variable="$1"
    shift
    if [[ "${!argpos}" == "${name}="* ]]; then # With equals format --name=value
        eval ${variable}="${!argpos#*=}"
    else # No equals format --name value
        argpos=$((argpos+1))
        eval $variable="${!argpos}"
    fi
}

# parse all arguments
for (( argpos=1; argpos <= "$#"; argpos++ )); do
    case "${!argpos}" in
        --directory*)
            parse_option "--directory" FLOWCTL_DIRECTORY $@
            if [[ "$FLOWCTL_DIRECTORY" != "$(realpath ${FLOWCTL_DIRECTORY})" ]]; then
                echo "flowctl.sh requires that you specify a full path with the --directory option"
                exit 255
            fi
            FLOWCTL_CONTAINAER_DIRECTORY="${FLOWCTL_DIRECTORY}"
            ;;
        --port*)
            parse_option "--port" FLOWCTL_PORT $@
            ;;
    esac
done

${DOCKER_EXEC} run -it --rm \
    --user ${UID} \
    -v ${FLOWCTL_DIRECTORY}:${FLOWCTL_CONTAINAER_DIRECTORY} \
    -v ${DOCKER_SOCK}:/var/run/docker.sock \
    -p ${FLOWCTL_PORT}:${FLOWCTL_PORT} \
    --group-add $(stat -c '%g' ${DOCKER_SOCK}) \
    -v /var/tmp:/var/tmp -e TMPDIR=/var/tmp \
    quay.io/estuary/flow:dev flowctl $@
