#!/bin/bash

# Make sure we have docker
DOCKER_EXEC=$(which docker)
if [[ ! -x "$DOCKER_EXEC" ]] ; then
    echo "flowctl.sh requires docker in order to operate."
    exit 254
fi

# Make a copy of arguments to manipulate
ARGS=(${@})
if [[ "${DEBUG_SCRIPT}" = true ]]; then for key in "${!ARGS[@]}"; do echo "ARG(${key}): ${ARGS[$key]}"; done; fi

# Default values
DOCKER_SOCK="/var/run/docker.sockasdf"
FLOWCTL_DIRECTORY=$(pwd)
FLOWCTL_CONTAINAER_DIRECTORY="/home/flow/project"
FLOWCTL_PORT="8080"
FLOWCTL_NETWORK="bridge"

# parse_option (option name, assign to variable, option, $@) (relies on global argpos for current position in args)
function parse_option {
    name="$1"; shift
    variable="$1"; shift
    option="$1"; shift
    equals_format=false
    value=""
    if [[ "${ARGS[$argpos]}" == "${name}="* ]]; then # With equals format --name=value
        equals_format="true"
        value="${ARGS[$argpos]#*=}"
    else # No equals format --name value
        argpos=$((argpos+1))
        value="${ARGS[$argpos]}"
    fi
    # Update the set variable and passed arguments to be the realpath
    if [[ "$option" == "realpath" ]]; then
        value=$(realpath ${value})
        if [[ "$equals_format" = true ]]; then
            ARGS[$argpos]="${name}=${value}"
        else
            ARGS[$argpos]="${value}"
        fi
    fi
    # Consume the argument from the list of passed arguments by blanking them so the array remains the same length
    if [[ "$option" == "consume" ]]; then
        if [[ "$equals_format" = true ]]; then
            ARGS[$argpos]=""
        else
            ARGS[$((argpos-1))]=""
            ARGS[$argpos]=""

        fi
    fi
    eval ${variable}="${value}"
    #echo "${variable}=${value}"
}

# parse all arguments
for (( argpos=0; argpos < "${#ARGS[@]}"; argpos++ )); do
    case "${ARGS[$argpos]}" in
        --directory*)
            parse_option "--directory" FLOWCTL_DIRECTORY "realpath" $ARGS
            FLOWCTL_CONTAINAER_DIRECTORY="${FLOWCTL_DIRECTORY}"
            ;;
        --port*)
            parse_option "--port" FLOWCTL_PORT "none" $ARGS
            ;;
        --network*)
            parse_option "--network" FLOWCTL_NETWORK "consume" $ARGS
            ;;
        --docker-sock*)
            parse_option "--docker-sock" DOCKER_SOCK "consume" $ARGS
            ;;
    esac
done

# Check that we can find the docker socket
if [[ ! -e "$DOCKER_SOCK" ]] ; then
    echo "Could not find the docker socket. You can specifiy the location with --docker-sock=/full/path"
    exit 254
fi

DOCKER_EXTRA_OPTS=""

# Get the docker socket group owner and add that to the container to allow manipulation by flowctl
DOCKER_SOCK_GID=$(stat -c '%g' ${DOCKER_SOCK})
if [[ ! -z "$DOCKER_SOCK_GID" ]]; then
    DOCKER_EXTRA_OPTS+="--group-add ${DOCKER_SOCK_GID} "
fi

# Build docker command
CMD="${DOCKER_EXEC} run -it --rm \
    --user ${UID} \
    -v ${FLOWCTL_DIRECTORY}:${FLOWCTL_CONTAINAER_DIRECTORY} \
    -v ${DOCKER_SOCK}:/var/run/docker.sock \
    -p ${FLOWCTL_PORT}:${FLOWCTL_PORT} \
    --network ${FLOWCTL_NETWORK} \
    ${DOCKER_EXTRA_OPTS} \
    -v /var/tmp:/var/tmp -e TMPDIR=/var/tmp \
    quay.io/estuary/flow:dev flowctl ${ARGS[*]}"

# Debug
if [[ "${DEBUG_SCRIPT}" = true ]]; then
    for varname in ${!FLOWCTL_*}; do echo "${varname}: ${!varname}"; done
    echo "ARGS: ${ARGS[*]}"
    echo "${CMD}"
fi

$CMD
