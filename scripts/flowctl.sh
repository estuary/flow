#!/bin/bash

function log_fatal {
    echo "ERROR: $1"
    exit 1
}

# Make sure we have docker
DOCKER_EXEC=$(which docker)
if [[ ! -x "$DOCKER_EXEC" ]] ; then
    log_fatal "flowctl.sh requires docker in order to operate."
fi

# Make sure we can invoke docker
if ! ${DOCKER_EXEC} info >/dev/null 2>&1 ; then
    log_fatal "flowctl.sh is unable to invoke 'docker info'. Ensure the current user has access to run docker. (Usually by making the user a member of the group docker ie: 'sudo usermod -a -G docker <username>'"
fi

# Print a warning if we're running inside of a container about paths
if grep -sq 'docker\|lxc' /proc/1/cgroup; then
   echo "WARNING: You appear to be running this inside of a container. You may have difficulties if the path parameters do not match the path parameters on the docker host."
fi

# Make a copy of arguments to manipulate
ARGS=(${@})

# Default values
DEBUG_SCRIPT="false"
DEBUG_SHELL="false"
DOCKER_SOCK="/var/run/docker.sock"
DOCKER_UID="$UID"
DOCKER_IMAGE="quay.io/estuary/flow:dev"
DOCKER_EXTRA_OPTS=""
DOCKER_COMMAND="flowctl"
FLOWCTL_DIRECTORY=$(pwd)
FLOWCTL_SOURCE=""
FLOWCTL_CONTAINAER_DIRECTORY="/home/flow/project"
FLOWCTL_PORT=""
FLOWCTL_NETWORK="bridge"

# Allow the script to be symlinked to flowctl or gazctl and make the appropriate call in the docker contianer
# This can also be overriden with the --command option
case "$(basename $0)" in
    flowctl)
        DOCKER_COMMAND="flowctl"
    ;;
    gazctl)
        DOCKER_COMMAND="gazctl"
    ;;
esac

# MacOS doesn't have realpath so this is a portable replacement
function realpath {
    OURPWD=$PWD
    cd "$(dirname "$1")"
    LINK=$(readlink "$(basename "$1")")
    while [ "$LINK" ]; do
        cd "$(dirname "$LINK")"
        LINK=$(readlink "$(basename "$LINK")")
    done
    REALPATH="$PWD/$(basename "$1")"
    cd "$OURPWD"
    echo "$REALPATH"
}

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
        --source*)
            parse_option "--source" FLOWCTL_SOURCE "realpath" $ARGS
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
        --docker-image*)
            parse_option "--docker-image" DOCKER_IMAGE "consume" $ARGS
            ;;
        --debug-script*)
            parse_option "--debug-script" DEBUG_SCRIPT "consume" $ARGS
            ;;
        --command*)
            parse_option "--command" DOCKER_COMMAND "consume" $ARGS
            ;;
    esac
done

if [[ "${DEBUG_SCRIPT}" != "true" && "${DEBUG_SCRIPT}" != "false" ]]; then
    log_fatal "The --debug-script option requires a value of true or false. Got '${DEBUG_SCRIPT}'"
fi
if [[ "${DEBUG_SCRIPT}" = true ]]; then for (( i=1; i <= "$#"; i++ )); do echo "BARG(${i}): ${!i}"; done; fi
if [[ "${DEBUG_SCRIPT}" = true ]]; then for key in "${!ARGS[@]}"; do echo "ARG(${key}): ${ARGS[$key]}"; done; fi

# Check that we can find the docker socket
if [[ ! -e "$DOCKER_SOCK" ]] ; then
    log_fatal "Could not find the docker socket. You can specifiy the location with --docker-sock=/full/path"
fi

# Get the docker socket group owner and add that to the container to allow manipulation by flowctl
if ! DOCKER_SOCK_GID=$(stat -Lc '%g' ${DOCKER_SOCK} 2>/dev/null); then
    # Try the BSD variant if that fails
    if ! DOCKER_SOCK_GID=$(stat -Lf '%g' ${DOCKER_SOCK} 2>/dev/null); then
        echo "WARNING: Could not determine gid of docker socket ${DOCKER_SOCK}."
        DOCKER_SOCK_GID=""
    fi
fi
if [[ ! -z "$DOCKER_SOCK_GID" ]]; then
    DOCKER_EXTRA_OPTS+="--group-add ${DOCKER_SOCK_GID} "
fi

# Make sure FLOWCTL_DIRECTORY exists before mapping
if [[ ! -e ${FLOWCTL_DIRECTORY} ]]; then
    if ! mkdir ${FLOWCTL_DIRECTORY}; then
        log_fatal "Could not create working directory ${FLOWCTL_DIRECTORY}"
    fi
fi

# Provide the full mapping to the source file if specified and make sure the workdir is set to the path
if [[ ! -z "${FLOWCTL_SOURCE}" ]]; then
    FLOWCTL_SOURCE_DIR="$(realpath $(dirname ${FLOWCTL_SOURCE}))"
    DOCKER_EXTRA_OPTS+="-v ${FLOWCTL_SOURCE_DIR}:${FLOWCTL_SOURCE_DIR} -w ${FLOWCTL_SOURCE_DIR} "
fi

# If the port option was specified map the same port outside of the container
if [[ ! -z "${FLOWCTL_PORT}" ]]; then
    DOCKER_EXTRA_OPTS+="-p ${FLOWCTL_PORT}:${FLOWCTL_PORT} "
fi

# This is a workaround on MacOS because it requires root in order to run docker-in-docker
if [[ "$OSTYPE" == "darwin"* ]]; then
    # Even though the container is running as root, files created on mapped volumes in MacOS still maintin the users ownership
    DOCKER_UID="0"

    # Attempt to use docker in qemu (assuming it's supported) until we can more accurately work with multiple architectures
    if [[ `uname -m` == 'arm64' ]]; then
        DOCKER_EXTRA_OPTS+="--platform linux/amd64 "
    fi
fi

# Build docker command
CMD="${DOCKER_EXEC} run -it --rm \
--user ${DOCKER_UID} \
-v ${FLOWCTL_DIRECTORY}:${FLOWCTL_CONTAINAER_DIRECTORY} \
-v ${DOCKER_SOCK}:/var/run/docker.sock \
--network ${FLOWCTL_NETWORK} \
${DOCKER_EXTRA_OPTS} \
-v /var/tmp:/var/tmp -e TMPDIR=/var/tmp \
-e HOME=/tmp \
${DOCKER_IMAGE}"

# Build the full docker command
CMD="${CMD} ${DOCKER_COMMAND} ${ARGS[*]}"

# Debug
if [[ "${DEBUG_SCRIPT}" = true ]]; then
    for varname in ${!DOCKER_*}; do echo "${varname}: ${!varname}"; done
    for varname in ${!FLOWCTL_*}; do echo "${varname}: ${!varname}"; done
    echo "ARGS: ${ARGS[*]}"
    echo "CMD: ${CMD}"
fi

$CMD
