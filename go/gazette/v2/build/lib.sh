# Log in to a Docker registry
#
# Globals:
#   - DOCKER_USER
#   - DOCKER_PASS
docker_login() {
    local restore_x
    if [[ $- =~ x ]]; then
      restore_x=true
    else
      restore_x=false
    fi

    # Disable xtrace to hide password ($DOCKER_PASS).
    set +x
    echo "$DOCKER_PASS" | docker login -u $DOCKER_USER --password-stdin
    if [[ $restore_x ]]; then
        set -x
    fi
}

# Tag and push a Docker image. The source image must already be tagged with
# "latest". That image will additionally be tagged with the given tag and both
# "latest" and that tag will be pushed.
#
# Arguments:
#   1. source image; e.g., "liveramp/gazette"
#   2. Docker tag
docker_tag_and_push() {
    declare -r image="$1"
    declare -r tag="$2"

    docker tag "${image}:latest" "${image}:${tag}"
    docker push "${image}:latest"
    docker push "${image}:${tag}"
}

# Build and tag all targets of the multi-stage build.
#
# The second argument, vendor_image, is optional. When provided, it will be
# pulled from the registry and used as the cache source of the builds. Note,
# using an image as a cache source and using the local build cache are mutually
# exclusive. The recommendation is to use the local build cache unless the
# local build cache is not persisted between builds, such as in a CI
# environment with transient workers.
#
# Arguments:
#   1. path to project root.
#   2. (Optional) name of vendor image; e.g., liveramp/gazette-vendor:latest.
docker_build_all() {
    declare -r root="$1"
    declare -r vendor_image="${2:-}"
    declare -r dockerfile="${root}/v2/build/Dockerfile"

    if [[ -n "${vendor_image}" ]]; then
        declare -r cf_build="--cache-from ${vendor_image}"
        declare -r cf_gazette="--cache-from liveramp/gazette-build:latest"
        declare -r cf_examples="--cache-from liveramp/gazette-build:latest --cache-from liveramp/gazette:latest"

        # Build the `gazette-vendor` image.
        docker pull "${vendor_image}"
        docker build "${root}" \
            --file "${dockerfile}" \
            --target vendor \
            --tag "${vendor_image}" \
            --cache-from "${vendor_image}"
    fi

    # Build and test Gazette. This image includes all Gazette source, vendored
    # dependencies, compiled packages and binaries, and only completes after
    # all tests pass.
    docker build ${root} \
        --file ${dockerfile} \
        --target build \
        --tag liveramp/gazette-build:latest \
        ${cf_build:-}

    # Create the `gazette` image, which plucks the `gazette`, `gazctl` and
    # `run-consumer` onto a base runtime image.
    docker build ${root} \
        --file ${dockerfile} \
        --target gazette \
        --tag liveramp/gazette:latest \
        ${cf_gazette:-}

    # Create the `gazette-examples` image, which further plucks `stream-sum` and
    # `word-count` example artifacts onto the `gazette` image.
    docker build ${root} \
        --file ${dockerfile} \
        --target examples \
        --tag liveramp/gazette-examples:latest \
        ${cf_examples:-}
}
