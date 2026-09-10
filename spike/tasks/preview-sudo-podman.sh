#!/usr/bin/env bash
# DOCKER_CLI for a runtime running on the host as an ordinary user. The spike's
# images and networks are root's, as production's are.
exec sudo podman "$@"
