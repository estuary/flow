# Constants for experiment 5. Sourced, never executed.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/helper-common.sh"

# CONTRACTS "Paths and names".
SPIKE_DERIVED_IMAGE=localhost/derive-python-pandas:spike
SPIKE_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev

# One pin, read by the Dockerfile's install and the exported venv's.
SPIKE_PANDAS_PIN=pandas==3.0.5

# Build outputs, outside the per-connector directories because they are reused
# across every boot of the matrix. Same filesystem as the reactor dir.
: "${SPIKE_EXP5_DIR:=/var/tmp/flow-spike/exp5}"
SPIKE_EXP5_VENV="$SPIKE_EXP5_DIR/venv"
SPIKE_EXP5_BENCH="$SPIKE_EXP5_DIR/bench"

# WP08b: the same venv as per-tag read-only disk images, which is the transport
# the design settled on after virtiofs missed the gate. Bound into the helper at
# /deps.img and mounted by flow-init at /opt/venv.
SPIKE_EXP5_EXT4="$SPIKE_EXP5_DIR/deps.ext4"
SPIKE_EXP5_EROFS="$SPIKE_EXP5_DIR/deps.erofs"
SPIKE_DEPS_IMG=/deps.img

# derive-python's interpreter, per CONTRACTS "Probes and benchmarks".
SPIKE_GUEST_PYTHON=/usr/local/bin/python

say() { printf '%s\n' "$*"; }
