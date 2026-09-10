#!/usr/bin/env bash
# Build everything experiment 5 measures. One command, no arguments.
#
#   - localhost/derive-python-pandas:spike, the derived image (venv as a layer)
#   - $SPIKE_EXP5_DIR/venv, the same venv exported to a host directory for the
#     separate-share cells, built at /venv inside a container of the derived
#     image so the interpreter and the venv's own baked-in paths both match
#     what the guest will see
#   - $SPIKE_EXP5_DIR/bench, a venv share holding only bench.py, for the cells
#     whose venv is the image layer
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp5-common.sh"

say "exp5-build.sh: building $SPIKE_DERIVED_IMAGE"
sudo podman build \
    --tag "$SPIKE_DERIVED_IMAGE" \
    --file "$SPIKE_DIR/derived/Dockerfile" \
    "$SPIKE_DIR/derived"

# Rebuilt from scratch rather than reused: a half-written venv from an
# interrupted run would be measured as a fast one.
say "exp5-build.sh: exporting the same venv to $SPIKE_EXP5_VENV"
sudo rm -rf "$SPIKE_EXP5_DIR"
sudo mkdir -p "$SPIKE_EXP5_VENV" "$SPIKE_EXP5_BENCH/spike"

# --user 0 to write the bind mount; the venv is created at /venv, the path the
# guest mounts it at, so pyvenv.cfg and the console scripts hold guest-correct
# paths. Same pin as the Dockerfile: the share cells and the primary cell must
# import identical bytes.
sudo podman run --rm --user 0 \
    --entrypoint /bin/sh \
    --mount "type=bind,source=$SPIKE_EXP5_VENV,target=/venv" \
    "$SPIKE_DERIVED_IMAGE" -c \
    "set -e
     uv venv /venv
     uv pip install --python /venv/bin/python --compile-bytecode $SPIKE_PANDAS_PIN
     chmod -R a+rX /venv"

# CONTRACTS "Probes and benchmarks": whatever the guest runs lives under
# `spike/` in the venv share. Created after `uv venv`, which refuses a
# directory that already has contents.
sudo mkdir -p "$SPIKE_EXP5_VENV/spike"
sudo cp "$SPIKE_DIR/derived/bench.py" "$SPIKE_DIR/derived/cpu.py" "$SPIKE_EXP5_VENV/spike/"
sudo cp "$SPIKE_DIR/derived/bench.py" "$SPIKE_DIR/derived/cpu.py" "$SPIKE_EXP5_BENCH/spike/"
sudo chmod -R a+rX "$SPIKE_EXP5_DIR"

say ""
say "exp5-build.sh: image venv and exported venv, for comparison:"
sudo podman run --rm --user 0 --entrypoint /bin/sh "$SPIKE_DERIVED_IMAGE" -c \
    'du -sk /opt/venv; find /opt/venv -name "*.pyc" | wc -l; /opt/venv/bin/python -c "import pandas; print(pandas.__version__)"' \
    | sed 's/^/      image    /'
sudo du -sk "$SPIKE_EXP5_VENV" | sed 's/^/      exported /'
sudo find "$SPIKE_EXP5_VENV" -name '*.pyc' | wc -l | sed 's/^/      exported /'
say ""
say "exp5-build.sh: ok"
