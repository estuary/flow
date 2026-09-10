#!/usr/bin/env bash
# Build everything experiment 5 measures. One command, no arguments.
#
#   - localhost/derive-python-pandas:spike, the derived image (venv as a layer)
#   - $SPIKE_EXP5_DIR/venv, the same venv exported to a host directory for the
#     separate-share cells, built at /venv inside a container of the derived
#     image so the interpreter and the venv's own baked-in paths both match
#     what the guest will see
#   - $SPIKE_EXP5_DIR/bench, a venv share holding only bench.py, for the cells
#     whose venv is not the `venv` share
#   - $SPIKE_EXP5_DIR/deps.ext4 and deps.erofs, the same venv as per-tag
#     read-only disk images (WP08b)
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
for target in "$SPIKE_EXP5_VENV/spike" "$SPIKE_EXP5_BENCH/spike"; do
    sudo cp "$SPIKE_DIR/derived/bench.py" "$SPIKE_DIR/derived/cpu.py" \
        "$SPIKE_DIR/derived/prefault.py" "$SPIKE_DIR/derived/attrib.py" "$target/"
done
sudo chmod -R a+rX "$SPIKE_EXP5_DIR"

# ---------------------------------------------------------- the deps images
# Built from the directory just exported, so the block cells and the virtiofs
# cells read the same bytes and the only difference is the transport. Note that
# the venv's own baked-in paths say `/venv` while the disk is mounted at
# `/opt/venv`; immaterial here because bench.py puts site-packages on sys.path
# rather than running the venv's interpreter. A phase-2 builder would create
# the venv at its final path.
venv_kib="$(sudo du -sk "$SPIKE_EXP5_VENV" | cut -f1)"
# Room for inode tables and group descriptors on top of the data. mkfs fails
# loudly if this is short, so a fixed margin beats guessing precisely.
ext4_mib=$(( venv_kib / 1024 + venv_kib / 1024 / 10 + 24 ))

say ""
say "exp5-build.sh: building the deps images from that directory (${venv_kib} KiB)"
# No journal and no reserved blocks: nothing will ever write to it.
# lazy_itable_init=0 zeroes the inode tables now, on the host, rather than
# leaving the guest's ext4lazyinit to do it on a read-only device.
sudo mkfs.ext4 -d "$SPIKE_EXP5_VENV" -O ^has_journal -E lazy_itable_init=0 -m 0 -q -F \
    "$SPIKE_EXP5_EXT4" "${ext4_mib}m"
sudo mkfs.erofs --quiet "$SPIKE_EXP5_EROFS" "$SPIKE_EXP5_VENV"
sudo chmod a+r "$SPIKE_EXP5_EXT4" "$SPIKE_EXP5_EROFS"

say ""
say "exp5-build.sh: image venv and exported venv, for comparison:"
sudo podman run --rm --user 0 --entrypoint /bin/sh "$SPIKE_DERIVED_IMAGE" -c \
    'du -sk /opt/venv; find /opt/venv -name "*.pyc" | wc -l; /opt/venv/bin/python -c "import pandas; print(pandas.__version__)"' \
    | sed 's/^/      image    /'
sudo du -sk "$SPIKE_EXP5_VENV" | sed 's/^/      exported /'
sudo find "$SPIKE_EXP5_VENV" -name '*.pyc' | wc -l | sed 's/^/      exported /'
say ""
say "exp5-build.sh: deps images (apparent size, then blocks actually used):"
for image in "$SPIKE_EXP5_EXT4" "$SPIKE_EXP5_EROFS"; do
    printf '      %-12s apparent %7s KiB   allocated %7s KiB\n' \
        "$(basename "$image")" \
        "$(sudo du -sk --apparent-size "$image" | cut -f1)" \
        "$(sudo du -sk "$image" | cut -f1)"
done
say ""
say "exp5-build.sh: ok"
