# The derived image of PLAN experiment 5: derive-python plus a pandas venv
# baked in as an image layer, at /opt/venv.
#
# This is the shape the design assumes for customer dependencies - the venv
# arrives as a layer of the connector image, so in the guest it is read over
# the same read-only virtiofs root share as the interpreter. The primary cell
# measures exactly that; the `share` cells swap the layer for a second
# virtiofs share of the identical venv.
FROM ghcr.io/estuary/derive-python:dev

# uv and the interpreter are the base image's; only root can write /opt. The
# final USER below restores the base image's value, so the derived image's
# `Config.User` - which the shim resolves into flow-init's --uid/--gid - is
# byte-identical to derive-python's.
USER root

# Pinned so the layer, the exported host venv the share cells read, and the
# baseline all import the same bytes.
#
# --compile-bytecode is load-bearing for the measurement, not a nicety.
# Without it site-packages ships no .pyc, and every cell's first `import
# pandas` pays a compile-and-write cost: on the overlay for the primary and
# the baseline, and not at all for the read-only share cells, which would
# recompile on every boot forever. Precompiling removes a confound that has
# nothing to do with virtiofs and is what a real dependency layer would do.
RUN uv venv /opt/venv \
 && uv pip install --python /opt/venv/bin/python --compile-bytecode pandas==3.0.5 \
 && chmod -R a+rX /opt/venv \
 && rm -rf /tmp/uv-cache

USER nobody
