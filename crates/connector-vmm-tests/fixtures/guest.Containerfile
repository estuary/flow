FROM ghcr.io/estuary/derive-python:stable

COPY probes.py /probes.py

# Values the runtime's environment contract must override: a guest that sees
# these instead of the launch line's has lost the contract's precedence.
ENV CONNECTOR_MOUNT=/image-default LOG_LEVEL=image-default
