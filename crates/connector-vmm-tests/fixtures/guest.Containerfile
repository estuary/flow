FROM ghcr.io/estuary/derive-python:stable

COPY probes.py /probes.py

# CONNECTOR_MOUNT is a value the runtime's environment contract must
# override: a guest that sees it has lost the contract's precedence. LOG_LEVEL
# is one the contract leaves to the image.
ENV CONNECTOR_MOUNT=/image-default LOG_LEVEL=image-default
