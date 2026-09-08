# connector-environment

Shared validation policy for environment variables exposed to user-authored
connector code. Language connectors remain responsible for applying validated
variables to their subprocesses and for configuring runtime-specific
permissions.

The entry points are `validate_python` and `validate_deno`. Both enforce
portable environment names, while each also rejects variables reserved by its
runtime toolchain.
