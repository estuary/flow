# Config-encryption

This crate contains the `flow-config-encryption` binary, which is an http service that encrypts Flow
endpoint configuration documents. 

**Dependencies:**

- The `sops` executable must be on the `PATH`, both at runtime, and for `cargo test` to pass.
- You must first set `APPLICATION_DEFAULT_CREDENTIALS`. This service account must
  have access to the GCP KMS key that's passed in the arguments.

Run like `flow-config-encryption --gcp-kms your/fully/qualified/key/name` to start the server
listening (port `8765` by default). See the `--help` for
more. Given a request with a payload like:

```json
{
  "schema": {
    "type": "object",
    "properties": {
      "supa_secret": {
        "type": "string",
        "secret": true
      },
      "not_secret": {
        "type": "string"
      }
    }
  },
  "config": {
    "not_secret": "remain plain",
    "supa_secret": "encrypt me"
  }
}
```

You'd get an encrypted document in return where the entry for `supa_secret` gets changed to
`"supa_secret_sops": "ENC[...]"`, and the entry for `not_secret` remains unchanged.

