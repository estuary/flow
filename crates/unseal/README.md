# unseal

Turns a task's stored endpoint configuration into the plaintext configuration
handed to its connector. Called by both runtimes at the point where a connector
is dialed, and by `flowctl` when it runs one locally.

A configuration takes exactly one of two forms, and they are mutually exclusive:

- **Wrapped** (legacy): the document as a whole is a `sops` envelope, decrypted
  by shelling out to `sops` under the data-plane's KMS key. A plaintext
  `sops.overlay` may adjust non-security-relevant locations without
  re-encrypting -- and thus re-MAC-ing -- the document.
- **Plaintext with a `secrets` stanza**: the configuration carries no ciphertext
  of its own. Each secret is resolved by catalog name through the
  config-encryption service and merged in at the JSON pointer which names it.

## Key Types and Entry Points

- `decrypt_sops()` - Decrypt a wrapped document, stripping any `encrypted_suffix`.
  A document with no `sops` stanza passes through unchanged.
- `overlay::decrypt_with_overlay()` - As above, additionally applying a
  `sops.overlay` which is first validated to touch only `nonsensitive: true`
  schema locations.
- `secrets::is_sops()` - The sniff which chooses between the two forms.
- `secrets::resolve()` - Resolve a `secrets` stanza into a plaintext
  configuration, generic over an async decrypt callback. This crate holds no
  transport and no tokens: callers supply the callback.

## Non-obvious Details

- Decryption shells out to the `sops` and `jq` binaries, located via
  `locate-bin`.
- `secrets::resolve` merge-patches (RFC 7396) each entry in lexicographic
  pointer order, so a deeper pointer wins wherever two entries overlap, and a
  `null` leaf deletes its property. Pointer tokens are always object property
  names -- never array indices.
- Error messages never carry secret material.
