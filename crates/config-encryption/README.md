# config-encryption

A small, stateless HTTP service which invokes the `sops` binary on behalf of the
control plane. It exists because the KMS grants that wrap and unwrap Estuary's
ciphertext live here and nowhere else.

The service holds **two independent keychains**:

| | legacy endpoint config | first-class secret |
|---|---|---|
| key | `--kms-key` | `--secrets-kms-key` |
| encrypt grant | this service | this service |
| decrypt grant | this service **and every data-plane reactor** | this service alone |
| route | `POST /v1/encrypt-config` | `POST /secret/encrypt`, `POST /secret/decrypt` |

A legacy sops-wrapped config travels inside a built spec and is unsealed by the
reactor that runs the task, which is why data-planes hold that decrypt grant.
A first-class secret never travels with a spec: the reactor asks this service
for the plaintext, one secret at a time, and this service asks the control plane
whether that caller may have it. See [issue #3366](https://github.com/estuary/flow/issues/3366).

## Entry points

- `src/main.rs` — args and routing. The two keychains are two `axum::Router`s
  with separate state, merged, so neither route can reach the other's key.
- `encrypt_config` (`src/lib.rs`) — the legacy route. Walks the connector's
  config schema for `secret: true` annotations, renames those locations with a
  `_sops` suffix, and encrypts by suffix.
- `secrets::encrypt_secret` / `secrets::decrypt_secret` (`src/secrets.rs`) — the
  secret routes, over documents of the shape:

  ```json
  {"name": "acmeCo/db/password", "value": "ENC[AES256_GCM,...]", "sops": {...}}
  ```

  Only `value` is encrypted (`--encrypted-regex '^value$'`, which covers an
  entire object subtree). `name` stays plaintext but is MAC-covered, because
  sops MACs unencrypted values too — that is the binding which lets `unwrap`
  reject a document wrapped for one secret and stored under another.

## Non-obvious details

- **`/secret/encrypt` is unauthenticated.** Wrapping a value the caller already
  holds discloses nothing. Authority is enforced where the secret is *set*, by
  the control plane's `setSecret` mutation. Keeping the two separable is what
  keeps this service small.
- **`/secret/decrypt` does not interpret its bearer token** beyond sniffing
  which authorize route can verify it: a data-plane token (carrying gazette
  `cap` / `sel` claims) is POSTed to `/authorize/task/decrypt-secret`, and
  anything else is forwarded as a Bearer to `/authorize/user/decrypt-secret`.
  A misclassification is harmless — the route we pick verifies the token.
- **This service never retries and never sleeps.** A control-plane retry
  response (200 bearing `retryMillis`) passes through as our own response body.
  Retry policy belongs to the client's `tokens::fetch_once`, which alone knows
  the deadline of the overall operation.
- **Browsers are first-class callers**, so `cors_layer()` must name both the
  `POST` method and the `Authorization` header: without them a browser's
  preflight of `/secret/decrypt` fails, and nothing outside a browser notices.
- **Error statuses are a retry contract.** A 4xx is terminal to
  `tokens::RestSource`, and a tampered or mis-wrapped document is terminal in
  exactly that sense -- no amount of retrying makes a MAC verify. Only a control
  plane we could not reach (503) or could not understand (502) is worth coming
  back for; the control plane's own status otherwise passes through as ours.
- **`sops` is invoked as a subprocess** over stdin, and finds its decryption
  keys through this process' ambient environment (`SOPS_AGE_KEY`, or cloud
  credentials for AWS/GCP KMS). `--secrets-kms-key` names only the *recipient*
  of a new wrapping.
