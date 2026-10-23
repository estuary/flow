# OAuth integration

`mod.rs` implements `oauth_server::Backend` for `App`. It authenticates consent,
rejects restricted tokens and service accounts, and delegates credential
creation and exchange to `public::token_exchange`. The API response
types live in `public::token_exchange`; this adapter converts them into the
OAuth backend response types.

The protocol routes, configuration, and dashboard contract are documented in
[`oauth-server`](../../../../../oauth-server/README.md).
