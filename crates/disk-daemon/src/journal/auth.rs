//! Broker credentials of a tenure, and the client every tenure derives its own from.
//!
//! The daemon holds the data plane's signing key and mints its own tokens. Each
//! tenure layers a token scoped to exactly its own journal over the shared client.

use proto_gazette::broker;

/// The client every tenure derives its own from, so that tenures share their
/// connections to brokers and to fragment stores.
///
/// `zone` is the availability zone of this host, which route selection prefers a
/// replica of. There is no default metadata, because each tenure layers a token of
/// its own over this.
pub fn shared_client(address: &str, zone: &str) -> gazette::journal::Client {
    gazette::journal::Client::new(
        address.to_string(),
        gazette::journal::Client::new_fragment_client(),
        proto_grpc::Metadata::new(),
        gazette::Router::new(zone),
    )
}

/// What a tenure authorizes its own broker calls with.
///
/// The daemon holds the data plane's signing key and mints its own tokens, rather
/// than being handed one by a client and having to be handed another before it
/// expires. This is how a reactor reaches its shard recovery logs: a journal of the
/// data plane's own state is self-signed, and only user collection data goes through
/// the control plane's authorization API.
pub struct Auth {
    /// Brokers serving every disk journal.
    pub endpoint: String,
    /// FQDN of the data plane, stamped as the `iss` of each token.
    pub fqdn: String,
    /// Key which signs them.
    pub key: tokens::jwt::EncodingKey,
}

/// Duration of a token the daemon signs. `tokens` refreshes ahead of expiry, and
/// no faster than once a minute, so this is minutes rather than seconds.
const TOKEN_DURATION: tokens::TimeDelta = match tokens::TimeDelta::try_minutes(10) {
    Some(duration) => duration,
    None => panic!("ten minutes is a valid duration"),
};

impl Auth {
    /// Claims of the tokens a tenure signs, scoped to exactly its own journal.
    ///
    /// The capabilities are everything a tenure does: it lists the journal to
    /// validate its spec, reads it back to recover a disk, appends its deltas, and
    /// applies the recovery floor it derives.
    fn claims(&self, journal: &str) -> proto_gazette::Claims {
        use proto_gazette::capability::{APPEND, APPLY, LIST, READ};

        proto_gazette::Claims {
            cap: APPEND | APPLY | LIST | READ,
            // Stamped at each signing, from `TOKEN_DURATION`.
            exp: 0,
            iat: 0,
            iss: self.fqdn.clone(),
            // Gazette indexes a journal's own name as a label of its spec, so a
            // token scoped to the name selects exactly that journal.
            sel: broker::LabelSelector {
                include: Some(labels::build_set([("name", journal)])),
                exclude: None,
            },
            sub: journal.to_string(),
        }
    }

    /// A client of `journal` alone, signed with this daemon's key.
    ///
    /// `client` is the shared one, which carries the connections to brokers and
    /// fragment stores and no credential of its own.
    pub fn signed_client(
        &self,
        client: &gazette::journal::Client,
        journal: &str,
    ) -> gazette::journal::Client {
        client.with_signed_claims(
            self.claims(journal),
            self.key.clone(),
            TOKEN_DURATION,
            self.endpoint.clone(),
        )
    }
}
