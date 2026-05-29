//! Inbound authentication/authorization for runtime gRPC services.
//!
//! [`Authenticator`] authenticates incoming bearer tokens against a known
//! issuer, keys, and required capability. It builds [`tonic::Interceptor`]'s
//! that stashes post-verification claims in request extensions for handler
//! use in downstream authorization checks.
//!
//! [`Authorizer`] authorizes an authenticated token's claims against a handler's
//! specific request context, returning an [`Authorized`] on success.
//!
//! [`Authorized`] is the output of a completed authorization check, and is used
//! for inspection of claims or to run work until token expiry.
//!
//! [`Signer`] is used for token minting. It self-signs data plane tokens scoped to
//! a caller-supplied label selector.
use proto_gazette::Claims;

/// Authenticates inbound bearer tokens for a single, trusted issuer.
#[derive(Clone)]
pub struct Authenticator {
    /// The one trusted issuer FQDN (`claims.iss`) recognized today.
    issuer: String,
    /// Verification keys for `issuer`; a token signed by any key is accepted
    /// (supporting key rotation).
    keys: std::sync::Arc<Vec<tokens::jwt::DecodingKey>>,
}

impl Authenticator {
    pub fn new(issuer: String, keys: Vec<tokens::jwt::DecodingKey>) -> Self {
        Self {
            issuer,
            keys: std::sync::Arc::new(keys),
        }
    }

    /// Verify the bearer token in `metadata`, requiring `required_capability`,
    /// and return the verified claims.
    pub fn authenticate(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        require_capability: u32,
    ) -> tonic::Result<tokens::jwt::Verified<Claims>> {
        let token = crate::extract_bearer(metadata)?;

        let verified = match tokens::jwt::verify::<Claims>(token, require_capability, &self.keys) {
            Ok(verified) => verified,
            Err(status) => {
                // Verification failed. If the token is well-formed but names an
                // issuer we don't recognize, prefer that clearer error. If it
                // can't even be parsed, surface the original failure unchanged.
                return match tokens::jwt::parse_unverified::<Claims>(token) {
                    Ok(unverified) if unverified.claims().iss != self.issuer => {
                        Err(self.unknown_issuer(&unverified.claims().iss))
                    }
                    _ => Err(status),
                };
            }
        };

        // A valid signature only proves the token was minted with one of our
        // keys. Still require it to name our trusted issuer.
        if verified.claims().iss != self.issuer {
            return Err(self.unknown_issuer(&verified.claims().iss));
        }
        Ok(verified)
    }

    fn unknown_issuer(&self, issuer: &str) -> tonic::Status {
        tonic::Status::unauthenticated(format!("unknown token issuer {issuer:?}"))
    }

    /// Build a per-service tonic interceptor that enforces `required_capability`
    /// as the AuthN floor. On success it inserts the [`tokens::jwt::Verified`]
    /// claims into request extensions.
    pub fn interceptor(self, require_capability: u32) -> impl tonic::service::Interceptor + Clone {
        move |mut request: tonic::Request<()>| -> tonic::Result<tonic::Request<()>> {
            let verified = self.authenticate(request.metadata(), require_capability)?;
            request.extensions_mut().insert(verified);
            Ok(request)
        }
    }
}

/// Authorizer holds authenticated claims which have yet to be verified
/// against an authorization context. It's must_use to catch omission of
/// authorize() checks.
#[derive(Debug)]
#[must_use]
pub struct Authorizer(Option<tokens::jwt::Verified<Claims>>);

impl Authorizer {
    /// An authorization context which is trusted by virtue of never crossing
    /// a gRPC boundary. Use this *only* for in-process calls to services taking
    /// an [`Authorizer`].
    pub fn trusted_local() -> Self {
        Self(None)
    }

    /// Build an Authorizer from the authentication context of the inbound request.
    ///
    /// Fails if an authentication context is missing, for example due to omitted
    /// tonic::Interceptor wiring of an [`Authenticator`] instance, unless `disarm`
    /// is true (an escape hatch for services which can be wired in both
    /// authenticated and unauthenticated contexts).
    pub fn from_request<T>(request: &mut tonic::Request<T>, disarm: bool) -> tonic::Result<Self> {
        match request
            .extensions_mut()
            .remove::<tokens::jwt::Verified<Claims>>()
        {
            Some(verified) => Ok(Self(Some(verified))),
            None if !disarm => Err(tonic::Status::unauthenticated(
                "request was not authenticated (missing Authenticator interceptor?)",
            )),
            None => Ok(Self(None)),
        }
    }

    /// Consume this Authorizer by verifying its claims authorize the shard
    /// `id`, returning an [`Authorized`] on success. This is the common case:
    /// a handler scoping authorization to the shard it's about to operate on.
    pub fn authorize_id(self, id: &str) -> tonic::Result<Authorized> {
        self.authorize(labels::build_set([("id", id)]))
    }

    /// Consume this Authorizer by verifying its claims against `set`,
    /// returning an [`Authorized`] on success.
    pub fn authorize(self, set: proto_gazette::broker::LabelSet) -> tonic::Result<Authorized> {
        let Self(Some(verified)) = self else {
            return Ok(Authorized(None)); // Trusted local Authorizer.
        };

        let matched = labels::matches(&verified.claims().sel, &set).map_err(|err| {
            tonic::Status::permission_denied(format!("invalid token selector: {err}"))
        })?;
        if !matched {
            let rendered = set
                .labels
                .iter()
                .map(|l| format!("{}={}", l.name, l.value))
                .collect::<Vec<_>>()
                .join(",");
            return Err(tonic::Status::permission_denied(format!(
                "token is not authorized for {{{rendered}}}"
            )));
        }
        Ok(Authorized(Some(verified)))
    }
}

/// Authorized holds claims which have been both authenticated and authorized.
/// It can only be constructed by [`Authorizer::authorize`], which is the proof
/// that an authorization check actually ran.
#[derive(Debug)]
pub struct Authorized(Option<tokens::jwt::Verified<Claims>>);

impl Authorized {
    /// The authorized claims, or `None` for a trusted-local context.
    pub fn claims(&self) -> Option<&Claims> {
        self.0.as_ref().map(|verified| verified.claims())
    }

    /// Run `work`, erroring with `DeadlineExceeded` if this authorization's
    /// expiration is reached first. A `TrustedLocal` authz never expires.
    ///
    /// Uses tokio time (paused-time-aware via `tokens::now()`).
    pub async fn expiry_guard<F, T, E>(&self, work: F) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: From<tonic::Status>,
    {
        let expiry = async {
            match &self.0 {
                Some(verified) => {
                    tokio::time::sleep(
                        verified
                            .valid_for()
                            .to_std()
                            .unwrap_or(std::time::Duration::ZERO),
                    )
                    .await
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;
            result = work => result,
            _ = expiry => {
                Err(tonic::Status::deadline_exceeded("request authorization expired").into())
            }
        }
    }
}

/// Self-signs data-plane bearer tokens scoped by a caller-supplied label
/// selector — the mint-side counterpart to [`Authorizer`].
#[derive(Clone)]
pub struct Signer {
    /// Issuer FQDN stamped as `claims.iss` (our own data-plane FQDN).
    issuer: String,
    /// Data-plane signing key.
    key: tokens::jwt::EncodingKey,
}

impl Signer {
    pub fn new(issuer: String, key: tokens::jwt::EncodingKey) -> Self {
        Self { issuer, key }
    }

    /// Mint a self-signed bearer granting `capability`, scoped to any label set
    /// matched by `selector`, valid for `duration`. `subject` is advisory (audit
    /// only; not cross-checked against scope).
    ///
    /// The selector is opaque to the `Signer`: callers express the full scope
    /// they need (e.g. an `id:prefix` include for a shard-id prefix).
    pub fn sign(
        &self,
        capability: u32,
        subject: String,
        selector: proto_gazette::broker::LabelSelector,
        duration: tokens::TimeDelta,
    ) -> tonic::Result<String> {
        let iat = tokens::now();
        let exp = iat + duration;

        let claims = Claims {
            cap: capability,
            exp: exp.timestamp() as u64,
            iat: iat.timestamp() as u64,
            iss: self.issuer.clone(),
            sel: selector,
            sub: subject,
        };
        tokens::jwt::sign(&claims, &self.key)
    }

    /// Build gRPC client [`crate::Metadata`] bearing a self-signed token that
    /// grants `capability` scoped to the task-creation prefix of `shard_id`
    /// (the id up to and including its final '/'). A leader or shuffle stream
    /// opened with it can therefore only operate on shards of this one task.
    pub fn shard_bearer(&self, capability: u32, shard_id: &str) -> tonic::Result<crate::Metadata> {
        // Fall back to the whole id if it somehow carries no '/'.
        let id_prefix = labels::shard::id_prefix(shard_id).unwrap_or(shard_id);

        let token = self.sign(
            capability,
            shard_id.to_string(),
            proto_gazette::broker::LabelSelector {
                include: Some(labels::build_set([("id:prefix", id_prefix)])),
                exclude: None,
            },
            tokens::TimeDelta::minutes(1),
        )?;
        crate::Metadata::new().with_bearer_token(&token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_gazette::broker::{LabelSelector, LabelSet};

    const ISSUER: &str = "data-plane.example";
    const SECRET: &[u8] = b"shared-secret";
    const CAP: u32 = 0x20; // Arbitrary capability bit.

    fn authenticator() -> Authenticator {
        Authenticator::new(
            ISSUER.into(),
            vec![tokens::jwt::DecodingKey::from_secret(SECRET)],
        )
    }

    /// Mint a token from `issuer`, signed by `secret`, granting `cap` and scoped
    /// to `id:prefix=acmeCo/`, valid for `ttl`.
    fn token(issuer: &str, secret: &[u8], cap: u32, ttl: tokens::TimeDelta) -> String {
        let signer = Signer::new(issuer.into(), tokens::jwt::EncodingKey::from_secret(secret));
        let selector = LabelSelector {
            include: Some(labels::build_set([("id:prefix", "acmeCo/")])),
            exclude: None,
        };
        signer
            .sign(cap, "acmeCo/task".into(), selector, ttl)
            .unwrap()
    }

    fn request_with(token: &str) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        *request.metadata_mut() = crate::Metadata::new().with_bearer_token(token).unwrap().0;
        request
    }

    /// Drive a token through the AuthN interceptor into an Authorizer, exactly
    /// as the tonic server stack does.
    fn authorizer_for(token: &str) -> Authorizer {
        let mut interceptor = authenticator().interceptor(CAP);
        let mut request =
            tonic::service::Interceptor::call(&mut interceptor, request_with(token)).unwrap();
        Authorizer::from_request(&mut request, false).unwrap()
    }

    fn id_set(id: &str) -> LabelSet {
        labels::build_set([("id", id)])
    }

    #[test]
    fn authenticate_rejects_bad_tokens() {
        let auth = authenticator();
        let ttl = tokens::TimeDelta::hours(1);

        // Missing bearer header.
        let err = auth
            .authenticate(&tonic::metadata::MetadataMap::new(), CAP)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        // Verifiable token, but missing a required capability bit.
        let md = request_with(&token(ISSUER, SECRET, CAP, ttl))
            .metadata()
            .clone();
        let err = auth.authenticate(&md, CAP | 0x1).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        // Wrong signing key *and* unknown issuer: the issuer is named.
        let md = request_with(&token("evil.example", b"evil", CAP, ttl))
            .metadata()
            .clone();
        let err = auth.authenticate(&md, CAP).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("unknown token issuer"), "{err}");

        // Valid signature (our key) but an issuer we don't recognize: rejected
        // even though the signature verifies and the capability is present.
        let md = request_with(&token("other.example", SECRET, CAP, ttl))
            .metadata()
            .clone();
        let err = auth.authenticate(&md, CAP).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("unknown token issuer"), "{err}");
    }

    #[test]
    fn shard_bearer_scopes_to_its_own_task() {
        let signer = Signer::new(ISSUER.into(), tokens::jwt::EncodingKey::from_secret(SECRET));
        let metadata = signer
            .shard_bearer(CAP, "acmeCo/foo/00112233-00000000")
            .unwrap();
        let token = std::str::from_utf8(crate::extract_bearer(&metadata.0).unwrap())
            .unwrap()
            .to_owned();

        // Another shard of the same task (differing only in key/r-clock) is in scope.
        authorizer_for(&token)
            .authorize_id("acmeCo/foo/ffffffff-00000000")
            .unwrap();

        // A sibling task sharing a name prefix is NOT in scope: the trailing '/'
        // in the signed `id:prefix` is what prevents `acmeCo/foo/` from matching
        // `acmeCo/foobar/...`.
        assert_eq!(
            authorizer_for(&token)
                .authorize_id("acmeCo/foobar/00000000-00000000")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
        );

        // A different tenant is denied.
        assert_eq!(
            authorizer_for(&token)
                .authorize_id("otherCo/foo/00000000-00000000")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
        );
    }

    #[test]
    fn from_request_fails_closed_unless_disarmed() {
        // No AuthN context (Authenticator interceptor not wired).
        let mut request = tonic::Request::new(());
        assert_eq!(
            Authorizer::from_request(&mut request, false)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated,
        );

        // Disarmed: absence of a token is treated as a trusted-local context,
        // which authorizes any scope and carries no claims.
        let authorizer = Authorizer::from_request(&mut request, true).unwrap();
        let authorized = authorizer.authorize(id_set("acmeCo/x")).unwrap();
        assert!(authorized.claims().is_none());
    }

    #[test]
    fn authorize_enforces_token_scope() {
        let ttl = tokens::TimeDelta::hours(1);

        // An in-scope shard id is authorized and carries the verified claims.
        let authorized = authorizer_for(&token(ISSUER, SECRET, CAP, ttl))
            .authorize(id_set("acmeCo/foo/bar"))
            .unwrap();
        assert_eq!(authorized.claims().unwrap().iss, ISSUER);

        // An out-of-scope shard id is denied.
        let err = authorizer_for(&token(ISSUER, SECRET, CAP, ttl))
            .authorize(id_set("otherCo/foo"))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        // A trusted-local Authorizer skips the scope check and exposes no claims.
        let authorized = Authorizer::trusted_local()
            .authorize(id_set("otherCo/foo"))
            .unwrap();
        assert!(authorized.claims().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_guard_bounds_work_by_token_lifetime() {
        // Work that finishes before expiry returns its value.
        let authorized = authorizer_for(&token(ISSUER, SECRET, CAP, tokens::TimeDelta::hours(1)))
            .authorize(id_set("acmeCo/x"))
            .unwrap();
        let out: Result<u32, tonic::Status> = authorized.expiry_guard(async { Ok(7) }).await;
        assert_eq!(out.unwrap(), 7);

        // Work that outlives the token is torn down with DeadlineExceeded.
        let authorized =
            authorizer_for(&token(ISSUER, SECRET, CAP, tokens::TimeDelta::seconds(30)))
                .authorize(id_set("acmeCo/x"))
                .unwrap();
        let out: Result<(), tonic::Status> = authorized.expiry_guard(std::future::pending()).await;
        assert_eq!(out.unwrap_err().code(), tonic::Code::DeadlineExceeded);

        // A trusted-local authorization never expires; work still completes.
        let authorized = Authorizer::trusted_local()
            .authorize(id_set("acmeCo/x"))
            .unwrap();
        let out: Result<u32, tonic::Status> = authorized.expiry_guard(async { Ok(9) }).await;
        assert_eq!(out.unwrap(), 9);
    }
}
