use models::authz::{CapabilityBundle, CapabilityMask, CapabilitySet};
use std::sync::Arc;

/// Requirement is a route's compile-time authorization precondition,
/// declared through its choice of [`Authority<R>`] extractor and evaluated
/// against the bearer's `capability_mask` claim during extraction.
///
/// A requirement is a fast-fail necessary condition on the *mask only*,
/// never a substitute for walk enforcement: the mask says nothing about
/// which grants the user actually holds, and an unmasked bearer passes any
/// requirement by definition. Full authorization is still evaluated against
/// the user grant walk under the extracted mask.
pub trait Requirement: Send + Sync + 'static {
    /// Capability bundles which the bearer's mask must enable, spelled in
    /// the same [`CapabilityBundle`] vocabulary the `capability_mask` claim
    /// speaks — a composite bundle or an individual capability's same-named
    /// bundle alike. A masked bearer whose mask does not cover the union of
    /// their capability bits is rejected at extraction with a structured
    /// `403` naming the missing capabilities, so that a client can request
    /// a fresh capability token which enables them.
    ///
    /// [`Self::required`] computes the union of the declared bundles'
    /// capability bits at evaluation time, because
    /// `CapabilityBundle::capabilities` is not a `const fn` and so cannot
    /// feed an associated const.
    const REQUIRED: &'static [CapabilityBundle];
    /// Whether the route refuses masked bearers outright, regardless of what
    /// their mask enables. This keys on the *presence* of the
    /// `capability_mask` claim — a mask which happens to enable everything
    /// is still a deliberately-reduced credential.
    const REQUIRE_UNMASKED: bool;

    /// The capability bits which [`Self::REQUIRED`] demands of the mask.
    fn required() -> CapabilitySet {
        Self::REQUIRED
            .iter()
            .map(|bundle| bundle.capabilities())
            .fold(CapabilitySet::empty(), |set, bits| set | bits)
    }
}

/// The default, vacuous [`Requirement`]: extraction is Maybe-shaped, so an
/// unauthenticated request extracts successfully and
/// [`crate::Envelope::claims`] remains the lazy per-callsite identity gate.
/// GraphQL depends on this: its one route serves every operation, so
/// identity errors must surface per-resolver rather than at extraction.
pub struct NoRequirement;

impl Requirement for NoRequirement {
    const REQUIRED: &'static [CapabilityBundle] = &[];
    const REQUIRE_UNMASKED: bool = false;
}

/// A [`Requirement`] which refuses masked bearers outright.
///
/// This is the fail-closed guard for surfaces whose authorization never
/// touches the capability mask — the `/admin` endpoints authorize via SQL
/// `internal.user_roles` rather than the snapshot walk — and for operations
/// which would let a masked bearer escape its mask, such as minting a
/// full-authority refresh credential.
pub struct RequireUnmasked;

impl Requirement for RequireUnmasked {
    const REQUIRED: &'static [CapabilityBundle] = &[];
    const REQUIRE_UNMASKED: bool = true;
}

/// A [`Requirement`] of the Viewer-bundle capability bits — what a legacy
/// `models::Capability::Read` walk requires.
///
/// Declared by routes whose entire authorization is a Read-capability walk
/// (`/api/v1/catalog/status`, `/api/v1/metrics`), so a mask shortfall is
/// rejected at extraction, before the handler runs. A test pins the bundle
/// to `bits_for_legacy(Read)` so the two cannot drift.
pub struct RequireViewer;

impl Requirement for RequireViewer {
    const REQUIRED: &'static [CapabilityBundle] = &[CapabilityBundle::Viewer];
    const REQUIRE_UNMASKED: bool = false;
}

/// Forbidden is the structured body of a capability-shortfall `403`.
///
/// Its shape is stable and machine-readable across the REST and GraphQL
/// surfaces: an MCP agent parses `missing_capabilities` and requests a fresh
/// capability token which enables them, so "you need capability X" must read
/// identically wherever it's said.
#[derive(Debug, serde::Serialize)]
pub struct Forbidden {
    /// Stable machine-readable code: `missing_capabilities` when the
    /// bearer's mask does not enable required capabilities,
    /// `unmasked_token_required` when the operation refuses masked bearers
    /// outright (which no re-mint can remedy), or
    /// `service_account_forbidden` when the operation is restricted to
    /// human users and the bearer is a service-account identity.
    pub error: &'static str,
    /// Human-readable description of the refusal.
    pub message: String,
    /// PascalCase names of capabilities which are required but not enabled
    /// by the bearer's mask. Empty for `unmasked_token_required` and
    /// `service_account_forbidden`.
    pub missing_capabilities: Vec<&'static str>,
}

impl Forbidden {
    pub fn missing_capabilities(missing: CapabilitySet) -> Self {
        let missing: Vec<&'static str> = missing.iter().map(|c| c.name()).collect();
        Self {
            error: "missing_capabilities",
            message: format!(
                "the bearer token's capability mask does not enable required capabilities: {}",
                missing.join(", "),
            ),
            missing_capabilities: missing,
        }
    }

    /// The mask-shortfall pre-check shared by every authorization policy
    /// function: a definitive denial — a pure function of the bearer's
    /// claims — when `mask` doesn't cover `required`, evaluated before the
    /// grant walk so the structured 403 names the missing capabilities
    /// without consulting (or disclosing anything about) the user's grants.
    pub fn required_covered(
        mask: CapabilityMask,
        required: impl Into<CapabilitySet>,
    ) -> Result<(), Self> {
        let required: CapabilitySet = required.into();
        let missing = required - mask.apply(required);
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Self::missing_capabilities(missing))
        }
    }

    pub fn unmasked_token_required() -> Self {
        Self {
            error: "unmasked_token_required",
            message: "this operation requires a full-authority token, but the bearer token carries a capability mask".to_string(),
            missing_capabilities: Vec::new(),
        }
    }

    pub fn service_account_forbidden() -> Self {
        Self {
            error: "service_account_forbidden",
            message: "this operation is restricted to human users, but the bearer token belongs to a service account".to_string(),
            missing_capabilities: Vec::new(),
        }
    }
}

impl axum::response::IntoResponse for Forbidden {
    fn into_response(self) -> axum::response::Response {
        (axum::http::StatusCode::FORBIDDEN, axum::Json(self)).into_response()
    }
}

/// AuthZError is a denial from an authorization policy evaluation, and the
/// distinction between its variants is load-bearing for
/// [`crate::Envelope::authorization_outcome`]: a `Retriable` denial may be
/// provisional — the Snapshot may simply not yet reflect a recently-committed
/// grant — and enters the refresh-and-retry machinery, while a `Definitive`
/// denial is a pure function of the bearer's verified claims (its capability
/// mask), which no future Snapshot can change, and fails immediately with the
/// structured `403` body.
#[derive(Debug)]
pub enum AuthZError {
    Retriable(tonic::Status),
    Definitive(Forbidden),
}

impl From<tonic::Status> for AuthZError {
    fn from(status: tonic::Status) -> Self {
        Self::Retriable(status)
    }
}

impl From<Forbidden> for AuthZError {
    fn from(forbidden: Forbidden) -> Self {
        Self::Definitive(forbidden)
    }
}

#[cfg(test)]
impl AuthZError {
    /// Map to the (HTTP status, message) pair which handler test harnesses
    /// snapshot as their `Outcome::Err` shape.
    pub(crate) fn into_status_message(self) -> (u16, String) {
        match self {
            Self::Retriable(status) => (
                tokens::rest::grpc_status_code_to_http(status.code()),
                status.message().to_string(),
            ),
            Self::Definitive(forbidden) => (403, forbidden.message),
        }
    }
}

/// Rejection is an error of Authority extraction.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    Envelope(#[from] crate::envelope::Rejection),
    #[error("{}", .0.message)]
    Forbidden(Forbidden),
}

impl From<tonic::Status> for Rejection {
    fn from(status: tonic::Status) -> Self {
        Self::Envelope(status.into())
    }
}

impl axum::response::IntoResponse for Rejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            Rejection::Envelope(rej) => rej.into_response(),
            Rejection::Forbidden(forbidden) => forbidden.into_response(),
        }
    }
}

/// Authority is the authenticated context of an API request: the extracted
/// [`crate::Envelope`] plus the capability mask computed from the bearer's
/// verified `capability_mask` claim, with the route's [`Requirement`] `R`
/// already evaluated against that mask.
///
/// Authority composes over `Envelope` extraction — JWT verification, the
/// `aud` check, refresh-token exchange, and the snapshot machinery are all
/// inherited unchanged. Call sites decompose it structurally —
/// `Authority { envelope: env, mask, .. }` — in the manner of axum's
/// `State`, and act on the parts directly.
///
/// Handlers extract Authority — never `Envelope` directly — so the bearer's
/// mask always reaches request processing. `Envelope`'s own
/// `FromRequestParts` impl exists to serve as the inner extraction this
/// composes over.
pub struct Authority<R: Requirement = NoRequirement> {
    /// The extracted request Envelope.
    pub envelope: crate::Envelope,
    /// The bearer's capability mask: a ceiling on the capabilities this
    /// request may exercise, applied wherever authority is derived from
    /// grants. An unmasked bearer carries
    /// [`CapabilityMask::ALL_CAPABILITIES`], which intersects as the
    /// identity.
    ///
    /// Whether the bearer *is* masked is a property of the claim's presence
    /// (`capability_mask.is_some()`), never of this value.
    pub mask: CapabilityMask,
    /// Private, so an Authority cannot be struct-literal constructed outside
    /// this module: a requirement-bearing `Authority<R>` is proof that `R`
    /// was evaluated, and [`Self::from_envelope`] is the only way to mint
    /// that proof.
    _requirement: std::marker::PhantomData<R>,
}

// Manual impl because a derived Debug would demand `R: Debug` of marker
// types which are never constructed.
impl<R: Requirement> std::fmt::Debug for Authority<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("envelope", &self.envelope)
            .field("mask", &self.mask)
            .finish()
    }
}

impl<R: Requirement> Authority<R> {
    /// Assemble an Authority over an already-extracted Envelope, evaluating
    /// `R` against its verified claims exactly as HTTP extraction does.
    ///
    /// This is the assembly path for callers which execute requests without
    /// HTTP extraction, such as test harnesses driving the GraphQL schema
    /// directly.
    pub fn from_envelope(envelope: crate::Envelope) -> Result<Self, Rejection> {
        let mask = evaluate_requirement::<R>(envelope.maybe_claims.maybe())?;
        Ok(Self {
            envelope,
            mask,
            _requirement: std::marker::PhantomData,
        })
    }
}

/// Evaluate `R` against a request's verified claims, if any, and compute the
/// bearer's capability mask.
///
/// A vacuous requirement (nothing required, masked bearers welcome) is
/// Maybe-shaped: an unauthenticated request passes, and its mask is the
/// identity because no grant-derived authority exists without claims. A
/// non-vacuous requirement is authenticated by definition, so a missing
/// bearer is rejected here rather than at a later `claims()` call.
fn evaluate_requirement<R: Requirement>(
    maybe_claims: Option<&crate::ControlClaims>,
) -> Result<CapabilityMask, Rejection> {
    let vacuous = R::REQUIRED.is_empty() && !R::REQUIRE_UNMASKED;

    let Some(claims) = maybe_claims else {
        if vacuous {
            return Ok(CapabilityMask::ALL_CAPABILITIES);
        }
        return Err(crate::envelope::MaybeControlClaims::unauthenticated().into());
    };
    let mask = CapabilityMask::from_claim(claims.capability_mask.as_deref());

    if R::REQUIRE_UNMASKED && claims.capability_mask.is_some() {
        return Err(Rejection::Forbidden(Forbidden::unmasked_token_required()));
    }

    let required = R::required();
    let missing = required - mask.apply(required);
    if !missing.is_empty() {
        return Err(Rejection::Forbidden(Forbidden::missing_capabilities(
            missing,
        )));
    }

    Ok(mask)
}

impl<R: Requirement> axum::extract::FromRequestParts<Arc<crate::App>> for Authority<R> {
    type Rejection = Rejection;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<crate::App>,
    ) -> Result<Self, Self::Rejection> {
        let envelope = crate::Envelope::from_request_parts(parts, state).await?;
        Self::from_envelope(envelope)
    }
}

// Empty impl allows aide to generate OpenAPI specs for handlers using this
// extractor. The extractor is an internal detail and doesn't appear in the
// API documentation.
impl<R: Requirement> aide::operation::OperationInput for Authority<R> {}

#[cfg(test)]
mod test {
    use super::evaluate_requirement;
    use super::{Authority, NoRequirement, Rejection, RequireUnmasked, Requirement};
    use models::authz::{Capability, CapabilityBundle, CapabilityMask, CapabilitySet};

    /// A representative capability-bearing requirement, as routes will
    /// declare once per-route requirements are put to use.
    struct RequireEdit;

    impl Requirement for RequireEdit {
        const REQUIRED: &'static [CapabilityBundle] =
            &[CapabilityBundle::CatalogRead, CapabilityBundle::SpecEdit];
        const REQUIRE_UNMASKED: bool = false;
    }

    fn claims(capability_mask: Option<Vec<&str>>) -> models::authorizations::ControlClaims {
        models::authorizations::ControlClaims {
            aud: "authenticated".to_string(),
            iat: 0,
            exp: u64::MAX,
            sub: uuid::Uuid::nil(),
            role: "authenticated".to_string(),
            email: None,
            capability_mask: capability_mask
                .map(|names| names.into_iter().map(String::from).collect()),
        }
    }

    #[test]
    fn test_no_requirement_is_maybe_shaped() {
        // An unauthenticated request extracts successfully; its mask is the
        // identity because no grant-derived authority exists without claims.
        assert_eq!(
            evaluate_requirement::<NoRequirement>(None).unwrap(),
            CapabilityMask::ALL_CAPABILITIES,
        );

        // An unmasked bearer carries the identity mask.
        assert_eq!(
            evaluate_requirement::<NoRequirement>(Some(&claims(None))).unwrap(),
            CapabilityMask::ALL_CAPABILITIES,
        );

        // A masked bearer extracts successfully — NoRequirement asserts
        // nothing — and carries the mask of its recognized names.
        assert_eq!(
            evaluate_requirement::<NoRequirement>(Some(&claims(Some(vec![
                "CatalogRead",
                "FutureCapability"
            ]))))
            .unwrap(),
            CapabilityMask::bounded(Capability::CatalogRead.into()),
        );

        // The empty mask is valid: an identity-only token extracts.
        assert_eq!(
            evaluate_requirement::<NoRequirement>(Some(&claims(Some(vec![])))).unwrap(),
            CapabilityMask::bounded(CapabilitySet::empty()),
        );
    }

    #[test]
    fn test_non_vacuous_requirements_are_authenticated() {
        // A requirement-bearing Authority is authenticated by definition:
        // a missing bearer is rejected at extraction, not deferred to a
        // later claims() call.
        for rejection in [
            evaluate_requirement::<RequireUnmasked>(None).unwrap_err(),
            evaluate_requirement::<RequireEdit>(None).unwrap_err(),
        ] {
            assert!(matches!(
                rejection,
                Rejection::Envelope(crate::envelope::Rejection::Status(ref status))
                    if status.code() == tonic::Code::Unauthenticated
            ));
        }
    }

    #[test]
    fn test_require_unmasked_keys_on_claim_presence() {
        // An unmasked bearer passes.
        assert_eq!(
            evaluate_requirement::<RequireUnmasked>(Some(&claims(None))).unwrap(),
            CapabilityMask::ALL_CAPABILITIES,
        );

        // Every masked bearer is refused — even one whose mask names every
        // capability this binary knows, because "is this bearer masked" is a
        // property of the claim's presence and never of the mask's value.
        let all_names: Vec<&str> = CapabilitySet::all().iter().map(|c| c.name()).collect();

        for mask in [vec![], all_names] {
            let rejection =
                evaluate_requirement::<RequireUnmasked>(Some(&claims(Some(mask)))).unwrap_err();

            let Rejection::Forbidden(forbidden) = rejection else {
                panic!("expected Forbidden, got {rejection:?}");
            };
            assert_eq!(forbidden.error, "unmasked_token_required");
            assert!(forbidden.missing_capabilities.is_empty());
        }
    }

    #[test]
    fn test_required_capabilities_check_the_mask() {
        // An unmasked bearer passes any requirement by definition.
        assert_eq!(
            evaluate_requirement::<RequireEdit>(Some(&claims(None))).unwrap(),
            CapabilityMask::ALL_CAPABILITIES,
        );

        // A mask covering the requirement passes, and the extracted mask is
        // the bearer's mask — not the requirement.
        assert_eq!(
            evaluate_requirement::<RequireEdit>(Some(&claims(Some(vec![
                "CatalogRead",
                "SpecEdit",
                "Delegate"
            ]))))
            .unwrap(),
            CapabilityMask::bounded(
                Capability::CatalogRead | Capability::SpecEdit | Capability::Delegate
            ),
        );

        // A partial mask is refused, naming exactly what's missing —
        // and unrecognized names are inert, so a mask of only unknown names
        // is missing the entire requirement, never treated as unmasked.
        for (mask, expect_missing) in [
            (vec!["CatalogRead", "Delegate"], vec!["SpecEdit"]),
            (vec!["FutureCapability"], vec!["CatalogRead", "SpecEdit"]),
        ] {
            let rejection =
                evaluate_requirement::<RequireEdit>(Some(&claims(Some(mask)))).unwrap_err();

            let Rejection::Forbidden(forbidden) = rejection else {
                panic!("expected Forbidden, got {rejection:?}");
            };
            assert_eq!(forbidden.error, "missing_capabilities");
            assert_eq!(forbidden.missing_capabilities, expect_missing);
        }
    }

    // === HTTP-level extraction tests ===
    //
    // These drive Authority<R> as a real axum extractor over a real router,
    // pinning the exact status and body of every refusal (the contract an
    // MCP agent parses) and the configuration an accepted request carries.
    // Behavior inherited from Envelope extraction — JWT verification, the
    // `aud` check — is exercised through the composed path.

    /// Report the extracted configuration: the identity, the wire claim,
    /// and the capabilities the mask enables.
    async fn probe<R: Requirement>(Authority { envelope, mask, .. }: Authority<R>) -> String {
        let enabled = if mask.has_all_capabilities() {
            "all".to_string()
        } else {
            let names: Vec<&'static str> = mask
                .apply(CapabilitySet::all())
                .iter()
                .map(|c| c.name())
                .collect();
            format!("{names:?}")
        };
        let claims = envelope.maybe_claims.maybe();

        format!(
            "sub: {:?}, capability_mask claim: {:?}, enabled: {enabled}",
            claims.map(|c| c.sub),
            claims.and_then(|c| c.capability_mask.as_deref()),
        )
    }

    async fn test_router() -> (axum::Router, tokens::jwt::EncodingKey) {
        let snapshot = crate::test_server::empty_snapshot().await;
        // The pool is never used: these tests present no dot-less
        // (refresh-token) bearers, which are the one extraction path that
        // reaches the database.
        let pg_pool = sqlx::PgPool::connect_lazy("postgres://unused-by-extraction").unwrap();
        let app = crate::test_server::build_app(pg_pool, snapshot, None);
        let encoding_key = app.control_plane_jwt_encode_key.clone();

        let router = axum::Router::new()
            .route("/none", axum::routing::get(probe::<NoRequirement>))
            .route("/unmasked", axum::routing::get(probe::<RequireUnmasked>))
            .route("/edit", axum::routing::get(probe::<RequireEdit>))
            .with_state(app);

        (router, encoding_key)
    }

    fn sign_token(
        encoding_key: &tokens::jwt::EncodingKey,
        aud: &str,
        expired: bool,
        capability_mask: Option<Vec<&str>>,
    ) -> String {
        let now = tokens::now();
        let exp = if expired {
            now - chrono::Duration::hours(1)
        } else {
            now + chrono::Duration::hours(1)
        };
        let claims = models::authorizations::ControlClaims {
            iat: (now - chrono::Duration::hours(2)).timestamp() as u64,
            exp: exp.timestamp() as u64,
            sub: uuid::Uuid::nil(),
            role: "authenticated".to_string(),
            aud: aud.to_string(),
            email: None,
            capability_mask: capability_mask
                .map(|names| names.into_iter().map(String::from).collect()),
        };
        jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, encoding_key).unwrap()
    }

    async fn fetch(router: &axum::Router, path: &str, bearer: Option<&str>) -> String {
        use tower::ServiceExt;

        let mut request = axum::http::Request::builder().uri(path);
        if let Some(bearer) = bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        let request = request.body(axum::body::Body::empty()).unwrap();

        let (parts, body) = router.clone().oneshot(request).await.unwrap().into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        format!("{}\n{}", parts.status, String::from_utf8_lossy(&body))
    }

    #[tokio::test]
    async fn test_http_no_requirement_configurations() {
        let (router, key) = test_router().await;

        // Unauthenticated requests extract; identity gating stays lazy.
        insta::assert_snapshot!(fetch(&router, "/none", None).await, @r"
        200 OK
        sub: None, capability_mask claim: None, enabled: all
        ");

        // An unmasked bearer: identity present, identity mask.
        let token = sign_token(&key, "authenticated", false, None);
        insta::assert_snapshot!(fetch(&router, "/none", Some(&token)).await, @r"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: None, enabled: all
        ");

        // A masked bearer: the claim carries through verbatim while the mask
        // enables only recognized names.
        let token = sign_token(
            &key,
            "authenticated",
            false,
            Some(vec!["CatalogRead", "FutureCapability"]),
        );
        insta::assert_snapshot!(fetch(&router, "/none", Some(&token)).await, @r#"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: Some(["CatalogRead", "FutureCapability"]), enabled: ["CatalogRead"]
        "#);

        // An empty mask mints an identity-only configuration.
        let token = sign_token(&key, "authenticated", false, Some(vec![]));
        insta::assert_snapshot!(fetch(&router, "/none", Some(&token)).await, @r#"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: Some([]), enabled: []
        "#);
    }

    #[tokio::test]
    async fn test_http_require_unmasked_responses() {
        let (router, key) = test_router().await;

        // An unmasked bearer passes.
        let token = sign_token(&key, "authenticated", false, None);
        insta::assert_snapshot!(fetch(&router, "/unmasked", Some(&token)).await, @r"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: None, enabled: all
        ");

        // A missing bearer is refused at extraction.
        insta::assert_snapshot!(fetch(&router, "/unmasked", None).await, @r"
        401 Unauthorized
        This is an authenticated API but the request is missing a required Authorization: Bearer token
        ");

        // Any masked bearer is refused, keyed on the claim's presence — even
        // a mask naming every capability this binary knows.
        let all_names: Vec<&str> = CapabilitySet::all().iter().map(|c| c.name()).collect();
        for mask in [vec![], all_names] {
            let token = sign_token(&key, "authenticated", false, Some(mask));
            let fixture = fetch(&router, "/unmasked", Some(&token)).await;
            insta::allow_duplicates! {
                insta::assert_snapshot!(fixture, @r#"
                403 Forbidden
                {"error":"unmasked_token_required","message":"this operation requires a full-authority token, but the bearer token carries a capability mask","missing_capabilities":[]}
                "#);
            }
        }
    }

    #[tokio::test]
    async fn test_http_required_capabilities_responses() {
        let (router, key) = test_router().await;

        // An unmasked bearer passes any requirement by definition.
        let token = sign_token(&key, "authenticated", false, None);
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: None, enabled: all
        ");

        // A covering mask passes, carrying its own mask — not the requirement.
        let token = sign_token(
            &key,
            "authenticated",
            false,
            Some(vec!["CatalogRead", "SpecEdit", "Delegate"]),
        );
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r#"
        200 OK
        sub: Some(00000000-0000-0000-0000-000000000000), capability_mask claim: Some(["CatalogRead", "SpecEdit", "Delegate"]), enabled: ["CatalogRead", "SpecEdit", "Delegate"]
        "#);

        // A missing bearer is refused at extraction.
        insta::assert_snapshot!(fetch(&router, "/edit", None).await, @r"
        401 Unauthorized
        This is an authenticated API but the request is missing a required Authorization: Bearer token
        ");

        // A partial mask is refused with a body naming exactly what's
        // missing: the stable contract an MCP agent parses to request a
        // fresh capability token which enables them.
        let token = sign_token(
            &key,
            "authenticated",
            false,
            Some(vec!["CatalogRead", "Delegate"]),
        );
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r#"
        403 Forbidden
        {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: SpecEdit","missing_capabilities":["SpecEdit"]}
        "#);

        // Unrecognized names are inert: a mask of only unknown names is
        // missing the entire requirement, never treated as unmasked.
        let token = sign_token(&key, "authenticated", false, Some(vec!["FutureCapability"]));
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r#"
        403 Forbidden
        {"error":"missing_capabilities","message":"the bearer token's capability mask does not enable required capabilities: CatalogRead, SpecEdit","missing_capabilities":["CatalogRead","SpecEdit"]}
        "#);
    }

    #[tokio::test]
    async fn test_http_inherits_envelope_authentication() {
        let (router, key) = test_router().await;

        // The `aud` check is inherited from Envelope extraction, and refuses
        // the bearer before any Requirement is considered.
        let token = sign_token(&key, "wrong-audience", false, None);
        insta::assert_snapshot!(fetch(&router, "/none", Some(&token)).await, @r"
        401 Unauthorized
        authorization bearer claims missing required `aud` of 'authenticated'
        ");

        // So is expiry...
        let token = sign_token(&key, "authenticated", true, None);
        insta::assert_snapshot!(fetch(&router, "/none", Some(&token)).await, @r"
        401 Unauthorized
        failed to verify token: ExpiredSignature
        ");

        // ...and signature verification of a malformed bearer.
        insta::assert_snapshot!(fetch(&router, "/none", Some("not.a.jwt")).await, @r"
        401 Unauthorized
        failed to verify token: Base64 error: Invalid last symbol 116, offset 2.
        ");
    }

    #[tokio::test]
    async fn test_http_envelope_refusal_precedes_requirements() {
        let (router, key) = test_router().await;

        // Each bearer below fails BOTH Envelope authentication and its
        // route's Requirement, pinning that the Envelope's 401 wins: were
        // Requirements evaluated first, these would be 403s. (The vacuous
        // `/none` cases in test_http_inherits_envelope_authentication can't
        // distinguish this ordering, because their Requirement passes any
        // bearer.)

        // An expired, masked bearer against RequireUnmasked...
        let token = sign_token(&key, "authenticated", true, Some(vec![]));
        insta::assert_snapshot!(fetch(&router, "/unmasked", Some(&token)).await, @r"
        401 Unauthorized
        failed to verify token: ExpiredSignature
        ");

        // ...and against required capabilities its mask doesn't cover, which
        // is the separate REQUIRED evaluation path.
        let token = sign_token(&key, "authenticated", true, Some(vec!["CatalogRead"]));
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r"
        401 Unauthorized
        failed to verify token: ExpiredSignature
        ");

        // A masked bearer whose signature verifies but whose `aud` doesn't:
        // the aud check refuses at a later point within Envelope extraction
        // than signature verification, and still precedes the Requirement.
        let token = sign_token(&key, "wrong-audience", false, Some(vec![]));
        insta::assert_snapshot!(fetch(&router, "/unmasked", Some(&token)).await, @r"
        401 Unauthorized
        authorization bearer claims missing required `aud` of 'authenticated'
        ");

        // The inverse direction: a mask which fully covers the requirement
        // cannot rescue a bearer signed with the wrong key.
        let wrong_key = jsonwebtoken::EncodingKey::from_secret(b"not-the-server-secret");
        let token = sign_token(
            &wrong_key,
            "authenticated",
            false,
            Some(vec!["CatalogRead", "SpecEdit"]),
        );
        insta::assert_snapshot!(fetch(&router, "/edit", Some(&token)).await, @r"
        401 Unauthorized
        failed to verify token: InvalidSignature
        ");

        // A malformed bearer offers no claims to evaluate a Requirement
        // against at all: requirement-bearing routes return the Envelope's
        // rejection verbatim.
        let malformed = fetch(&router, "/unmasked", Some("not.a.jwt")).await;
        assert_eq!(malformed, fetch(&router, "/edit", Some("not.a.jwt")).await);
        insta::assert_snapshot!(malformed, @r"
        401 Unauthorized
        failed to verify token: Base64 error: Invalid last symbol 116, offset 2.
        ");
    }
}
