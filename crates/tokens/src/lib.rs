use std::sync::Arc;

pub mod jwt;
pub mod rest;
mod stream;
mod watch;

// Re-export CancellationToken and friends from tokio-util.
// These are foundational for signal handling in this crate and its users.
pub use tokio_util::sync::{
    CancellationToken, WaitForCancellationFuture, WaitForCancellationFutureOwned,
};

// Re-export of chrono::DateTime<Utc>, as the foundational datetime type.
pub type DateTime = chrono::DateTime<chrono::Utc>;

// Re-export of chrono::TimeDelta, as the foundational duration type.
pub use chrono::TimeDelta;

pub use rest::RestSource;
pub use stream::StreamSource;
pub use watch::{fixed, manual, map, watch};

/// Source is a trait for producing an associated Token type on demand.
pub trait Source: Send + Sized + 'static {
    type Token: Send + Sync + 'static;

    /// Future type that, when it resolves, signals early token revocation.
    /// Use `std::future::Pending<()>` for sources that never revoke early.
    type Revoke: std::future::Future<Output = ()> + Send;

    /// Refresh a Token from the Source.
    ///
    /// `started` is when the overall refresh operation began,
    /// and is held constant across retries.
    ///
    /// Refresh returns a future that resolves to:
    /// - Ok(Ok((Token, valid_for, revoke))) if the refresh was successful,
    ///   where `valid_for` is the remaining lifetime of the yielded Token,
    ///   and `revoke` is a future that, when it resolves, signals that the
    ///   Token should be refreshed immediately rather than waiting for
    ///   `valid_for` to elapse. Sources that don't need early revocation
    ///   should use `std::future::pending()`.
    /// - Ok(Err(retry_after)) if the refresh result was indeterminate
    ///   and should be retried after `retry_after`.
    /// - Err(err) if a client-facing error occurred.
    fn refresh(
        &mut self,
        started: DateTime,
    ) -> impl std::future::Future<
        Output = tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>>,
    > + Send;
}

/// Verified is a type-safe wrapper around verified claims: holding one is a
/// type-level proof that authentication ran. Its fields are private, so it
/// can be constructed only through assert_authenticity() — the single root
/// through which every credential-verification path mints its proof.
// `Clone` is required because Verified is used in tonic request
// extensions (`http::Extensions`, whose values must be `Clone`).
#[derive(Debug, Clone)]
pub struct Verified<Claims>(Claims, DateTime);

impl<Claims> Verified<Claims> {
    /// Assert that `claims` were established by verification of a presented
    /// credential — whether cryptographic (a JWT signature; see jwt::verify)
    /// or stateful (a database check of a presented secret).
    ///
    /// SECURITY: this is the single root through which a request becomes
    /// authenticated. Every call site is an authentication entry point — code
    /// past it runs trusting `claims`. Each authorized caller is itself a
    /// credential-verification routine, invoked with claims derived from the
    /// credential it just verified. The complete set is:
    ///   - `tokens::jwt::verify` — verified a JWT signature (cryptographic)
    ///   - control-plane-api `authenticate_refresh_token` — verified a
    ///     refresh-token secret against the database (stateful)
    ///
    /// Adding a caller adds a way for a request to become authenticated and
    /// MUST get human security review — do not add one by analogy to an
    /// existing site. The `authenticity_census` test below enumerates these
    /// call sites and fails the build if the set changes, so a new caller
    /// cannot land unreviewed.
    pub fn assert_authenticity(claims: Claims, expiry: DateTime) -> Self {
        Self(claims, expiry)
    }

    /// Return the verified claims.
    #[inline]
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    /// Return the token's expiry.
    #[inline]
    pub fn expiry(&self) -> DateTime {
        self.1
    }

    /// Return the remaining TimeDelta of the token.
    #[inline]
    pub fn valid_for(&self) -> TimeDelta {
        self.1 - now()
    }
}

/// Refresh represents the result of a Token refresh operation.
pub struct Refresh<Token> {
    result: tonic::Result<Token>,
    version: u64,
    expired: CancellationToken,
}

impl<Token> Refresh<Token> {
    /// Result of this token refresh.
    #[inline]
    pub fn result(&self) -> tonic::Result<&Token> {
        self.result.as_ref().map_err(Clone::clone)
    }
    /// Returns true if this Refresh has expired.
    pub fn is_expired(&self) -> bool {
        self.expired.is_cancelled()
    }
    /// Future that resolves when this Refresh has expired.
    #[inline]
    pub fn expired<'a>(&'a self) -> WaitForCancellationFuture<'a> {
        self.expired.cancelled()
    }
    /// Owned Future that resolves when this Refresh has expired.
    #[inline]
    pub fn expired_owned(&self) -> WaitForCancellationFutureOwned {
        self.expired.clone().cancelled_owned()
    }
    /// Version of this Token refresh.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl<Token> std::fmt::Debug for Refresh<Token> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let token_type = std::any::type_name::<Token>();

        f.debug_struct(&format!("Refresh<{token_type}>"))
            .field("result", &self.result().map(|_ok| "Ok(...)"))
            .field("version", &self.version)
            .field("is_expired", &self.is_expired())
            .finish()
    }
}

/// Watch provides access to a Token that is periodically refreshed.
pub trait Watch<Token>: Send + Sync {
    /// Get the current Refresh of the Token.
    fn token(&self) -> Arc<Refresh<Token>>;
    /// Get the current version of the Token.
    /// This is more efficient than calling token().version.
    fn version(&self) -> u64;
}

/// PendingWatch wraps a dyn Watch which may not yet be ready for use.
pub struct PendingWatch<Token> {
    inner: Arc<dyn Watch<Token>>,
    signal: CancellationToken,
}

// Manual Clone impl to avoid requiring Token: Clone
impl<Token> Clone for PendingWatch<Token> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            signal: self.signal.clone(),
        }
    }
}

impl<Token> PendingWatch<Token> {
    /// Return a future that resolves when the PendingWatch becomes ready,
    /// yielding a reference to the dyn Watch.
    #[inline]
    pub fn ready<'a>(
        &'a self,
    ) -> impl std::future::Future<Output = &'a Arc<dyn Watch<Token>>> + 'a {
        let Self { inner, signal } = self;

        async move {
            signal.cancelled().await;
            inner
        }
    }

    /// Return a future that resolves when the PendingWatch becomes ready,
    /// yielding an owned dyn Watch.
    #[inline]
    pub fn ready_owned(self) -> impl std::future::Future<Output = Arc<dyn Watch<Token>>> {
        let Self { inner, signal } = self;

        async move {
            signal.cancelled().await;
            inner
        }
    }

    /// Return a future that resolves when the PendingWatch becomes ready.
    #[inline]
    pub fn ready_signal(&self) -> WaitForCancellationFutureOwned {
        self.signal.clone().cancelled_owned()
    }

    /// Borrow the underlying Watch without awaiting readiness. Before the first
    /// refresh its `token()` is an "awaiting initial refresh" error; callers
    /// that require a resolved Token should `ready().await` instead. Intended
    /// for synchronous reads once readiness has already been established.
    #[inline]
    pub fn watch(&self) -> &Arc<dyn Watch<Token>> {
        &self.inner
    }

    /// Consume this PendingWatch and return its components.
    #[inline]
    pub fn into_parts(self) -> (Arc<dyn Watch<Token>>, CancellationToken) {
        (self.inner, self.signal)
    }
}

/// Return the current DateTime.
///
/// This routine is intended for Tokens and JWTs which generally use Unix
/// timestamps for validity periods that are communicated between systems.
///
/// The timestamps returned by this routine are aware of tokio test time,
/// and will return coherent values in paused testing contexts where
/// tokio::time::advance() or auto-advance is used.
///
/// Use this routine over SystemTime or jsonwebtoken::get_current_timestamp().
pub fn now() -> DateTime {
    // In testing contexts, use a fixed point to map between Instant and DateTime,
    // allowing tokio test time to influence the result.
    // CurrentThread is the default for tokio::test / sqlx::test, is not
    // typically used outside of tests and WASM, and is the only executor that
    // supports paused time.
    if cfg!(debug_assertions)
        && tokio::runtime::Handle::try_current()
            .ok()
            .map(|h| h.runtime_flavor())
            == Some(tokio::runtime::RuntimeFlavor::CurrentThread)
    {
        // TIME_POINT is a (Instant, DateTime) pair captured at the same time.
        // It allows for mapping between Instant and DateTime.
        //
        // Beware! There's potential for clock drift over time due to NTP steps
        // or because of subtle drifts between the two clocks. This is acceptable
        // in tests (only).
        static TIME_POINT: std::sync::LazyLock<(std::time::Instant, DateTime)> =
            std::sync::LazyLock::new(|| (std::time::Instant::now(), chrono::Utc::now()));

        let (start_instant, start_unix) = *TIME_POINT;
        let elapsed = tokio::time::Instant::now()
            .duration_since(tokio::time::Instant::from_std(start_instant));

        start_unix + elapsed
    } else {
        chrono::Utc::now()
    }
}

// SECURITY GUARD: a census of every `Verified::assert_authenticity` call site.
//
// Each call site is an authentication entry point (see the docs on that
// function): code past it runs with the request treated as authenticated. This
// test enumerates the authorized callers and fails if the set — or the number
// of calls in any file — changes. A new way to authenticate a request thus
// cannot land without a human editing the allowlist below, which is the cue to
// get security review. An agent copying an existing call site trips this.
#[cfg(test)]
mod authenticity_census {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    // Authorized call sites, as (workspace-relative path, number of calls).
    // Editing this list is a security-relevant change — review before you do,
    // do not adjust it merely to make the test pass.
    const AUTHORIZED_CALLERS: &[(&str, usize)] = &[
        // Cryptographic: claims minted from a verified JWT signature.
        ("crates/tokens/src/jwt.rs", 1),
        // Stateful: authenticate_refresh_token, a database check of a presented
        // refresh-token secret.
        ("crates/control-plane-api/src/server/mod.rs", 1),
    ];

    fn scan(dir: &Path, needle: &str, root: &Path, out: &mut BTreeMap<String, usize>) {
        for entry in std::fs::read_dir(dir).expect("readable directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                // Skip build artifacts; everything else is source to audit.
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                scan(&path, needle, root, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let src = std::fs::read_to_string(&path).expect("readable source file");
                let count = src.matches(needle).count();
                if count > 0 {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.insert(rel, count);
                }
            }
        }
    }

    #[test]
    fn assert_authenticity_callers_are_known() {
        // Assemble the search needle from fragments so this guard file does not
        // count as one of its own matches.
        let needle = format!("::{}(", "assert_authenticity");

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root resolves");

        let mut found = BTreeMap::new();
        scan(&root.join("crates"), &needle, &root, &mut found);

        let expected: BTreeMap<String, usize> = AUTHORIZED_CALLERS
            .iter()
            .map(|(path, count)| (path.to_string(), *count))
            .collect();

        assert_eq!(
            found, expected,
            "\n\
             The set of `Verified::assert_authenticity` call sites changed.\n\
             Each is an AUTHENTICATION ENTRY POINT — code past it runs with the request\n\
             treated as authenticated. A new or removed caller is a security-relevant\n\
             change that needs human review; do not just edit AUTHORIZED_CALLERS to make\n\
             this pass.\n\
             expected = {expected:#?}\n\
             found    = {found:#?}\n"
        );
    }
}
