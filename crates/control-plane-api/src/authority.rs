/// Authority is the caller's resolved authorization inputs for one request: the
/// Snapshot's grant tables, the authenticated user, and the scope their token
/// confines them to.
///
/// It exists so that a scope cannot be forgotten. Handlers and resolvers ask an
/// Authority their authorization questions rather than calling
/// `tables::UserGrant` directly, and because the Authority already carries the
/// scope there is no per-call-site argument to omit — the failure mode of
/// threading an `Option` through a dozen call sites, where the value that
/// compiles by default is also the unscoped one, does not arise.
///
/// A call site that must answer a question without a scope has to build its own
/// `tables::AuthScope::unscoped()` and say why, which makes the complete set of
/// unscoped authorizations greppable.
pub struct Authority<'a> {
    role_grants: &'a tables::RoleGrants,
    user_grants: &'a tables::UserGrants,
    user_id: uuid::Uuid,
    /// The caller's email, or "user" when the token carries none. Only for
    /// operator-facing error text.
    user_email: &'a str,
    scope: tables::AuthScope<'a>,
}

impl<'a> Authority<'a> {
    /// Resolve authority from a Snapshot and verified control-plane claims.
    ///
    /// Resolving the token's scope walks the role-grant graph out from its
    /// prefix. That is the same order of work as a single authorization check
    /// (which walks the graph out from the user), so an Authority is resolved per
    /// request rather than memoized: memoizing would require the scope to own its
    /// prefixes instead of borrowing them from the Snapshot.
    pub fn resolve(snapshot: &'a crate::Snapshot, claims: &'a crate::ControlClaims) -> Self {
        let scope = match &claims.scope_prefix {
            Some(prefix) => tables::AuthScope::resolve(&snapshot.role_grants, prefix),
            None => tables::AuthScope::unscoped(),
        };

        Self {
            role_grants: &snapshot.role_grants,
            user_grants: &snapshot.user_grants,
            user_id: claims.sub,
            user_email: claims.email.as_deref().unwrap_or("user"),
            scope,
        }
    }

    /// Assemble authority from its parts.
    ///
    /// Prefer [`Self::resolve`], which derives them from a request's Snapshot and
    /// claims. This exists for tests and for callers holding grant tables that
    /// did not come from a Snapshot.
    pub fn new(
        role_grants: &'a tables::RoleGrants,
        user_grants: &'a tables::UserGrants,
        user_id: uuid::Uuid,
        user_email: &'a str,
        scope: tables::AuthScope<'a>,
    ) -> Self {
        Self {
            role_grants,
            user_grants,
            user_id,
            user_email,
            scope,
        }
    }

    /// The authenticated user.
    pub fn user_id(&self) -> uuid::Uuid {
        self.user_id
    }

    /// The scope confining this caller.
    pub fn scope(&self) -> &tables::AuthScope<'a> {
        &self.scope
    }

    /// The caller's email, or "user" when their token carries none. For
    /// operator-facing error text only.
    pub fn user_email(&self) -> &'a str {
        self.user_email
    }

    /// Whether the caller holds `capability` on `prefix_or_name`, as a pure check
    /// against the request's Snapshot and scope.
    ///
    /// This is the visibility gate: use it to hide a field or filter a list,
    /// failing closed to an empty or default value. Unlike [`Self::evaluate`] it
    /// neither errors nor asks for a Snapshot refresh on a negative result,
    /// because momentarily hiding a field against a slightly-stale Snapshot is
    /// the correct, low-cost behavior.
    pub fn is_authorized(
        &self,
        prefix_or_name: &str,
        capability: impl Into<models::authz::CapabilitySet>,
    ) -> bool {
        tables::UserGrant::is_authorized(
            self.role_grants,
            self.user_grants,
            self.user_id,
            prefix_or_name,
            capability,
            &self.scope,
        )
    }

    /// The caller's legacy `capability` column value at `prefix_or_name`, or None
    /// if they hold none there.
    pub fn capability_at(&self, prefix_or_name: &str) -> Option<models::Capability> {
        tables::UserGrant::get_user_capability(
            self.role_grants,
            self.user_grants,
            self.user_id,
            prefix_or_name,
            &self.scope,
        )
    }

    /// Every prefix the caller is authorized to, mapped to the capabilities they
    /// hold there. Already narrowed by the scope.
    pub fn reachable_prefixes(
        &self,
    ) -> std::collections::BTreeMap<&'a str, (models::authz::CapabilitySet, models::Capability)>
    {
        tables::UserGrant::reachable_prefixes(
            self.role_grants,
            self.user_grants,
            self.user_id,
            &self.scope,
        )
    }

    /// Evaluate whether the caller holds at least `min_capability` on every one
    /// of `prefixes_or_names`, returning a policy result shaped for
    /// [`crate::Envelope::authorization_outcome`].
    ///
    /// This is the hard gate for mutations and access-controlled queries: a
    /// denial becomes `permission_denied`, and a provisional denial against a
    /// stale Snapshot follows the standard refresh-and-retry path.
    pub fn evaluate<Iter, S, C>(
        &self,
        min_capability: C,
        prefixes_or_names: Iter,
    ) -> crate::AuthZResult<()>
    where
        Iter: IntoIterator<Item = S>,
        S: AsRef<str> + std::fmt::Display,
        C: Into<models::authz::CapabilitySet> + std::fmt::Display + Copy,
    {
        for prefix_or_name in prefixes_or_names {
            if !self.is_authorized(prefix_or_name.as_ref(), min_capability) {
                return Err(tonic::Status::permission_denied(format!(
                    "{} is not authorized to access prefix or name '{prefix_or_name}' with required capability {min_capability}{}",
                    self.user_email,
                    self.scope_suffix(),
                )));
            }
        }
        Ok((None, ()))
    }

    /// Looks up the caller's capability at each item of `prefixes_or_names` and
    /// calls `attach` with the item and that capability, collecting the `Some`
    /// results.
    pub fn attach_capabilities<Iter, F, T>(&self, prefixes_or_names: Iter, mut attach: F) -> Vec<T>
    where
        Iter: IntoIterator<Item = String>,
        F: FnMut(String, Option<models::Capability>) -> Option<T>,
    {
        prefixes_or_names
            .into_iter()
            .flat_map(|prefix| {
                let capability = self.capability_at(&prefix);
                attach(prefix, capability)
            })
            .collect()
    }

    /// Names the scope in a denial message, so an operator can tell "you were
    /// never granted this" apart from "your token is confined elsewhere" — the
    /// two look identical to a caller who does in fact hold the grant.
    fn scope_suffix(&self) -> String {
        match self.scope.prefix() {
            Some(prefix) => format!(" within the token's scope of '{prefix}'"),
            None => String::new(),
        }
    }
}
