//! Schema directives declaring how a field is authorized, and the field guard
//! that enforces the mutation-side directive.
//!
//! Every access-controlled field is authorized as a set of orthogonal
//! capability bits (`models::authz::Capability`) over a catalog prefix, in one
//! of two modes. Each mode has its own directive, rendered into the exported
//! SDL — the contract surface for human readers and for agents that read the
//! schema to plan what they can do:
//!
//! ```graphql
//! createInviteLink(catalogPrefix: Prefix!, ...): InviteLink!
//!   @requiresCapabilities(
//!     allOf: [CreateInviteLink],
//!     target: { argument: "catalogPrefix" }
//!   )
//! inviteLinks(...): InviteLinkConnection!
//!   @scopedByCapabilities(allOf: [QueryInviteLinks])
//! ```
//!
//! - `@requiresCapabilities` is a hard precondition: the caller must hold every
//!   listed `allOf` bit on the catalog prefix identified by `target`, or the
//!   field errors with `permission_denied`. The target either names the field
//!   argument carrying the prefix or describes how the resolver derives it
//!   (e.g. from the row a mutation targets).
//! - `@scopedByCapabilities` declares visibility scoping: the field never
//!   errors on authorization, and resolves the records under prefixes where
//!   the caller holds every listed `allOf` bit. Prefix filter arguments narrow
//!   within that scope; they can never widen it.
//!
//! Requirements are always declared as atomic bits — never as
//! `models::authz::CapabilityBundle`, which is grant-side vocabulary (what a
//! principal holds). The authorization check meets in the middle by expanding
//! the caller's bundles to bits and testing superset against the field's
//! declared atoms. `CapabilityBundle` deliberately has no GraphQL derive, so
//! a bundle in a directive is a type error, not a review comment.
//!
//! Enforcement placement follows from the mode. A `@requiresCapabilities` field
//! whose prefix is an argument attaches [`CapabilityGuard`] to run the check
//! before the resolver body; when the prefix is derived, the resolver calls
//! [`super::verify_authorization`] itself. `@scopedByCapabilities` fields
//! enforce by construction, filtering with the request's authorization
//! Snapshot (see [`super::authorized_prefixes`]).
//!
//! Resolvers apply these directives with ordinary `#[graphql(...)]` field
//! attributes. Argument-based hard preconditions use [`CapabilityGuard`];
//! derived targets and scoped queries pass the same capability bits to their
//! in-resolver authorization helpers.

use async_graphql::TypeDirective;

/// Where an operation obtains the catalog prefix over which its capabilities
/// are evaluated.
#[derive(async_graphql::OneofObject)]
#[graphql(name = "CapabilityTarget")]
pub(super) enum CapabilityTarget {
    /// The name of the GraphQL field argument carrying the catalog prefix.
    Argument(String),
    /// A stable description of how the resolver derives the catalog prefix.
    Derived(String),
}

/// Hard precondition: the caller must hold every listed capability bit on the
/// catalog prefix identified by `target`, or this field errors with
/// `permission_denied`.
#[TypeDirective(name = "requiresCapabilities", location = "FieldDefinition")]
pub(super) fn requires_capabilities(
    all_of: Vec<models::authz::Capability>,
    target: CapabilityTarget,
) {
}

/// Visibility scoping: this field resolves the records under catalog prefixes
/// where the caller holds every listed capability bit. It never errors on
/// authorization, and prefix filter arguments only narrow the scope.
#[TypeDirective(name = "scopedByCapabilities", location = "FieldDefinition")]
pub(super) fn scoped_by_capabilities(all_of: Vec<models::authz::Capability>) {}

/// The capability bits a field requires, as a set. Wraps
/// `models::authz::CapabilitySet` to add the `Display` that
/// [`super::verify_authorization`] renders into denial messages, which
/// `enumset::EnumSet` doesn't implement.
#[derive(Clone, Copy)]
pub(super) struct RequiredCapabilities(pub(super) models::authz::CapabilitySet);

impl std::fmt::Display for RequiredCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for bit in self.0.iter() {
            if !first {
                f.write_str(" | ")?;
            }
            write!(f, "{bit}")?;
            first = false;
        }
        Ok(())
    }
}

impl From<RequiredCapabilities> for models::authz::CapabilitySet {
    fn from(required: RequiredCapabilities) -> Self {
        required.0
    }
}

/// Field guard enforcing a set of capability bits over a catalog prefix
/// supplied as a field argument. Runs before the resolver body; a denial
/// errors as `permission_denied`, with a provisional denial against a stale
/// Snapshot following the standard refresh-and-retry path (see
/// [`super::verify_authorization`]).
pub(super) struct CapabilityGuard {
    prefix: String,
    capabilities: RequiredCapabilities,
}

impl CapabilityGuard {
    pub(super) fn new(prefix: &str, capabilities: impl Into<models::authz::CapabilitySet>) -> Self {
        Self {
            prefix: prefix.to_string(),
            capabilities: RequiredCapabilities(capabilities.into()),
        }
    }
}

impl async_graphql::Guard for CapabilityGuard {
    async fn check(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<()> {
        let env = ctx.data::<crate::Envelope>()?;
        super::verify_authorization(env, &self.prefix, self.capabilities).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn capability_contract_is_exported_in_sdl() {
        let sdl = super::super::schema_sdl();

        assert!(sdl.contains(
            r#"@requiresCapabilities(allOf: [CreateInviteLink], target: {argument: "catalogPrefix"})"#
        ));
        assert!(sdl.contains(
            r#"@requiresCapabilities(allOf: [DeleteInviteLink], target: {derived: "the invite link's catalogPrefix"})"#
        ));
        assert!(sdl.contains("@scopedByCapabilities(allOf: [QueryInviteLinks])"));
    }
}
