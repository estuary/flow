//! Transitional compatibility shim for the GraphQL `Capability` →
//! `LegacyCapability` rename.
//!
//! `models::Capability` now publishes under the name `LegacyCapability`. This
//! enum re-publishes the identical `none`/`read`/`write`/`admin` values under
//! the original name `Capability`, so client operations written against the
//! pre-rename schema keep validating while the frontend migrates to the
//! `LegacyCapability`-typed inputs. It is wired only to the deprecated
//! `capability` / `minCapability` input positions and is deleted once those
//! inputs are removed.

/// Deprecated alias of `LegacyCapability`, preserved under the original
/// `Capability` name while clients migrate off it. Do not use in new
/// operations; prefer `withCapability` (on the `prefixes` filter) and
/// `capabilityLegacy` (on `createInviteLink`). Removed once the deprecated
/// inputs are dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, async_graphql::Enum)]
#[graphql(name = "Capability", rename_items = "lowercase")]
pub enum CapabilityCompat {
    None,
    Read,
    Write,
    Admin,
}

impl From<CapabilityCompat> for models::Capability {
    fn from(value: CapabilityCompat) -> Self {
        match value {
            CapabilityCompat::None => models::Capability::None,
            CapabilityCompat::Read => models::Capability::Read,
            CapabilityCompat::Write => models::Capability::Write,
            CapabilityCompat::Admin => models::Capability::Admin,
        }
    }
}
