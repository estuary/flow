use enumset::{EnumSet, EnumSetType};
use serde::{Deserialize, Serialize};

/// A set of fine-grained authorization capabilities. Used throughout the
/// authorization BFS and at authorization-check call sites.
pub type CapabilitySet = EnumSet<Capability>;

#[derive(EnumSetType, Debug)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(name = "CapabilityBit", rename_items = "PascalCase")
)]
pub enum Capability {
    CatalogRead,
    JournalRead,
    JournalAppend,
    SpecEdit,
    CreateGrant,
    DeleteGrant,
    CreateInviteLink,
    // `ViewDataPlanePrivateNetworking` permits reading per-data-plane
    // private-networking configuration (such as the `private_links`
    // column).
    ViewDataPlanePrivateNetworking,
    // `ModifyDataPlanePrivateNetworking` permits mutating that same
    // configuration; the data-plane controller converges to it.
    ModifyDataPlanePrivateNetworking,
    // `ViewBilling` permits reading a tenant's billing surface (contact,
    // payment methods, invoices).
    ViewBilling,
    // `EditBilling` permits mutating a tenant's billing contact
    EditBilling,
    Delegate,
    Assume,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    feature = "sqlx-support",
    derive(sqlx::Type),
    sqlx(type_name = "capability_bundle", rename_all = "snake_case")
)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "PascalCase")
)]
pub enum CapabilityBundle {
    View,
    Write,
    Edit,
    Admin,
    ManageBilling,
    ManageUsers,
    ManageDataPlanes,
    Delegate,
    Assume,
}

impl CapabilityBundle {
    /// Every bundle variant, in declaration order.
    pub const ALL: [Self; 9] = [
        Self::View,
        Self::Write,
        Self::Edit,
        Self::Admin,
        Self::ManageBilling,
        Self::ManageUsers,
        Self::ManageDataPlanes,
        Self::Delegate,
        Self::Assume,
    ];

    /// Returns every bundle whose full capability set is covered by `set`.
    /// These are exactly the bundles that would match `set` under a
    /// superset filter such as the `prefixes` query's `withCapabilities`.
    pub fn covered_by(set: CapabilitySet) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|bundle| set.is_superset(bundle.capabilities()))
            .collect()
    }

    pub fn capabilities(&self) -> CapabilitySet {
        use Capability::*;
        match self {
            // `ViewDataPlanePrivateNetworking` is bundled here because
            // `read` on a data-plane prefix already conveys deploy-level
            // trust (it's what authorizes deploying tasks into the plane),
            // so viewing the plane's private-networking configuration comes
            // with it. Mutating that configuration stays in the separately
            // granted `ManageDataPlanes` bundle.
            Self::View => CatalogRead | JournalRead | ViewDataPlanePrivateNetworking,
            Self::Write => Self::View.capabilities() | JournalAppend,
            // `Edit` is the bundle for users who exercise authority
            // over a catalog namespace, not just observe it:
            // - `SpecEdit`: publish or modify specs at this prefix.
            // - `Delegate`: enters the user's `user_grant` into the
            //   `role_grants` graph for authorization checks. Without
            //   `Delegate` the user's BFS terminates at the user_grant
            //   edge, leaving them authorized only at their direct
            //   grant's prefix and blind to anything reachable via
            //   `role_grants`. Editors need this because they publish
            //   specs that reference resources at prefixes connected to
            //   theirs via role_grants (think `acmeCo/foo` reading from
            //   `sharedCo/upstream/` through an `acmeCo/ -> sharedCo/`
            //   edge), and publish-time validation has to cover the
            //   same graph the eventual running task does. `Delegate`
            //   is per-grant rather than implied by any capability so
            //   that different bundles can take different positions on
            //   chaining: `View` deliberately omits it so view access
            //   to `acmeCo/` does not silently leak through to every
            //   upstream `acmeCo/` consumes from (the `C reads B reads
            //   A` privacy case). Editors opt in because they're the
            //   bundle whose purpose is to act over the namespace,
            //   which intrinsically reaches everything the namespace
            //   reaches.
            // - `JournalRead` grants an editor the ability to test or preview the
            //   tasks they author (e.g. `flowctl preview` against a
            //   derivation under edit).
            // - `CatalogRead` (inherited from `View`): on a separate
            //   axis from the bits above. Included because editing
            //   without seeing the model is awkward, not because of
            //   functional coupling.
            Self::Edit => CatalogRead | JournalRead | SpecEdit | Delegate,
            Self::Admin => {
                Self::Edit.capabilities()
                    // Because `Edit` doesn't bundle `JournalAppend`,
                    // and we haven't unbundled things from Admin yet
                    | Self::Write.capabilities()
                    | Self::ManageUsers.capabilities()
                    | Self::ManageBilling.capabilities()
                    | Self::ManageDataPlanes.capabilities()
            }
            Self::ManageBilling => ViewBilling | EditBilling,
            Self::ManageUsers => CreateGrant | DeleteGrant | CreateInviteLink,
            Self::ManageDataPlanes => {
                ViewDataPlanePrivateNetworking | ModifyDataPlanePrivateNetworking
            }
            Self::Delegate => Delegate.into(),
            Self::Assume => Assume.into(),
        }
    }
}

/// Maps a legacy capability to the bundle it explicitly selects.
/// Grant-shaped objects (invite links, grants) use this to echo the
/// selection that was made, where `CapabilityBundle::covered_by` would
/// also list every bundle the selection happens to imply (an `admin`
/// grant covers `ManageUsers`, `ManageBilling`, and so on).
pub fn bundles_for_legacy(capability: super::Capability) -> Vec<CapabilityBundle> {
    match capability {
        super::Capability::None => Vec::new(),
        super::Capability::Read => vec![CapabilityBundle::View],
        super::Capability::Write => vec![CapabilityBundle::Write],
        super::Capability::Admin => vec![CapabilityBundle::Admin],
    }
}

pub fn bits_for_legacy(capability: super::Capability) -> CapabilitySet {
    match capability {
        super::Capability::None => CapabilitySet::empty(),
        super::Capability::Read => CapabilityBundle::View.capabilities(),
        super::Capability::Write => CapabilityBundle::Write.capabilities(),
        super::Capability::Admin => CapabilityBundle::Admin.capabilities(),
    }
}

impl From<super::Capability> for CapabilitySet {
    fn from(capability: super::Capability) -> Self {
        bits_for_legacy(capability)
    }
}
