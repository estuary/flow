use enumset::{EnumSet, EnumSetType};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// The subject of an authorization check: whose grants to evaluate and any
/// restrictions on their use.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subject {
    pub user_id: uuid::Uuid,
    pub capability_mask: Option<CapabilityMask>,
}

impl Subject {
    /// Creates a Subject covering the entirety of a user's grants, with no additional restrictions.
    pub fn unrestricted(user_id: uuid::Uuid) -> Self {
        Self {
            user_id,
            capability_mask: None,
        }
    }
}

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
    QueryServiceAccounts,
    CreateServiceAccount,
    CreateApiKey,
    RevokeApiKey,
    // `ViewSecret` permits listing the secrets under a prefix: their catalog
    // names and lifecycle ids, never their documents or plaintext.
    ViewSecret,
    // `EditSecret` permits setting and deleting secrets.
    EditSecret,
    // `DecryptSecret` permits a user to decrypt a secret and read its
    // plaintext value. Task decryption is authorized separately, by the
    // sibling rule, and does not consult this bit.
    DecryptSecret,
    Delegate,
    Assume,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// The capability bundle has two function jobs:
/// 1) These bundles are stored within the grant part of the database, and are
/// used along side of the capability column to enable graph traversals. Beyond
/// storage these things are expanded into capability bits in order to support
/// a more fine grain access control between nodes of the role graph.
/// 2) To provide the users with an interface for selecting capabilities that
/// can be used as part of the capability mask feature.
///
/// This means that there are two representations of this, while there isn't
/// really another way to do this currently, we are leveraging strum to provide
/// access to parsing of the enum in pascal case and serde to provide parsing of
/// this in snake case. The snake case lives within the database, while the
/// external representation is accepted by the users. The only overlap these
/// features have is during part of `Display`/`Debug` when the strum
/// representation is printed.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "PascalCase")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    feature = "sqlx-support",
    derive(sqlx::Type),
    sqlx(type_name = "capability_bundle", rename_all = "snake_case")
)]
pub enum CapabilityBundle {
    Viewer,
    Writer,
    Editor,
    Admin,
    Billing,
    TeamAdmin,
    ManageServiceAccounts,
    ManageDataPlane,
    Delegate,
    Assume,
}

impl CapabilityBundle {
    pub fn capabilities(&self) -> CapabilitySet {
        use Capability::*;
        match self {
            // `ViewDataPlanePrivateNetworking` is bundled here because
            // `read` on a data-plane prefix already conveys deploy-level
            // trust (it's what authorizes deploying tasks into the plane),
            // so viewing the plane's private-networking configuration comes
            // with it. Mutating that configuration stays in the separately
            // granted `ManageDataPlane` bundle.
            Self::Viewer => CatalogRead | JournalRead | ViewDataPlanePrivateNetworking,
            Self::Writer => Self::Viewer.capabilities() | JournalAppend,
            // `Editor` is the bundle for users who exercise authority
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
            //   chaining: `Viewer` deliberately omits it so view access
            //   to `acmeCo/` does not silently leak through to every
            //   upstream `acmeCo/` consumes from (the `C reads B reads
            //   A` privacy case). Editors opt in because they're the
            //   bundle whose purpose is to act over the namespace,
            //   which intrinsically reaches everything the namespace
            //   reaches.
            // - `JournalRead` grants an editor the ability to test or preview the
            //   tasks they author (e.g. `flowctl preview` against a
            //   derivation under edit).
            // - `CatalogRead` (inherited from `Viewer`): on a separate
            //   axis from the bits above. Included because editing
            //   without seeing the model is awkward, not because of
            //   functional coupling.
            // - `ViewSecret` / `EditSecret` / `DecryptSecret`: the secrets an
            //   editor's tasks reference are theirs to manage. Publish
            //   authority is already disclosure authority — someone who can
            //   publish a task at a prefix can point a connector at any
            //   sibling secret and exfiltrate its plaintext — so withholding
            //   decrypt from an editor buys nothing and only adds friction.
            Self::Editor => {
                CatalogRead
                    | JournalRead
                    | SpecEdit
                    | Delegate
                    | ViewSecret
                    | EditSecret
                    | DecryptSecret
            }
            Self::Admin => {
                Self::Editor.capabilities()
                    // Because Editor doesn't bundle `JournalAppend`,
                    // and we haven't unbundled things from Admin yet
                    | Self::Writer.capabilities()
                    | Self::TeamAdmin.capabilities()
                    | Self::Billing.capabilities()
                    | Self::ManageDataPlane.capabilities()
            }
            Self::Billing => ViewBilling | EditBilling,
            Self::ManageServiceAccounts => {
                QueryServiceAccounts | CreateServiceAccount | CreateApiKey | RevokeApiKey
            }
            Self::TeamAdmin => {
                CreateGrant
                    | DeleteGrant
                    | CreateInviteLink
                    | Self::ManageServiceAccounts.capabilities()
            }
            Self::ManageDataPlane => {
                ViewDataPlanePrivateNetworking | ModifyDataPlanePrivateNetworking
            }
            Self::Delegate => Delegate.into(),
            Self::Assume => Assume.into(),
        }
    }

    /// This returns the pascal cased name of the enum variant.
    pub fn name(self) -> &'static str {
        self.into()
    }
}

pub fn bits_for_legacy(capability: super::Capability) -> CapabilitySet {
    match capability {
        super::Capability::None => CapabilitySet::empty(),
        super::Capability::Read => CapabilityBundle::Viewer.capabilities(),
        super::Capability::Write => CapabilityBundle::Writer.capabilities(),
        super::Capability::Admin => CapabilityBundle::Admin.capabilities(),
    }
}

impl From<super::Capability> for CapabilitySet {
    fn from(capability: super::Capability) -> Self {
        bits_for_legacy(capability)
    }
}

impl From<CapabilityBundle> for CapabilitySet {
    fn from(bundle: CapabilityBundle) -> Self {
        bundle.capabilities()
    }
}

/// This capability mask type. this is a set of capabilities associated with a
/// token. This is a new type so we don't confuse a capability mask with
/// a set of required capabilities, as they would both be a capability set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CapabilityMask(CapabilitySet);

impl CapabilityMask {
    /// A mask enabling exactly `set`.
    pub fn new(set: CapabilitySet) -> Self {
        Self(set)
    }

    /// Returns true if there are no capabilities enabled within the mask.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Parse the capability mask from the claims string, the claims strings
    /// are a list of stringified CapabilityBundles. The bundles are expanded to
    /// their Capability and their union creates capability set.
    ///
    /// A `None` value means the token does not have a mask and it shouldn't be
    /// restricted, so all of the users capabilities are enabled.
    ///
    /// An empty array means that no capabilities have been granted yet. That
    /// means that the token is restricted from doing anything that requires any
    /// kind of permission.
    pub fn from_claims(mask: Option<&Vec<String>>) -> Option<Self> {
        if let Some(mask) = mask {
            Some(Self(
                mask.iter()
                    .filter_map(|name| CapabilityBundle::from_str(name).ok())
                    .map(|bundle| bundle.capabilities())
                    .fold(CapabilitySet::empty(), |set, bits| set | bits),
            ))
        } else {
            None
        }
    }

    /// walk's result: the mask has to gate traversal itself, so that a mask
    /// without `Delegate` (and `Assume`) confines the token to direct user
    /// grants, and it must not be re-widened by `Assume`, which makes all of
    /// an edge's bits delegatable as it passes through.
    pub fn apply(self, capabilities: CapabilitySet) -> CapabilitySet {
        capabilities & self.0
    }

    pub fn has_all_capabilities(self) -> bool {
        self.0 == CapabilitySet::all()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn names(set: CapabilitySet) -> Vec<String> {
        set.iter().map(|bit| bit.to_string()).collect()
    }

    fn mask_of(names: &[&str]) -> CapabilityMask {
        let claim: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        CapabilityMask::from_claims(Some(&claim)).unwrap()
    }

    const VARIANTS: [CapabilityBundle; 10] = [
        CapabilityBundle::Viewer,
        CapabilityBundle::Writer,
        CapabilityBundle::Editor,
        CapabilityBundle::Admin,
        CapabilityBundle::Billing,
        CapabilityBundle::TeamAdmin,
        CapabilityBundle::ManageServiceAccounts,
        CapabilityBundle::ManageDataPlane,
        CapabilityBundle::Delegate,
        CapabilityBundle::Assume,
    ];

    #[test]
    fn from_name_is_strict() {
        for bad in [
            "viewer",
            "VIEWER",
            "Team_Admin",
            "team_admin",
            "",
            " Viewer",
            "Viewer ",
        ] {
            assert_eq!(CapabilityBundle::from_str(bad).ok(), None, "{bad:?}");
        }
    }

    #[test]
    fn all_bundles_together_are_unrestricted() {
        let union = VARIANTS
            .iter()
            .fold(CapabilitySet::empty(), |acc, b| acc | b.capabilities());
        assert_eq!(union, CapabilitySet::all());

        let every_name: Vec<&str> = VARIANTS.iter().map(|b| b.name()).collect();
        assert!(mask_of(&every_name).has_all_capabilities());
    }

    #[test]
    fn mask_from_claim() {
        let all = CapabilitySet::all();
        let viewer = CapabilityBundle::Viewer.capabilities();

        let absent = CapabilityMask::from_claims(None);
        assert_eq!(absent, None);

        let empty = mask_of(&[]);
        assert_eq!(empty.apply(all), CapabilitySet::empty());

        assert_eq!(mask_of(&["Viewer"]), CapabilityMask::new(viewer));
        assert_eq!(mask_of(&["Viewer", "Bogus"]), CapabilityMask::new(viewer));
        assert_eq!(
            mask_of(&["Bogus"]),
            CapabilityMask::new(CapabilitySet::empty())
        );
        assert_eq!(
            mask_of(&["viewer"]),
            CapabilityMask::new(CapabilitySet::empty())
        );
        assert_eq!(
            mask_of(&["Viewer", "Delegate"]),
            CapabilityMask::new(viewer | Capability::Delegate)
        );
    }

    #[test]
    fn mask_apply_intersects() {
        let admin = CapabilityBundle::Admin.capabilities();
        let editor = CapabilityBundle::Editor.capabilities();
        let viewer = CapabilityBundle::Viewer.capabilities();

        assert_eq!(CapabilityMask::new(viewer).apply(admin), viewer);
        assert_eq!(CapabilityMask::new(admin).apply(viewer), viewer);
        assert_eq!(CapabilityMask::new(editor).apply(viewer), viewer & editor);

        // A mask without `Delegate` confines a token to direct grants even
        // when the underlying grant would delegate.
        assert!(editor.contains(Capability::Delegate));
        assert!(
            !CapabilityMask::new(viewer)
                .apply(editor)
                .contains(Capability::Delegate)
        );

        insta::assert_debug_snapshot!(names(CapabilityMask::new(editor).apply(viewer)), @r###"
        [
            "CatalogRead",
            "JournalRead",
        ]
        "###);
    }

    #[test]
    fn legacy_capability_parity() {
        use super::super::Capability as Legacy;
        assert_eq!(bits_for_legacy(Legacy::None), CapabilitySet::empty());
        assert_eq!(
            bits_for_legacy(Legacy::Read),
            CapabilityBundle::Viewer.capabilities()
        );
        assert_eq!(
            bits_for_legacy(Legacy::Write),
            CapabilityBundle::Writer.capabilities()
        );
        assert_eq!(
            bits_for_legacy(Legacy::Admin),
            CapabilityBundle::Admin.capabilities()
        );
        assert_eq!(
            CapabilitySet::from(Legacy::Write),
            bits_for_legacy(Legacy::Write)
        );
        assert_eq!(
            CapabilitySet::from(CapabilityBundle::Writer),
            bits_for_legacy(Legacy::Write)
        );
    }
}
