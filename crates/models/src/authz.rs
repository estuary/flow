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
    QueryServiceAccounts,
    CreateServiceAccount,
    CreateApiKey,
    RevokeApiKey,
    Delegate,
    Assume,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl Capability {
    /// PascalCase wire name of this capability.
    ///
    /// One vocabulary is shared by the `capability_mask` token claim and by
    /// GraphQL's `CapabilityBit` enum (which derives the same spelling from
    /// these variant identifiers), so that "you need capability X" reads
    /// identically wherever it's said. Names are minted into tokens which
    /// outlive a deploy and are interpreted by instances of differing
    /// versions, so they must remain stable: the mapping is written out
    /// rather than derived from `Debug` precisely so that renaming a variant
    /// is not silently a wire-format change — `test_graphql_names_match_claim_names`
    /// holds this mapping and GraphQL's derived spelling together.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::CatalogRead => "CatalogRead",
            Self::JournalRead => "JournalRead",
            Self::JournalAppend => "JournalAppend",
            Self::SpecEdit => "SpecEdit",
            Self::CreateGrant => "CreateGrant",
            Self::DeleteGrant => "DeleteGrant",
            Self::CreateInviteLink => "CreateInviteLink",
            Self::ViewDataPlanePrivateNetworking => "ViewDataPlanePrivateNetworking",
            Self::ModifyDataPlanePrivateNetworking => "ModifyDataPlanePrivateNetworking",
            Self::ViewBilling => "ViewBilling",
            Self::EditBilling => "EditBilling",
            Self::QueryServiceAccounts => "QueryServiceAccounts",
            Self::CreateServiceAccount => "CreateServiceAccount",
            Self::CreateApiKey => "CreateApiKey",
            Self::RevokeApiKey => "RevokeApiKey",
            Self::Delegate => "Delegate",
            Self::Assume => "Assume",
        }
    }

    /// Parse a PascalCase capability name, or `None` if this binary doesn't
    /// recognize it.
    ///
    /// An unrecognized name is inert rather than an error: a token minted by
    /// a newer control plane must still authenticate against an older one,
    /// and a capability we cannot enforce must never widen what we allow.
    pub fn from_name(name: &str) -> Option<Self> {
        // Linear over the variants, which keeps `name()` the mapping's
        // single source of truth.
        CapabilitySet::all().iter().find(|c| c.name() == name)
    }
}

/// The capability mask carried by a request: a ceiling on the capabilities it
/// may exercise, independent of the grants held by its user. It's computed
/// from the authenticating token's `capability_mask` claim, and callers apply
/// it wherever authority is derived from grants — so that a masked token can
/// only ever attenuate its user's live authority, never amplify it.
///
/// The mask is an enable/disable filter, never a grant: `apply` is pure
/// intersection, so naming a capability the user doesn't hold conveys
/// nothing, while omitting one they do hold disables it. An unmasked bearer
/// simply carries the full set ([`Self::UNMASKED`]) and intersects as the
/// identity.
///
/// This is a newtype over [`CapabilitySet`] rather than a bare set because
/// authorization call sites take both "the capabilities requested" and "the
/// ceiling enforced" — often side by side. As bare sets those swap silently
/// and compile; as distinct types the swap is a compile error.
///
/// This type answers *what may be exercised*, never *whether the bearer is
/// masked*. A token whose mask happens to enable everything is still a
/// deliberately-reduced credential, and surfaces that fail closed for masked
/// bearers (the `/admin` endpoints, the mint) must key on the claim's
/// presence — `capability_mask.is_some()` — and never on [`Self::is_all`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityMask(CapabilitySet);

impl CapabilityMask {
    /// The mask of an unmasked bearer: every capability this binary knows,
    /// so intersection is the identity. Adding a future `Capability`
    /// variant automatically widens this, which is correct — an unmasked
    /// token's authority is bounded only by grants.
    ///
    /// There is deliberately no `Default` and no `From<CapabilitySet>`:
    /// every caller must name its mask, and constructing an unrestricted
    /// one must be a visible, greppable choice.
    pub const UNMASKED: Self = Self(CapabilitySet::all());

    /// A mask enabling exactly `set`. An empty set is valid and yields a
    /// token which authenticates an identity but authorizes nothing.
    pub fn bounded(set: CapabilitySet) -> Self {
        Self(set)
    }

    /// Build a mask from a token's verified `capability_mask` claim.
    ///
    /// An absent claim is [`Self::UNMASKED`]; a present claim enables its
    /// recognized names, and that includes an empty list — "no mask" and
    /// "an empty mask" are distinct on the wire and the difference is
    /// load-bearing. Unrecognized names contribute nothing, so a claim
    /// naming only names we don't know bounds the token to nothing at all;
    /// see [`Capability::from_name`].
    pub fn from_claim(mask: Option<&[String]>) -> Self {
        let Some(mask) = mask else {
            return Self::UNMASKED;
        };
        Self(
            mask.iter()
                .filter_map(|name| Capability::from_name(name))
                .collect(),
        )
    }

    /// Attenuate `capabilities` to this mask.
    ///
    /// Apply this at each node emission of the user grant walk, never to the
    /// walk's result: the mask has to gate `Delegate` itself, so that a mask
    /// without it confines the token to direct user grants, and it must not
    /// be re-widened by `Assume`, which makes all of an edge's bits
    /// delegatable as it passes through.
    pub fn apply(self, capabilities: CapabilitySet) -> CapabilitySet {
        capabilities & self.0
    }

    /// True when this mask attenuates nothing this binary can enforce.
    ///
    /// This exists for *leak-prevention* decisions only — e.g. whether
    /// `reachable_prefixes` may keep emitting fully-attenuated legacy nodes,
    /// which is safe when the mask hides nothing. It must NEVER stand in
    /// for "is this bearer unmasked?": that is a property of the claim
    /// (`capability_mask.is_some()`), not of this value.
    pub fn is_all(self) -> bool {
        self.0 == CapabilitySet::all()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
            Self::Editor => CatalogRead | JournalRead | SpecEdit | Delegate,
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

#[cfg(test)]
mod test {
    use super::{Capability, CapabilityBundle, CapabilityMask, CapabilitySet};

    #[test]
    fn test_capability_names_round_trip() {
        // Every capability has a name which parses back to itself, and the
        // set of names is the vocabulary of the `capability_mask` claim.
        let names: Vec<&str> = CapabilitySet::all().iter().map(|c| c.name()).collect();

        for capability in CapabilitySet::all() {
            assert_eq!(Capability::from_name(capability.name()), Some(capability));
        }
        // Names a binary doesn't know about are not errors, they're absences.
        assert_eq!(Capability::from_name("NotACapability"), None);
        assert_eq!(Capability::from_name("catalogRead"), None);
        assert_eq!(Capability::from_name(""), None);

        insta::assert_debug_snapshot!(names, @r#"
        [
            "CatalogRead",
            "JournalRead",
            "JournalAppend",
            "SpecEdit",
            "CreateGrant",
            "DeleteGrant",
            "CreateInviteLink",
            "ViewDataPlanePrivateNetworking",
            "ModifyDataPlanePrivateNetworking",
            "ViewBilling",
            "EditBilling",
            "QueryServiceAccounts",
            "CreateServiceAccount",
            "CreateApiKey",
            "RevokeApiKey",
            "Delegate",
            "Assume",
        ]
        "#);
    }

    #[test]
    fn test_capability_mask_from_claim() {
        // An absent claim is an unmasked token: the full set.
        assert_eq!(CapabilityMask::from_claim(None), CapabilityMask::UNMASKED);

        let cases = [
            // Recognized names enable exactly what they name.
            Some(vec!["CatalogRead".to_string(), "Delegate".to_string()]),
            // An empty mask is valid, and authorizes nothing.
            Some(vec![]),
            // Unknown names are inert alongside known ones...
            Some(vec![
                "SpecEdit".to_string(),
                "FutureCapability".to_string(),
                "catalog_read".to_string(),
            ]),
            // ...and a claim of only unknown names bounds the token to
            // nothing, never leaving it unmasked.
            Some(vec!["FutureCapability".to_string()]),
            // Duplicates and ordering are immaterial to a set.
            Some(vec![
                "Delegate".to_string(),
                "CatalogRead".to_string(),
                "CatalogRead".to_string(),
            ]),
        ];
        let masks: Vec<CapabilityMask> = cases
            .iter()
            .map(|claim| CapabilityMask::from_claim(claim.as_deref()))
            .collect();

        insta::assert_debug_snapshot!(masks, @r"
        [
            CapabilityMask(
                EnumSet(CatalogRead | Delegate),
            ),
            CapabilityMask(
                EnumSet(),
            ),
            CapabilityMask(
                EnumSet(SpecEdit),
            ),
            CapabilityMask(
                EnumSet(),
            ),
            CapabilityMask(
                EnumSet(CatalogRead | Delegate),
            ),
        ]
        ");
    }

    #[test]
    fn test_capability_mask_apply() {
        let editor = CapabilityBundle::Editor.capabilities();

        // The unmasked mask is the identity, and enabling every capability
        // is the same thing by construction.
        assert_eq!(CapabilityMask::UNMASKED.apply(editor), editor);
        assert_eq!(
            CapabilityMask::bounded(CapabilitySet::all()),
            CapabilityMask::UNMASKED,
        );

        // A mask intersects: it can only ever disable bits, and bits it
        // enables which the grant doesn't hold stay absent.
        assert_eq!(
            CapabilityMask::bounded(Capability::CatalogRead | Capability::EditBilling)
                .apply(editor),
            CapabilitySet::from(Capability::CatalogRead),
        );
        assert_eq!(
            CapabilityMask::bounded(CapabilitySet::empty()).apply(editor),
            CapabilitySet::empty(),
        );
        assert!(!CapabilityMask::bounded(editor).is_all());
        assert!(CapabilityMask::UNMASKED.is_all());
    }

    // The claim vocabulary and GraphQL's `CapabilityBit` vocabulary are one
    // and the same, and must not drift: an agent told it needs `SpecEdit`
    // must be able to name `SpecEdit` in a mask request. Reading the derive's
    // own item list catches a variant rename, where GraphQL's spelling moves
    // and `name()`'s hard-coded string doesn't.
    //
    // Requires the `async-graphql` feature, so `cargo test -p models` alone
    // skips this; a workspace-wide run enables it by feature unification.
    #[cfg(feature = "async-graphql")]
    #[test]
    fn test_graphql_names_match_claim_names() {
        for item in <Capability as async_graphql::resolver_utils::EnumType>::items() {
            assert_eq!(item.name, item.value.name());
        }
    }
}
