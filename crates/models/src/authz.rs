use enumset::{EnumSet, EnumSetType};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// A set of fine-grained authorization capabilities. Used throughout the
/// authorization BFS and at authorization-check call sites.
pub type CapabilitySet = EnumSet<Capability>;

/// This capability mask type. this is a set of capabilities associated with a
/// token. This is a new type so we don't confuse a capability mask with
/// a set of required capabilities, as they would both be a capability set.
/// This represents a set of capabilities that a user is allowed to preform.
/// A token with a capability mask is designed to limit a capability that a
/// is allowed for a user. The capability mask does not grant additional
/// permissions, it only restricts them. So a capability mask cannot be used to
/// exceed the permissions a user has. If a capability mask is full it's
/// considered unrestricted.
///
/// The capability mask is filled by parsing CapabilityBundle from a given
/// token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityMask(CapabilitySet);

impl CapabilityMask {
    /// This means a user is unrestricted, and the capability mask has all bits
    /// enabled. This does not increase privilege, it only acts as permission
    /// to traverse the graph.
    pub const ALL_CAPABILITIES: Self = Self(CapabilitySet::all());

    /// A mask enabling exactly `set`.
    pub fn new(set: CapabilitySet) -> Self {
        Self(set)
    }

    /// Parse the capability mask from the claims string, the claims strings
    /// are a list of stringified CapabilityBundles. The bundles are expanded to
    /// their Capability and their union creates capability set.
    ///
    /// A `None` value means the token does not have a mask and it shouldn't be
    /// restricted, so all of the users capabilities are enabled.
    pub fn from_claim(mask: Option<&[String]>) -> Self {
        let Some(mask) = mask else {
            return Self::ALL_CAPABILITIES;
        };
        Self(
            mask.iter()
                .filter_map(|name| CapabilityBundle::from_name(name))
                .map(|bundle| bundle.capabilities())
                .fold(CapabilitySet::empty(), |set, bits| set | bits),
        )
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
    strum::VariantArray,
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

    CatalogRead,
    JournalRead,
    JournalAppend,
    SpecEdit,
    CreateGrant,
    DeleteGrant,
    CreateInviteLink,
    ViewDataPlanePrivateNetworking,
    ModifyDataPlanePrivateNetworking,
    ViewBilling,
    EditBilling,
    QueryServiceAccounts,
    CreateServiceAccount,
    CreateApiKey,
    RevokeApiKey,
    ViewSecret,
    EditSecret,
    DecryptSecret,
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

            Self::CatalogRead => CatalogRead.into(),
            Self::JournalRead => JournalRead.into(),
            Self::JournalAppend => JournalAppend.into(),
            Self::SpecEdit => SpecEdit.into(),
            Self::CreateGrant => CreateGrant.into(),
            Self::DeleteGrant => DeleteGrant.into(),
            Self::CreateInviteLink => CreateInviteLink.into(),
            Self::ViewDataPlanePrivateNetworking => ViewDataPlanePrivateNetworking.into(),
            Self::ModifyDataPlanePrivateNetworking => ModifyDataPlanePrivateNetworking.into(),
            Self::ViewBilling => ViewBilling.into(),
            Self::EditBilling => EditBilling.into(),
            Self::QueryServiceAccounts => QueryServiceAccounts.into(),
            Self::CreateServiceAccount => CreateServiceAccount.into(),
            Self::CreateApiKey => CreateApiKey.into(),
            Self::RevokeApiKey => RevokeApiKey.into(),
            Self::ViewSecret => ViewSecret.into(),
            Self::EditSecret => EditSecret.into(),
            Self::DecryptSecret => DecryptSecret.into(),
        }
    }

    /// This is the name within the token's capability mask. This is what the
    /// user passes in. This is different than the serialized version of the
    /// mask that we store within our DB.
    pub fn name(self) -> &'static str {
        self.into()
    }

    /// Parse a PascalCase name and suppress any parsing errors for missing or
    /// incorrect names. We do this because in the future, if there's ever a
    /// name that we don't know we can simply ignore it, meaning that privilege
    /// is never allowed, meaning we are still secure. This case will only
    /// happen during rollout of new instances of new services, and only during
    /// window when a user issue a token from a new service and uses it with an
    /// old service that is going down. If this does happen a simple retry will
    /// and hitting a new server will fix the problem.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::from_str(name).ok()
    }
}

impl From<CapabilityBundle> for CapabilitySet {
    fn from(bundle: CapabilityBundle) -> Self {
        bundle.capabilities()
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
    use super::*;
    use strum::VariantArray;

    fn names(set: CapabilitySet) -> Vec<String> {
        set.iter().map(|bit| bit.to_string()).collect()
    }

    fn mask_of(names: &[&str]) -> CapabilityMask {
        let claim: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        CapabilityMask::from_claim(Some(&claim))
    }

    // The PascalCase claim spelling (strum: `FromStr` / `From<_> for &str`)
    // and the snake_case serde / Postgres spelling are separate codecs. Each
    // must round-trip on its own and reject the other's spelling, exercised
    // here directly on the trait impls rather than through `from_name`.
    #[test]
    fn strum_and_serde_codecs_are_independent() {
        for bundle in CapabilityBundle::VARIANTS {
            let pascal: &'static str = (*bundle).into();
            assert_eq!(pascal, bundle.name());
            assert_eq!(
                pascal,
                format!("{bundle:?}"),
                "strum uses the variant ident"
            );
            assert_eq!(CapabilityBundle::from_str(pascal), Ok(*bundle));

            let json = serde_json::to_string(bundle).unwrap();
            let snake = json.trim_matches('"').to_string();
            assert_eq!(
                snake,
                snake.to_ascii_lowercase(),
                "{bundle:?} serde is snake_case"
            );
            assert_ne!(snake, pascal, "{bundle:?} spellings must differ");
            assert_eq!(
                serde_json::from_str::<CapabilityBundle>(&json).unwrap(),
                *bundle
            );

            let yaml = serde_yaml::to_string(bundle).unwrap();
            assert_eq!(yaml.trim(), snake, "serde spelling is format-independent");
            assert_eq!(
                serde_yaml::from_str::<CapabilityBundle>(&yaml).unwrap(),
                *bundle
            );

            assert_eq!(
                CapabilityBundle::from_str(&snake),
                Err(strum::ParseError::VariantNotFound),
                "strum must not accept the serde spelling of {bundle:?}"
            );
            assert!(
                serde_json::from_str::<CapabilityBundle>(&format!("\"{pascal}\"")).is_err(),
                "serde must not accept the strum spelling of {bundle:?}"
            );
        }

        // A DB-shaped `bundles` array round-trips through serde as a whole.
        let grant = vec![
            CapabilityBundle::TeamAdmin,
            CapabilityBundle::ManageDataPlane,
        ];
        let json = serde_json::to_string(&grant).unwrap();
        assert_eq!(json, r#"["team_admin","manage_data_plane"]"#);
        assert_eq!(
            serde_json::from_str::<Vec<CapabilityBundle>>(&json).unwrap(),
            grant
        );
    }

    #[test]
    fn bundle_vocabulary() {
        let table: Vec<serde_json::Value> = CapabilityBundle::VARIANTS
            .iter()
            .map(|bundle| {
                serde_json::json!({
                    "name": bundle.name(),
                    "serde": bundle,
                })
            })
            .collect();
        insta::assert_json_snapshot!(table);
    }

    #[test]
    fn bundle_capabilities() {
        let table: std::collections::BTreeMap<&str, Vec<String>> = CapabilityBundle::VARIANTS
            .iter()
            .map(|bundle| (bundle.name(), names(bundle.capabilities())))
            .collect();
        insta::assert_json_snapshot!(table);
    }

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
            assert_eq!(CapabilityBundle::from_name(bad), None, "{bad:?}");
        }
    }

    // Every `Capability` bit has a same-named bundle that expands to exactly
    // that bit, so a denial's `missing_capabilities` names are valid claim
    // vocabulary.
    #[test]
    fn every_capability_has_a_single_bit_bundle() {
        for bit in CapabilitySet::all() {
            let bundle = CapabilityBundle::from_name(&bit.to_string())
                .unwrap_or_else(|| panic!("no bundle named {bit}"));
            assert_eq!(bundle.capabilities(), CapabilitySet::only(bit));
        }
    }

    #[test]
    fn all_bundles_together_are_unrestricted() {
        let union = CapabilityBundle::VARIANTS
            .iter()
            .fold(CapabilitySet::empty(), |acc, b| acc | b.capabilities());
        assert_eq!(union, CapabilitySet::all());

        let every_name: Vec<&str> = CapabilityBundle::VARIANTS
            .iter()
            .map(|b| b.name())
            .collect();
        assert!(mask_of(&every_name).has_all_capabilities());
    }

    #[test]
    fn mask_from_claim() {
        let all = CapabilitySet::all();
        let viewer = CapabilityBundle::Viewer.capabilities();

        let absent = CapabilityMask::from_claim(None);
        assert_eq!(absent, CapabilityMask::ALL_CAPABILITIES);
        assert!(absent.has_all_capabilities());
        assert_eq!(absent.apply(all), all);

        let empty = mask_of(&[]);
        assert!(!empty.has_all_capabilities());
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
