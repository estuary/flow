use enumset::{EnumSet, EnumSetType};
use serde::{Deserialize, Serialize};

/// The subject of an authorization check: whose grants to evaluate and any
/// restrictions on their use.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subject {
    pub user_id: uuid::Uuid,
    /// Limits the user's reachable authority to these capability bits: the
    /// union of the bundles a restricted token names. None is unrestricted.
    pub capability_mask: Option<CapabilityBits>,
    /// Limits the user's reachable authority to the union of these prefixes.
    /// None is unrestricted; an empty list denies all access.
    pub prefix_mask: Option<Vec<String>>,
}

impl Subject {
    /// Creates a Subject covering the entirety of a user's grants, with no additional restrictions.
    pub fn unrestricted(user_id: uuid::Uuid) -> Self {
        Self {
            user_id,
            capability_mask: None,
            prefix_mask: None,
        }
    }
}

/// A set of capability bits. Used throughout the authorization BFS and at
/// authorization-check call sites.
pub type CapabilityBits = EnumSet<CapabilityBit>;

/// A fine-grained capability bit. Each check of the authorization model
/// requires one or more bits. Bits are internal: grants and token capability
/// masks name capability bundles, which are stable sets of bits.
#[derive(EnumSetType, Debug)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(name = "CapabilityBit", rename_items = "PascalCase")
)]
pub enum CapabilityBit {
    CatalogRead,
    JournalRead,
    JournalAppend,
    SpecEdit,
    CreateGrant,
    DeleteGrant,
    ManageInviteLinks,
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
    CreateSandbox,
    ManageAlerts,
    ManageLegal,
}

impl std::fmt::Display for CapabilityBit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// A stable, named bundle of capability bits. Grants and token capability
/// masks name bundles rather than bits, so bits may be added, split, or
/// merged without changing stored grants or minted tokens.
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
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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
    Viewer,
    Writer,
    Editor,
    Admin,
    Billing,
    TeamAdmin,
    ManageServiceAccounts,
    ManageDataPlane,
    CreateSandbox,
    Delegate,
    Assume,
    ViewBilling,
    EditBilling,
    EditSecret,
    ViewDataPlanePrivateNetworking,
    ModifyDataPlanePrivateNetworking,
    CatalogRead,
}

impl CapabilityBundle {
    pub fn bits(&self) -> CapabilityBits {
        use CapabilityBit::*;
        match self {
            // `ViewDataPlanePrivateNetworking` is bundled here because
            // `read` on a data-plane prefix already conveys deploy-level
            // trust (it's what authorizes deploying tasks into the plane),
            // so viewing the plane's private-networking configuration comes
            // with it. Mutating that configuration stays in the separately
            // granted `ManageDataPlane` bundle.
            Self::Viewer => CatalogRead | JournalRead | ViewDataPlanePrivateNetworking,
            Self::Writer => Self::Viewer.bits() | JournalAppend,
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
                Self::Editor.bits()
                    // Because Editor doesn't bundle `JournalAppend`,
                    // and we haven't unbundled things from Admin yet
                    | Self::Writer.bits()
                    | Self::TeamAdmin.bits()
                    | Self::Billing.bits()
                    | Self::ManageDataPlane.bits()
                    | ManageLegal
                    | CreateSandbox
            }
            Self::Billing => ViewBilling | EditBilling,
            Self::ManageServiceAccounts => {
                QueryServiceAccounts | CreateServiceAccount | CreateApiKey | RevokeApiKey
            }
            Self::TeamAdmin => {
                CreateGrant
                    | DeleteGrant
                    | ManageInviteLinks
                    | ManageAlerts
                    | Self::ManageServiceAccounts.bits()
            }
            Self::ManageDataPlane => {
                ViewDataPlanePrivateNetworking | ModifyDataPlanePrivateNetworking
            }
            Self::Delegate => Delegate.into(),
            Self::Assume => Assume.into(),
            Self::CreateSandbox => CreateSandbox.into(),
            Self::ViewBilling => ViewBilling.into(),
            Self::EditBilling => EditBilling.into(),
            Self::EditSecret => EditSecret.into(),
            Self::ViewDataPlanePrivateNetworking => ViewDataPlanePrivateNetworking.into(),
            Self::ModifyDataPlanePrivateNetworking => ModifyDataPlanePrivateNetworking.into(),
            Self::CatalogRead => CatalogRead.into(),
        }
    }
}

/// Parses the snake_case name of any `CapabilityBundle`.
pub fn parse_capability_bundle(name: &str) -> Result<CapabilityBundle, String> {
    name.parse().map_err(|_| {
        let known: Vec<&str> = <CapabilityBundle as strum::VariantArray>::VARIANTS
            .iter()
            .map(<&str>::from)
            .collect();
        format!(
            "Unknown capability bundle {name:?}: use any of {}.",
            known.join(", ")
        )
    })
}

/// Parses a token's capability mask into the union of its named bundles.
pub fn parse_capability_mask(names: &[String]) -> Result<CapabilityBits, String> {
    names
        .iter()
        .try_fold(CapabilityBits::empty(), |bits, name| {
            Ok(bits | parse_capability_bundle(name)?.bits())
        })
}

/// Parses a token's prefix mask. Each prefix must be a nonempty catalog prefix.
/// The result is sorted, and drops duplicates and prefixes nested under
/// another listed prefix, since they add no authority.
pub fn parse_prefix_mask(prefixes: &[String]) -> Result<Vec<String>, String> {
    for prefix in prefixes {
        // CATALOG_PREFIX_RE also matches the empty string, which would be a
        // prefix of every name.
        if prefix.is_empty() || validator::Validate::validate(&crate::Prefix::new(prefix)).is_err()
        {
            return Err(format!(
                "Invalid prefix {prefix:?}: use a nonempty catalog prefix ending in '/'."
            ));
        }
    }
    let mut prefixes = prefixes.to_vec();
    prefixes.sort_unstable();
    prefixes.dedup_by(|prefix, previous| prefix.starts_with(previous.as_str()));

    Ok(prefixes)
}

/// Returns bundles whose bits are all contained in `bits`.
pub fn bundles_covering(bits: CapabilityBits) -> impl Iterator<Item = CapabilityBundle> {
    <CapabilityBundle as strum::VariantArray>::VARIANTS
        .iter()
        .copied()
        .filter(move |bundle| bits.is_superset(bundle.bits()))
}

/// Returns bundles which contain all of `bits`, fewest bits first.
pub fn bundles_containing(bits: CapabilityBits) -> impl Iterator<Item = CapabilityBundle> {
    let mut bundles: Vec<_> = <CapabilityBundle as strum::VariantArray>::VARIANTS
        .iter()
        .copied()
        .filter(|bundle| bundle.bits().is_superset(bits))
        .collect();
    bundles.sort_by_cached_key(|bundle| {
        (
            bundle.bits().len(),
            serde_json::to_string(bundle).expect("bundle serializes"),
        )
    });
    bundles.into_iter()
}

pub fn bits_for_legacy(capability: super::Capability) -> CapabilityBits {
    match capability {
        super::Capability::None => CapabilityBits::empty(),
        super::Capability::Read => CapabilityBundle::Viewer.bits(),
        super::Capability::Write => CapabilityBundle::Writer.bits(),
        super::Capability::Admin => CapabilityBundle::Admin.bits(),
    }
}

impl From<super::Capability> for CapabilityBits {
    fn from(capability: super::Capability) -> Self {
        bits_for_legacy(capability)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn bundles_covering() {
        let cases = [
            ("empty", super::CapabilityBits::empty()),
            ("partial", super::CapabilityBit::CatalogRead.into()),
            ("single_bit", super::CapabilityBit::ViewBilling.into()),
            ("exact", super::CapabilityBundle::Viewer.bits()),
            (
                "overlapping",
                super::CapabilityBundle::Writer.bits() | super::CapabilityBundle::Billing.bits(),
            ),
        ];
        let results =
            cases.map(|(name, bits)| (name, super::bundles_covering(bits).collect::<Vec<_>>()));
        insta::assert_json_snapshot!(results, @r#"
        [
          [
            "empty",
            []
          ],
          [
            "partial",
            [
              "catalog_read"
            ]
          ],
          [
            "single_bit",
            [
              "view_billing"
            ]
          ],
          [
            "exact",
            [
              "viewer",
              "view_data_plane_private_networking",
              "catalog_read"
            ]
          ],
          [
            "overlapping",
            [
              "viewer",
              "writer",
              "billing",
              "view_billing",
              "edit_billing",
              "view_data_plane_private_networking",
              "catalog_read"
            ]
          ]
        ]
        "#);
    }

    #[test]
    fn bundle_names_are_serde_names() {
        for bundle in <super::CapabilityBundle as strum::VariantArray>::VARIANTS {
            let name: &str = bundle.into();
            assert_eq!(serde_json::to_value(bundle).unwrap(), name);
            assert_eq!(bundle.to_string(), name);
            assert_eq!(super::parse_capability_bundle(name), Ok(*bundle));
        }
    }
}
