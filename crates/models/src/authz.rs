use enumset::{EnumSet, EnumSetType};
use serde::{Deserialize, Serialize};

#[derive(EnumSetType, Debug)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(name = "Permission", rename_items = "PascalCase")
)]
pub enum Capability {
    CatalogRead,
    JournalRead,
    JournalAppend,
    SpecEdit,
    CreateGrant,
    DeleteGrant,
    CreateInviteLink,
    Delegate,
    Assume,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    feature = "sqlx-support",
    derive(sqlx::Type),
    sqlx(type_name = "capability_bundle", rename_all = "snake_case")
)]
pub enum Bundle {
    Viewer,
    Writer,
    Editor,
    Admin,
    Billing,
    TeamAdmin,
    Delegate,
    Assume,
}

impl Bundle {
    pub fn capabilities(&self) -> EnumSet<Capability> {
        use Capability::*;
        match self {
            Self::Viewer => CatalogRead | JournalRead,
            Self::Writer => Self::Viewer.capabilities() | JournalAppend,
            Self::Editor => Self::Writer.capabilities() | SpecEdit | Delegate,
            Self::Admin => {
                Self::Editor.capabilities()
                    | Self::TeamAdmin.capabilities()
                    | Self::Billing.capabilities()
            }
            Self::Billing => EnumSet::empty(),
            Self::TeamAdmin => CreateGrant | DeleteGrant | CreateInviteLink,
            Self::Delegate => EnumSet::from(Delegate),
            Self::Assume => EnumSet::from(Assume),
        }
    }
}

pub fn bits_for_legacy(capability: super::Capability) -> EnumSet<Capability> {
    match capability {
        super::Capability::None => EnumSet::empty(),
        super::Capability::Read => Bundle::Viewer.capabilities(),
        super::Capability::Write => Bundle::Writer.capabilities(),
        super::Capability::Admin => Bundle::Admin.capabilities(),
    }
}

impl From<super::Capability> for EnumSet<Capability> {
    fn from(capability: super::Capability) -> Self {
        bits_for_legacy(capability)
    }
}

/// Reverse-map a set of capability bits to the best matching legacy
/// capability name, for use in error messages and logging.
pub fn capability_name(capabilities: EnumSet<Capability>) -> &'static str {
    if capabilities == Bundle::Admin.capabilities() {
        "admin"
    } else if capabilities == Bundle::Editor.capabilities() {
        "editor"
    } else if capabilities == Bundle::Writer.capabilities() {
        "write"
    } else if capabilities == Bundle::Viewer.capabilities() {
        "read"
    } else {
        "required capabilities"
    }
}
