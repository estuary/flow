use std::convert::Infallible;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use super::collate::{collate, normalize};

/// The canonical entity name as it is understood in the data plane. This is a
/// unicode-normalized version of the user-provided name. This will be what
/// users understand as the entity's name.
///
/// eg. "acmeCo", "acmeCo/anvils"
#[serde_as]
#[derive(Clone, Debug, DeserializeFromStr, SerializeDisplay, sqlx::Type)]
#[sqlx(transparent)]
pub struct CatalogName(String);

impl CatalogName {
    pub fn new(name: &str) -> Self {
        Self(normalize(name.chars()).collect::<String>())
    }
}

impl FromStr for CatalogName {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl Display for CatalogName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (&self.0[..]).fmt(f)
    }
}

impl Deref for CatalogName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl From<CatalogName> for String {
    fn from(name: CatalogName) -> Self {
        name.0.clone()
    }
}

/// The representation of the entity name which will be used for enforcing
/// uniqueness and prefixing constraints. Entity names should not differ by
/// unicode constructor or by case. By unicode-normalizing and case-folding the
/// name we can reliably uniquely-index catalog entity names.
///
/// eg. "acmeco", "acmeco/anvils"
#[serde_as]
#[derive(Clone, Debug, DeserializeFromStr, SerializeDisplay, sqlx::Type)]
#[sqlx(transparent)]
pub struct UniqueName(String);

impl UniqueName {
    pub fn new(name: &str) -> Self {
        Self(collate(name.chars()).collect::<String>())
    }
}

impl FromStr for UniqueName {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl Display for UniqueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (&self.0[..]).fmt(f)
    }
}

impl Deref for UniqueName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
