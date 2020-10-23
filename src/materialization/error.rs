use super::FieldProjection;
use crate::catalog;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::fmt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    InvalidProjections(#[from] NaughtyProjections),

    #[error("catalog database error")]
    SQLiteErr(#[from] rusqlite::Error),

    // TODO: this is pretty ugly, but it seems better than movinng this whole materialization
    // module underneath catalog.
    #[error(transparent)]
    CatalogError(#[from] catalog::Error),

    #[error("Invalid target type '{0}' for materialization. Perhaps this catalog was created using a more recent version of flowctl?")]
    InvalidTargetType(String),

    #[error("No such field named: '{0}'")]
    NoSuchField(String),

    #[error("No collection exists with the name: '{0}'")]
    NoSuchCollection(String),

    #[error("The Collection's key is not fully represented in the list of projections. The missing key pointers are: {}", .0.iter().join(", "))]
    MissingCollectionKeys(Vec<String>),

    // TODO: figure out a reasonable error message
    #[error("Materialization setup was aborted by user.")]
    ActionAborted,

    #[error("Encountered an I/O error while setting up materialization")]
    IoError(#[from] std::io::Error),

    #[error("Failed to read json from catalog")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct NaughtyProjections {
    pub materialization_type: &'static str,
    pub naughty_projections: BTreeMap<String, Vec<FieldProjection>>,
}
impl NaughtyProjections {
    pub fn empty(materialization_type: &'static str) -> NaughtyProjections {
        NaughtyProjections {
            materialization_type,
            naughty_projections: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        !self
            .naughty_projections
            .values()
            .any(|naughty| !naughty.is_empty())
    }
}

const MAX_PROJECTION_ERROR_MSGS: usize = 5;

impl fmt::Display for NaughtyProjections {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "There are projections that are incompatible with the materialization of type '{}':",
            self.materialization_type
        )?;
        for (reason, naughty) in self.naughty_projections.iter() {
            writeln!(f, "{}:", reason)?;

            for field in naughty.iter().take(MAX_PROJECTION_ERROR_MSGS) {
                writeln!(f, "\t{}", field)?;
            }
            if naughty.len() > MAX_PROJECTION_ERROR_MSGS {
                writeln!(
                    f,
                    "\t...and {} more projections",
                    naughty.len() - MAX_PROJECTION_ERROR_MSGS
                )?;
            }
        }
        Ok(())
    }
}
impl std::error::Error for NaughtyProjections {}
