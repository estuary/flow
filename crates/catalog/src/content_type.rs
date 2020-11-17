use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef},
    Result,
};
use std::convert::TryFrom;
use std::fmt;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContentType {
    CatalogSpec,
    CatalogFixtures,
    Schema,
    Sql,
    NpmPack,
}

/// InvalidContentType is an error
#[derive(Debug)]
pub struct InvalidContentType(String);

impl std::error::Error for InvalidContentType {}

impl fmt::Display for InvalidContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} is not a valid catalog content type", self.0)
    }
}

const CATALOG_SPEC: &str = "application/vnd.estuary.dev-catalog-spec+yaml";
const CATALOG_FIXTURES: &str = "application/vnd.estuary.dev-catalog-fixtures+yaml";
const SCHEMA: &str = "application/schema+yaml";
const SQL: &str = "application/sql";
const NPM_PACK: &str = "application/vnd.estuary.dev-catalog-npm-pack";

impl ContentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentType::CatalogSpec => CATALOG_SPEC,
            ContentType::CatalogFixtures => CATALOG_FIXTURES,
            ContentType::Schema => SCHEMA,
            ContentType::Sql => SQL,
            ContentType::NpmPack => NPM_PACK,
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ContentType {
    type Error = InvalidContentType;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            CATALOG_SPEC => ContentType::CatalogSpec,
            CATALOG_FIXTURES => ContentType::CatalogFixtures,
            SCHEMA => ContentType::Schema,
            SQL => ContentType::Sql,
            NPM_PACK => ContentType::NpmPack,
            _ => return Err(InvalidContentType(value.to_owned())),
        })
    }
}

impl ToSql for ContentType {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_str().to_sql()
    }
}

impl FromSql for ContentType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        ContentType::try_from(value.as_str()?).map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_trip_conversions() -> Result<(), InvalidContentType> {
        for s in &[CATALOG_SPEC, CATALOG_FIXTURES, SCHEMA, SQL, NPM_PACK] {
            let ct = ContentType::try_from(*s)?;
            assert_eq!(*s, ct.as_str());
        }
        assert_eq!(
            format!("{}", ContentType::try_from("foobar").unwrap_err()),
            "\"foobar\" is not a valid catalog content type"
        );
        Ok(())
    }
}
