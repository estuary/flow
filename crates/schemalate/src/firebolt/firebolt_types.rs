use lazy_static::lazy_static;
use std::fmt::{self, Display};

use regex::Regex;

use super::reserved_words::RESERVED_WORDS;

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub r#type: TableType,
    pub schema: TableSchema,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableType {
    Fact,
    Dimension,
    External,
}

impl Display for TableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableType::Fact => write!(f, "{}", "FACT"),
            TableType::Dimension => write!(f, "{}", "DIMENSION"),
            TableType::External => write!(f, "{}", "EXTERNAL"),
        }
    }
}

impl From<String> for TableType {
    fn from(string: String) -> Self {
        match string.as_str() {
            "fact" => TableType::Fact,
            "dimension" => TableType::Dimension,
            "external" => TableType::External,
            _ => panic!("could not parse table type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableSchema {
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub key: String,
    pub r#type: FireboltType,
    pub nullable: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FireboltType {
    // Numeric Types https://docs.firebolt.io/general-reference/data-types.html#numeric
    Int,
    BigInt,
    Float,
    Double,

    // String type https://docs.firebolt.io/general-reference/data-types.html#string
    Text,

    // Date and time types https://docs.firebolt.io/general-reference/data-types.html#date-and-time
    // YYYY-MM-DD
    Date,
    // YYYY-MM-DD hh:mm:ss
    Timestamp,

    // Boolean https://docs.firebolt.io/general-reference/data-types.html#boolean
    Boolean,

    // Array type https://docs.firebolt.io/general-reference/data-types.html#array
    Array(Box<FireboltType>),
}

impl Display for FireboltType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FireboltType::Int => write!(f, "{}", "INT"),
            FireboltType::BigInt => write!(f, "{}", "BIGINT"),
            FireboltType::Float => write!(f, "{}", "FLOAT"),
            FireboltType::Double => write!(f, "{}", "DOUBLE"),
            FireboltType::Text => write!(f, "{}", "TEXT"),
            FireboltType::Date => write!(f, "{}", "DATE"),
            FireboltType::Timestamp => write!(f, "{}", "TIMESTAMP"),
            FireboltType::Boolean => write!(f, "{}", "BOOLEAN"),
            FireboltType::Array(nested_type) => write!(f, "ARRAY({})", nested_type),
        }
    }
}

lazy_static! {
    static ref VALID_FIELD_REGEX: Regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
}

// non-lower-case keys, reserved words, and fields not conforming to the regex must
// be quoted. see https://docs.firebolt.io/general-reference/identifier-requirements.html
pub fn identifier_quote(s: &str) -> String {
    if !VALID_FIELD_REGEX.is_match(s)
        || RESERVED_WORDS.contains(&s.to_lowercase())
        || s != s.to_lowercase()
    {
        format!("\"{}\"", s)
    } else {
        s.to_string()
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}",
            identifier_quote(&self.key),
            self.r#type,
            if self.nullable { " NULL" } else { "" }
        )
    }
}
impl Display for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, column) in self.columns.iter().enumerate() {
            column.fmt(f)?;
            if i < self.columns.len() - 1 {
                f.write_str(",")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_display() {
        assert_eq!(
            TableSchema {
                columns: vec![
                    Column {
                        key: "str".to_string(),
                        r#type: FireboltType::Text,
                        nullable: false,
                        is_key: false,
                    },
                    Column {
                        // Reserved word, must be quoted
                        key: "int".to_string(),
                        r#type: FireboltType::Int,
                        nullable: true,
                        is_key: true,
                    },
                    Column {
                        key: "num".to_string(),
                        r#type: FireboltType::Double,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        key: "big".to_string(),
                        r#type: FireboltType::BigInt,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        // Reserved word, must be quoted
                        key: "float".to_string(),
                        r#type: FireboltType::Float,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        // Reserved word, must be quoted
                        key: "date".to_string(),
                        r#type: FireboltType::Date,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        // Reserved word, must be quoted
                        key: "timestamp".to_string(),
                        r#type: FireboltType::Timestamp,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        // Reserved word, must be quoted
                        key: "boolean".to_string(),
                        r#type: FireboltType::Boolean,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        key: "Nonlowercase".to_string(),
                        r#type: FireboltType::Boolean,
                        nullable: false,
                        is_key: true,
                    },
                    Column {
                        // Unusual characters, must be quoted
                        key: "x%@!#sk".to_string(),
                        r#type: FireboltType::Text,
                        nullable: false,
                        is_key: false,
                    }
                ],
            }
            .to_string(),
            "str TEXT,\"int\" INT NULL,num DOUBLE,big BIGINT,\"float\" FLOAT,\"date\" DATE,\"timestamp\" TIMESTAMP,\"boolean\" BOOLEAN,\"Nonlowercase\" BOOLEAN,\"x%@!#sk\" TEXT"
        );

        assert_eq!(
            TableSchema {
                columns: vec![Column {
                    key: "arr".to_string(),
                    r#type: FireboltType::Array(Box::new(FireboltType::Text)),
                    nullable: false,
                    is_key: true,
                }]
            }
            .to_string(),
            "arr ARRAY(TEXT)"
        );
    }
}
