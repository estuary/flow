#[derive(Clone, Debug)]
pub struct Table {
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub key: String,
    pub typ: FireboltType,
    pub nullable: bool,
}

// The basic elastic search data types to represent data in Flow.
#[derive(Clone, Debug)]
pub enum BasicType {
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
}
#[derive(Clone, Debug)]
pub enum FireboltType {
    Basic(BasicType),
    // Array type https://docs.firebolt.io/general-reference/data-types.html#array
    Array(Box<FireboltType>),
}

use std::fmt::{self, Display};
use FireboltType::{Array, Basic};
impl Display for BasicType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            BasicType::Int => "INT",
            BasicType::BigInt => "BIGINT",
            BasicType::Float => "FLOAT",
            BasicType::Double => "DOUBLE",
            BasicType::Text => "TEXT",
            BasicType::Date => "DATE",
            BasicType::Timestamp => "TIMESTAMP",
            BasicType::Boolean => "BOOLEAN",
        };
        write!(f, "{}", str)
    }
}
impl Display for FireboltType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Basic(basic_type) => basic_type.fmt(f),
            Array(nested_type) => write!(f, "ARRAY({})", nested_type),
        }
    }
}
impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}",
            self.key,
            self.typ,
            if self.nullable { " NULL" } else { "" }
        )
    }
}
impl Display for Table {
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
