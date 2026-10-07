//! Mapping of JSON schemas into Pydantic models.

mod ast;
mod mapper;

pub use ast::{Class, Mapping, python_literal};
pub use mapper::{Mapper, field_name, sanitize_python_identifier};

/// Map `name` into a PascalCase identifier, as used for generated classes.
pub fn to_pascal_case(name: &str) -> String {
    let mut result = String::new();
    let mut uppercase_next = true;

    for c in name.chars() {
        if !c.is_alphanumeric() {
            uppercase_next = true;
        } else if uppercase_next {
            result.extend(c.to_uppercase());
            uppercase_next = false;
        } else {
            result.push(c);
        }
    }
    result
}
