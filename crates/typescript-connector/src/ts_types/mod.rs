//! Mapping of JSON schemas into TypeScript types.

mod ast;
mod mapper;

pub use ast::{AST, Context};
pub use mapper::Mapper;

/// Map `name` into a camelCase identifier, or a PascalCase one if `upper`,
/// as used for generated methods and types.
pub fn camel_case(name: &str, mut upper: bool) -> String {
    let mut w = String::new();

    for c in name.chars() {
        if !c.is_alphanumeric() {
            upper = true
        } else if upper {
            w.extend(c.to_uppercase());
            upper = false;
        } else {
            w.push(c);
        }
    }
    w
}
