//! Mapping of JSON schemas into Pydantic models.

mod ast;
mod mapper;

pub use ast::{Mapping, python_literal};
pub use mapper::{Mapper, field_default, sanitize_python_identifier};

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

/// Map `name` into snake case: words of ASCII letters and digits, split at
/// other characters and at lower-to-upper case boundaries, lower-cased and
/// joined by `_`.
pub fn to_snake_case(name: &str) -> String {
    let mut words: Vec<String> = vec![String::new()];
    let mut prior: Option<char> = None;

    for c in name.chars() {
        if !c.is_ascii_alphanumeric() {
            words.push(String::new());
        } else {
            if c.is_ascii_uppercase()
                && prior.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
            {
                words.push(String::new());
            }
            words.last_mut().unwrap().push(c.to_ascii_lowercase());
        }
        prior = Some(c);
    }
    words
        .into_iter()
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod test {
    use super::{to_pascal_case, to_snake_case};

    #[test]
    fn names_are_cased() {
        let cases: Vec<_> = [
            "contacts",
            "myTable",
            "HTTPServer",
            "process-strings",
            "crm/Contacts",
            "a1B",
            "2024-orders",
            "über table",
            "!!",
        ]
        .into_iter()
        .map(|name| {
            format!(
                "{name} => {} / {}",
                to_snake_case(name),
                to_pascal_case(name),
            )
        })
        .collect();

        // Each name => its snake case / its Pascal case.
        insta::assert_snapshot!(cases.join("\n"), @r"
        contacts => contacts / Contacts
        myTable => my_table / MyTable
        HTTPServer => httpserver / HTTPServer
        process-strings => process_strings / ProcessStrings
        crm/Contacts => crm_contacts / CrmContacts
        a1B => a1_b / A1B
        2024-orders => 2024_orders / 2024Orders
        über table => ber_table / ÜberTable
        !! =>  /
        ");
    }
}
