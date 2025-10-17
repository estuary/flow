use super::ast::{AST, Class, Field, Mapping};
use super::to_pascal_case;
use doc::shape::{ArrayShape, ObjShape, Provenance, Shape};
use json::schema::types;
use regex::Regex;
use std::collections::BTreeMap;

pub struct Mapper {
    top_level: BTreeMap<url::Url, String>,
    validator: doc::Validator,
}

impl Mapper {
    pub fn new(bundle: &[u8], anchor_prefix: &str) -> Self {
        let schema = doc::validation::build_bundle(bundle).unwrap();
        let validator = doc::validation::Validator::new(schema).unwrap();

        let mut top_level = BTreeMap::new();

        if !anchor_prefix.is_empty() {
            // Iterate through the schema index to find all anchors
            let index = validator.schema_index();
            for (_uri, _is_dynamic, schema) in index.iter() {
                for kw in schema.keywords.iter() {
                    let json::schema::Keyword::Anchor { anchor } = kw else {
                        continue;
                    };
                    let Some((_, name)) = anchor.split_once("#") else {
                        continue;
                    };
                    if NAMED_SCHEMA_RE.is_match(name) {
                        // Parse the full anchor URI and insert it as the key, mapping to the class name.
                        if let Ok(anchor_url) = url::Url::parse(anchor) {
                            top_level.insert(anchor_url, format!("{anchor_prefix}{name}"));
                        }
                    }
                }
            }
        }

        Mapper {
            validator,
            top_level,
        }
    }

    /// Access the root schema of this Mapper.
    pub fn schema(&self) -> &doc::validation::Schema {
        self.validator.schema()
    }

    /// Access the schema index of this Mapper.
    pub fn index(&self) -> &doc::validation::SchemaIndex<'_> {
        self.validator.schema_index()
    }

    /// Map a schema into a set of classes and a type reference.
    /// The `class_name` is used if the root schema is an object (not an anchor reference).
    pub fn map(&self, schema: &doc::validation::Schema, type_name: &str) -> Mapping {
        let mut classes = Vec::new();
        let mut aliases = Vec::new();

        // Map top-level anchors first
        for (url, class_name) in &self.top_level {
            let (schema, _is_dynamic) = self.index().fetch(url.as_str()).unwrap();
            let shape = Shape::infer(schema, self.index());
            let ast = self.to_ast(&shape, class_name, &mut classes);

            if !matches!(&ast, AST::Anchor(name) if name == class_name) {
                aliases.push((class_name.clone(), ast));
            }
        }

        let shape = Shape::infer(schema, self.index());
        let ast = self.to_ast(&shape, type_name, &mut classes);

        if !matches!(&ast, AST::Anchor(name) if name == type_name) {
            aliases.push((type_name.to_string(), ast));
        }
        Mapping { classes, aliases }
    }

    fn to_ast(&self, shape: &Shape, type_name: &str, classes: &mut Vec<Class>) -> AST {
        if let Provenance::Reference(uri) = &shape.provenance {
            if let Some(class_name) = self.top_level.get(uri) {
                return AST::Anchor(class_name.clone());
            }
        }

        // Is this a trivial Any type?
        if shape.type_ == types::ANY
            && shape.enum_.is_none()
            && shape.array.additional_items.is_none()
            && shape.array.tuple.is_empty()
            && shape.object.properties.is_empty()
            && shape.object.additional_properties.is_none()
        {
            return AST::Any;
        }
        if let Some(enum_) = &shape.enum_ {
            return self.enum_to_ast(enum_, type_name, classes);
        }

        let mut disjunct = Vec::new();

        if shape.type_.overlaps(types::OBJECT) {
            if shape.type_ != types::OBJECT {
                disjunct.push(self.object_to_ast(shape, &format!("{type_name}Object"), classes));
            } else {
                disjunct.push(self.object_to_ast(shape, type_name, classes));
            }
        }
        if shape.type_.overlaps(types::ARRAY) {
            disjunct.push(self.array_to_ast(&shape.array, type_name, classes));
        }
        if shape.type_.overlaps(types::BOOLEAN) {
            disjunct.push(AST::Bool);
        }
        if shape.type_.overlaps(types::INTEGER) {
            disjunct.push(AST::Int);
        }
        if shape.type_.overlaps(types::FRACTIONAL) {
            disjunct.push(AST::Float);
        }
        if shape.type_.overlaps(types::STRING) {
            disjunct.push(AST::Str);
        }
        if shape.type_.overlaps(types::NULL) {
            disjunct.push(AST::None);
        }

        if disjunct.is_empty() {
            AST::Never
        } else if disjunct.len() == 1 {
            disjunct.pop().unwrap()
        } else {
            AST::Union { variants: disjunct }
        }
    }

    fn object_to_ast(&self, shape: &Shape, type_name: &str, classes: &mut Vec<Class>) -> AST {
        let ObjShape {
            properties,
            pattern_properties,
            additional_properties,
        } = &shape.object;

        // Collect nested classes for THIS class's properties
        let mut nested = Vec::new();
        let mut fields: Vec<Field> = Vec::new();

        for prop in properties {
            let mut sanitized = sanitize_python_identifier(&prop.name);

            // Pydantic doesn't allow field names starting with single underscore,
            // and __dunders__ must also map into public fields.
            if sanitized.starts_with('_') {
                sanitized = format!("m{sanitized}");
            }

            let (name, alias) = if sanitized == *prop.name {
                (prop.name.to_string(), None) // Name is valid as-is.
            } else {
                (sanitized, Some(prop.name.to_string()))
            };

            // Extract docstring from the property's shape
            let docstring = match (&prop.shape.title, &prop.shape.description) {
                (Some(title), Some(description)) => Some(format!("{} {}", title, description)),
                (Some(s), None) | (None, Some(s)) => Some(s.to_string()),
                (None, None) => None,
            };

            let type_ = self.to_ast(&prop.shape, &to_pascal_case(&name), &mut nested);

            fields.push(Field {
                name,
                alias,
                docstring,
                type_,
                is_required: prop.is_required,
            });
        }

        // Determine the type for additional fields by merging "additionalProperties"
        // and "patternProperties" keywords.
        //
        // Note: Unlike TypeScript, we do NOT merge declared property types here.
        // TypeScript requires index signatures to accommodate all property types,
        // but Pydantic's __pydantic_extra__ only validates *extra* fields, not declared ones.
        let mut merged = Shape::nothing();

        if let Some(additional_properties) = additional_properties {
            merged = Shape::union(merged, additional_properties.as_ref().clone());
        } else {
            merged = Shape::anything(); // Unconstrained when omitted.
        }
        for prop in pattern_properties {
            merged = Shape::union(merged, prop.shape.clone());
        }

        let additional = if merged.type_ != types::INVALID {
            Some(self.to_ast(&merged, "PydanticExtra", &mut nested))
        } else {
            None
        };

        // Extract docstring from shape
        let docstring = match (&shape.title, &shape.description) {
            (Some(title), Some(description)) => Some(format!("{} {}", title, description)),
            (Some(s), None) | (None, Some(s)) => Some(s.to_string()),
            (None, None) => None,
        };

        classes.push(Class {
            name: type_name.to_owned(),
            docstring,
            nested,
            fields,
            additional,
        });

        AST::Anchor(type_name.to_owned())
    }

    fn array_to_ast(&self, arr: &ArrayShape, type_name: &str, classes: &mut Vec<Class>) -> AST {
        let ArrayShape {
            tuple,
            additional_items,
            min_items,
            max_items,
        } = arr;

        // Is this a fixed-length tuple?
        if let Some(max_items) = max_items
            && *max_items == *min_items
            // Limit to explicit tuples, or reasonably small numbers of items.
            && (tuple.len() == *min_items as usize || *max_items <= 10)
        {
            let items = (0..*max_items)
                .map(|i| {
                    let item_shape = tuple
                        .get(i as usize)
                        .or(additional_items.as_ref().map(|s| s.as_ref()));

                    self.to_ast(
                        item_shape.unwrap_or(&Shape::anything()),
                        &format!("{type_name}Tuple{i}"),
                        classes,
                    )
                })
                .collect::<Vec<_>>();

            return AST::Tuple { items };
        }

        // Determine the type for additional items by merging "prefixItems"
        // and "items" keywords.
        let mut merged = Shape::nothing();

        if let Some(additional_items) = additional_items {
            merged = Shape::union(merged, additional_items.as_ref().clone());
        } else {
            merged = Shape::anything(); // Unconstrained when omitted.
        }

        for item in tuple {
            merged = Shape::union(merged, item.clone());
        }

        AST::List {
            of: Box::new(self.to_ast(&merged, &format!("{type_name}Item"), classes)),
        }
    }

    fn enum_to_ast(
        &self,
        enum_: &[serde_json::Value],
        type_name: &str,
        classes: &mut Vec<Class>,
    ) -> AST {
        let mut disjunct = Vec::new();

        let (simple, complex): (Vec<_>, Vec<_>) = enum_.iter().partition(|v| match v {
            serde_json::Value::Number(n) if n.is_f64() => false,
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => false,
            _ => true,
        });

        // Add simple literals as a single AST::Literals with all values
        if !simple.is_empty() {
            disjunct.push(AST::Literals {
                values: simple.into_iter().cloned().collect(),
            });
        }

        // Add object or array literals as type-erased Union members.
        if complex
            .iter()
            .any(|v| matches!(v, serde_json::Value::Object(_)))
        {
            let mut obj_shape = Shape::anything();
            obj_shape.type_ = types::OBJECT;
            disjunct.push(self.to_ast(&obj_shape, &format!("{type_name}Object"), classes));
        }
        if complex
            .iter()
            .any(|v| matches!(v, serde_json::Value::Array(_)))
        {
            let mut arr_shape = Shape::anything();
            arr_shape.type_ = types::ARRAY;
            disjunct.push(self.to_ast(&arr_shape, &format!("{type_name}Array"), classes));
        }
        if complex
            .iter()
            .any(|v| matches!(v, serde_json::Value::Number(_)))
        {
            // Floating point numbers cannot be python Literals, so we widen to all floats.
            disjunct.push(AST::Float);
        }

        return if disjunct.len() == 1 {
            disjunct.pop().unwrap()
        } else {
            AST::Union { variants: disjunct }
        };
    }
}

/// Sanitize a name into a valid Python identifier.
/// Replaces invalid characters with underscores and ensures it starts with letter or underscore.
pub fn sanitize_python_identifier(name: &str) -> String {
    let mut result = String::new();

    let is_reserved_keyword = matches!(
        name,
        "False"
            | "None"
            | "True"
            | "and"
            | "as"
            | "assert"
            | "async"
            | "await"
            | "break"
            | "class"
            | "continue"
            | "def"
            | "del"
            | "elif"
            | "else"
            | "except"
            | "finally"
            | "for"
            | "from"
            | "global"
            | "if"
            | "import"
            | "in"
            | "is"
            | "lambda"
            | "nonlocal"
            | "not"
            | "or"
            | "pass"
            | "raise"
            | "return"
            | "try"
            | "while"
            | "with"
            | "yield"
    );
    if is_reserved_keyword {
        return format!("{name}_");
    }

    for (i, c) in name.chars().enumerate() {
        if i == 0 {
            // First character must be letter or underscore
            if c.is_alphabetic() || c == '_' {
                result.push(c);
            } else if c.is_numeric() {
                // Prefix with underscore if starts with digit
                result.push('_');
                result.push(c);
            } else {
                result.push('_');
            }
        } else {
            // Subsequent characters can be alphanumeric or underscore
            if c.is_alphanumeric() || c == '_' {
                result.push(c);
            } else {
                result.push('_');
            }
        }
    }

    // Handle edge case of empty string
    if result.is_empty() {
        result.push('_');
    }

    result
}

lazy_static::lazy_static! {
    // Named schema anchor regex: PascalCase names starting with capital letter
    static ref NAMED_SCHEMA_RE: Regex = Regex::new(r"^[A-Z][\w_]+$").unwrap();
}

#[cfg(test)]
mod test {
    use super::{Mapper, sanitize_python_identifier};
    use std::fmt::Write;

    #[test]
    fn test_sanitize_python_identifier() {
        let cases = vec![
            // (input, expected_output, description)
            // Valid identifiers that should remain unchanged
            ("validName", "validName", "valid identifier unchanged"),
            ("_private", "_private", "leading underscore preserved"),
            ("CamelCase", "CamelCase", "CamelCase unchanged"),
            ("snake_case", "snake_case", "snake_case unchanged"),
            ("UPPER_CASE", "UPPER_CASE", "UPPER_CASE unchanged"),
            ("name123", "name123", "ASCII with trailing digits"),
            (
                "name_123_test",
                "name_123_test",
                "mixed underscores and digits",
            ),
            ("_123", "_123", "underscore followed by digits"),
            // Invalid starting characters
            ("123start", "_123start", "starts with digit"),
            ("9field", "_9field", "starts with single digit"),
            ("-field", "_field", "starts with hyphen"),
            ("@special", "_special", "starts with @ symbol"),
            ("$money", "_money", "starts with $ symbol"),
            // Invalid characters in middle/end
            ("my-field", "my_field", "hyphen in middle"),
            ("dot.name", "dot_name", "dot in middle"),
            ("has space", "has_space", "space in middle"),
            ("multi-word-name", "multi_word_name", "multiple hyphens"),
            ("field@end", "field_end", "@ at end"),
            // Unicode and special cases
            ("lğmöm", "lğmöm", "unicode letters preserved"),
            ("café", "café", "unicode in continuation position"),
            ("café123", "café123", "unicode with digits"),
            ("μάθημα", "μάθημα", "Greek identifier"),
            ("ñoño", "ñoño", "Spanish identifier"),
            ("переменная", "переменная", "Cyrillic identifier"),
            ("变量", "变量", "Chinese identifier"),
            ("field🎉", "field_", "emoji converted to underscore"),
            ("name/path", "name_path", "slash converted"),
            ("a:b", "a_b", "colon converted"),
            // Edge cases
            ("", "_", "empty string becomes underscore"),
            ("_", "_", "single underscore preserved"),
            ("__", "__", "double underscore preserved"),
            ("123", "_123", "only digits prefixed"),
            ("___start", "___start", "multiple underscores preserved"),
            // Complex cases
            (
                "my-complex.field@123",
                "my_complex_field_123",
                "multiple special chars",
            ),
            ("__private__", "__private__", "dunder style preserved"),
            (
                "field-1-name",
                "field_1_name",
                "mixed alphanumeric with hyphens",
            ),
            // Special characters
            ("!", "_", "single exclamation"),
            ("@", "_", "single @ symbol"),
            ("#hashtag", "_hashtag", "starts with hash"),
            ("100%", "_100_", "starts with digit, has percent"),
        ];

        for (input, expected, description) in cases {
            let result = sanitize_python_identifier(input);
            assert_eq!(
                result, expected,
                "Failed: {} - input: '{}', expected: '{}', got: '{}'",
                description, input, expected, result
            );
        }
    }

    #[test]
    fn schema_generation() {
        let fixture = serde_json::from_slice(include_bytes!("mapper_test.json")).unwrap();
        let mut sources = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);
        sources::inline_draft_catalog(&mut sources);

        let tables::DraftCatalog {
            collections,
            errors,
            ..
        } = sources;

        if !errors.is_empty() {
            panic!("unexpected errors: {errors:?}");
        }
        let mut w = String::new();

        for collection in collections.iter() {
            let bundle = collection
                .model
                .as_ref()
                .unwrap()
                .schema
                .as_ref()
                .unwrap()
                .get()
                .as_bytes();

            let m = Mapper::new(bundle, "Doc");
            writeln!(
                &mut w,
                "Schema for {name} with CURI {curi}:",
                name = collection.collection.as_str(),
                curi = m.schema().curi(),
            )
            .unwrap();
            m.map(m.schema(), "Document").render(&mut w);

            let m = Mapper::new(bundle, "");
            writeln!(
                &mut w,
                "Schema for {name} with CURI {curi} without anchors:",
                name = collection.collection.as_str(),
                curi = m.schema().curi(),
            )
            .unwrap();
            m.map(m.schema(), "Document").render(&mut w);
        }

        insta::assert_snapshot!(w);
    }
}
