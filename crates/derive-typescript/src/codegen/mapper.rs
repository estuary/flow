use super::ast::{ASTProperty, ASTTuple, AST};
use doc::shape::{ArrayShape, ObjShape, Provenance, Shape};
use json::schema::{types, Keyword};
use regex::Regex;
use std::collections::BTreeMap;

pub struct Mapper {
    pub top_level: BTreeMap<url::Url, String>,
    validator: doc::Validator,
    anchor_prefix: String,
}

impl Mapper {
    pub fn new(bundle: &[u8], anchor_prefix: &str) -> Self {
        let schema = doc::validation::build_bundle(bundle).unwrap();
        let validator = doc::validation::Validator::new(schema).unwrap();

        let mut top_level = BTreeMap::new();

        if !anchor_prefix.is_empty() {
            let mut stack = vec![&validator.schemas()[0]];
            while let Some(schema) = stack.pop() {
                for kw in &schema.kw {
                    match kw {
                        Keyword::Anchor(anchor_uri) => {
                            // Does this anchor meet our definition of a named schema?
                            if let Some((_, anchor)) = anchor_uri
                                .as_str()
                                .split_once('#')
                                .filter(|(_, s)| NAMED_SCHEMA_RE.is_match(s))
                            {
                                top_level.insert(anchor_uri.clone(), anchor.to_owned());
                            }
                        }
                        Keyword::Application(_, child) => {
                            stack.push(child);
                        }
                        _ => (),
                    }
                }
            }
        }

        // We don't verify index references, as validation is handled
        // elsewhere and this is a best-effort attempt.

        Mapper {
            validator,
            top_level,
            anchor_prefix: anchor_prefix.to_string(),
        }
    }

    // Map the schema having |url| into an abstract syntax tree.
    pub fn map(&self, url: &url::Url) -> AST {
        let index = self.validator.schema_index();
        let shape = match index.fetch(url) {
            Some(schema) => Shape::infer(schema, index),
            None => Shape::anything(),
        };
        self.to_ast(&shape)
    }

    pub fn root(&self) -> &url::Url {
        &self.validator.schemas()[0].curi
    }

    fn to_ast(&self, shape: &Shape) -> AST {
        if let Provenance::Reference(uri) = &shape.provenance {
            if let Some(anchor) = self.top_level.get(uri) {
                let mut ast = AST::Anchor(format!("{}{anchor}", &self.anchor_prefix));

                // Wrap with a `title` keyword comment, but not `description`.
                if let Some(title) = &shape.title {
                    ast = AST::Comment {
                        body: title.to_string(),
                        of: Box::new(ast),
                    };
                }

                return ast;
            }
        }

        let mut ast = self.to_ast_inner(shape);

        match (&shape.title, &shape.description) {
            (Some(title), Some(description)) => {
                ast = AST::Comment {
                    body: format!("{} {}", title, description),
                    of: Box::new(ast),
                };
            }
            (Some(s), None) | (None, Some(s)) => {
                ast = AST::Comment {
                    body: s.to_string(),
                    of: Box::new(ast),
                };
            }
            (None, None) => {}
        }

        ast
    }

    fn to_ast_inner(&self, shape: &Shape) -> AST {
        // Is this a trivial ANY type?
        if shape.type_ == types::ANY
            && shape.enum_.is_none()
            && shape.array.additional_items.is_none()
            && shape.array.tuple.is_empty()
            && shape.object.properties.is_empty()
            && shape.object.additional_properties.is_none()
        {
            return AST::Unknown;
        }
        // Is this an enum? Just emit the variants.
        if let Some(enum_) = shape.enum_.as_ref().filter(|e| !e.is_empty()) {
            return AST::Union {
                variants: enum_
                    .iter()
                    .map(|l| AST::Literal { value: l.clone() })
                    .collect(),
            };
        }

        let mut disjunct = Vec::new();

        if shape.type_.overlaps(types::OBJECT) {
            disjunct.push(self.object_to_ast(&shape.object));
        }
        if shape.type_.overlaps(types::ARRAY) {
            disjunct.push(self.array_to_ast(&shape.array));
        }
        if shape.type_.overlaps(types::BOOLEAN) {
            disjunct.push(AST::Boolean);
        }
        if shape.type_.overlaps(types::INT_OR_FRAC) {
            // TypeScript doesn't distinguish integer vs fractional numbers.
            disjunct.push(AST::Number);
        }
        if shape.type_.overlaps(types::STRING) {
            disjunct.push(AST::String);
        }
        if shape.type_.overlaps(types::NULL) {
            disjunct.push(AST::Null);
        }

        if disjunct.is_empty() {
            AST::Never
        } else if disjunct.len() == 1 {
            disjunct.pop().unwrap()
        } else {
            AST::Union { variants: disjunct }
        }
    }

    fn object_to_ast(&self, obj: &ObjShape) -> AST {
        let mut props: Vec<ASTProperty> = Vec::new();

        for prop in &obj.properties {
            let field = if TS_VARIABLE_RE.is_match(&prop.name) {
                prop.name.to_string()
            } else {
                // Use JSON encoding to escape and quote the property.
                serde_json::Value::String(prop.name.to_string()).to_string()
            };

            props.push(ASTProperty {
                field,
                value: self.to_ast(&prop.shape),
                is_required: prop.is_required,
            });
        }

        if !obj.pattern_properties.is_empty()
            || matches!(&obj.additional_properties, Some(addl) if addl.type_ != types::INVALID)
        {
            // TypeScript indexers can model additional properties, but they must be a union
            // type that accommodates *all* types used across any property.
            // See: https://basarat.gitbook.io/typescript/type-system/index-signatures

            let mut merged = Shape::nothing();
            let mut has_optional = false;

            for prop in &obj.properties {
                merged = Shape::union(merged, prop.shape.clone());
                has_optional = has_optional || !prop.is_required;
            }
            for prop in &obj.pattern_properties {
                merged = Shape::union(merged, prop.shape.clone());
            }
            match &obj.additional_properties {
                Some(addl) if addl.type_ != types::INVALID => {
                    merged = Shape::union(merged, addl.as_ref().clone());
                }
                _ => (),
            }

            let merged = match (has_optional, self.to_ast(&merged)) {
                (true, AST::Union { mut variants }) => {
                    variants.push(AST::Undefined);
                    AST::Union { variants }
                }
                (true, merged) => AST::Union {
                    variants: vec![merged, AST::Undefined],
                },
                (false, merged) => merged,
            };

            props.push(ASTProperty {
                field: "[k: string]".to_owned(),
                value: merged,
                // Optional '?' has no meaning for variadic properties.
                is_required: true,
            });
        }

        AST::Object { properties: props }
    }

    fn array_to_ast(&self, arr: &ArrayShape) -> AST {
        if arr.tuple.is_empty() {
            let spread = match &arr.additional_items {
                None => AST::Unknown,
                Some(shape) => self.to_ast(&shape),
            };
            return AST::Array {
                of: Box::new(spread),
            };
        }

        let items = arr.tuple.iter().map(|l| self.to_ast(l)).collect::<Vec<_>>();

        let spread = match &arr.additional_items {
            // The test filters cases of, eg, additionalItems: false.
            Some(addl) if addl.type_ != types::INVALID => Some(Box::new(self.to_ast(&addl))),
            _ => None,
        };

        AST::Tuple(ASTTuple {
            items,
            spread,
            min_items: arr.min_items as usize,
        })
    }
}

lazy_static::lazy_static! {
    // The set of allowed characters in a bare TypeScript variable name.
    static ref TS_VARIABLE_RE : Regex = Regex::new(r"^\pL[\pL\pN_]*$").unwrap();
    // The set of allowed characters in a schema `$anchor` is quite limited,
    // by Sec 8.2.3.
    //
    // To identify named schemas, we further restrict to anchors which start
    // with a capital letter and include only '_' as punctuation.
    // See: https://json-schema.org/draft/2019-09/json-schema-core.html#anchor
    static ref NAMED_SCHEMA_RE: regex::Regex = regex::Regex::new("^[A-Z][\\w_]+$").unwrap();
}

#[cfg(test)]
mod test {

    use super::super::ast::Context;
    use super::Mapper;
    use std::fmt::Write;

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
            let m = Mapper::new(
                collection.model.clone().unwrap().schema.unwrap().get().as_bytes(),
                "Doc",
            );
            writeln!(
                &mut w,
                "Schema for {name} with CURI {curi} with anchors:",
                name = collection.collection.as_str(),
                curi = m.root(),
            )
            .unwrap();
            m.map(m.root()).render(&mut Context::new(&mut w));
            w.push_str("\n\n");

            let m = Mapper::new(collection.model.clone().unwrap().schema.unwrap().get().as_bytes(), "");
            writeln!(
                &mut w,
                "Schema for {name} with CURI {curi} without anchors:",
                name = collection.collection.as_str(),
                curi = m.root(),
            )
            .unwrap();
            m.map(m.root()).render(&mut Context::new(&mut w));
            w.push_str("\n\n");
        }

        insta::assert_snapshot!(w);
    }
}
