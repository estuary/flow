use super::ast::{ASTProperty, ASTTuple, AST};
use doc::{
    inference::{ArrayShape, ObjShape, Provenance, Shape},
    SchemaIndex,
};
use json::schema::types;
use regex::Regex;
use std::collections::BTreeMap;

pub struct Mapper<'a> {
    pub index: SchemaIndex<'a>,
    pub schema: url::Url,
    pub top_level: BTreeMap<&'a url::Url, String>,
}

impl<'a> Mapper<'a> {
    // Map the schema having |url| into an abstract syntax tree.
    pub fn map(&self, url: &url::Url) -> AST {
        let shape = match self.index.fetch(url) {
            Some(schema) => Shape::infer(schema, &self.index),
            None => Shape::default(),
        };
        self.to_ast(&shape)
    }

    fn to_ast(&self, shape: &Shape) -> AST {
        if let Provenance::Reference(uri) = &shape.provenance {
            if let Some(anchor) = self.top_level.get(uri) {
                let mut ast = AST::Anchor((*anchor).to_owned());

                // Wrap with a `title` keyword comment, but not `description`.
                if let Some(title) = &shape.title {
                    ast = AST::Comment {
                        body: title.clone(),
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
                    body: s.clone(),
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
            && shape.array.additional.is_none()
            && shape.array.tuple.is_empty()
            && shape.object.properties.is_empty()
            && shape.object.additional.is_none()
        {
            return AST::Unknown;
        }
        // Is this an enum? Just emit the variants.
        if let Some(enum_) = &shape.enum_ {
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
                prop.name.clone()
            } else {
                // Use JSON encoding to escape and quote the property.
                serde_json::Value::String(prop.name.clone()).to_string()
            };

            props.push(ASTProperty {
                field,
                value: self.to_ast(&prop.shape),
                is_required: prop.is_required,
            });
        }

        if !obj.patterns.is_empty()
            || matches!(&obj.additional, Some(addl) if addl.type_ != types::INVALID)
        {
            // TypeScript indexers can model additional properties, but they must be a union
            // type that accommodates *all* types used across any property.
            // See: https://basarat.gitbook.io/typescript/type-system/index-signatures

            let mut merged = Shape {
                type_: types::INVALID,
                ..Shape::default()
            };
            let mut has_optional = false;

            for prop in &obj.properties {
                merged = Shape::union(merged, prop.shape.clone());
                has_optional = has_optional || !prop.is_required;
            }
            for prop in &obj.patterns {
                merged = Shape::union(merged, prop.shape.clone());
            }
            match &obj.additional {
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

    fn array_to_ast(&self, obj: &ArrayShape) -> AST {
        if obj.tuple.is_empty() {
            let spread = match &obj.additional {
                None => AST::Unknown,
                Some(shape) => self.to_ast(&shape),
            };
            return AST::Array {
                of: Box::new(spread),
            };
        }

        let items = obj.tuple.iter().map(|l| self.to_ast(l)).collect::<Vec<_>>();

        let spread = match &obj.additional {
            // The test filters cases of, eg, additionalItems: false.
            Some(addl) if addl.type_ != types::INVALID => Some(Box::new(self.to_ast(&addl))),
            _ => None,
        };

        AST::Tuple(ASTTuple {
            items,
            spread,
            min_items: obj.min.unwrap_or(0),
        })
    }
}

lazy_static::lazy_static! {
    // The set of allowed characters in a bare TypeScript variable name.
    static ref TS_VARIABLE_RE : Regex = Regex::new(r"^\pL[\pL\pN_]*$").unwrap();
}
