use super::ast::{ASTProperty, ASTTuple, AST};
use doc::{
    inference::{ArrayShape, ObjShape, Provenance, Shape},
    Schema, SchemaIndex,
};
use json::schema::types;
use std::collections::BTreeMap;

pub struct Mapper<'a> {
    pub index: &'a SchemaIndex<'a>,
    pub top_level: &'a BTreeMap<&'a str, &'a str>,
}

impl<'a> Mapper<'a> {
    pub fn map(&self, scm: &Schema) -> AST {
        let shape = Shape::infer(scm, self.index);
        self.to_ast(&shape)
    }

    fn to_ast(&self, shape: &Shape) -> AST {
        let mut ast = self.to_ast_inner(shape);

        if let Some(desc) = &shape.description {
            ast = AST::Comment {
                body: desc.clone(),
                of: Box::new(ast),
            };
        }
        if let Some(title) = &shape.title {
            ast = AST::Comment {
                body: title.clone(),
                of: Box::new(ast),
            };
        }

        ast
    }

    fn to_ast_inner(&self, shape: &Shape) -> AST {
        if let Provenance::Reference(uri) = &shape.provenance {
            if let Some(anchor) = self.top_level.get(uri.as_str()) {
                return AST::Anchor((*anchor).to_owned());
            }
        }

        // Is this a trivial ANY type?
        if shape.type_ == types::ANY
            && shape.enum_.is_none()
            && shape.array.additional.is_none()
            && shape.array.tuple.is_empty()
            && shape.object.properties.is_empty()
            && shape.object.additional.is_none()
        {
            return AST::Any;
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
            props.push(ASTProperty {
                field: prop.name.clone(),
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
                None => AST::Any,
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
