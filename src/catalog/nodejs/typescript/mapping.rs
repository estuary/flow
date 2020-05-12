use super::ast::{ASTProperty, ASTTuple, AST};
use crate::doc::{inference, Schema, SchemaIndex};
use estuary_json::schema::types;
use std::collections::BTreeMap;
use url::Url;

pub struct Mapper<'a> {
    pub index: &'a SchemaIndex<'a>,
    pub top_level: &'a BTreeMap<Url, String>,
}

impl<'a> Mapper<'a> {
    pub fn map(&self, scm: &Schema) -> AST {
        let shape = inference::Shape::infer(scm, self.index);
        to_ast(&shape)
    }
}

fn to_ast(shape: &inference::Shape) -> AST {
    let mut ast = to_ast_inner(shape);

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

fn to_ast_inner(shape: &inference::Shape) -> AST {
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
        disjunct.push(object_to_ast(&shape.object));
    }
    if shape.type_.overlaps(types::ARRAY) {
        disjunct.push(array_to_ast(&shape.array));
    }
    if shape.type_.overlaps(types::BOOLEAN) {
        disjunct.push(AST::Boolean);
    }
    if shape.type_.overlaps(types::INTEGER | types::NUMBER) {
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

fn object_to_ast(obj: &inference::ObjShape) -> AST {
    let mut props: Vec<ASTProperty> = Vec::new();

    for prop in &obj.properties {
        props.push(ASTProperty {
            field: prop.name.clone(),
            is_required: prop.is_required,
            value: to_ast(&prop.shape),
        });
    }
    if let Some(addl) = &obj.additional {
        props.push(ASTProperty {
            field: "[k: string]".to_owned(),
            value: to_ast(&addl),
            is_required: false,
        })
    }
    AST::Object { properties: props }
}

fn array_to_ast(obj: &inference::ArrayShape) -> AST {
    if obj.tuple.is_empty() {
        let spread = match &obj.additional {
            None => AST::Any,
            Some(shape) => to_ast(&shape),
        };
        return AST::Array {
            of: Box::new(spread),
        };
    }

    let items = obj.tuple.iter().map(|l| to_ast(l)).collect::<Vec<_>>();

    let spread = match &obj.additional {
        None => None,
        Some(shape) => Some(Box::new(to_ast(&shape))),
    };

    AST::Tuple(ASTTuple {
        items,
        spread,
        min_items: obj.min.unwrap_or(0),
    })
}
