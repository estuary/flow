use super::ast::{ASTProperty, ASTTuple, AST};
use crate::doc::{Schema, SchemaIndex};
use estuary_json::schema::{inference, types};
use std::collections::BTreeMap;
use url::Url;

pub struct Mapper<'a> {
    pub index: &'a SchemaIndex<'a>,
    pub top_level: &'a BTreeMap<Url, String>,
}

impl<'a> Mapper<'a> {
    pub fn map(&self, scm: &Schema) -> AST {
        let loc = inference::Location::infer(scm, self.index);
        to_ast(&loc)
    }
}

fn to_ast(loc: &inference::Location) -> AST {
    let mut ast = to_ast_inner(loc);

    if let Some(desc) = &loc.description {
        ast = AST::Comment {
            body: desc.clone(),
            of: Box::new(ast),
        };
    }
    if let Some(title) = &loc.title {
        ast = AST::Comment {
            body: title.clone(),
            of: Box::new(ast),
        };
    }

    ast
}

fn to_ast_inner(loc: &inference::Location) -> AST {
    // Is this a trivial ANY type?
    if loc.type_ == types::ANY
        && loc.enum_.is_none()
        && loc.array.additional.is_none()
        && loc.array.tuple.is_empty()
        && loc.object.properties.is_empty()
        && loc.object.additional.is_none()
    {
        return AST::Any;
    }
    // Is this an enum? Just emit the variants.
    if let Some(enum_) = &loc.enum_ {
        return AST::Union {
            variants: enum_
                .iter()
                .map(|l| AST::Literal { value: l.clone() })
                .collect(),
        };
    }

    let mut disjunct = Vec::new();

    if loc.type_.overlaps(types::OBJECT) {
        disjunct.push(object_to_ast(&loc.object));
    }
    if loc.type_.overlaps(types::ARRAY) {
        disjunct.push(array_to_ast(&loc.array));
    }
    if loc.type_.overlaps(types::BOOLEAN) {
        disjunct.push(AST::Boolean);
    }
    if loc.type_.overlaps(types::INTEGER | types::NUMBER) {
        disjunct.push(AST::Number);
    }
    if loc.type_.overlaps(types::STRING) {
        disjunct.push(AST::String);
    }
    if loc.type_.overlaps(types::NULL) {
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

fn object_to_ast(obj: &inference::ObjLocation) -> AST {
    let mut props: Vec<ASTProperty> = Vec::new();

    for prop in &obj.properties {
        props.push(ASTProperty {
            field: prop.name.clone(),
            is_required: prop.is_required,
            value: to_ast(&prop.value),
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

fn array_to_ast(obj: &inference::ArrayLocation) -> AST {
    if obj.tuple.is_empty() {
        let spread = match &obj.additional {
            None => AST::Any,
            Some(loc) => to_ast(&loc),
        };
        return AST::Array {
            of: Box::new(spread),
        };
    }

    let items = obj.tuple.iter().map(|l| to_ast(l)).collect::<Vec<_>>();

    let spread = match &obj.additional {
        None => None,
        Some(loc) => Some(Box::new(to_ast(&loc))),
    };

    AST::Tuple(ASTTuple {
        items,
        spread,
        min_items: obj.min.unwrap_or(0),
    })
}
