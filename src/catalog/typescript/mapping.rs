use super::ast::{ASTProperty, AST};
use crate::doc::{Schema, SchemaIndex};
use estuary_json::schema::{
    intern, types, Application as App, HashedLiteral, Keyword as KW, Validation as Val,
};

pub fn map_ast(scm: &Schema, ind: &SchemaIndex) -> AST {
    let mut _types = types::ANY;
    let mut _const: Option<&'_ HashedLiteral> = None;
    let mut _enum: Option<&'_ Vec<HashedLiteral>> = None;
    let mut min_items: Option<usize> = None;
    let mut required_props: intern::Set = 0;

    let mut maybe: Vec<AST> = Vec::new();
    let mut must_be: Vec<AST> = Vec::new();

    let mut named_props: Vec<(&String, u64, AST)> = Vec::new();
    let mut extra_props: Vec<AST> = Vec::new(); // Union of additionalProps, patternProps, and unevaluatedProps.

    let mut tuple_items: Vec<AST> = Vec::new();
    let mut extra_items: Vec<AST> = Vec::new(); // Union of items, additionalItems, and unevaluatedItems.

    for kw in &scm.kw {
        if let KW::Validation(val) = kw {
            match val {
                Val::Type(set) => _types = *set,
                Val::Const(literal) => _const = Some(literal),
                Val::Enum { variants } => _enum = Some(variants),
                Val::MinItems(n) => min_items = Some(*n),
                Val::Required(set) => required_props = *set,

                _ => {} // No-op.
            }
        } else if let KW::Application(app, child) = kw {
            match app {
                // Conjunctions.
                App::AllOf { .. } => must_be.push(map_ast(child, ind)),
                App::Ref(url) => must_be.push(map_ast(ind.fetch(url).unwrap(), ind)),
                // Disjunctions.
                App::AnyOf { .. } => maybe.push(map_ast(child, ind)),
                App::OneOf { .. } => maybe.push(map_ast(child, ind)),
                App::Then { .. } => maybe.push(map_ast(child, ind)),
                App::Else { .. } => maybe.push(map_ast(child, ind)),
                App::DependentSchema { .. } => maybe.push(map_ast(child, ind)),

                // Property applications.
                App::Properties {
                    name,
                    name_interned,
                } => named_props.push((name, *name_interned, map_ast(child, ind))),
                App::AdditionalProperties => extra_props.push(map_ast(child, ind)),
                App::UnevaluatedProperties => extra_props.push(map_ast(child, ind)),

                // Item applications.
                App::Items { index: None } => extra_items.push(map_ast(child, ind)),
                App::Items { .. } => tuple_items.push(map_ast(child, ind)),
                App::AdditionalItems => extra_items.push(map_ast(child, ind)),
                App::UnevaluatedItems => extra_items.push(map_ast(child, ind)),

                _ => {} // No-op.
            }
        }
    }

    // Build an interface AST.
    let mut interface: Option<AST> = None;

    if !named_props.is_empty() || !extra_props.is_empty() {
        let mut props: Vec<ASTProperty> = Vec::new();

        for (name, hash, value) in named_props {
            props.push(ASTProperty {
                field: name.clone(),
                value,
                is_required: hash & required_props != 0,
            });
        }
        if !extra_props.is_empty() {
            props.push(ASTProperty {
                field: "[k: string]".to_owned(),
                value: AST::Union {
                    variants: extra_props,
                },
                is_required: false,
            })
        }
        interface = Some(AST::Interface { properties: props });
    }

    // Build an array AST.
    let mut array: Option<AST> = None;

    if !tuple_items.is_empty() || !extra_items.is_empty() {
        let mut cases = Vec::new();

        // For each subset of [min_items, items.len()), emit a union type.
        for bound in min_items.unwrap_or(0)..tuple_items.len() {
            cases.push(AST::Tuple {
                items: tuple_items[0..bound].to_vec(),
                spread: None,
            });
        }

        if !tuple_items.is_empty() && !extra_items.is_empty() {
            cases.push(AST::Tuple {
                items: tuple_items,
                spread: Some(Box::new(AST::Union {
                    variants: extra_items,
                })),
            });
        } else if !tuple_items.is_empty() {
            cases.push(AST::Tuple {
                items: tuple_items,
                spread: None,
            });
        } else {
            cases.push(AST::Array {
                of: Box::new(AST::Union {
                    variants: extra_items,
                }),
            })
        }
        array = Some(AST::Union { variants: cases });
    }

    let mut self_terms = Vec::new();

    if _types == types::ANY && array.is_none() && interface.is_none() {
        self_terms.push(AST::Any)
    } else {
        if _types & types::ARRAY != types::INVALID {
            match array {
                Some(ast) => self_terms.push(ast),
                None => self_terms.push(AST::Array {
                    of: Box::new(AST::Any),
                }),
            }
        }
        if _types & types::OBJECT != types::INVALID {
            match interface {
                Some(ast) => self_terms.push(ast),
                None => self_terms.push(AST::Object),
            }
        }
        if _types & types::BOOLEAN != types::INVALID {
            self_terms.push(AST::Boolean)
        }
        if _types & (types::INTEGER | types::NUMBER) != types::INVALID {
            self_terms.push(AST::Number)
        }
        if _types & types::STRING != types::INVALID {
            self_terms.push(AST::String)
        }
        if _types & types::NULL != types::INVALID {
            self_terms.push(AST::Null)
        }
    }
    must_be.push(AST::Union {
        variants: self_terms,
    });

    if !maybe.is_empty() {
        must_be.push(AST::Union { variants: maybe });
    }

    AST::Intersection { variants: must_be }
}
