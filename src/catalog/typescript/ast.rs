use serde_json::Value;
use estuary_json::schema::types;

#[derive(Debug, Clone, PartialEq)]
pub enum AST {
    Any,
    Boolean,
    Null,
    Number,
    Object,
    String,
    Reference {
        to: String,
    },
    Literal {
        value: Value,
    },
    Array {
        of: Box<AST>,
    },
    Tuple {
        items: Vec<AST>,
        spread: Option<Box<AST>>,
    },
    Intersection {
        variants: Vec<AST>,
    },
    Union {
        variants: Vec<AST>,
    },
    Interface {
        properties: Vec<ASTProperty>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ASTProperty {
    pub field: String,
    pub value: AST,
    pub is_required: bool,
}

impl AST {

    pub fn possible_types(&self) -> types::Set {
        match self {
            AST::Any => types::ANY,
            AST::Boolean => types::BOOLEAN,
            AST::Null => types::NULL,
            AST::Number => types::NUMBER,
            AST::Object => types::OBJECT,
            AST::String => types::STRING,
            AST::Reference { .. } => types::ANY,
            AST::Literal { value } => {
                match value {
                    Value::String(_) => types::STRING,
                    Value::Object(_) => types::OBJECT,
                    Value::Number(_) => types::NUMBER,
                    Value::Null => types::NULL,
                    Value::Bool(_) => types::BOOLEAN,
                    Value::Array(_) => types::ARRAY,
                }
            },
            AST::Array { .. } => types::ARRAY,
            AST::Tuple { .. } => types::ARRAY,
            AST::Interface { .. } => types::OBJECT,
            AST::Union { variants } => {
                variants
                    .iter()
                    .fold(types::INVALID, |cur, v| { cur | v.possible_types() })
            }
            AST::Intersection { variants } => {
                variants
                    .iter()
                    .fold(types::ANY, |cur, v| { cur & v.possible_types() })
            }
        }
    }

    pub fn optimize(self) -> AST {
        let pt = self.possible_types();
        self.optimize_(pt)
    }

    fn optimize_(self, possible_types: types::Set) -> AST {
        match self {

            // Host inner AST of a Union or Intersection of length 1.
            AST::Intersection { variants } => Self::optimize_set(variants, possible_types, false),
            AST::Union { variants } => Self::optimize_set(variants, possible_types, true),

            // Pass-through rules.
            AST::Array { of } => {
                AST::Array { of: Box::from(of.optimize()) }
            }
            AST::Tuple { items, spread } => {
                AST::Tuple {
                    items: items.into_iter().map(|v| v.optimize()).collect(),
                    spread: spread.map(|v| Box::from(v.optimize())),
                }
            }
            AST::Interface { properties } => {
                AST::Interface {
                    properties: properties
                        .into_iter()
                        .map(|p| ASTProperty {
                            field: p.field,
                            value: p.value.optimize(),
                            is_required: p.is_required,
                        })
                        .collect()
                }
            }

            // Pass-through all other (literal) cases.
            ast @ _=> ast,
        }
    }

    fn optimize_set(mut variants: Vec<AST>, possible_types: types::Set, is_union: bool) -> AST {
        variants = variants.into_iter().map(|v| v.optimize_(possible_types)).collect();

        // Rule: Remove union variants which have an incompatible
        // type with the current document location.
        if is_union {
            variants = variants.into_iter().filter(|v| {
                v.possible_types() & possible_types != types::INVALID
            }).collect();
        }

        // Rule: Remove intersections with the "Any" production.
        if !is_union {
            variants = variants.into_iter().filter(|v| *v != AST::Any).collect();
        }

        // For sets of one, we hoist out the inner element.
        if variants.len() == 1 {
            return variants.into_iter().next().unwrap();
        }

        // If we removed _all_ variants (eg, an intersection of multiple AST::Any),
        // then just return Any.
        if variants.is_empty() {
            return AST::Any;
        }

        if is_union {
            AST::Union { variants }
        } else {
            AST::Intersection { variants }
        }
    }

    pub fn render(&self, into: &mut Vec<u8>) {
        match self {
            AST::Any => into.extend_from_slice(b"any"),
            AST::Boolean => into.extend_from_slice(b"boolean"),
            AST::Null => into.extend_from_slice(b"null"),
            AST::Number => into.extend_from_slice(b"number"),
            AST::Object => into.extend_from_slice(b"object"),
            AST::String => into.extend_from_slice(b"string"),
            AST::Reference { to } => into.extend_from_slice(to.as_bytes()),
            AST::Literal { value } => serde_json::to_writer(into, value).unwrap(),
            AST::Array { of } => Self::render_array(into, &of),
            AST::Tuple { items, spread } => Self::render_tuple(into, items, spread.as_deref()),
            AST::Intersection { variants } => Self::render_set(into, variants, b" & "),
            AST::Union { variants } => Self::render_set(into, variants, b" | "),
            AST::Interface { properties } => Self::render_interface(into, properties),
        }
    }

    fn render_array(into: &mut Vec<u8>, of: &AST) {
        let mut inner = Vec::new();
        of.render(&mut inner);

        if inner.ends_with(b"\"") {
            into.push(b'(');
            into.extend_from_slice(&inner);
            into.extend_from_slice(b")[]");
        } else {
            into.extend_from_slice(&inner);
            into.extend_from_slice(b"[]");
        }
    }

    fn render_tuple(into: &mut Vec<u8>, items: &Vec<AST>, spread: Option<&AST>) {
        into.push(b'[');
        for (ind, item) in items.iter().enumerate() {
            if ind != 0 {
                into.extend_from_slice(b", ");
            }
            item.render(into);
        }
        // Tack on spread AST, if present.
        if let Some(spread) = spread {
            into.extend_from_slice(b"...(");
            spread.render(into);
            into.extend_from_slice(b")[]");
        }
        into.extend_from_slice(b"]");
    }

    fn render_set(into: &mut Vec<u8>, variants: &[AST], sep: &[u8]) {
        into.push(b'(');
        for (ind, item) in variants.iter().enumerate() {
            if ind != 0 {
                into.extend_from_slice(sep);
            }
            item.render(into);
        }
        into.push(b')');
    }

    fn render_interface(into: &mut Vec<u8>, properties: &[ASTProperty]) {
        into.extend_from_slice(b"{\n");
        for prop in properties.iter() {
            into.extend_from_slice(prop.field.as_bytes());
            if !prop.is_required {
                into.push(b'?');
            }
            into.extend_from_slice(b": ");
            prop.value.render(into);
            into.extend_from_slice(b";\n");
        }
        into.push(b'}');
    }
}
