use serde_json::Value;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ASTProperty {
    pub field: String,
    pub value: AST,
    pub is_required: bool,
}

impl AST {
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
            into.push(b'\n');
        }
        into.push(b'}');
    }
}
