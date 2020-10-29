use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum AST {
    Comment { body: String, of: Box<AST> },
    Never,
    Any,
    Boolean,
    Null,
    Number,
    String,
    Literal { value: Value },
    Array { of: Box<AST> },
    Tuple(ASTTuple),
    Object { properties: Vec<ASTProperty> },
    Union { variants: Vec<AST> },
    Anchor(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ASTTuple {
    pub items: Vec<AST>,
    pub min_items: usize,
    pub spread: Option<Box<AST>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ASTProperty {
    pub field: String,
    pub value: AST,
    pub is_required: bool,
}

impl AST {
    pub fn render(&self, into: &mut Vec<u8>) {
        match self {
            AST::Comment { body, of } => {
                into.extend_from_slice(b"/*");
                into.extend_from_slice(body.as_bytes());
                into.extend_from_slice(b"*/");
                of.render(into);
            }
            AST::Never => into.extend_from_slice(b"never"),
            AST::Any => into.extend_from_slice(b"any"),
            AST::Boolean => into.extend_from_slice(b"boolean"),
            AST::Null => into.extend_from_slice(b"null"),
            AST::Number => into.extend_from_slice(b"number"),
            AST::String => into.extend_from_slice(b"string"),
            AST::Literal { value } => serde_json::to_writer(into, value).unwrap(),
            AST::Array { of } => Self::render_array(into, &of),
            AST::Tuple(tuple) => Self::render_tuple(into, tuple),
            AST::Object { properties } => Self::render_object(into, properties),
            AST::Union { variants } => Self::render_set(into, variants, b" | "),
            AST::Anchor(anchor) => {
                into.extend_from_slice(b"anchors.");
                into.extend_from_slice(anchor.as_bytes());
            }
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

    fn render_tuple(into: &mut Vec<u8>, tuple: &ASTTuple) {
        into.push(b'[');
        for (ind, item) in tuple.items.iter().enumerate() {
            if ind != 0 {
                into.extend_from_slice(b", ");
            }
            item.render(into);

            if ind >= tuple.min_items {
                into.push(b'?');
            }
        }
        // Tack on spread AST, if present.
        if let Some(spread) = &tuple.spread {
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

    fn render_object(into: &mut Vec<u8>, properties: &[ASTProperty]) {
        if properties.is_empty() {
            into.extend_from_slice(b"{}");
            return;
        }

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
