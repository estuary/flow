use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum AST {
    Comment { body: String, of: Box<AST> },
    Never,
    Unknown,
    Boolean,
    Null,
    Number,
    String,
    Undefined,
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
    pub fn render(&self, indent: usize, into: &mut String) {
        match self {
            AST::Comment { body, of } => {
                into.push_str("/* ");
                into.push_str(body);
                into.push_str(" */ ");
                of.render(indent, into);
            }
            AST::Never => into.push_str("never"),
            AST::Unknown => into.push_str("unknown"),
            AST::Boolean => into.push_str("boolean"),
            AST::Null => into.push_str("null"),
            AST::Number => into.push_str("number"),
            AST::String => into.push_str("string"),
            AST::Undefined => into.push_str("undefined"),
            AST::Literal { value } => into.push_str(&value.to_string()),
            AST::Array { of } => Self::render_array(indent, into, &of),
            AST::Tuple(tuple) => Self::render_tuple(indent, into, tuple),
            AST::Object { properties } if properties.is_empty() => {
                into.push_str("Record<string, unknown>")
            }
            AST::Object { properties } => Self::render_object(indent, into, properties),
            AST::Union { variants } => Self::render_disjunction(indent, into, variants),
            AST::Anchor(anchor) => {
                into.push_str("anchors.");
                into.push_str(anchor);
            }
        }
    }

    fn render_array(indent: usize, into: &mut String, of: &AST) {
        let mut inner = String::new();
        of.render(indent, &mut inner);

        if inner.ends_with("\"") {
            into.push('(');
            into.push_str(&inner);
            into.push_str(")[]");
        } else {
            into.push_str(&inner);
            into.push_str("[]");
        }
    }

    fn render_tuple(indent: usize, into: &mut String, tuple: &ASTTuple) {
        into.push('[');
        for (ind, item) in tuple.items.iter().enumerate() {
            if ind != 0 {
                into.push_str(", ");
            }
            item.render(indent, into);

            if ind >= tuple.min_items {
                into.push('?');
            }
        }
        // Tack on spread AST, if present.
        if let Some(spread) = &tuple.spread {
            into.push_str("...(");
            spread.render(indent, into);
            into.push_str(")[]");
        }
        into.push_str("]");
    }

    fn render_disjunction(indent: usize, into: &mut String, variants: &[AST]) {
        for (ind, item) in variants.iter().enumerate() {
            if ind != 0 {
                into.push_str(" | ");
            }
            item.render(indent, into);
        }
    }

    fn render_object(indent: usize, into: &mut String, properties: &[ASTProperty]) {
        if properties.is_empty() {
            into.push_str("{}");
            return;
        }

        into.push('{');

        for prop in properties.iter() {
            Self::push_newline(indent + 1, into);
            into.push_str(&prop.field);
            if !prop.is_required {
                into.push('?');
            }
            into.push_str(": ");
            prop.value.render(indent + 1, into);
            into.push_str(";");
        }
        Self::push_newline(indent, into);
        into.push('}');
    }

    fn push_newline(indent: usize, into: &mut String) {
        into.push('\n');
        into.extend(std::iter::repeat(' ').take(indent * 4));
    }
}
