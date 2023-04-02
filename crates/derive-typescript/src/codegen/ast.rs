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

pub struct Context<'a> {
    pub into: &'a mut String,
    pub indent: usize,
}

impl<'a> Context<'a> {
    pub fn new(into: &'a mut String) -> Self {
        Self { into, indent: 0 }
    }

    fn push_inner(&self, into: &'a mut String) -> Self {
        Self {
            into,
            indent: self.indent,
        }
    }
}

impl AST {
    pub fn render(&self, ctx: &mut Context) {
        match self {
            AST::Comment { body, of } => {
                ctx.into.push_str("/* ");
                ctx.into.push_str(body);
                ctx.into.push_str(" */ ");
                of.render(ctx);
            }
            AST::Never => ctx.into.push_str("never"),
            AST::Unknown => ctx.into.push_str("unknown"),
            AST::Boolean => ctx.into.push_str("boolean"),
            AST::Null => ctx.into.push_str("null"),
            AST::Number => ctx.into.push_str("number"),
            AST::String => ctx.into.push_str("string"),
            AST::Undefined => ctx.into.push_str("undefined"),
            AST::Literal { value } => ctx.into.push_str(&value.to_string()),
            AST::Array { of } => Self::render_array(ctx, &of),
            AST::Tuple(tuple) => Self::render_tuple(ctx, tuple),
            AST::Object { properties } if properties.is_empty() => {
                ctx.into.push_str("Record<string, unknown>")
            }
            AST::Object { properties } => Self::render_object(ctx, properties),
            AST::Union { variants } => Self::render_disjunction(ctx, variants),
            AST::Anchor(anchor) => ctx.into.push_str(anchor),
        }
    }

    fn render_array(ctx: &mut Context, of: &AST) {
        let mut inner = String::new();
        of.render(&mut ctx.push_inner(&mut inner));

        if inner.ends_with("\"") {
            ctx.into.push('(');
            ctx.into.push_str(&inner);
            ctx.into.push_str(")[]");
        } else {
            ctx.into.push_str(&inner);
            ctx.into.push_str("[]");
        }
    }

    fn render_tuple(ctx: &mut Context, tuple: &ASTTuple) {
        ctx.into.push('[');
        for (ind, item) in tuple.items.iter().enumerate() {
            if ind != 0 {
                ctx.into.push_str(", ");
            }
            item.render(ctx);

            if ind >= tuple.min_items {
                ctx.into.push('?');
            }
        }
        // Tack on spread AST, if present.
        if let Some(spread) = &tuple.spread {
            ctx.into.push_str(", ...(");
            spread.render(ctx);
            ctx.into.push_str(")[]");
        }
        ctx.into.push_str("]");
    }

    fn render_disjunction(ctx: &mut Context, variants: &[AST]) {
        for (ind, item) in variants.iter().enumerate() {
            if ind != 0 {
                ctx.into.push_str(" | ");
            }
            item.render(ctx);
        }
    }

    fn render_object(ctx: &mut Context, properties: &[ASTProperty]) {
        if properties.is_empty() {
            ctx.into.push_str("{}");
            return;
        }
        ctx.into.push('{');

        for prop in properties.iter() {
            ctx.indent += 1;

            Self::push_newline(ctx);
            ctx.into.push_str(&prop.field);
            if !prop.is_required {
                ctx.into.push('?');
            }
            ctx.into.push_str(": ");
            prop.value.render(ctx);
            ctx.into.push_str(";");
            ctx.indent -= 1;
        }
        Self::push_newline(ctx);
        ctx.into.push('}');
    }

    fn push_newline(ctx: &mut Context) {
        ctx.into.push('\n');
        ctx.into.extend(std::iter::repeat(' ').take(ctx.indent * 4));
    }
}
