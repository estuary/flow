use std::fmt::Write;

/// Represents a Pydantic class definition with fields.
/// Classes are hoisted during mapping and rendered as top-level or nested class definitions.
#[derive(Debug, Clone, PartialEq)]
pub struct Class {
    pub name: String,
    pub docstring: Option<String>,
    pub nested: Vec<Class>,
    pub fields: Vec<Field>,
    pub additional: Option<AST>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub alias: Option<String>,     // Field(alias=...)
    pub docstring: Option<String>, // Field(description=...)
    pub is_required: bool,         // Wrap in Optional[...] ?
    pub type_: AST,
}

/// AST nodes that are valid in type annotation contexts.
#[derive(Debug, Clone, PartialEq)]
pub enum AST {
    Never,
    Any,
    Bool,
    None,
    Int,
    Float,
    Str,
    Literals { values: Vec<serde_json::Value> },
    List { of: Box<AST> },
    Tuple { items: Vec<AST> },
    Union { variants: Vec<AST> },
    Anchor(String),
}

pub struct Mapping {
    pub classes: Vec<Class>,
    pub aliases: Vec<(String, AST)>,
}

pub struct Context<'a> {
    pub into: &'a mut String,
    pub indent: usize,
}

impl<'a> Context<'a> {
    pub fn new(into: &'a mut String) -> Self {
        Self { into, indent: 0 }
    }
}

impl Mapping {
    pub fn render(&self, w: &mut String) {
        let mut ctx = Context::new(w);
        for class in &self.classes {
            class.render(&mut ctx);
        }
        std::mem::drop(ctx);

        for (name, ast) in &self.aliases {
            write!(w, "{name}: typing.TypeAlias = ").unwrap();
            ast.render(w);
            w.push('\n');
        }
        w.push_str("\n\n");
    }
}

impl Class {
    pub fn render(&self, ctx: &mut Context) {
        ctx.push_indent();
        write!(ctx.into, "class {}(pydantic.BaseModel):\n", self.name).unwrap();
        ctx.indent += 1;

        if let Some(docstring) = &self.docstring {
            ctx.push_docstring(docstring);
        }

        // Nested classes are defined before they're used.
        for nested_class in &self.nested {
            nested_class.render(ctx);
        }

        if let Some(additional) = &self.additional {
            if !matches!(additional, AST::Any) {
                ctx.push_indent();
                ctx.into.push_str("__pydantic_extra__: dict[str, ");
                additional.render(ctx.into);
                ctx.into.push_str("] = pydantic.Field(init=False) # type: ignore[reportIncompatibleVariableOverride]\n");
            }

            ctx.push_indent();
            ctx.into
                .push_str("model_config = pydantic.ConfigDict(extra='allow')\n");
            ctx.into.push('\n');
        }

        for Field {
            name,
            alias,
            docstring,
            is_required,
            type_,
        } in &self.fields
        {
            ctx.push_indent();
            ctx.into.push_str(name);
            ctx.into.push_str(": ");

            if !*is_required {
                ctx.into.push_str("typing.Optional[");
            }
            type_.render(ctx.into);
            if !*is_required {
                ctx.into.push_str("]");
            }

            if let Some(alias) = alias {
                ctx.into.push_str(" = pydantic.Field(");

                // For optional fields, add None as first argument
                if !is_required {
                    ctx.into.push_str("default=None, ");
                }
                ctx.into.push_str(&format!("alias=\"\"\"{alias}\"\"\""));
                ctx.into.push(')');
            } else if !*is_required {
                // Otherwise add "= None" for optional fields without Field(...)
                ctx.into.push_str(" = None");
            }
            ctx.into.push('\n');

            if let Some(docstring) = docstring {
                ctx.push_docstring(docstring);
            }
        }

        if self.fields.is_empty() && self.nested.is_empty() && self.additional.is_none() {
            ctx.push_indent();
            ctx.into.push_str("pass\n");
        }

        ctx.indent -= 1;
        ctx.into.push('\n');
    }
}

impl AST {
    pub fn render(&self, w: &mut String) {
        match self {
            AST::Any => w.push_str("typing.Any"),
            AST::Bool => w.push_str("bool"),
            AST::Float => w.push_str("float"),
            AST::Int => w.push_str("int"),
            // Pydantic doesn't support typing.Never, use a sentinel literal instead
            AST::Never => w.push_str(
                "typing.Literal[\"this field is constrained by its schema to never exist\"]",
            ),
            AST::None => w.push_str("None"),
            AST::Str => w.push_str("str"),
            AST::Literals { values } => {
                w.push_str("typing.Literal[");
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        w.push_str(", ");
                    }

                    // Render individual literal value
                    match value {
                        serde_json::Value::String(s) => {
                            w.push('"');
                            w.push_str(&s.replace('\\', "\\\\").replace('"', "\\\""));
                            w.push('"');
                        }
                        serde_json::Value::Number(n) => {
                            w.push_str(&n.to_string());
                        }
                        serde_json::Value::Bool(b) => {
                            w.push_str(if *b { "True" } else { "False" });
                        }
                        serde_json::Value::Null => {
                            w.push_str("None");
                        }
                        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                            // This should never happen since we filter at mapper level
                            // Panic to catch bugs during development
                            panic!(
                                "Complex literal (object/array) should have been filtered at mapper level"
                            );
                        }
                    }
                }
                w.push(']');
            }
            AST::List { of } => {
                w.push_str("list[");
                let mut inner = String::new();
                of.render(&mut inner);
                w.push_str(&inner);
                w.push(']');
            }
            AST::Tuple { items } => {
                w.push_str("tuple[");
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        w.push_str(", ");
                    }
                    let mut inner = String::new();
                    item.render(&mut inner);
                    w.push_str(&inner);
                }
                w.push(']');
            }
            AST::Union { variants } => {
                w.push_str("typing.Union[");
                for (i, variant) in variants.iter().enumerate() {
                    if i > 0 {
                        w.push_str(", ");
                    }
                    let mut inner = String::new();
                    variant.render(&mut inner);
                    w.push_str(&inner);
                }
                w.push(']');
            }
            AST::Anchor(anchor) => {
                w.push('"');
                w.push_str(anchor);
                w.push('"');
            }
        }
    }
}

impl Context<'_> {
    fn push_indent(&mut self) {
        self.into
            .extend(std::iter::repeat(' ').take(self.indent * 4));
    }

    fn push_docstring(&mut self, body: &str) {
        self.push_indent();
        self.into.push_str("\"\"\"");
        self.into.push_str(body);
        self.into.push_str("\"\"\"\n");
    }
}
