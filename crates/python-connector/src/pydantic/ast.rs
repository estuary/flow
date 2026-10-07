use std::fmt::Write;

/// Represents a Pydantic class definition with fields.
/// Classes are hoisted during mapping and rendered as top-level or nested class definitions.
#[derive(Debug, Clone, PartialEq)]
pub struct Class {
    pub name: String,
    /// Base class, or None for `pydantic.BaseModel`.
    pub base: Option<String>,
    pub docstring: Option<String>,
    pub nested: Vec<Class>,
    pub fields: Vec<Field>,
    pub additional: Option<AST>,
    /// Further lines of the class body (such as methods), which follow
    /// its fields and are indented by the class.
    pub body: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub alias: Option<String>,     // Field(alias=...)
    pub docstring: Option<String>, // Field(description=...)
    pub is_required: bool,         // Wrap in Optional[...] ?
    pub type_: AST,
    /// Default of an optional field, which is its value if omitted.
    pub default: Option<serde_json::Value>,
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
    Timedelta,
    Datetime,
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
            write!(w, "{name}: _typing.TypeAlias = ").unwrap();
            ast.render(w);
            w.push('\n');
        }
        w.push_str("\n\n");
    }
}

impl Class {
    pub fn render(&self, ctx: &mut Context) {
        ctx.push_indent();
        write!(
            ctx.into,
            "class {}({}):\n",
            self.name,
            self.base.as_deref().unwrap_or("_pydantic.BaseModel")
        )
        .unwrap();
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
                ctx.into.push_str("] = _pydantic.Field(init=False) # type: ignore[reportIncompatibleVariableOverride]\n");
            }

            ctx.push_indent();
            ctx.into
                .push_str("model_config = _pydantic.ConfigDict(extra='allow')\n");
            ctx.into.push('\n');
        }

        for field in &self.fields {
            ctx.push_indent();
            field.render(ctx.into);
            ctx.into.push('\n');

            if let Some(docstring) = &field.docstring {
                ctx.push_docstring(docstring);
            }
        }

        for line in &self.body {
            if !line.is_empty() {
                ctx.push_indent();
                ctx.into.push_str(line);
            }
            ctx.into.push('\n');
        }

        if self.fields.is_empty()
            && self.nested.is_empty()
            && self.additional.is_none()
            && self.body.is_empty()
        {
            ctx.push_indent();
            ctx.into.push_str("pass\n");
        }

        ctx.indent -= 1;
        ctx.into.push('\n');
    }
}

impl Field {
    fn render(&self, w: &mut String) {
        let Field {
            name,
            alias,
            docstring: _,
            is_required,
            type_,
            default,
        } = self;

        w.push_str(name);
        w.push_str(": ");

        // A field which is omitted without a default is None.
        let is_none_default =
            !*is_required && matches!(default, None | Some(serde_json::Value::Null));
        if is_none_default {
            w.push_str("_typing.Optional[");
        }
        type_.render(w);
        if is_none_default {
            w.push(']');
        }

        let mut args = Vec::new();
        let mut is_plain = true; // Can the default be assigned directly?

        match default {
            _ if *is_required => {}
            None | Some(serde_json::Value::Null) => args.push("default=None".to_string()),
            Some(value) => {
                // An integer field's default may be written as `10.0`.
                let value = match (type_, value.as_f64()) {
                    (AST::Int, Some(f)) if value.is_f64() && f.fract() == 0.0 => {
                        serde_json::json!(f as i64)
                    }
                    _ => value.clone(),
                };
                args.push(format!("default={}", python_literal(&value)));

                // Pydantic doesn't validate a default unless asked, which is
                // needed for a value which parses into another type.
                if value.is_object() || value.is_array() || type_.has_parsed_types() {
                    args.push("validate_default=True".to_string());
                    is_plain = false;
                }
            }
        }
        if let Some(alias) = alias {
            args.push(format!(
                "alias={}",
                python_literal(&serde_json::Value::String(alias.clone()))
            ));
            is_plain = false;
        }

        match args.as_slice() {
            [] => {}
            [default] if is_plain => {
                w.push_str(" = ");
                w.push_str(&default["default=".len()..]);
            }
            args => {
                w.push_str(" = _pydantic.Field(");
                w.push_str(&args.join(", "));
                w.push(')');
            }
        }
    }
}

/// Render user-authored text (such as a schema `description`) as a
/// triple-quoted docstring literal. Backslashes and every quote are escaped,
/// so the text can't end or escape the literal, and control characters other
/// than newlines and tabs (which Python source can't carry) are escaped.
pub fn docstring_literal(body: &str) -> String {
    let mut w = String::with_capacity(body.len() + 6);
    w.push_str("\"\"\"");
    for c in body.chars() {
        match c {
            '\\' => w.push_str("\\\\"),
            '"' => w.push_str("\\\""),
            '\n' | '\t' => w.push(c),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                write!(w, "\\x{:02x}", c as u32).unwrap();
            }
            c => w.push(c),
        }
    }
    w.push_str("\"\"\"");
    w
}

/// Render a JSON value as an equivalent Python literal.
pub fn python_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(true) => "True".to_string(),
        serde_json::Value::Bool(false) => "False".to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        // JSON string escapes are also valid Python string escapes.
        serde_json::Value::String(_) => value.to_string(),
        serde_json::Value::Array(items) => {
            format!(
                "[{}]",
                items
                    .iter()
                    .map(python_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        serde_json::Value::Object(fields) => {
            format!(
                "{{{}}}",
                fields
                    .iter()
                    .map(|(key, value)| format!(
                        "{}: {}",
                        serde_json::Value::String(key.clone()),
                        python_literal(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

impl AST {
    /// Does this type parse (some) JSON values into another Python type?
    fn has_parsed_types(&self) -> bool {
        match self {
            AST::Timedelta | AST::Datetime | AST::Anchor(_) => true,
            AST::List { of } => of.has_parsed_types(),
            AST::Tuple { items } | AST::Union { variants: items } => {
                items.iter().any(AST::has_parsed_types)
            }
            _ => false,
        }
    }

    pub fn render(&self, w: &mut String) {
        match self {
            AST::Any => w.push_str("_typing.Any"),
            AST::Timedelta => w.push_str("_datetime.timedelta"),
            AST::Datetime => w.push_str("_datetime.datetime"),
            AST::Bool => w.push_str("bool"),
            AST::Float => w.push_str("float"),
            AST::Int => w.push_str("int"),
            // Pydantic doesn't support typing.Never, use a sentinel literal instead
            AST::Never => w.push_str(
                "_typing.Literal[\"this field is constrained by its schema to never exist\"]",
            ),
            AST::None => w.push_str("None"),
            AST::Str => w.push_str("str"),
            AST::Literals { values } => {
                w.push_str("_typing.Literal[");
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        w.push_str(", ");
                    }

                    // Render individual literal value
                    match value {
                        serde_json::Value::String(_) => w.push_str(&python_literal(value)),
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
                w.push_str("_typing.Union[");
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
        self.into.push_str(&docstring_literal(body));
        self.into.push('\n');
    }
}
