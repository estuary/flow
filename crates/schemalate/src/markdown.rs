use doc::{inference::Shape, Schema, SchemaIndexBuilder};
use json::schema::build::build_schema;
use url::Url;

pub fn run() -> anyhow::Result<()> {
    let dom: serde_json::Value = serde_json::from_reader(std::io::stdin())?;
    let curi = Url::parse("https://example/schema").unwrap();
    let root: Schema = build_schema(curi, &dom).unwrap();

    let mut index = SchemaIndexBuilder::new();
    index.add(&root).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    let shape = Shape::infer(&root, &index);

    println!("| Location | Title | Description | Type | Default |");
    println!("|---|---|---|---|---|");

    for (ptr, pattern, shape, exists) in shape.locations() {
        let italic = if pattern { "_" } else { "" };
        let bold = if exists.must() { "*" } else { "" };
        let strike = if exists.cannot() { "~~" } else { "" };

        let title = shape.title.as_deref().unwrap_or("");
        let desc = shape.description.as_deref().unwrap_or("");
        let def = shape
            .default
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default();

        let type_ = shape.type_.to_vec().join(", ");

        println!(
            "| {}{}{}<code>{}</code>{}{}{} | {} | {} | {} | <code>{}</code> |",
            strike,
            italic,
            bold,
            md_escape(&ptr),
            bold,
            italic,
            strike,
            md_escape(title),
            md_escape(desc),
            type_,
            md_escape(&def),
        );
    }

    Ok(())
}

// md_escape aggressively escapes any characters which have
// significance for either Markdown or HTML.
fn md_escape(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '"' => out.push_str("&quot;"),
            '$' => out.push_str("&#x24;"),
            '&' => out.push_str("&amp;"),
            '*' => out.push_str("&#x2A;"),
            '/' => out.push_str("&#x2F;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '[' => out.push_str("&#x5B;"),
            '\'' => out.push_str("&#x27;"),
            '\\' => out.push_str("&#x5C;"),
            '\n' => out.push_str("<br>"),
            '\r' => {}
            ']' => out.push_str("&#x5D;"),
            '_' => out.push_str("&#x5F;"),
            '`' => out.push_str("&#x60;"),
            '|' => out.push_str("&#x7C;"),
            '~' => out.push_str("&#x7E;"),
            _ => out.push(c),
        }
    }
    out
}
