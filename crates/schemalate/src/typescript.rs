use doc::{Schema, SchemaIndexBuilder};
use json::schema::{build::build_schema, Application, Keyword};
use std::io::Write as IoWrite;
use typescript::{ast::Context, Mapper};
use url::Url;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Name of the root TypeScript type to generate.
    #[clap(short, long)]
    name: String,

    /// Hoist definitions of the root schema into their own types?
    #[clap(long)]
    hoist_definitions: bool,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let dom: serde_json::Value = serde_json::from_reader(std::io::stdin())?;
    let curi = Url::parse("https://example/schema").unwrap();
    let root: Schema = build_schema(curi, &dom).unwrap();

    let mut index = SchemaIndexBuilder::new();
    index.add(&root).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    // |top_level| is schemas which become named top-level type definitions.
    let mut top_level = std::collections::BTreeMap::new();

    if args.hoist_definitions {
        for (key, child) in root.kw.iter().filter_map(|kw| match kw {
            Keyword::Application(Application::Def { key }, child)
            | Keyword::Application(Application::Definition { key }, child) => Some((key, child)),
            _ => None,
        }) {
            top_level.insert(&child.curi, key.clone());
        }
    }

    let mut w = std::io::stdout();
    let mapper = Mapper {
        schema: root.curi.clone(),
        index,
        top_level,
    };

    // Write the root schema type.
    write!(w, "export type {} = ", args.name)?;
    let mut tmp = String::new();
    mapper.map(&root.curi).render(&mut Context::new(&mut tmp));
    write!(w, "{};\n\n", tmp)?;

    // Write other hoisted and top-level types.
    for (uri, name) in &mapper.top_level {
        write!(w, "export type {} = ", name)?;
        let mut tmp = String::new();
        mapper.map(uri).render(&mut Context::new(&mut tmp));
        write!(w, "{};\n\n", tmp)?;
    }

    Ok(())
}
