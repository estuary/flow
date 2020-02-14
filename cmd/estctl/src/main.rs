use clap;
use estuary_json::schema;
use exitfailure;
use failure::{self, err_msg, format_err};
use serde_yaml;
use std::boxed::Box;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::BufReader;
use std::path;
use url;

mod specs;

fn main() -> Result<(), exitfailure::ExitFailure> {
    let matches = clap::App::new("Estuary CLI")
        .version("v0.1.0")
        .author("Estuary Technologies, Inc. \u{00A9}2020")
        .about("Command-line interface for working with Estuary projects")
        .subcommand(
            clap::SubCommand::with_name("build")
                .about("Build an Estuary project into a bundle")
                .arg(
                    clap::Arg::with_name("path")
                        .short("p")
                        .long("path")
                        .takes_value(true)
                        .required(true)
                        .help("Path to project directory"),
                ),
        )
        .get_matches();

    let sub = matches.subcommand();
    Ok(match sub {
        ("build", Some(sub_m)) => do_build(sub_m)?,
        _ => (),
    })
}

type Schema = schema::Schema<schema::CoreAnnotation>;

fn do_build(args: &clap::ArgMatches) -> Result<(), failure::Error> {
    let path = args.value_of("path").unwrap();
    let path = path::PathBuf::from(path);
    let path = fs::canonicalize(path)?;

    let project_url = url::Url::from_file_path(&path).unwrap();

    let file = fs::File::open(&path)?;
    let spec: specs::Project = serde_yaml::from_reader(io::BufReader::new(file))?;

    let mut raw_schemas: BTreeMap<url::Url, Box<serde_json::Value>> = BTreeMap::new();

    for c in &spec.collections {
        let schema_url = project_url.join(&c.schema)?;

        let mut load_url = schema_url.clone();
        load_url.set_fragment(None);

        if raw_schemas.contains_key(&load_url) {
            continue;
        }

        let r = open_schema(&load_url)?;
        let r = io::BufReader::new(r);

        let raw_schema: serde_json::Value = {
            if load_url.path().ends_with(".yaml") {
                serde_yaml::from_reader(r)?
            } else if load_url.path().ends_with(".json") {
                serde_json::from_reader(r)?
            } else {
                Err(format_err!("unsupported schema extension '{}'", load_url))?
            }
        };
        raw_schemas.insert(load_url.clone(), Box::new(raw_schema));

        let compiled_schema: schema::Schema<schema::CoreAnnotation> =
            schema::build::build_schema(load_url.clone(), &raw_schemas[&load_url])
                .map_err(failure::SyncFailure::new)?;

        println!("loaded {:?}", load_url);
        println!("");
        println!("raw {:?}", &raw_schemas[&load_url]);
        println!("");
        println!("compiled {:?}", &compiled_schema);
    }

    println!("spec: {:?}", spec);
    Ok(())
}

fn open_schema(url: &url::Url) -> Result<Box<dyn io::Read>, failure::Error> {
    match url.scheme() {
        "file" => open_file_schema(&url),
        _ => Err(format_err!("unknown schema '{}'", url.scheme())),
    }
}

fn open_file_schema(url: &url::Url) -> Result<Box<dyn io::Read>, failure::Error> {
    let path = url
        .to_file_path()
        .map_err(|_| format_err!("failed to map '{}' to path", url))?;
    Ok(Box::new(fs::File::open(path)?))
}
