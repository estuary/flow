use clap;
use estuary_json::schema;
use std::boxed::Box;
use std::fs;
use url;

mod specs;
mod catalog;

type Error = Box<dyn std::error::Error>;

fn main() {
    let matches = clap::App::new("Estuary CLI")
        .version("v0.1.0")
        .author("Estuary Technologies, Inc. \u{00A9}2020")
        .about("Command-line interface for working with Estuary projects")
        .subcommand(
            clap::SubCommand::with_name("build")
                .about("Build an Estuary specification tree into a bundle")
                .arg(
                    clap::Arg::with_name("root")
                        .short("r")
                        .long("root")
                        .takes_value(true)
                        .required(true)
                        .help("Path to specification which roots the hierarchy"),
                )
                .arg(
                    clap::Arg::with_name("catalog")
                        .short("b")
                        .long("catalog")
                        .takes_value(true)
                        .required(true)
                        .help("Path to output catalog"),
                ),
        )
        .get_matches();

    let result: Result<(), Error> = match matches.subcommand() {
        ("build", Some(sub)) => do_build(sub),
        _ => Ok(()),
    };

    match result {
        Ok(_) => (),
        Err(e) => println!("Error: {}", e),
    };
}

fn do_build(args: &clap::ArgMatches) -> Result<(), Error> {
    let root = args.value_of("root").unwrap();
    let root = fs::canonicalize(root)?;
    let root = url::Url::from_file_path(&root).unwrap();

    let db = args.value_of("catalog").unwrap();
    let db = rusqlite::Connection::open(db)?;
    db.execute_batch("BEGIN;")?;
    catalog::create_schema(&db)?;

    let b = catalog::Builder::new(db);
    println!("root specification is {}", &root);

    b.process_specs(root)?;
    b.do_inference()?;

    b.done().execute_batch("COMMIT;")?;
    Ok(())
}