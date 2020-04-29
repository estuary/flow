use clap;
use estuary::catalog;
use std::boxed::Box;
use std::fs;
use std::path::Path;
use url;

type Error = Box<dyn std::error::Error + 'static>;

fn main() {
    let matches = clap::App::new("Estuary CLI")
        .version("v0.1.0")
        .author("Estuary Technologies, Inc. \u{00A9}2020")
        .about("Command-line interface for working with Estuary projects")
        .subcommand(
            clap::SubCommand::with_name("build")
                .about("Build an Estuary specification into a catalog")
                .arg(
                    clap::Arg::with_name("path")
                        .short("p")
                        .long("path")
                        .takes_value(true)
                        .required(true)
                        .help("Path to input specification file"),
                )
                .arg(
                    clap::Arg::with_name("catalog")
                        .short("c")
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
    let root = args.value_of("path").unwrap();
    let root = fs::canonicalize(root)?;
    let root = url::Url::from_file_path(&root).unwrap();

    let db = args.value_of("catalog").unwrap();
    let db = catalog::open(db)?;

    db.execute_batch("BEGIN;")?;
    catalog::init_db_schema(&db)?;
    catalog::Source::register(&db, root)?;
    catalog::build_nodejs_package(&db, Path::new("/home/johnny/test-pkg"))?;

    db.execute_batch("COMMIT;")?;
    Ok(())
}
