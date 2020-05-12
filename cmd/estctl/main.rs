use clap;
use estuary::catalog;
use std::boxed::Box;
use std::fs;
use std::path::Path;
use url;

type Error = Box<dyn std::error::Error + 'static>;

fn main() {
    pretty_env_logger::init();

    let matches = clap::App::new("Estuary CLI")
        .version("v0.1.0")
        .author("Estuary Technologies, Inc. \u{00A9}2020")
        .about("Command-line interface for working with Estuary projects")
        .subcommand(
            clap::SubCommand::with_name("build")
                .about("Build an Estuary specification into a catalog")
                .arg(
                    clap::Arg::with_name("path")
                        .long("path")
                        .takes_value(true)
                        .required(true)
                        .help("Path to input specification file"),
                )
                .arg(
                    clap::Arg::with_name("catalog")
                        .long("catalog")
                        .takes_value(true)
                        .default_value("catalog.db")
                        .help("Path to output catalog database"),
                )
                .arg(
                    clap::Arg::with_name("nodejs-package-path")
                        .long("nodejs")
                        .takes_value(true)
                        .default_value("catalog-nodejs")
                        .help("Path to NodeJS package holding JavaScript lambdas")
                        .long_help(
"Path to NodeJS package which will hold JavaScript lambdas. If this directory
doesn't exist, it will be automatically created from a template. The package
is used temporarily during the catalog build process -- it's compiled and
then packed into the output catalog database -- but re-using the same directory
across invocations will save time otherwise spent fetching npm packages."),
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

    let node = Path::new(args.value_of("nodejs-package-path").unwrap());

    db.execute_batch("BEGIN;")?;
    catalog::init_db_schema(&db)?;
    catalog::Source::register(&db, root)?;

    // TODO:
    // - Verify collection primary key matches inferred schema (table 'collections').
    // - Verify shuffle keys matches source schema in use ('transform_details')
    //    (note there could be multliple shuffle keys & alternate source schemas).
    // - Verify projected field pointers matched inferred schema.
    // - Deduce additional projections from schema & add to catalog table?

    catalog::build_nodejs_package(&db, node)?;

    db.execute_batch("COMMIT;")?;
    Ok(())
}
