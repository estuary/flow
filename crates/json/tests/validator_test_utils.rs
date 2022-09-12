//! Test utilities that are used by the automaically generated validation tests.
use glob;
use json::{
    de,
    schema::{build, index, CoreAnnotation, Schema},
    validator,
};
use serde_json as sj;
use std::{env, fs, io, path};

/// Runs tests from the given file within the `draft2019-09/` directory.
// This is not actually dead code (used by draft2019_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_test(target: &str) {
    run_file_test(&["official", "tests", "draft2019-09", target]);
}

/// Runs tests from the given file within the `draft2019-09/optional/format` directory.
// This is not actually dead code (used by draft2019_format_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_format_test(target: &str) {
    run_file_test(&[
        "official",
        "tests",
        "draft2019-09",
        "optional",
        "format",
        target,
    ]);
}

fn read_json_file(target: &[&str]) -> sj::Value {
    let root_dir = &env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_owned());

    let mut path = path::PathBuf::from(root_dir);
    path.push("tests");
    path.extend(target.iter());

    let file = fs::File::open(path).unwrap();
    sj::from_reader(io::BufReader::new(file)).unwrap()
}

fn read_json_files(dir: path::PathBuf) -> impl Iterator<Item = (path::PathBuf, sj::Value)> {
    let g = dir.join("**").join("*.json");
    let paths = glob::glob(g.to_str().unwrap()).unwrap();

    paths
        .filter(|p| {
            // Gross hack to avoid parsing subSchemas, which fails with an unknown keyword error.
            !p.as_ref().unwrap().ends_with("subSchemas.json")
        })
        .map(move |p| {
            let path = p.unwrap();
            let name = path.strip_prefix(&dir).unwrap().to_owned();

            let file = fs::File::open(path).unwrap();
            let v: sj::Value = sj::from_reader(io::BufReader::new(file)).unwrap();
            (name, v)
        })
}

fn read_json_schemas<A>(dir: path::PathBuf, base: url::Url) -> impl Iterator<Item = Schema<A>>
where
    A: build::AnnotationBuilder,
{
    read_json_files(dir).map(move |(name, v)| {
        let url = base.join(name.to_str().unwrap()).unwrap();
        let s = build::build_schema::<A>(url.clone(), &v).unwrap();
        s
    })
}

fn run_file_test(target: &[&str]) {
    let test_root = &env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_owned());

    let url = url::Url::parse("http://localhost:1234").unwrap();

    let mut fixtures = path::PathBuf::from(test_root);
    fixtures.extend(["tests", "schema-fixtures"].iter());
    let mut catalog: Vec<Schema<CoreAnnotation>> =
        read_json_schemas(fixtures, url.clone()).collect();

    let mut fixtures = path::PathBuf::from(test_root);
    fixtures.extend(["tests", "official", "remotes"].iter());
    catalog.extend(read_json_schemas(fixtures, url.clone()));

    let cases = read_json_file(target);
    let cases = cases.as_array().expect("test file not an array");
    let url = url::Url::parse("http://example/test.json").unwrap();

    for case in cases {
        let schema = case.get("schema").expect("missing test case schema");
        let desc = case
            .get("description")
            .and_then(|s| s.as_str())
            .unwrap_or("<no description>");

        println!("{}:", desc);
        println!("\t{}:", schema);
        let schema = build::build_schema::<CoreAnnotation>(url.clone(), schema).unwrap();
        println!("\t{:?}", schema);

        let mut ind = index::IndexBuilder::new();
        for s in &catalog {
            ind.add(s).unwrap();
        }
        ind.add(&schema).unwrap();
        let ind = ind.into_index();

        /*
        ind.verify_references()
            .expect("failed to verify references");
        */

        let mut val = validator::Validator::<CoreAnnotation, validator::FullContext>::new(&ind);

        for sub_case in case
            .get("tests")
            .expect("missing test sub-cases")
            .as_array()
            .expect("sub-cases not an array")
            .iter()
            .skip(0)
        {
            let data = sub_case.get("data").expect("missing sub-case data");
            let sub_desc = sub_case
                .get("description")
                .and_then(|s| s.as_str())
                .unwrap_or("<no description>");
            let valid = sub_case
                .get("valid")
                .expect("missing sub-case valid")
                .as_bool()
                .expect("valid is not a bool");

            println!("\t{} ({}): {}", sub_desc, valid, data);
            val.prepare(&schema.curi).unwrap();

            let out = de::walk(data, &mut val).expect("validation error");
            println!("\t\t{:?}", out);

            println!("\t\toutcomes: {:?}", val.outcomes());

            let validity = match valid {
                true => "valid",
                false => "invalid",
            };

            assert_eq!(
                !val.invalid(),
                valid,
                "Expected {} to be {} because '{}'",
                data,
                validity,
                sub_desc
            );
        }
    }
}
