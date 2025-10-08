use itertools::{EitherOrBoth, Itertools};
use json::{
    Schema, Validator,
    schema::{self, Annotation, build::ScopedError},
};

/// Runs tests from the given file within the `draft2019-09/optional/format` directory.
// This is not actually dead code (used by draft2019_format_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_format_test(
    target: &str,
    expected_failures: &[(&str, &str, serde_json::Value)],
) {
    run_file_test(
        &[
            "..",
            "json",
            "tests",
            "official",
            "tests",
            "draft2019-09",
            "optional",
            "format",
            target,
        ],
        expected_failures,
    );
}

/// Runs tests from the given file within the `draft2019-09/` directory.
// This is not actually dead code (used by draft2019_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_test(target: &str, expected_failures: &[(&str, &str, serde_json::Value)]) {
    run_file_test(
        &[
            "..",
            "json",
            "tests",
            "official",
            "tests",
            "draft2019-09",
            target,
        ],
        expected_failures,
    );
}

/// Runs tests from the given file within the `draft2020-12/` directory.
// This is not actually dead code (used by draft2020_tests.rs).
#[allow(dead_code)]
pub fn run_draft12_test(target: &str, expected_failures: &[(&str, &str, serde_json::Value)]) {
    run_file_test(
        &[
            "..",
            "json",
            "tests",
            "official",
            "tests",
            "draft2020-12",
            target,
        ],
        expected_failures,
    );
}

/// Runs tests from the given file within the `draft2020-12/optional/format` directory.
// This is not actually dead code (used by draft2020_format_tests.rs).
#[allow(dead_code)]
pub fn run_draft12_format_test(
    target: &str,
    expected_failures: &[(&str, &str, serde_json::Value)],
) {
    run_file_test(
        &[
            "..",
            "json",
            "tests",
            "official",
            "tests",
            "draft2020-12",
            "optional",
            "format",
            target,
        ],
        expected_failures,
    );
}

fn read_json_file(target: &[&str]) -> serde_json::Value {
    let root_dir = &std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_owned());

    let mut path = std::path::PathBuf::from(root_dir);
    path.extend(target.iter());

    let file = std::fs::File::open(path).unwrap();
    serde_json::from_reader(std::io::BufReader::new(file)).unwrap()
}

fn read_json_files(
    dir: std::path::PathBuf,
) -> impl Iterator<Item = (std::path::PathBuf, serde_json::Value)> {
    let g = dir.join("**").join("*.json");
    let paths = glob::glob(g.to_str().unwrap()).unwrap();

    paths
        .filter(|p| {
            // Avoid reading schemas for drafts with keywords we don't support.
            for d in &["draft4", "draft6", "draft7", "draft-next"] {
                if p.as_ref().unwrap().to_str().unwrap().contains(d) {
                    return false;
                }
            }
            true
        })
        .map(move |p| {
            let path = p.unwrap();
            let name = path.strip_prefix(&dir).unwrap().to_owned();

            let file = std::fs::File::open(path).unwrap();
            let v: serde_json::Value =
                serde_json::from_reader(std::io::BufReader::new(file)).unwrap();
            (name, v)
        })
}

fn read_json_schemas<A>(dir: std::path::PathBuf, base: url::Url) -> impl Iterator<Item = Schema<A>>
where
    A: Annotation,
{
    read_json_files(dir).map(move |(name, v)| {
        let url = base.join(name.to_str().unwrap()).unwrap();
        let s = schema::build::<A>(&url, &v).unwrap();
        s
    })
}

fn run_file_test(target: &[&str], expected_failures: &[(&str, &str, serde_json::Value)]) {
    let test_root = &std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_owned());

    let url = url::Url::parse("http://localhost:1234").unwrap();

    let mut fixtures = std::path::PathBuf::from(test_root);
    fixtures.extend(["..", "json", "tests", "schema-fixtures"].iter());
    let mut catalog: Vec<Schema<schema::CoreAnnotation>> =
        read_json_schemas(fixtures, url.clone()).collect();

    let mut fixtures = std::path::PathBuf::from(test_root);
    fixtures.extend(["..", "json", "tests", "official", "remotes"].iter());
    catalog.extend(read_json_schemas(fixtures, url.clone()));

    let cases = read_json_file(target);
    let cases = cases.as_array().expect("test file not an array");
    let url = url::Url::parse("http://example/test.json").unwrap();

    let mut case_outcomes = Vec::new();

    for case in cases {
        let schema = case.get("schema").expect("missing test case schema");
        let desc = case
            .get("description")
            .and_then(|s| s.as_str())
            .unwrap_or("<no description>");

        println!("{}:", desc);
        println!("\t{}:", schema);

        let schema = match schema::build::<schema::CoreAnnotation>(&url, schema) {
            Ok(schema) => schema,
            Err(errors) => {
                for ScopedError { scope, inner: err } in errors.0 {
                    println!("\tSCHEMA ERROR @{scope}: {err}");
                }
                schema::build(&url, &serde_json::json!(false)).unwrap()
            }
        };

        let mut ind = schema::index::Builder::new();
        for s in &catalog {
            ind.add(s).unwrap();
        }
        ind.add(&schema).unwrap();

        if let Err(err) = ind.verify_references() {
            println!("\tSCHEMA ERROR (verifying references): {err}");
        }
        let ind = ind.into_index();

        let mut val = Validator::new(&ind);

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
            let expect = sub_case
                .get("valid")
                .expect("missing sub-case valid")
                .as_bool()
                .expect("valid is not a bool");

            let (actual, outcomes) = val.validate(&schema, data, |o| Some(o));
            println!("\t{sub_desc} ({expect}): {data}");
            println!("\t\tOutcomes: {outcomes:?}");
            if expect == actual {
                println!("\t\tPASS");
            } else {
                println!("\t\tFAIL");
            }

            case_outcomes.push((
                (desc.to_string(), sub_desc.to_string(), data.to_string()),
                expect,
                actual,
            ));
        }
    }
    case_outcomes.sort();

    let mut failed = false;

    for eob in case_outcomes.into_iter().merge_join_by(
        expected_failures
            .iter()
            .map(|(desc, sub_desc, data)| {
                (desc.to_string(), sub_desc.to_string(), data.to_string())
            })
            .sorted(),
        |(lhs, _, _), rhs| lhs.cmp(rhs),
    ) {
        match eob {
            EitherOrBoth::Left(((desc, sub_desc, data), expect, actual)) => {
                if expect == actual {
                    continue; // Passing case.
                }
                println!(
                    "FAILURE: unexpected failure (desc: {desc:?} sub_desc: {sub_desc:?} data:{data})"
                );
                failed = true;
            }
            EitherOrBoth::Right((desc, sub_desc, data)) => {
                println!(
                    "FAILURE: expected failure not observed (desc: {desc:?} sub_desc: {sub_desc:?} data:{data})"
                );
                failed = true;
            }
            EitherOrBoth::Both(((desc, sub_desc, data), expect, actual), _case) => {
                if expect != actual {
                    continue; // Expected failure.
                }
                println!(
                    "FAILURE: expected failure did not fail (desc: {desc:?} sub_desc: {sub_desc:?} data:{data})"
                );
                failed = true;
            }
        }
    }
    assert!(!failed)
}
