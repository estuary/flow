/// Runs tests from the given file within the `draft2019-09/optional/format` directory.
// This is not actually dead code (used by draft2019_format_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_format_test(target: &str) {
    run_file_test(&[
        "..",
        "json",
        "tests",
        "official",
        "tests",
        "draft2019-09",
        "optional",
        "format",
        target,
    ]);
}

/// Runs tests from the given file within the `draft2019-09/` directory.
// This is not actually dead code (used by draft2019_tests.rs).
#[allow(dead_code)]
pub fn run_draft09_test(target: &str) {
    run_file_test(&[
        "..",
        "json",
        "tests",
        "official",
        "tests",
        "draft2019-09",
        target,
    ]);
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
            // Gross hack to avoid parsing subSchemas, which fails with an unknown keyword error.
            !p.as_ref().unwrap().ends_with("subSchemas.json")
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

fn read_json_schemas<A>(
    dir: std::path::PathBuf,
    base: url::Url,
) -> impl Iterator<Item = json2::Schema<A>>
where
    A: json2::schema::Annotation,
{
    read_json_files(dir).map(move |(name, v)| {
        let url = base.join(name.to_str().unwrap()).unwrap();
        let s = json2::schema::build::<A>(&url, &v).unwrap();
        s
    })
}

fn run_file_test(target: &[&str]) {
    let test_root = &std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".to_owned());

    let url = url::Url::parse("http://localhost:1234").unwrap();

    let mut fixtures = std::path::PathBuf::from(test_root);
    fixtures.extend(["..", "json", "tests", "schema-fixtures"].iter());
    let mut catalog: Vec<json2::Schema<json2::schema::CoreAnnotation>> =
        read_json_schemas(fixtures, url.clone()).collect();

    let mut fixtures = std::path::PathBuf::from(test_root);
    fixtures.extend(["..", "json", "tests", "official", "remotes"].iter());
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
        let schema = json2::schema::build::<json2::schema::CoreAnnotation>(&url, schema).unwrap();
        println!("\t{:?}", schema);

        let mut ind = json2::schema::index::Builder::new();
        for s in &catalog {
            ind.add(s).unwrap();
        }
        ind.add(&schema).unwrap();
        let ind = ind.into_index();

        /*
        ind.verify_references()
            .expect("failed to verify references");
        */

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

            println!("\t{sub_desc} ({expect}): {data}");
            let (valid, outcomes) = json2::validation::do_it(data, &|o| Some(o), &ind, &schema);
            println!("\t\tValid: {valid}");
            println!("\t\tOutcomes: {outcomes:?}");

            let validity = match expect {
                true => "valid",
                false => "invalid",
            };

            assert_eq!(
                expect, valid,
                "Expected {data} to be {validity} because '{sub_desc}'",
            );
        }
    }
}
