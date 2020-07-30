use estuary_json::{
    de,
    schema::{build, index, CoreAnnotation, Schema},
    validator,
};
use glob;
use serde_json as sj;
use std::{env, fs, io, path};

#[test]
fn test_d09_type() {
    run_draft09_test("type.json");
}

#[test]
fn test_d09_additional_items() {
    run_draft09_test("additionalItems.json");
}

#[test]
fn test_d09_additional_properties() {
    run_draft09_test("additionalProperties.json");
}

#[test]
fn test_d09_const() {
    run_draft09_test("const.json");
}

#[test]
fn test_d09_enum() {
    run_draft09_test("enum.json");
}

#[test]
fn test_d09_required() {
    run_draft09_test("required.json");
}

#[test]
fn test_d09_boolean_schema() {
    run_draft09_test("boolean_schema.json");
}

#[test]
fn test_d09_minimum() {
    run_draft09_test("minimum.json");
}

#[test]
fn test_d09_maximum() {
    run_draft09_test("maximum.json");
}

#[test]
fn test_d09_exclusive_minimum() {
    run_draft09_test("exclusiveMinimum.json");
}

#[test]
fn test_d09_exclusive_maximum() {
    run_draft09_test("exclusiveMaximum.json");
}

#[test]
fn test_d09_all_of() {
    run_draft09_test("allOf.json");
}

#[test]
fn test_d09_any_of() {
    run_draft09_test("anyOf.json");
}

#[test]
fn test_d09_one_of() {
    run_draft09_test("oneOf.json");
}

#[test]
fn test_d09_id() {
    run_draft09_test("id.json");
}

#[test]
fn test_d09_if_then_else() {
    run_draft09_test("if-then-else.json");
}

#[test]
fn test_d09_min_length() {
    run_draft09_test("minLength.json");
}

#[test]
fn test_d09_max_length() {
    run_draft09_test("maxLength.json");
}

#[test]
fn test_d09_min_properties() {
    run_draft09_test("minProperties.json");
}

#[test]
fn test_d09_max_properties() {
    run_draft09_test("maxProperties.json");
}

#[test]
fn test_d09_min_items() {
    run_draft09_test("minItems.json");
}

#[test]
fn test_d09_max_contains() {
    run_draft09_test("maxContains.json");
}

#[test]
fn test_d09_max_items() {
    run_draft09_test("maxItems.json");
}

#[test]
fn test_d09_multiple_of() {
    run_draft09_test("multipleOf.json");
}

#[test]
fn test_d09_ref() {
    run_draft09_test("ref.json");
}

#[test]
fn test_d09_anchor() {
    run_draft09_test("anchor.json");
}

#[test]
fn test_d09_items() {
    run_draft09_test("items.json");
}

#[test]
fn test_d09_not() {
    run_draft09_test("not.json");
}

#[test]
fn test_d09_property_names() {
    run_draft09_test("propertyNames.json");
}

#[test]
fn test_d09_defs() {
    run_draft09_test("defs.json");
}

#[test]
fn test_d09_dependent_requried() {
    run_draft09_test("dependentRequired.json");
}

#[test]
fn test_d09_dependent_schemas() {
    run_draft09_test("dependentSchemas.json");
}

#[test]
fn test_d09_unevaluated_properties() {
    run_draft09_test("unevaluatedProperties.json");
}

#[test]
fn test_d09_unevaluated_items() {
    run_draft09_test("unevaluatedItems.json");
}

#[test]
fn test_d09_pattern_properties() {
    run_draft09_test("patternProperties.json");
}

#[test]
fn test_d09_default() {
    run_draft09_test("default.json");
}

#[test]
fn test_d09_contains() {
    run_draft09_test("contains.json");
}

#[test]
fn test_d09_pattern() {
    run_draft09_test("pattern.json");
}

#[test]
fn test_d09_properties() {
    run_draft09_test("properties.json");
}

#[test]
fn test_d09_unique_items() {
    run_draft09_test("uniqueItems.json");
}

#[test]
fn test_d09_ref_remote() {
    run_draft09_test("refRemote.json");
}

#[test]
fn test_d09_format() {
    run_draft09_test("format.json");
}

/*
additionalItems.json        const.json                  exclusiveMaximum.json       maxItems.json               minProperties.json          patternProperties.json      type.json
additionalProperties.json   contains.json               exclusiveMinimum.json       maxLength.json              multipleOf.json             properties.json             unevaluatedProperties.json
allOf.json                  default.json                format.json                 maxProperties.json          not.json                    propertyNames.json          uniqueItems.json
anchor.json                 defs.json                   if-then-else.json           minimum.json                oneOf.json                  ref.json
anyOf.json                  dependencies.json           items.json                  minItems.json               optional/                   refRemote.json
boolean_schema.json         enum.json                   maximum.json                minLength.json              pattern.json                required.json
*/

fn run_draft09_test(target: &str) {
    run_file_test(&["official", "tests", "draft2019-09", target]);
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

        let mut ind = index::Index::new();
        for s in &catalog {
            ind.add(s).unwrap();
        }
        ind.add(&schema).unwrap();

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
            let valid = sub_case
                .get("valid")
                .expect("missing sub-case valid")
                .as_bool()
                .expect("valid is not a bool");

            println!("\t{} ({}): {}", sub_desc, valid, data);

            let mut val = validator::Validator::<CoreAnnotation, validator::FullContext>::new(
                &ind,
                &schema.curi,
            )
            .unwrap();

            let out = de::walk(data, &mut val).expect("validation error");
            println!("\t\t{:?}", out);

            println!("\t\toutcomes: {:?}", val.outcomes());
            assert_eq!(!val.invalid(), valid);
        }
    }
}
