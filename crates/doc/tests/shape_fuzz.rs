use doc::{
    shape::{limits::enforce_shape_complexity_limit, schema::to_schema},
    Shape, Validator,
};
use itertools::Itertools;
use quickcheck::{Gen, QuickCheck, TestResult};
use serde_json::{json, Value};

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

fn assert_docs_fit_schema(docs: Vec<Value>, shape: Shape) -> bool {
    let schema = json::schema::build::build_schema(
        url::Url::parse("https://example").unwrap(),
        &serde_json::to_value(to_schema(shape.clone())).unwrap(),
    )
    .unwrap();

    let schema_yaml = serde_yaml::to_string(&to_schema(shape)).unwrap();

    let mut validator = Validator::new(schema).unwrap();

    for val in docs {
        let res = validator.validate(None, &val);
        if let Ok(validation) = res {
            if let Err(invalid) = validation.ok() {
                println!(
                    r#"Schema {schema_yaml} failed validation for document {val}: {invalid:?}"#,
                );
                return false;
            }
        } else {
            return false;
        }
    }
    return true;
}

fn shape_limits(vals: Vec<Value>, limit: usize, depth_limit: usize) -> bool {
    let mut shape = Shape::nothing();
    for val in &vals {
        shape.widen(val);
    }

    let initial_locations = shape.locations().len();
    let initial_schema_yaml = serde_yaml::to_string(&to_schema(shape.clone())).unwrap();

    enforce_shape_complexity_limit(&mut shape, limit, depth_limit);

    let enforced_locations = shape
        .locations()
        .iter()
        .filter(|(ptr, pattern, _, _)| !pattern && ptr.0.len() > 0)
        .collect_vec()
        .len();

    if enforced_locations > limit || !assert_docs_fit_schema(vals.clone(), shape.clone()) {
        let schema_yaml = serde_yaml::to_string(&to_schema(shape)).unwrap();
        println!("Started with {initial_locations} initial locations, enforced down to {enforced_locations}, limit was {limit}");
        println!("start: {initial_schema_yaml}\nenforced down to: {schema_yaml}\ndocs:{vals:?}");
        return false;
    } else {
        return true;
    }
}

fn roundtrip_schema_widening_validation(vals: Vec<Value>) -> bool {
    let mut shape = Shape::nothing();
    for val in &vals {
        shape.widen(val);
    }

    assert_docs_fit_schema(vals, shape)
}

#[test]
fn test_case_obj() {
    assert_eq!(true, shape_limits(vec![json!({"":{"":55}})], 1, 3));
}

#[test]
fn fuzz_roundtrip() {
    fn inner_test(av: Vec<ArbitraryValue>) -> bool {
        let vals = av.into_iter().map(|v| v.0).collect_vec();
        roundtrip_schema_widening_validation(vals)
    }

    QuickCheck::new()
        .gen(Gen::new(100))
        .quickcheck(inner_test as fn(Vec<ArbitraryValue>) -> bool);
}

#[test]
fn fuzz_limiting() {
    fn inner_test(av: Vec<ArbitraryValue>, limit: usize, depth_limit: usize) -> TestResult {
        if limit < 1 || depth_limit < 1 {
            return TestResult::discard();
        }
        let vals = av.into_iter().map(|v| v.0).collect_vec();
        TestResult::from_bool(shape_limits(vals, limit, depth_limit))
    }

    QuickCheck::new()
        .gen(Gen::new(100))
        .tests(1000)
        .quickcheck(inner_test as fn(Vec<ArbitraryValue>, usize, usize) -> TestResult);
}
