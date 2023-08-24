use itertools::Itertools;
use quickcheck::Arbitrary;
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, ops::Range};

#[derive(Clone, Debug)]
struct ArbitraryValue(Value);

impl quickcheck::Arbitrary for ArbitraryValue {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        Self(gen_value(g, 10))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self.0.clone() {
            Value::Null => quickcheck::empty_shrinker(),
            Value::Bool(b) => Box::new(b.shrink().map(|v| Self(Value::Bool(v)))),
            Value::Number(n) if n.is_f64() => {
                Box::new(n.as_f64().unwrap_or(0.0).shrink().map(|v| {
                    Self(Value::Number(
                        Number::from_f64(v).unwrap_or(Number::from_f64(0.0).unwrap()),
                    ))
                }))
            }
            Value::Number(n) if n.is_u64() => Box::new(
                n.as_u64()
                    .unwrap()
                    .shrink()
                    .map(|v| Self(Value::Number(Number::from(v)))),
            ),
            Value::Number(n) if n.is_i64() => Box::new(
                n.as_i64()
                    .unwrap()
                    .shrink()
                    .map(|v| Self(Value::Number(Number::from(v)))),
            ),
            Value::Number(_) => unreachable!(),
            Value::String(ref s) => Box::new(s.shrink().map(|v| Self(Value::String(v)))),
            Value::Array(ref v) => Box::new(
                v.into_iter()
                    .map(|val| Self(val.to_owned()))
                    .collect_vec()
                    .shrink()
                    .map(|v| Self(Value::Array(v.into_iter().map(|av| av.0).collect_vec()))),
            ),
            Value::Object(ref m) => {
                let btreetmap = m
                    .iter()
                    .map(|(k, v)| (k.to_owned(), Self(v.clone())))
                    .collect::<BTreeMap<_, _>>();
                Box::new(btreetmap.shrink().map(|v| {
                    Self(Value::Object(Map::from_iter(
                        v.into_iter().map(|(k, v)| (k, v.0)),
                    )))
                }))
            }
        }
    }
}

fn gen_range(gen: &mut quickcheck::Gen, range: Range<u64>) -> u64 {
    u64::arbitrary(gen) % (range.end - range.start) + range.start
}

fn gen_value(g: &mut quickcheck::Gen, n: usize) -> Value {
    match gen_range(g, 0..if n != 0 { 8 } else { 6 }) {
        0 => Value::Null,
        1 => Value::Bool(bool::arbitrary(g)),
        2 => Value::Number(Number::from(i64::arbitrary(g).min(2 ^ 53).max(2 ^ 53 * -1))),
        3 => Value::Number(Number::from(u64::arbitrary(g).min(2 ^ 53))),
        4 => Number::from_f64(f64::arbitrary(g))
            .map(|v| Value::Number(v))
            .unwrap_or(Value::Number(Number::from(0))),
        5 => Value::String(<String as quickcheck::Arbitrary>::arbitrary(g)),
        6 => Value::Array(gen_array(g, n / 2)),
        7 => Value::Object(gen_map(g, n / 2)),
        _ => unreachable!(),
    }
}

fn gen_array(g: &mut quickcheck::Gen, n: usize) -> Vec<Value> {
    (0..gen_range(g, 2..(n as u64) + 3))
        .map(|_| gen_value(g, n))
        .collect()
}

fn gen_map(g: &mut quickcheck::Gen, n: usize) -> Map<String, Value> {
    (0..gen_range(g, 2..(n as u64) + 3))
        .map(|_| {
            (
                <String as quickcheck::Arbitrary>::arbitrary(g),
                gen_value(g, n),
            )
        })
        .collect()
}

#[cfg(test)]
mod test {
    use crate::ArbitraryValue;
    use doc::{
        shape::{limits::enforce_shape_complexity_limit, schema::to_schema},
        Shape, Validator,
    };
    use itertools::Itertools;
    use quickcheck::{Gen, QuickCheck, TestResult};
    use serde_json::{json, Value};

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
                if validation.validator.invalid() {
                    let errs = validation
                        .validator
                        .outcomes()
                        .iter()
                        .map(|(outcome, _span)| format!("{}", outcome))
                        .collect_vec()
                        .join(r#","#);

                    println!(
                        r#"Schema {} failed validation for document {}: "{}\n"#,
                        schema_yaml, val, errs
                    );
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    fn shape_limits(vals: Vec<Value>, limit: usize) -> bool {
        let mut shape = Shape::nothing();
        for val in &vals {
            shape.widen(val);
        }

        let initial_locations = shape.locations().len();
        let initial_schema_yaml = serde_yaml::to_string(&to_schema(shape.clone())).unwrap();

        enforce_shape_complexity_limit(&mut shape, limit);

        let enforced_locations = shape
            .locations()
            .iter()
            .filter(|(ptr, pattern, _, _)| !pattern && ptr.0.len() > 0)
            .collect_vec()
            .len();

        if enforced_locations > limit || !assert_docs_fit_schema(vals.clone(), shape.clone()) {
            let schema_yaml = serde_yaml::to_string(&to_schema(shape)).unwrap();
            println!("Started with {initial_locations} initial locations, enforced down to {enforced_locations}, limit was {limit}");
            println!(
                "start: {initial_schema_yaml}\nenforced down to: {schema_yaml}\ndocs:{vals:?}"
            );
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
        assert_eq!(true, shape_limits(vec![json!({"":{"":55}})], 1));
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
        fn inner_test(av: Vec<ArbitraryValue>, limit: usize) -> TestResult {
            if limit < 1 {
                return TestResult::discard();
            }
            let vals = av.into_iter().map(|v| v.0).collect_vec();
            TestResult::from_bool(shape_limits(vals, limit))
        }

        QuickCheck::new()
            .gen(Gen::new(100))
            .tests(1000)
            .quickcheck(inner_test as fn(Vec<ArbitraryValue>, usize) -> TestResult);
    }
}
