use quickcheck::Arbitrary;
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, ops::Range};

#[derive(Clone, Debug)]
pub struct ArbitraryValue(pub Value);

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
                    .collect::<Vec<_>>()
                    .shrink()
                    .map(|v| {
                        Self(Value::Array(
                            v.into_iter().map(|av| av.0).collect::<Vec<_>>(),
                        ))
                    }),
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

fn gen_range(generator: &mut quickcheck::Gen, range: Range<u64>) -> u64 {
    u64::arbitrary(generator) % (range.end - range.start) + range.start
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
