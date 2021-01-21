use doc::Pointer;
use protocol::flow::Projection;
use rand::Rng;
use serde_json::Value;

use std::borrow::Borrow;
use std::fmt::Debug;

/// A TestDoc represents a JSON document that can be stored by a driver.
#[derive(Debug, Clone, PartialEq)]
pub struct TestDoc {
    /// The complete json document.
    pub json: Value,
    /// Indicates whether this document is expected to exist within the remote store. This field is
    /// used to set the `exists` value within a `StoreRequest`.
    pub exists: bool,
}

impl TestDoc {
    /// Simulate the mutation of an existing document that has previously been stored. This
    /// (probably) mutates a single field by randomly generating a new value for the given
    /// projection, and it sets `exists` to `true`. It's possible that the newly generated value may
    /// be the same as the existing one, and we still set `exists` to `true` in that case, which
    /// will result in a Store call with the same exact document and values. Though it may seem like
    /// that situation is best avoided, this is actually something that can happen in production, so
    /// we might as well test it here as well.
    pub fn update_in_place(&mut self, rng: &mut impl Rng, projection: &Projection) {
        let new_value = rand_test_value(rng, projection);
        let ptr = Pointer::from(projection.ptr.as_str());
        let loc = ptr
            .create(&mut self.json)
            .expect("Pointer::create returned None");
        *loc = new_value.unwrap_or(Value::Null);
        // set exists to true so that the next time this document is used to create a store
        // request, that bit will be set to true.
        self.exists = true;
    }
}

impl Borrow<Value> for TestDoc {
    fn borrow(&self) -> &Value {
        &self.json
    }
}

/// Generates multiple TestDocs using `rand_test_doc` and returns the results as a Vec.
pub fn rand_test_docs(
    rng: &mut impl Rng,
    projections: &[Projection],
    count: usize,
) -> Vec<TestDoc> {
    std::iter::repeat_with(|| rand_test_doc(rng, projections))
        .take(count)
        .collect()
}

/// Quick and dirty implementation of random document generation. This function works to generate
/// random documents that are suitable for testing materializations of collections with simple
/// schemas. **It it not guaranteed that the returned JSON document will be valid against the
/// collection schema** because this uses inferences to drive the generation, which may not include
/// enough information to guarantee all objects are valid. This is considered "goodenuf" for now,
/// because we don't actually need complex schemas to propertly test materializations, and because
/// a more complete implementation sounds like more work than it's worth.
pub fn rand_test_doc(rng: &mut impl Rng, projections: &[Projection]) -> TestDoc {
    let mut doc = Value::Object(Default::default());

    for projection in projections {
        let pointer = Pointer::from(&projection.ptr);
        if let Some(val) = rand_test_value(rng, projection) {
            let loc = pointer.create(&mut doc).expect("cannot construct value");
            *loc = val;
        }
    }
    TestDoc {
        json: doc,
        // This is a randomly generated document, so we'll assume that it doesn't exist already.
        // This assumption may not hold in all scenarios. For example, if key fields are nullable,
        // then there's a significant chance for the rng to generate multiple documents with the
        // same key.
        exists: false,
    }
}

// Generates a random value for a test document. This is a super quick and dirty implementation,
// which does not (yet) handle objects or arrays.
fn rand_test_value(rng: &mut impl Rng, projection: &Projection) -> Option<Value> {
    use rand::seq::SliceRandom;

    let inference = projection
        .inference
        .as_ref()
        .expect("projection must have inference");

    // If this is allowed to be undefined, we might short circuit here.
    if !inference.must_exist && rng.gen_bool(0.4) {
        return None;
    }

    let types = inference.types.as_slice();
    let ty = types.choose(rng).map(String::as_str).unwrap_or("null");

    match ty {
        "integer" => Some(rng.gen::<i64>().into()),
        "number" => {
            // the rng will generate a number between 0 and 1, so we multiply it to distribute the
            // range a bit better.
            let mul = rng.gen_range(1i32, 4000i32);
            let n = rng.gen::<f64>() * mul as f64;
            Some(n.into())
        }
        "string" => {
            let mut max_len: usize = 32;
            if let Some(string) = inference.string.as_ref() {
                if string.is_base64 || !string.format.is_empty() || !string.content_type.is_empty()
                {
                    panic!("fancy string types aren't yet supported, and this is better than a TODO comment ;)");
                }
                if string.max_length > 0 {
                    max_len = string.max_length as usize;
                }
            }
            let len: usize = rng.gen_range(0, max_len);
            Some(gen_rand_alphanumeric(len, rng).into())
        }
        "boolean" => Some(rng.gen::<bool>().into()),
        "null" => Some(Value::Null),
        _ => None,
    }
}

fn gen_rand_alphanumeric(len: usize, rng: &mut impl Rng) -> String {
    std::iter::repeat_with(|| rng.sample(rand::distributions::Alphanumeric))
        .take(len)
        .collect::<String>()
}
