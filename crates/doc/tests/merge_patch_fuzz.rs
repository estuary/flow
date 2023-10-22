use doc::{reduce::Error, validation::build_schema, HeapNode, LazyNode, SerPolicy, Validator};
use quickcheck::quickcheck;
use serde_json::Value;

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

quickcheck! {
    fn merge_patch_fuzz(input: Vec<ArbitraryValue>) -> bool {
        compare_merge_patch(input)
    }
}

fn compare_merge_patch(input: Vec<ArbitraryValue>) -> bool {
    let alloc = HeapNode::new_allocator();
    let curi = url::Url::parse("http://example").unwrap();
    let mut validator =
        Validator::new(build_schema(curi, &doc::reduce::merge_patch_schema()).unwrap()).unwrap();

    let mut it = input.into_iter().map(|a| a.0);

    // Base document into which all others are reduced.
    let Some(seed) = it.next() else { return true };
    let Some(doc) = it.next() else { return true };

    let mut expect = seed.clone();
    json_patch::merge(&mut expect, &doc);

    // Initialize the reduction stack.
    let mut stack = vec![HeapNode::from_node(&doc, &alloc)];

    // Perform a pass of associative reductions.
    for rhs in it {
        json_patch::merge(&mut expect, &rhs);
        let rhs_valid = validator.validate(None, &rhs).unwrap().ok().unwrap();

        // Attempt to associatively-reduce `doc` into the top of the stack.
        let top = stack.last_mut().unwrap();

        match doc::reduce::reduce(
            LazyNode::Heap(top),
            LazyNode::Node(&rhs),
            rhs_valid,
            &alloc,
            false,
        ) {
            Err(Error::NotAssociative) => {
                // Push `rhs` to the top of the stack.
                stack.push(HeapNode::from_node(&rhs, &alloc))
            }
            Ok((doc, _delete)) => {
                // Replace the stack tip with reduced `doc`.
                *top = doc;
            }
            Err(err) => panic!("{err:#}"),
        }
    }

    // Now perform full reductions.
    let mut reduced = HeapNode::from_node(&seed, &alloc);
    for rhs in stack {
        let rhs_valid = validator.validate(None, &rhs).unwrap().ok().unwrap();

        (reduced, _) = doc::reduce::reduce(
            LazyNode::Heap::<Value>(&reduced),
            LazyNode::Heap(&rhs),
            rhs_valid,
            &alloc,
            true,
        )
        .unwrap();
    }

    let reduced = serde_json::to_value(&SerPolicy::default().on(&reduced)).unwrap();

    reduced == expect
}
