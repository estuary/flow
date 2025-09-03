use doc::{
    combine::{MemTable, Spec, SpillDrainer, SpillWriter},
    reduce::Error,
    validation::build_schema,
    HeapNode, LazyNode, SerPolicy, Validator,
};
use quickcheck::quickcheck;
use serde_json::{json, Value};
use std::io;

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

quickcheck! {
    fn reduce_stack_fuzz(input: Vec<ArbitraryValue>) -> bool {
        reduce_stack(input)
    }

    fn reduce_combiner_fuzz(input: Vec<ArbitraryValue>) -> bool {
        reduce_combiner(input)
    }
}

fn reduce_stack(input: Vec<ArbitraryValue>) -> bool {
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

    let reduced = serde_json::to_value(&SerPolicy::noop().on(&reduced)).unwrap();

    reduced == expect
}

fn reduce_combiner(input: Vec<ArbitraryValue>) -> bool {
    let spec = |is_full| {
        let curi = url::Url::parse("http://example").unwrap();
        let schema = build_schema(curi, &doc::reduce::merge_patch_schema()).unwrap();
        (
            is_full,
            [], // Empty key (all docs are equal)
            "source-name",
            None,
            Validator::new(schema).unwrap(),
        )
    };
    let memtable_1 = MemTable::new(Spec::with_bindings([spec(false), spec(true)].into_iter(), Vec::new()));
    let memtable_2 = MemTable::new(Spec::with_bindings([spec(false), spec(true)].into_iter(), Vec::new()));

    let seed = json!({"hello": "world", "null": null});
    let mut expect = seed.clone();

    for rhs in input.into_iter().map(|a| a.0) {
        for binding in 0..2 {
            let d1 = HeapNode::from_node(&rhs, memtable_1.alloc());
            () = memtable_1.add(binding, d1, false).unwrap();
            let d2 = HeapNode::from_node(&rhs, memtable_2.alloc());
            () = memtable_2.add(binding, d2, false).unwrap();
        }
        json_patch::merge(&mut expect, &rhs);
    }

    // Add initial `seed` at the front.
    for binding in 0..2 {
        let d1 = HeapNode::from_node(&seed, memtable_1.alloc());
        memtable_1.add(binding, d1, true).unwrap();
        let d2 = HeapNode::from_node(&seed, memtable_2.alloc());
        memtable_2.add(binding, d2, true).unwrap();
    }

    // Drain `memtable_1` using a MemDrainer.
    let mut mem_drainer = memtable_1.try_into_drainer().unwrap().peekable();

    // Drain `memtable_2` using a SpillDrainer.
    let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
    let spec = memtable_2.spill(&mut spill, 1 << 18).unwrap();
    let (mut spill, ranges) = spill.into_parts();

    let mut spill_drainer = SpillDrainer::new(spec, &mut spill, &ranges).unwrap();

    let mut actual_associative = None;
    let mut actual_full = json!(null);

    while let Some(mem) = mem_drainer.next() {
        let mem = mem.unwrap();
        let spill = spill_drainer.next().unwrap().unwrap();

        assert_eq!(
            mem.meta, spill.meta,
            "MemDrainer and SpillDrainer return identical Meta values"
        );

        let binding = mem.meta.binding();
        let deleted = mem.meta.deleted();

        let mem = serde_json::to_value(&SerPolicy::noop().on_owned(&mem.root)).unwrap();
        let spill = serde_json::to_value(&SerPolicy::noop().on_owned(&spill.root)).unwrap();

        assert_eq!(
            mem, spill,
            "MemDrainer and SpillDrainer return identical documents"
        );

        if binding == 0 && actual_associative.is_none() {
            actual_associative = Some(mem); // Initial value.
        } else if binding == 0 {
            json_patch::merge(actual_associative.as_mut().unwrap(), &mem);
        } else {
            assert_eq!(binding, 1);
            actual_full = mem;

            assert_eq!(
                deleted,
                expect.is_null(),
                "null is surfaced as a deletion tombstone"
            );
        }
    }
    assert!(spill_drainer.next().is_none());

    actual_associative.unwrap() == expect && actual_full == expect
}

#[test]
fn test_partial_drain_regression() {
    // This test case failed under the initial implementation of associative
    // reductions within the combiner, which was too aggressive in compacting
    // down to a single document for each binding group (it didn't properly
    // hold back an initial document of each group).
    assert!(reduce_combiner(vec![ArbitraryValue(json!({"": null}))]))
}
