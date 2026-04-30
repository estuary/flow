use doc::{ArchivedNode, Extractor, ExtractorPlan, HeapNode, SerPolicy};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::{Value, json};
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;

// Differential fuzz test: `ExtractorPlan` must produce byte-for-byte identical
// output to the reference `Extractor` path for the same extractor list.
//
// The planner has one optimized behavior: runs of two or more ordered sibling
// leaves under a common parent are emitted by walking to the parent once and
// merge-joining the parent's fields. Singletons fall through to the reference
// path. This test keeps the document fixed and fuzzes extractor plans that
// mix runs of varying length (including 1) with fallback singles, so both
// sides of the block/no-block boundary are exercised.
//
// Each generated plan includes three runs (under `/wide`, `/nested/inner`,
// and a randomly-chosen *unusable* parent) of randomized length in
// `[1, MAX_RUN_LEN]`, plus interspersed singles and optional
// truncation-indicator / UUID extractors.

const UUID_STR: &str = "85bad119-15f2-11ee-8401-43f05f562888";
const UUID_PTR: &str = "/_meta/uuid";

const FIELD_COUNT: usize = 24;
const MAX_RUN_LEN: usize = 12;

#[derive(Clone, Copy, Debug)]
enum ExtractorDefault {
    None,
    Number,
    String,
    Object,
}

impl ExtractorDefault {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => ExtractorDefault::Number,
            1 => ExtractorDefault::String,
            2 => ExtractorDefault::Object,
            _ => ExtractorDefault::None,
        }
    }

    fn value(self) -> Option<Value> {
        match self {
            ExtractorDefault::None => None,
            ExtractorDefault::Number => Some(json!(42)),
            ExtractorDefault::String => Some(json!("default")),
            ExtractorDefault::Object => Some(json!({"default": true})),
        }
    }
}

#[derive(Clone, Debug)]
enum ExtractorSpec {
    Regular {
        ptr: String,
        default: ExtractorDefault,
    },
    Uuid,
    Indicator,
}

impl ExtractorSpec {
    fn regular(ptr: impl Into<String>, default: ExtractorDefault) -> Self {
        Self::Regular {
            ptr: ptr.into(),
            default,
        }
    }

    fn build(&self, policy: &SerPolicy) -> Extractor {
        match self {
            Self::Regular { ptr, default } => match default.value() {
                None => Extractor::new(ptr.as_str(), policy),
                Some(v) => Extractor::with_default(ptr, policy, v),
            },
            Self::Uuid => Extractor::for_uuid_v1_date_time(UUID_PTR),
            Self::Indicator => Extractor::for_truncation_indicator(),
        }
    }
}

#[derive(Clone, Debug)]
struct PlanSpec {
    entries: Vec<ExtractorSpec>,
    policy_kind: u8,
}

impl Arbitrary for PlanSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut entries = Vec::new();
        let indicator_slot = usize::arbitrary(g) % 7;
        let uuid_slot = usize::arbitrary(g) % 7;
        let unusable_parent = match u8::arbitrary(g) % 4 {
            0 => "/missing_parent",
            1 => "/null_parent",
            2 => "/str_parent",
            _ => "/arr",
        };

        for slot in 0..=6 {
            if uuid_slot == slot {
                entries.push(ExtractorSpec::Uuid);
            }
            if indicator_slot == slot {
                entries.push(ExtractorSpec::Indicator);
            }

            match slot {
                0 | 2 | 5 => push_random_singles(&mut entries, g),
                1 => push_run(&mut entries, "/wide", g),
                3 => push_run(&mut entries, "/nested/inner", g),
                4 => push_run(&mut entries, unusable_parent, g),
                6 => {}
                _ => unreachable!(),
            }
        }

        Self {
            entries,
            policy_kind: u8::arbitrary(g) % 3,
        }
    }
}

impl PlanSpec {
    fn build_extractors(&self) -> Vec<Extractor> {
        let policy = match self.policy_kind {
            0 => SerPolicy::noop(),
            1 => SerPolicy::truncate_strings(4),
            _ => SerPolicy {
                str_truncate_after: 8,
                array_truncate_after: 2,
                nested_obj_truncate_after: 2,
            },
        };

        self.entries.iter().map(|e| e.build(&policy)).collect()
    }
}

fn push_run(entries: &mut Vec<ExtractorSpec>, parent: &str, g: &mut Gen) {
    let len = 1 + usize::arbitrary(g) % MAX_RUN_LEN;
    let offset = usize::arbitrary(g) % (FIELD_COUNT - len + 1);

    for i in 0..len {
        let field = if i > 0 && u8::arbitrary(g) % 12 == 0 {
            i - 1 // duplicate the prior target; duplicate names are block-eligible.
        } else {
            i
        };
        entries.push(ExtractorSpec::regular(
            format!("{parent}/f_{:02}", offset + field),
            ExtractorDefault::arbitrary(g),
        ));
    }
}

fn push_random_singles(entries: &mut Vec<ExtractorSpec>, g: &mut Gen) {
    let count = usize::arbitrary(g) % 4;
    for _ in 0..count {
        let ptr = match u8::arbitrary(g) % 12 {
            0 => "",
            1 => "/arr",
            2 => "/arr/0",
            3 => "/arr/9",
            4 => "/wide/f_00",
            5 => "/wide/f_01",
            6 => "/nested/inner/f_02",
            7 => "/missing",
            8 => "/missing/child",
            9 => "/null_parent/child",
            10 => "/str_parent/child",
            _ => "/long",
        };
        entries.push(ExtractorSpec::regular(ptr, ExtractorDefault::arbitrary(g)));
    }
}

fn build_doc() -> Value {
    json!({
        "_meta": {"uuid": UUID_STR},
        "wide": field_map(),
        "nested": {"inner": field_map()},
        "arr": ["v0", "v1", "v2", "v3"],
        "null_parent": null,
        "str_parent": "not-an-object",
        "long": "this string is intentionally long enough to truncate",
    })
}

fn field_map() -> Value {
    let mut fields = serde_json::Map::new();
    for i in 0..FIELD_COUNT {
        if i % 2 == 0 {
            fields.insert(format!("f_{i:02}"), field_value(i));
        }
    }
    Value::Object(fields)
}

fn field_value(i: usize) -> Value {
    match i % 8 {
        0 => json!(i as i64),
        2 => json!(format!("value-{i}-with-tail")),
        4 => json!(true),
        _ => json!({"nested": i, "values": [1, 2, 3, 4]}),
    }
}

fn pack_reference<N: json::AsNode>(doc: &N, extractors: &[Extractor]) -> bytes::Bytes {
    let mut buf = bytes::BytesMut::new();
    let indicator = AtomicBool::new(false);
    Extractor::extract_all_indicate_truncation(doc, extractors, &mut buf, &indicator);
    buf.freeze()
}

fn pack_plan<N: json::AsNode>(plan: &ExtractorPlan, doc: &N) -> bytes::Bytes {
    let mut buf = bytes::BytesMut::new();
    let indicator = AtomicBool::new(false);
    plan.extract_all_indicate_truncation(doc, &mut buf, &indicator);
    buf.freeze()
}

fn check_representation<N: json::AsNode>(
    name: &str,
    doc: &Value,
    node: &N,
    plan: &ExtractorPlan,
    extractors: &[Extractor],
) -> bool {
    let ref_bytes = pack_reference(node, extractors);
    let plan_bytes = pack_plan(plan, node);

    if ref_bytes == plan_bytes {
        true
    } else {
        eprintln!(
            "{name} divergence\ndoc={doc}\nextractors={extractors:#?}\nref={ref_bytes:02x?}\nplan={plan_bytes:02x?}",
        );
        false
    }
}

static DOC: LazyLock<Value> = LazyLock::new(build_doc);

fn assert_equivalent(plan_spec: PlanSpec) -> bool {
    let doc: &Value = &DOC;
    let extractors = plan_spec.build_extractors();
    let plan = ExtractorPlan::new(&extractors);

    if !check_representation("serde_json::Value", doc, doc, &plan, &extractors) {
        return false;
    }

    let alloc = HeapNode::new_allocator();
    let heap = HeapNode::from_serde(doc, &alloc).expect("build_doc produces valid HeapNode input");
    if !check_representation("HeapNode", doc, &heap, &plan, &extractors) {
        return false;
    }

    let archive = heap.to_archive();
    let archived = ArchivedNode::from_archive(&archive);
    check_representation("ArchivedNode", doc, archived, &plan, &extractors)
}

#[test]
fn fuzz_plan_matches_extractor() {
    QuickCheck::new()
        .r#gen(Gen::new(50))
        .tests(1_000)
        .quickcheck(assert_equivalent as fn(PlanSpec) -> bool);
}
