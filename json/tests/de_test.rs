use estuary_json::{de::walk, LocatedItem, LocatedProperty, Location, Number, Span, Walker};
use serde_json::{json, Deserializer};

#[test]
fn test_visit_raw_json() -> Result<(), serde_json::Error> {
    let raw = r#"
    {
        "name": "John Doe",
        "age": 43,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ],
        "children": {
            "mary": [12, 3.45],
            "robert": [-23.4, -12],
            "john": null
        },
        "foo": [
            {"k": "key", "v": "value"},
            {"k2": "key2", "v2": "value2"}
        ]
    }"#;

    let mut jde = Deserializer::from_str(raw);
    let mut walker = PrintWalker {};
    let span1 = walk(&mut jde, &mut walker)?;

    assert_eq!(
        span1,
        Span {
            begin: 0,
            end: 21,
            hashed: 2246500406210156237,
        }
    );
    Ok(())
}

#[test]
fn test_raw_vs_json_equivalence() -> Result<(), serde_json::Error> {
    let raw = r#"
        {
            "phones": [
                "+44 2345678",
                "+44 1234569"
            ],
            "name": "John Doe",
            "age": 43
        }"#;

    let mut jde = Deserializer::from_str(raw);
    let mut walker = PrintWalker {};
    let span1 = walk(&mut jde, &mut walker)?;

    let parsed = json!({
        "phones": [
            "+44 2345678",
            "+44 1234569"
        ],
        "name": "John Doe",
        "age": 43
    });

    let span2 = walk(&parsed, &mut walker)?;

    assert_eq!(span1, span2);
    Ok(())
}

struct PrintWalker;

impl Walker for PrintWalker {
    fn push_property<'a>(&mut self, span: &Span, loc: &'a LocatedProperty<'a>) {
        println!(
            "push_property {} @ {:?}",
            Location::Property(*loc).pointer_str(),
            span
        );
    }
    fn push_item<'a>(&mut self, span: &Span, loc: &'a LocatedItem<'a>) {
        println!(
            "push_item {} @ {:?}",
            Location::Item(*loc).pointer_str(),
            span
        );
    }
    fn pop_object<'a>(&mut self, span: &Span, loc: &'a Location<'a>, num_properties: usize) {
        println!(
            "pop_object {:?} @ {}:{:?}",
            num_properties,
            loc.pointer_str(),
            span
        );
    }
    fn pop_array<'a>(&mut self, span: &Span, loc: &'a Location<'a>, num_items: usize) {
        println!(
            "pop_array {:?} @ {}:{:?}",
            num_items,
            loc.pointer_str(),
            span
        );
    }
    fn pop_bool<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: bool) {
        println!("pop_bool {:?} @ {}:{:?}", val, loc.pointer_str(), span);
    }
    fn pop_numeric<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: Number) {
        println!("pop_numeric {:?} @ {}:{:?}", val, loc.pointer_str(), span);
    }
    fn pop_str<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: &'a str) {
        println!("pop_str {:?} @ {}:{:?}", val, loc.pointer_str(), span);
    }
    fn pop_null<'a>(&mut self, span: &Span, loc: &'a Location<'a>) {
        println!("pop_null <null> @ {}:{:?}", loc.pointer_str(), span);
    }
}
