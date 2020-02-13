use estuary_json::{LocatedItem, LocatedProperty, Location};

#[test]
fn test_formatting() {
    // TODO - proper property name escaping.

    let l1 = Location::Root;
    let l2 = Location::Property(LocatedProperty {
        parent: &l1,
        name: "foo",
        index: 0,
    });
    let l3 = Location::Item(LocatedItem {
        parent: &l2,
        index: 42,
    });
    let l4 = Location::Property(LocatedProperty {
        parent: &l3,
        name: "bar",
        index: 0,
    });
    assert_eq!(format!("{}", l4), "#/foo/42/bar");

    let l5 = Location::Property(LocatedProperty {
        parent: &l3,
        name: "baz",
        index: 1,
    });
    assert_eq!(format!("{}", l5), "#/foo/42/baz");
}
