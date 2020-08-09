use estuary_json::Location;

#[test]
fn test_formatting() {
    let l1 = Location::Root;
    let l2 = l1.push_prop_with_index("foo~", 0);
    let l3 = l2.push_item(42);
    let l4 = l3.push_prop_with_index("bar/baz", 0);
    assert_eq!(format!("#{}", l4), "#/foo~0/42/bar~1baz");

    let l5 = l3.push_prop_with_index("baz", 1);
    assert_eq!(format!("#{}", l5), "#/foo~0/42/baz");
}
