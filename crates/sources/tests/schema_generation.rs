#[test]
fn test_catalog_schema_snapshot() {
    let schema = models::Catalog::root_json_schema();
    insta::assert_json_snapshot!(&schema);
}
