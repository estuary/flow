use connector_protocol::{capture, materialize};

#[test]
fn test_catalog_schema_snapshot() {
    let mut settings = schemars::gen::SchemaSettings::draft2019_09();
    settings.option_add_null_type = false;
    let mut generator = schemars::gen::SchemaGenerator::new(settings);

    insta::assert_json_snapshot!(&generator.root_schema_for::<capture::Request>());
    insta::assert_json_snapshot!(&generator.root_schema_for::<capture::Response>());
    insta::assert_json_snapshot!(&generator.root_schema_for::<materialize::Request>());
    insta::assert_json_snapshot!(&generator.root_schema_for::<materialize::Response>());
}
