mod testutil;

use parser::{Format, ParseConfig, xml::XmlConfig};
use testutil::{input_for_file, run_test};

#[test]
fn purchase_order_is_parsed_with_array_elements() {
    let path = "tests/examples/valid-purchase-order.xml";
    let config = ParseConfig {
        format: Format::Xml(XmlConfig {
            array_elements: [
                "Tag",
                "Note",
                "Delivery",
                "Package",
                "Line",
                "Step",
                "TaxId",
                "LocalName",
            ]
            .map(String::from)
            .to_vec(),
            ..Default::default()
        }),
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let result = run_test(&config, input_for_file(path));
    result.assert_success(1);
    insta::assert_json_snapshot!(result.parsed);
}

#[test]
fn batch_is_parsed_with_record_path() {
    let path = "tests/examples/valid-xml-batch.xml";
    let config = ParseConfig {
        format: Format::Xml(XmlConfig {
            record_path: Some("/Batch/Orders/Order".to_string()),
            array_elements: vec!["Line".to_string()],
        }),
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let result = run_test(&config, input_for_file(path));
    result.assert_success(3);
    insta::assert_json_snapshot!(result.parsed);
}
