use json::schema::{build, index, CoreAnnotation};
use serde_json::json;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn test_bassssic() -> Result {
    let j = json!({
       "type": ["string", "object", "number"],
       "properties": {
           "foo": {
               "type": "string",
               "$ref": "other.json#/items/1",
               "description": "A fooer",
           },
           "bar": {
               "$id": "other.json",
               "items": [{}, false],
           },
           "ring": true,
           "ling": false,
       },
       "patternProperties": {
           "foo$": true,
       },
       "readOnly": true,
       "required": ["baz", "foo"],
       "dependentRequired": {
           "bar": ["foo"],
           "baz": ["bar", "foo", "!!"],
       },
       "maxProperties": 32,
       "minimum": -3.24,
       "exclusiveMinimum": -30001230,
       "maximum": 1234,
       "multipleOf": 12,
    });

    println!("raw J: {}", j);

    let url = url::Url::parse("http://example.com/root.json")?;
    let sch = build::build_schema::<CoreAnnotation>(url, &j)?;

    let mut ind = index::IndexBuilder::new();
    ind.add(&sch)?;
    ind.verify_references()?;

    print!("{:?}", sch);
    Ok(())
}
