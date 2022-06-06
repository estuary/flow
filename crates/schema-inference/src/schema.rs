use super::validations;
use doc::inference::Shape;
use schemars::gen::SchemaGenerator;
use schemars::schema::*;

#[derive(Debug, Default)]
pub struct JsonSchema {
    pub metadata: Metadata,
    pub root: Shape,
}

impl JsonSchema {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let schema_obj = SchemaObject {
            instance_type: Some(SingleOrVec::from(InstanceType::Object)),
            metadata: Some(Box::new(self.metadata.clone())),
            ..SchemaObject::default()
        };

        let mut root = RootSchema {
            schema: schema_obj,
            meta_schema: SchemaGenerator::default().settings().meta_schema.clone(),
            ..RootSchema::default()
        };

        root.schema.object = validations::object(&self.root);

        serde_json::to_string(&root)
    }
}
