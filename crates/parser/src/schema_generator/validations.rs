use doc::inference::{ObjProperty, Shape};
use schemars::schema::*;

pub fn property<'a>(prop: &ObjProperty) -> Schema {
    let mut schema = SchemaObject {
        metadata: Some(Box::new(Metadata::default())),
        ..SchemaObject::default()
    };

    for value in prop.shape.type_.iter().collect::<Vec<&'static str>>() {
        match value {
            "string" => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)))
            }
            "boolean" => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Boolean)))
            }
            "number" => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Number)))
            }
            "null" => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Null)))
            }
            // If the value is not matched, the schema returns is a "failed validation" where it
            // will always fail the schema validation. This could eventually be changed to be open by
            // default, but this is only a temporary measure until more data types gets implemented.
            _ => {
                return Schema::Bool(false);
            }
        }
    }

    return Schema::Object(schema);
}

pub fn object(shape: &Shape) -> Option<Box<ObjectValidation>> {
    let mut validation = ObjectValidation::default();
    shape.object.properties.iter().for_each(|prop| {
        validation
            .properties
            .insert(prop.name.clone(), property(&prop));
    });
    return Some(Box::new(validation));
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::inference::ObjShape;
    use json::schema::types;

    #[test]
    fn test_object_includes_all_properties() {
        let properties = vec![
            ObjProperty {
                name: "property1".to_string(),
                is_required: true,
                shape: Shape {
                    type_: types::STRING,
                    ..Shape::default()
                },
            },
            ObjProperty {
                name: "property2".to_string(),
                is_required: true,
                shape: Shape {
                    type_: types::BOOLEAN,
                    ..Shape::default()
                },
            },
        ];

        let result = object(&Shape {
            object: ObjShape {
                properties: properties,
                ..ObjShape::default()
            },
            ..Shape::default()
        });

        let validation = *result.expect("expected to generate a boxed objectValidation");
        assert_eq!(validation.properties.len(), 2)
    }
}
