use crate::ParseConfig;
use doc::ptr::{Pointer, Token};
use serde_json::Value;

pub struct Decorator {
    fields: Vec<(Pointer, Value)>,
    line_number_location: Option<Pointer>,
}

impl Decorator {
    pub fn from_config(config: &ParseConfig) -> Decorator {
        let fields = config
            .add_values
            .iter()
            .map(|(ptr, val)| (Pointer::from_str(ptr.as_ref()), val.clone()))
            .collect();
        let line_number_location = config.add_source_offset.as_ref().map(Pointer::from);
        Decorator {
            fields,
            line_number_location,
        }
    }

    pub fn add_fields(&self, line_num: Option<u64>, doc: &mut Value) -> Result<(), AddFieldError> {
        if let Some((location, line)) = self.line_number_location.as_ref().zip(line_num) {
            let value = Value::from(line);
            add_field(doc, location, &value)?;
        }
        for (pointer, value) in self.fields.iter() {
            add_field(doc, pointer, value)?;
        }
        Ok(())
    }
}

fn add_field(target: &mut Value, location: &Pointer, value: &Value) -> Result<(), AddFieldError> {
    if let Some(target_location) = location.create(target) {
        *target_location = value.clone();
        Ok(())
    } else {
        Err(AddFieldError {
            property: value.clone(),
            location: display_ptr(location),
            document: target.clone(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unable to add value: {property} at location: {location} in document: {document}")]
pub struct AddFieldError {
    property: Value,
    location: String,
    document: Value,
}

fn display_ptr(ptr: &Pointer) -> String {
    use std::fmt::Write;
    let mut buf = String::new();
    for token in ptr.iter() {
        buf.push('/');
        match token {
            Token::Property(p) => buf.push_str(p),
            Token::Index(i) => write!(&mut buf, "{}", i).unwrap(),
            Token::NextIndex => buf.push('-'),
        }
    }
    buf
}
