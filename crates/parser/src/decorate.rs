use crate::ParseConfig;
use doc::ptr::{Pointer, Token};
use serde_json::Value;

/// Decorator adds properties to parsed JSON documents.
pub struct Decorator {
    fields: Vec<(Pointer, Value)>,
    offset_location: Option<Pointer>,
}

impl Decorator {
    pub fn from_config(config: &ParseConfig) -> Decorator {
        let fields = config
            .add_values
            .iter()
            .map(|(ptr, val)| (Pointer::from_str(ptr.as_ref()), val.clone()))
            .collect();
        let offset_location = config.add_record_offset.as_ref().map(Pointer::from);
        Decorator {
            fields,
            offset_location,
        }
    }

    /// Adds the properties to the given `doc`. If any field cannot be added, this function returns
    /// immediately with the first error, and leaves the document in a partially modified state.
    pub fn add_fields(&self, record_offset: u64, doc: &mut Value) -> Result<(), AddFieldError> {
        if let Some(location) = self.offset_location.as_ref() {
            let value = Value::from(record_offset);
            add_field(doc, location, &value)?;
        }
        for (pointer, value) in self.fields.iter() {
            add_field(doc, pointer, value)?;
        }
        Ok(())
    }
}

fn add_field(target: &mut Value, location: &Pointer, value: &Value) -> Result<(), AddFieldError> {
    if let Some(target_location) = location.create_value(target) {
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

// TODO: move this impl into the doc crate. For real next time ;)
pub fn display_ptr(ptr: &Pointer) -> String {
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
