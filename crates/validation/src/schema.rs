use super::Error;
use doc::{inference, validation};
use json::schema::types;
use proto_flow::flow::collection_spec::derivation::ShuffleType;

pub struct Schema {
    // Canonical schema URI, which may include a fragment pointer if the schema
    // is inline to a Flow specification.
    pub curi: url::Url,
    // Validator of this schema.
    pub validator: validation::Validator,
    // Inferred schema shape.
    pub shape: inference::Shape,
}

impl Schema {
    pub fn new(bundle: &str) -> Result<Self, Error> {
        let schema = doc::validation::build_bundle(bundle)?;
        let validator = doc::Validator::new(schema)?;
        let shape = inference::Shape::infer(&validator.schemas()[0], validator.schema_index());

        Ok(Self {
            curi: validator.schemas()[0].curi.clone(),
            validator,
            shape,
        })
    }

    // Walk a JSON pointer which was explicitly provided by the user.
    // Examples include collection key components, shuffle key components,
    // and collection projections.
    // A location which is serving as a key has additional restrictions
    // on its required existence and applicable types.
    pub fn walk_ptr(&self, ptr: &models::JsonPointer, ptr_is_key: bool) -> Result<(), Error> {
        let (start, stop) = models::JsonPointer::regex()
            .find(ptr)
            .map(|m| (m.start(), m.end()))
            .unwrap_or((0, 0));
        let unmatched = [&ptr[..start], &ptr[stop..]].concat();

        let (shape, exists) = self.shape.locate(&doc::Pointer::from_str(ptr));

        // These checks return early if matched because
        // further errors are likely spurious.
        if !ptr.is_empty() && !ptr.starts_with("/") {
            return Err(Error::PtrMissingLeadingSlash {
                ptr: ptr.to_string(),
            });
        } else if !unmatched.is_empty() {
            return Err(Error::PtrRegexUnmatched {
                ptr: ptr.to_string(),
                unmatched,
            });
        } else if exists == inference::Exists::Implicit {
            return Err(Error::PtrIsImplicit {
                ptr: ptr.to_string(),
                schema: self.curi.clone(),
            });
        } else if exists == inference::Exists::Cannot {
            return Err(Error::PtrCannotExist {
                ptr: ptr.to_string(),
                schema: self.curi.clone(),
            });
        }

        // Remaining validations apply only to key locations.
        if !ptr_is_key {
            return Ok(());
        }

        if !shape.type_.is_keyable_type() {
            return Err(Error::KeyWrongType {
                ptr: ptr.to_string(),
                type_: shape.type_,
                schema: self.curi.clone(),
            });
        }

        if !matches!(
            shape.reduction,
            inference::Reduction::Unset | inference::Reduction::LastWriteWins,
        ) {
            return Err(Error::KeyHasReduction {
                ptr: ptr.to_string(),
                schema: self.curi.clone(),
                strategy: shape.reduction.clone(),
            });
        }

        Ok(())
    }

    // Gather all key-able types for the given `key`.
    // If a location is not a key-able type, None is returned.
    pub fn shuffle_key_types<I, S>(&self, key: I) -> Vec<ShuffleType>
    where
        S: AsRef<str>,
        I: Iterator<Item = S>,
    {
        key.map(|ptr| {
            let (shape, _exists) = self.shape.locate(&doc::Pointer::from_str(ptr.as_ref()));

            match shape.type_ - types::NULL {
                types::BOOLEAN => ShuffleType::Boolean,
                types::INTEGER => ShuffleType::Integer,
                types::STRING => ShuffleType::String,
                _ => ShuffleType::InvalidShuffleType,
            }
        })
        .collect()
    }
}
