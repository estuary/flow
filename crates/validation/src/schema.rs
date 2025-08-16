use super::Error;
use doc::{
    reduce,
    shape::{self, location::Exists},
    validation, Shape,
};
use json::schema::types;
use proto_flow::flow::collection_spec::derivation::ShuffleType as ProtoShuffleType;

pub struct Schema {
    // Canonical schema URI, which may include a fragment pointer if the schema
    // is inline to a Flow specification.
    pub curi: url::Url,
    // Validator of this schema.
    pub validator: validation::Validator,
    // Inferred schema shape.
    pub shape: Shape,
}

impl Schema {
    pub fn new(bundle: &[u8]) -> Result<Self, Error> {
        let schema = doc::validation::build_bundle(bundle)?;
        let validator = doc::Validator::new(schema)?;
        let shape = Shape::infer(&validator.schemas()[0], validator.schema_index());

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
    pub fn walk_ptr(
        read: &Self,
        write: Option<&Self>,
        ptr: &models::JsonPointer,
        ptr_is_key: bool,
    ) -> Result<(), Error> {
        let (start, stop) = models::JsonPointer::regex()
            .find(ptr)
            .map(|m| (m.start(), m.end()))
            .unwrap_or((0, 0));
        let unmatched = [&ptr[..start], &ptr[stop..]].concat();

        let (read_shape, read_exists) = read.shape.locate(&doc::Pointer::from_str(ptr));

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
        } else if read_exists == Exists::Implicit {
            return Err(Error::PtrIsImplicit {
                ptr: ptr.to_string(),
                schema: read.curi.clone(),
            });
        } else if read_exists == Exists::Cannot {
            return Err(Error::PtrCannotExist {
                ptr: ptr.to_string(),
                schema: read.curi.clone(),
            });
        }

        // Remaining validations apply only to key locations.
        if !ptr_is_key {
            return Ok(());
        }

        if !read_shape.type_.is_keyable_type() {
            return Err(Error::KeyWrongType {
                ptr: ptr.to_string(),
                type_: read_shape.type_,
                schema: read.curi.clone(),
            });
        }

        if !matches!(
            read_shape.reduction,
            shape::Reduction::Unset
                | shape::Reduction::Strategy(reduce::Strategy::LastWriteWins(_)),
        ) {
            return Err(Error::KeyHasReduction {
                ptr: ptr.to_string(),
                schema: read.curi.clone(),
                strategy: read_shape.reduction.clone(),
            });
        }

        if let Some(write) = write {
            let (write_shape, write_exists) = write.shape.locate(&doc::Pointer::from_str(ptr));

            if write_exists == Exists::Implicit {
                return Err(Error::PtrIsImplicit {
                    ptr: ptr.to_string(),
                    schema: write.curi.clone(),
                });
            } else if write_exists == Exists::Cannot {
                return Err(Error::PtrCannotExist {
                    ptr: ptr.to_string(),
                    schema: write.curi.clone(),
                });
            }

            // Keyed location types may differ only in null-ability between
            // the read and write schemas.
            let read_type = read_shape.type_ - types::NULL;
            let write_type = write_shape.type_ - types::NULL;

            if read_type != write_type {
                return Err(Error::KeyReadWriteTypesDiffer {
                    ptr: ptr.to_string(),
                    read_type: read_type,
                    read_schema: read.curi.clone(),
                    write_type: write_type,
                    write_schema: write.curi.clone(),
                });
            }
        }

        Ok(())
    }

    // Gather all key-able types for the given `key`.
    // If a location is not a key-able type, None is returned.
    pub fn shuffle_key_types<I, S>(&self, key: I) -> Vec<ProtoShuffleType>
    where
        S: AsRef<str>,
        I: Iterator<Item = S>,
    {
        key.map(|ptr| {
            let (shape, _exists) = self.shape.locate(&doc::Pointer::from_str(ptr.as_ref()));

            match shape.type_ - types::NULL {
                types::BOOLEAN => ProtoShuffleType::Boolean,
                types::INTEGER => ProtoShuffleType::Integer,
                types::STRING => ProtoShuffleType::String,
                _ => ProtoShuffleType::InvalidShuffleType,
            }
        })
        .collect()
    }
}
