impl serde::Serialize for Request {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.spec.is_some() {
            len += 1;
        }
        if self.validate.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        if self.read.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        if self.start_commit.is_some() {
            len += 1;
        }
        if self.reset.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.validate.as_ref() {
            struct_ser.serialize_field("validate", v)?;
        }
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.read.as_ref() {
            struct_ser.serialize_field("read", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        if let Some(v) = self.start_commit.as_ref() {
            struct_ser.serialize_field("startCommit", v)?;
        }
        if let Some(v) = self.reset.as_ref() {
            struct_ser.serialize_field("reset", v)?;
        }
        if !self.internal.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("$internal", pbjson::private::base64::encode(&self.internal).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Request {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "validate",
            "open",
            "read",
            "flush",
            "start_commit",
            "startCommit",
            "reset",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Validate,
            Open,
            Read,
            Flush,
            StartCommit,
            Reset,
            Internal,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "spec" => Ok(GeneratedField::Spec),
                            "validate" => Ok(GeneratedField::Validate),
                            "open" => Ok(GeneratedField::Open),
                            "read" => Ok(GeneratedField::Read),
                            "flush" => Ok(GeneratedField::Flush),
                            "startCommit" | "start_commit" => Ok(GeneratedField::StartCommit),
                            "reset" => Ok(GeneratedField::Reset),
                            "$internal" | "internal" => Ok(GeneratedField::Internal),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Request;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Request, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut validate__ = None;
                let mut open__ = None;
                let mut read__ = None;
                let mut flush__ = None;
                let mut start_commit__ = None;
                let mut reset__ = None;
                let mut internal__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::Validate => {
                            if validate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validate"));
                            }
                            validate__ = map_.next_value()?;
                        }
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Read => {
                            if read__.is_some() {
                                return Err(serde::de::Error::duplicate_field("read"));
                            }
                            read__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                        GeneratedField::StartCommit => {
                            if start_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startCommit"));
                            }
                            start_commit__ = map_.next_value()?;
                        }
                        GeneratedField::Reset => {
                            if reset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reset"));
                            }
                            reset__ = map_.next_value()?;
                        }
                        GeneratedField::Internal => {
                            if internal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("$internal"));
                            }
                            internal__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Request {
                    spec: spec__,
                    validate: validate__,
                    open: open__,
                    read: read__,
                    flush: flush__,
                    start_commit: start_commit__,
                    reset: reset__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("derive.Request.Flush", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Flush {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(request::Flush {
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.collection.is_some() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if !self.state_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Open", len)?;
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.state_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("state", &crate::as_raw_json(&self.state_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "collection",
            "version",
            "range",
            "state_json",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Collection,
            Version,
            Range,
            StateJson,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "collection" => Ok(GeneratedField::Collection),
                            "version" => Ok(GeneratedField::Version),
                            "range" => Ok(GeneratedField::Range),
                            "state" | "state_json" => Ok(GeneratedField::StateJson),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut collection__ = None;
                let mut version__ = None;
                let mut range__ = None;
                let mut state_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map_.next_value()?;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map_.next_value()?;
                        }
                        GeneratedField::StateJson => {
                            if state_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::Open {
                    collection: collection__,
                    version: version__.unwrap_or_default(),
                    range: range__,
                    state_json: state_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Read {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.transform != 0 {
            len += 1;
        }
        if self.uuid.is_some() {
            len += 1;
        }
        if self.shuffle.is_some() {
            len += 1;
        }
        if !self.doc_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Read", len)?;
        if self.transform != 0 {
            struct_ser.serialize_field("transform", &self.transform)?;
        }
        if let Some(v) = self.uuid.as_ref() {
            struct_ser.serialize_field("uuid", v)?;
        }
        if let Some(v) = self.shuffle.as_ref() {
            struct_ser.serialize_field("shuffle", v)?;
        }
        if !self.doc_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("doc", &crate::as_raw_json(&self.doc_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Read {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "transform",
            "uuid",
            "shuffle",
            "doc_json",
            "doc",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Transform,
            Uuid,
            Shuffle,
            DocJson,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "transform" => Ok(GeneratedField::Transform),
                            "uuid" => Ok(GeneratedField::Uuid),
                            "shuffle" => Ok(GeneratedField::Shuffle),
                            "doc" | "doc_json" => Ok(GeneratedField::DocJson),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Read;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Read")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Read, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut transform__ = None;
                let mut uuid__ = None;
                let mut shuffle__ = None;
                let mut doc_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Transform => {
                            if transform__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transform"));
                            }
                            transform__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Uuid => {
                            if uuid__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuid"));
                            }
                            uuid__ = map_.next_value()?;
                        }
                        GeneratedField::Shuffle => {
                            if shuffle__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffle"));
                            }
                            shuffle__ = map_.next_value()?;
                        }
                        GeneratedField::DocJson => {
                            if doc_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doc"));
                            }
                            doc_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::Read {
                    transform: transform__.unwrap_or_default(),
                    uuid: uuid__,
                    shuffle: shuffle__,
                    doc_json: doc_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Read", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::read::Shuffle {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.key_json.is_empty() {
            len += 1;
        }
        if !self.packed.is_empty() {
            len += 1;
        }
        if self.hash != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Read.Shuffle", len)?;
        if !self.key_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("key", &crate::as_raw_json(&self.key_json)?)?;
        }
        if !self.packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("packed", pbjson::private::base64::encode(&self.packed).as_str())?;
        }
        if self.hash != 0 {
            struct_ser.serialize_field("hash", &self.hash)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::read::Shuffle {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key_json",
            "key",
            "packed",
            "hash",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            KeyJson,
            Packed,
            Hash,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "key" | "key_json" => Ok(GeneratedField::KeyJson),
                            "packed" => Ok(GeneratedField::Packed),
                            "hash" => Ok(GeneratedField::Hash),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::read::Shuffle;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Read.Shuffle")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::read::Shuffle, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key_json__ = None;
                let mut packed__ = None;
                let mut hash__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::KeyJson => {
                            if key_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::Packed => {
                            if packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packed"));
                            }
                            packed__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Hash => {
                            if hash__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hash"));
                            }
                            hash__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::read::Shuffle {
                    key_json: key_json__.unwrap_or_default(),
                    packed: packed__.unwrap_or_default(),
                    hash: hash__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Read.Shuffle", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Reset {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("derive.Request.Reset", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Reset {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Reset;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Reset")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Reset, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(request::Reset {
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Reset", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Spec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.connector_type != 0 {
            len += 1;
        }
        if !self.config_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Spec", len)?;
        if self.connector_type != 0 {
            let v = super::flow::collection_spec::derivation::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("config", &crate::as_raw_json(&self.config_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Spec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_type",
            "connectorType",
            "config_json",
            "config",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorType,
            ConfigJson,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "connectorType" | "connector_type" => Ok(GeneratedField::ConnectorType),
                            "config" | "config_json" => Ok(GeneratedField::ConfigJson),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Spec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Spec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Spec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_type__ = None;
                let mut config_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map_.next_value::<super::flow::collection_spec::derivation::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::Spec {
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Spec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::StartCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.runtime_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.StartCommit", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::StartCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "runtime_checkpoint",
            "runtimeCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RuntimeCheckpoint,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "runtimeCheckpoint" | "runtime_checkpoint" => Ok(GeneratedField::RuntimeCheckpoint),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::StartCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.StartCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::StartCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut runtime_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RuntimeCheckpoint => {
                            if runtime_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runtimeCheckpoint"));
                            }
                            runtime_checkpoint__ = map_.next_value()?;
                        }
                    }
                }
                Ok(request::StartCommit {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.StartCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Validate {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.connector_type != 0 {
            len += 1;
        }
        if !self.config_json.is_empty() {
            len += 1;
        }
        if self.collection.is_some() {
            len += 1;
        }
        if !self.transforms.is_empty() {
            len += 1;
        }
        if !self.shuffle_key_types.is_empty() {
            len += 1;
        }
        if !self.project_root.is_empty() {
            len += 1;
        }
        if !self.import_map.is_empty() {
            len += 1;
        }
        if self.last_collection.is_some() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Validate", len)?;
        if self.connector_type != 0 {
            let v = super::flow::collection_spec::derivation::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("config", &crate::as_raw_json(&self.config_json)?)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if !self.transforms.is_empty() {
            struct_ser.serialize_field("transforms", &self.transforms)?;
        }
        if !self.shuffle_key_types.is_empty() {
            let v = self.shuffle_key_types.iter().cloned().map(|v| {
                super::flow::collection_spec::derivation::ShuffleType::try_from(v)
                    .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", v)))
                }).collect::<std::result::Result<Vec<_>, _>>()?;
            struct_ser.serialize_field("shuffleKeyTypes", &v)?;
        }
        if !self.project_root.is_empty() {
            struct_ser.serialize_field("projectRoot", &self.project_root)?;
        }
        if !self.import_map.is_empty() {
            struct_ser.serialize_field("importMap", &self.import_map)?;
        }
        if let Some(v) = self.last_collection.as_ref() {
            struct_ser.serialize_field("lastCollection", v)?;
        }
        if !self.last_version.is_empty() {
            struct_ser.serialize_field("lastVersion", &self.last_version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Validate {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_type",
            "connectorType",
            "config_json",
            "config",
            "collection",
            "transforms",
            "shuffle_key_types",
            "shuffleKeyTypes",
            "project_root",
            "projectRoot",
            "import_map",
            "importMap",
            "last_collection",
            "lastCollection",
            "last_version",
            "lastVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorType,
            ConfigJson,
            Collection,
            Transforms,
            ShuffleKeyTypes,
            ProjectRoot,
            ImportMap,
            LastCollection,
            LastVersion,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "connectorType" | "connector_type" => Ok(GeneratedField::ConnectorType),
                            "config" | "config_json" => Ok(GeneratedField::ConfigJson),
                            "collection" => Ok(GeneratedField::Collection),
                            "transforms" => Ok(GeneratedField::Transforms),
                            "shuffleKeyTypes" | "shuffle_key_types" => Ok(GeneratedField::ShuffleKeyTypes),
                            "projectRoot" | "project_root" => Ok(GeneratedField::ProjectRoot),
                            "importMap" | "import_map" => Ok(GeneratedField::ImportMap),
                            "lastCollection" | "last_collection" => Ok(GeneratedField::LastCollection),
                            "lastVersion" | "last_version" => Ok(GeneratedField::LastVersion),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Validate;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Validate")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Validate, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_type__ = None;
                let mut config_json__ = None;
                let mut collection__ = None;
                let mut transforms__ = None;
                let mut shuffle_key_types__ = None;
                let mut project_root__ = None;
                let mut import_map__ = None;
                let mut last_collection__ = None;
                let mut last_version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map_.next_value::<super::flow::collection_spec::derivation::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map_.next_value()?;
                        }
                        GeneratedField::Transforms => {
                            if transforms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transforms"));
                            }
                            transforms__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ShuffleKeyTypes => {
                            if shuffle_key_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleKeyTypes"));
                            }
                            shuffle_key_types__ = Some(map_.next_value::<Vec<super::flow::collection_spec::derivation::ShuffleType>>()?.into_iter().map(|x| x as i32).collect());
                        }
                        GeneratedField::ProjectRoot => {
                            if project_root__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projectRoot"));
                            }
                            project_root__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ImportMap => {
                            if import_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("importMap"));
                            }
                            import_map__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::LastCollection => {
                            if last_collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastCollection"));
                            }
                            last_collection__ = map_.next_value()?;
                        }
                        GeneratedField::LastVersion => {
                            if last_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastVersion"));
                            }
                            last_version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Validate {
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.unwrap_or_default(),
                    collection: collection__,
                    transforms: transforms__.unwrap_or_default(),
                    shuffle_key_types: shuffle_key_types__.unwrap_or_default(),
                    project_root: project_root__.unwrap_or_default(),
                    import_map: import_map__.unwrap_or_default(),
                    last_collection: last_collection__,
                    last_version: last_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Validate", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::validate::Transform {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.collection.is_some() {
            len += 1;
        }
        if !self.shuffle_lambda_config_json.is_empty() {
            len += 1;
        }
        if !self.lambda_config_json.is_empty() {
            len += 1;
        }
        if self.backfill != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Request.Validate.Transform", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if !self.shuffle_lambda_config_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("shuffleLambdaConfig", &crate::as_raw_json(&self.shuffle_lambda_config_json)?)?;
        }
        if !self.lambda_config_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lambdaConfig", &crate::as_raw_json(&self.lambda_config_json)?)?;
        }
        if self.backfill != 0 {
            struct_ser.serialize_field("backfill", &self.backfill)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::validate::Transform {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "collection",
            "shuffle_lambda_config_json",
            "shuffleLambdaConfig",
            "lambda_config_json",
            "lambdaConfig",
            "backfill",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Collection,
            ShuffleLambdaConfigJson,
            LambdaConfigJson,
            Backfill,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "collection" => Ok(GeneratedField::Collection),
                            "shuffleLambdaConfig" | "shuffle_lambda_config_json" => Ok(GeneratedField::ShuffleLambdaConfigJson),
                            "lambdaConfig" | "lambda_config_json" => Ok(GeneratedField::LambdaConfigJson),
                            "backfill" => Ok(GeneratedField::Backfill),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::validate::Transform;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Request.Validate.Transform")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::validate::Transform, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut collection__ = None;
                let mut shuffle_lambda_config_json__ = None;
                let mut lambda_config_json__ = None;
                let mut backfill__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map_.next_value()?;
                        }
                        GeneratedField::ShuffleLambdaConfigJson => {
                            if shuffle_lambda_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleLambdaConfig"));
                            }
                            shuffle_lambda_config_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::LambdaConfigJson => {
                            if lambda_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lambdaConfig"));
                            }
                            lambda_config_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::Backfill => {
                            if backfill__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backfill"));
                            }
                            backfill__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::validate::Transform {
                    name: name__.unwrap_or_default(),
                    collection: collection__,
                    shuffle_lambda_config_json: shuffle_lambda_config_json__.unwrap_or_default(),
                    lambda_config_json: lambda_config_json__.unwrap_or_default(),
                    backfill: backfill__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Request.Validate.Transform", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Response {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.spec.is_some() {
            len += 1;
        }
        if self.validated.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.published.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        if self.started_commit.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.validated.as_ref() {
            struct_ser.serialize_field("validated", v)?;
        }
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.published.as_ref() {
            struct_ser.serialize_field("published", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        if let Some(v) = self.started_commit.as_ref() {
            struct_ser.serialize_field("startedCommit", v)?;
        }
        if !self.internal.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("$internal", pbjson::private::base64::encode(&self.internal).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Response {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "validated",
            "opened",
            "published",
            "flushed",
            "started_commit",
            "startedCommit",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Validated,
            Opened,
            Published,
            Flushed,
            StartedCommit,
            Internal,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "spec" => Ok(GeneratedField::Spec),
                            "validated" => Ok(GeneratedField::Validated),
                            "opened" => Ok(GeneratedField::Opened),
                            "published" => Ok(GeneratedField::Published),
                            "flushed" => Ok(GeneratedField::Flushed),
                            "startedCommit" | "started_commit" => Ok(GeneratedField::StartedCommit),
                            "$internal" | "internal" => Ok(GeneratedField::Internal),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Response;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Response, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut validated__ = None;
                let mut opened__ = None;
                let mut published__ = None;
                let mut flushed__ = None;
                let mut started_commit__ = None;
                let mut internal__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::Validated => {
                            if validated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validated"));
                            }
                            validated__ = map_.next_value()?;
                        }
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::Published => {
                            if published__.is_some() {
                                return Err(serde::de::Error::duplicate_field("published"));
                            }
                            published__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                        GeneratedField::StartedCommit => {
                            if started_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startedCommit"));
                            }
                            started_commit__ = map_.next_value()?;
                        }
                        GeneratedField::Internal => {
                            if internal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("$internal"));
                            }
                            internal__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Response {
                    spec: spec__,
                    validated: validated__,
                    opened: opened__,
                    published: published__,
                    flushed: flushed__,
                    started_commit: started_commit__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Response", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("derive.Response.Flushed", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(response::Flushed {
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("derive.Response.Opened", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Opened {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(response::Opened {
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Published {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.doc_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response.Published", len)?;
        if !self.doc_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("doc", &crate::as_raw_json(&self.doc_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Published {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "doc_json",
            "doc",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DocJson,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "doc" | "doc_json" => Ok(GeneratedField::DocJson),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Published;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Published")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Published, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut doc_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::DocJson => {
                            if doc_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doc"));
                            }
                            doc_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                    }
                }
                Ok(response::Published {
                    doc_json: doc_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Published", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Spec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.protocol != 0 {
            len += 1;
        }
        if !self.config_schema_json.is_empty() {
            len += 1;
        }
        if !self.resource_config_schema_json.is_empty() {
            len += 1;
        }
        if !self.documentation_url.is_empty() {
            len += 1;
        }
        if self.oauth2.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response.Spec", len)?;
        if self.protocol != 0 {
            struct_ser.serialize_field("protocol", &self.protocol)?;
        }
        if !self.config_schema_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("configSchema", &crate::as_raw_json(&self.config_schema_json)?)?;
        }
        if !self.resource_config_schema_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("resourceConfigSchema", &crate::as_raw_json(&self.resource_config_schema_json)?)?;
        }
        if !self.documentation_url.is_empty() {
            struct_ser.serialize_field("documentationUrl", &self.documentation_url)?;
        }
        if let Some(v) = self.oauth2.as_ref() {
            struct_ser.serialize_field("oauth2", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Spec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "protocol",
            "config_schema_json",
            "configSchema",
            "resource_config_schema_json",
            "resourceConfigSchema",
            "documentation_url",
            "documentationUrl",
            "oauth2",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Protocol,
            ConfigSchemaJson,
            ResourceConfigSchemaJson,
            DocumentationUrl,
            Oauth2,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "protocol" => Ok(GeneratedField::Protocol),
                            "configSchema" | "config_schema_json" => Ok(GeneratedField::ConfigSchemaJson),
                            "resourceConfigSchema" | "resource_config_schema_json" => Ok(GeneratedField::ResourceConfigSchemaJson),
                            "documentationUrl" | "documentation_url" => Ok(GeneratedField::DocumentationUrl),
                            "oauth2" => Ok(GeneratedField::Oauth2),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Spec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Spec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Spec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut protocol__ = None;
                let mut config_schema_json__ = None;
                let mut resource_config_schema_json__ = None;
                let mut documentation_url__ = None;
                let mut oauth2__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Protocol => {
                            if protocol__.is_some() {
                                return Err(serde::de::Error::duplicate_field("protocol"));
                            }
                            protocol__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ConfigSchemaJson => {
                            if config_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("configSchema"));
                            }
                            config_schema_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::ResourceConfigSchemaJson => {
                            if resource_config_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfigSchema"));
                            }
                            resource_config_schema_json__ = 
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::DocumentationUrl => {
                            if documentation_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("documentationUrl"));
                            }
                            documentation_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Oauth2 => {
                            if oauth2__.is_some() {
                                return Err(serde::de::Error::duplicate_field("oauth2"));
                            }
                            oauth2__ = map_.next_value()?;
                        }
                    }
                }
                Ok(response::Spec {
                    protocol: protocol__.unwrap_or_default(),
                    config_schema_json: config_schema_json__.unwrap_or_default(),
                    resource_config_schema_json: resource_config_schema_json__.unwrap_or_default(),
                    documentation_url: documentation_url__.unwrap_or_default(),
                    oauth2: oauth2__,
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Spec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::StartedCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.state.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response.StartedCommit", len)?;
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::StartedCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            State,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "state" => Ok(GeneratedField::State),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::StartedCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.StartedCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::StartedCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut state__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::State => {
                            if state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state__ = map_.next_value()?;
                        }
                    }
                }
                Ok(response::StartedCommit {
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.StartedCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Validated {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.transforms.is_empty() {
            len += 1;
        }
        if !self.generated_files.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response.Validated", len)?;
        if !self.transforms.is_empty() {
            struct_ser.serialize_field("transforms", &self.transforms)?;
        }
        if !self.generated_files.is_empty() {
            struct_ser.serialize_field("generatedFiles", &self.generated_files)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Validated {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "transforms",
            "generated_files",
            "generatedFiles",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Transforms,
            GeneratedFiles,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "transforms" => Ok(GeneratedField::Transforms),
                            "generatedFiles" | "generated_files" => Ok(GeneratedField::GeneratedFiles),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Validated;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Validated")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Validated, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut transforms__ = None;
                let mut generated_files__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Transforms => {
                            if transforms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transforms"));
                            }
                            transforms__ = Some(map_.next_value()?);
                        }
                        GeneratedField::GeneratedFiles => {
                            if generated_files__.is_some() {
                                return Err(serde::de::Error::duplicate_field("generatedFiles"));
                            }
                            generated_files__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(response::Validated {
                    transforms: transforms__.unwrap_or_default(),
                    generated_files: generated_files__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Validated", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::validated::Transform {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.read_only {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("derive.Response.Validated.Transform", len)?;
        if self.read_only {
            struct_ser.serialize_field("readOnly", &self.read_only)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::validated::Transform {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "read_only",
            "readOnly",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ReadOnly,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "readOnly" | "read_only" => Ok(GeneratedField::ReadOnly),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::validated::Transform;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct derive.Response.Validated.Transform")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::validated::Transform, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut read_only__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ReadOnly => {
                            if read_only__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readOnly"));
                            }
                            read_only__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::validated::Transform {
                    read_only: read_only__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("derive.Response.Validated.Transform", FIELDS, GeneratedVisitor)
    }
}
