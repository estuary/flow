impl serde::Serialize for ApplyRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.changes.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ApplyRequest", len)?;
        if !self.changes.is_empty() {
            struct_ser.serialize_field("changes", &self.changes)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ApplyRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "changes",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Changes,
            Extension,
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
                            "changes" => Ok(GeneratedField::Changes),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ApplyRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ApplyRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ApplyRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut changes__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Changes => {
                            if changes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("changes"));
                            }
                            changes__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ApplyRequest {
                    changes: changes__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ApplyRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for apply_request::Change {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expect_mod_revision != 0 {
            len += 1;
        }
        if self.upsert.is_some() {
            len += 1;
        }
        if !self.delete.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ApplyRequest.Change", len)?;
        if self.expect_mod_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("expectModRevision", ToString::to_string(&self.expect_mod_revision).as_str())?;
        }
        if let Some(v) = self.upsert.as_ref() {
            struct_ser.serialize_field("upsert", v)?;
        }
        if !self.delete.is_empty() {
            struct_ser.serialize_field("delete", &self.delete)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for apply_request::Change {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expect_mod_revision",
            "expectModRevision",
            "upsert",
            "delete",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExpectModRevision,
            Upsert,
            Delete,
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
                            "expectModRevision" | "expect_mod_revision" => Ok(GeneratedField::ExpectModRevision),
                            "upsert" => Ok(GeneratedField::Upsert),
                            "delete" => Ok(GeneratedField::Delete),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = apply_request::Change;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ApplyRequest.Change")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<apply_request::Change, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expect_mod_revision__ = None;
                let mut upsert__ = None;
                let mut delete__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ExpectModRevision => {
                            if expect_mod_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expectModRevision"));
                            }
                            expect_mod_revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Upsert => {
                            if upsert__.is_some() {
                                return Err(serde::de::Error::duplicate_field("upsert"));
                            }
                            upsert__ = map_.next_value()?;
                        }
                        GeneratedField::Delete => {
                            if delete__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delete"));
                            }
                            delete__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(apply_request::Change {
                    expect_mod_revision: expect_mod_revision__.unwrap_or_default(),
                    upsert: upsert__,
                    delete: delete__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ApplyRequest.Change", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ApplyResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if self.header.is_some() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ApplyResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ApplyResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Extension,
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
                            "status" => Ok(GeneratedField::Status),
                            "header" => Ok(GeneratedField::Header),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ApplyResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ApplyResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ApplyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ApplyResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ApplyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Checkpoint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sources.is_empty() {
            len += 1;
        }
        if !self.ack_intents.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.Checkpoint", len)?;
        if !self.sources.is_empty() {
            struct_ser.serialize_field("sources", &self.sources)?;
        }
        if !self.ack_intents.is_empty() {
            let v: std::collections::HashMap<_, _> = self.ack_intents.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("ackIntents", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Checkpoint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sources",
            "ack_intents",
            "ackIntents",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Sources,
            AckIntents,
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
                            "sources" => Ok(GeneratedField::Sources),
                            "ackIntents" | "ack_intents" => Ok(GeneratedField::AckIntents),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Checkpoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.Checkpoint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Checkpoint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sources__ = None;
                let mut ack_intents__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Sources => {
                            if sources__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sources"));
                            }
                            sources__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::AckIntents => {
                            if ack_intents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ackIntents"));
                            }
                            ack_intents__ = Some(
                                map_.next_value::<std::collections::HashMap<_, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                    }
                }
                Ok(Checkpoint {
                    sources: sources__.unwrap_or_default(),
                    ack_intents: ack_intents__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.Checkpoint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for checkpoint::ProducerState {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.last_ack != 0 {
            len += 1;
        }
        if self.begin != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.Checkpoint.ProducerState", len)?;
        if self.last_ack != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastAck", ToString::to_string(&self.last_ack).as_str())?;
        }
        if self.begin != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("begin", ToString::to_string(&self.begin).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for checkpoint::ProducerState {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "last_ack",
            "lastAck",
            "begin",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LastAck,
            Begin,
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
                            "lastAck" | "last_ack" => Ok(GeneratedField::LastAck),
                            "begin" => Ok(GeneratedField::Begin),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = checkpoint::ProducerState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.Checkpoint.ProducerState")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<checkpoint::ProducerState, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut last_ack__ = None;
                let mut begin__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LastAck => {
                            if last_ack__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastAck"));
                            }
                            last_ack__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Begin => {
                            if begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("begin"));
                            }
                            begin__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(checkpoint::ProducerState {
                    last_ack: last_ack__.unwrap_or_default(),
                    begin: begin__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.Checkpoint.ProducerState", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for checkpoint::Source {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.read_through != 0 {
            len += 1;
        }
        if !self.producers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.Checkpoint.Source", len)?;
        if self.read_through != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("readThrough", ToString::to_string(&self.read_through).as_str())?;
        }
        if !self.producers.is_empty() {
            struct_ser.serialize_field("producers", &self.producers)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for checkpoint::Source {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "read_through",
            "readThrough",
            "producers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ReadThrough,
            Producers,
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
                            "readThrough" | "read_through" => Ok(GeneratedField::ReadThrough),
                            "producers" => Ok(GeneratedField::Producers),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = checkpoint::Source;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.Checkpoint.Source")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<checkpoint::Source, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut read_through__ = None;
                let mut producers__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ReadThrough => {
                            if read_through__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readThrough"));
                            }
                            read_through__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Producers => {
                            if producers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("producers"));
                            }
                            producers__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(checkpoint::Source {
                    read_through: read_through__.unwrap_or_default(),
                    producers: producers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.Checkpoint.Source", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for checkpoint::source::ProducerEntry {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.state.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.Checkpoint.Source.ProducerEntry", len)?;
        if !self.id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("id", pbjson::private::base64::encode(&self.id).as_str())?;
        }
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for checkpoint::source::ProducerEntry {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
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
            type Value = checkpoint::source::ProducerEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.Checkpoint.Source.ProducerEntry")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<checkpoint::source::ProducerEntry, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut state__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::State => {
                            if state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state__ = map_.next_value()?;
                        }
                    }
                }
                Ok(checkpoint::source::ProducerEntry {
                    id: id__.unwrap_or_default(),
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("consumer.Checkpoint.Source.ProducerEntry", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConsumerSpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.process_spec.is_some() {
            len += 1;
        }
        if self.shard_limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ConsumerSpec", len)?;
        if let Some(v) = self.process_spec.as_ref() {
            struct_ser.serialize_field("processSpec", v)?;
        }
        if self.shard_limit != 0 {
            struct_ser.serialize_field("shardLimit", &self.shard_limit)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConsumerSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "process_spec",
            "processSpec",
            "shard_limit",
            "shardLimit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProcessSpec,
            ShardLimit,
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
                            "processSpec" | "process_spec" => Ok(GeneratedField::ProcessSpec),
                            "shardLimit" | "shard_limit" => Ok(GeneratedField::ShardLimit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConsumerSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ConsumerSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConsumerSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut process_spec__ = None;
                let mut shard_limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ProcessSpec => {
                            if process_spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("processSpec"));
                            }
                            process_spec__ = map_.next_value()?;
                        }
                        GeneratedField::ShardLimit => {
                            if shard_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardLimit"));
                            }
                            shard_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ConsumerSpec {
                    process_spec: process_spec__,
                    shard_limit: shard_limit__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ConsumerSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetHintsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.shard.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.GetHintsRequest", len)?;
        if !self.shard.is_empty() {
            struct_ser.serialize_field("shard", &self.shard)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetHintsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "shard",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Shard,
            Extension,
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
                            "shard" => Ok(GeneratedField::Shard),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetHintsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.GetHintsRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetHintsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut shard__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Shard => {
                            if shard__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shard"));
                            }
                            shard__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(GetHintsRequest {
                    shard: shard__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.GetHintsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetHintsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if self.header.is_some() {
            len += 1;
        }
        if self.primary_hints.is_some() {
            len += 1;
        }
        if !self.backup_hints.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.GetHintsResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if let Some(v) = self.primary_hints.as_ref() {
            struct_ser.serialize_field("primaryHints", v)?;
        }
        if !self.backup_hints.is_empty() {
            struct_ser.serialize_field("backupHints", &self.backup_hints)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetHintsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "primary_hints",
            "primaryHints",
            "backup_hints",
            "backupHints",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            PrimaryHints,
            BackupHints,
            Extension,
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
                            "status" => Ok(GeneratedField::Status),
                            "header" => Ok(GeneratedField::Header),
                            "primaryHints" | "primary_hints" => Ok(GeneratedField::PrimaryHints),
                            "backupHints" | "backup_hints" => Ok(GeneratedField::BackupHints),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetHintsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.GetHintsResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetHintsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut primary_hints__ = None;
                let mut backup_hints__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::PrimaryHints => {
                            if primary_hints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("primaryHints"));
                            }
                            primary_hints__ = map_.next_value()?;
                        }
                        GeneratedField::BackupHints => {
                            if backup_hints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backupHints"));
                            }
                            backup_hints__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(GetHintsResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    primary_hints: primary_hints__,
                    backup_hints: backup_hints__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.GetHintsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for get_hints_response::ResponseHints {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.hints.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.GetHintsResponse.ResponseHints", len)?;
        if let Some(v) = self.hints.as_ref() {
            struct_ser.serialize_field("hints", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for get_hints_response::ResponseHints {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hints",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Hints,
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
                            "hints" => Ok(GeneratedField::Hints),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = get_hints_response::ResponseHints;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.GetHintsResponse.ResponseHints")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<get_hints_response::ResponseHints, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hints__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Hints => {
                            if hints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hints"));
                            }
                            hints__ = map_.next_value()?;
                        }
                    }
                }
                Ok(get_hints_response::ResponseHints {
                    hints: hints__,
                })
            }
        }
        deserializer.deserialize_struct("consumer.GetHintsResponse.ResponseHints", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.selector.is_some() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ListRequest", len)?;
        if let Some(v) = self.selector.as_ref() {
            struct_ser.serialize_field("selector", v)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "selector",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Selector,
            Extension,
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
                            "selector" => Ok(GeneratedField::Selector),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ListRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut selector__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Selector => {
                            if selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selector"));
                            }
                            selector__ = map_.next_value()?;
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ListRequest {
                    selector: selector__,
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ListRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if self.header.is_some() {
            len += 1;
        }
        if !self.shards.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ListResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.shards.is_empty() {
            struct_ser.serialize_field("shards", &self.shards)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "shards",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Shards,
            Extension,
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
                            "status" => Ok(GeneratedField::Status),
                            "header" => Ok(GeneratedField::Header),
                            "shards" => Ok(GeneratedField::Shards),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ListResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut shards__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Shards => {
                            if shards__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shards"));
                            }
                            shards__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ListResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    shards: shards__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ListResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for list_response::Shard {
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
        if self.mod_revision != 0 {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        if !self.status.is_empty() {
            len += 1;
        }
        if self.create_revision != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ListResponse.Shard", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if self.mod_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("modRevision", ToString::to_string(&self.mod_revision).as_str())?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        if !self.status.is_empty() {
            struct_ser.serialize_field("status", &self.status)?;
        }
        if self.create_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("createRevision", ToString::to_string(&self.create_revision).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for list_response::Shard {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "mod_revision",
            "modRevision",
            "route",
            "status",
            "create_revision",
            "createRevision",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            ModRevision,
            Route,
            Status,
            CreateRevision,
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
                            "modRevision" | "mod_revision" => Ok(GeneratedField::ModRevision),
                            "route" => Ok(GeneratedField::Route),
                            "status" => Ok(GeneratedField::Status),
                            "createRevision" | "create_revision" => Ok(GeneratedField::CreateRevision),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = list_response::Shard;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ListResponse.Shard")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<list_response::Shard, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut mod_revision__ = None;
                let mut route__ = None;
                let mut status__ = None;
                let mut create_revision__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::ModRevision => {
                            if mod_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modRevision"));
                            }
                            mod_revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Route => {
                            if route__.is_some() {
                                return Err(serde::de::Error::duplicate_field("route"));
                            }
                            route__ = map_.next_value()?;
                        }
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CreateRevision => {
                            if create_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createRevision"));
                            }
                            create_revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(list_response::Shard {
                    spec: spec__,
                    mod_revision: mod_revision__.unwrap_or_default(),
                    route: route__,
                    status: status__.unwrap_or_default(),
                    create_revision: create_revision__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ListResponse.Shard", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReplicaStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.code != 0 {
            len += 1;
        }
        if !self.errors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ReplicaStatus", len)?;
        if self.code != 0 {
            let v = replica_status::Code::try_from(self.code)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.code)))?;
            struct_ser.serialize_field("code", &v)?;
        }
        if !self.errors.is_empty() {
            struct_ser.serialize_field("errors", &self.errors)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReplicaStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "code",
            "errors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Code,
            Errors,
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
                            "code" => Ok(GeneratedField::Code),
                            "errors" => Ok(GeneratedField::Errors),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReplicaStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ReplicaStatus")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReplicaStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut code__ = None;
                let mut errors__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Code => {
                            if code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("code"));
                            }
                            code__ = Some(map_.next_value::<replica_status::Code>()? as i32);
                        }
                        GeneratedField::Errors => {
                            if errors__.is_some() {
                                return Err(serde::de::Error::duplicate_field("errors"));
                            }
                            errors__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ReplicaStatus {
                    code: code__.unwrap_or_default(),
                    errors: errors__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ReplicaStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for replica_status::Code {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Idle => "IDLE",
            Self::Backfill => "BACKFILL",
            Self::Standby => "STANDBY",
            Self::Primary => "PRIMARY",
            Self::Failed => "FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for replica_status::Code {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "IDLE",
            "BACKFILL",
            "STANDBY",
            "PRIMARY",
            "FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = replica_status::Code;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "IDLE" => Ok(replica_status::Code::Idle),
                    "BACKFILL" => Ok(replica_status::Code::Backfill),
                    "STANDBY" => Ok(replica_status::Code::Standby),
                    "PRIMARY" => Ok(replica_status::Code::Primary),
                    "FAILED" => Ok(replica_status::Code::Failed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ShardSpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.sources.is_empty() {
            len += 1;
        }
        if !self.recovery_log_prefix.is_empty() {
            len += 1;
        }
        if !self.hint_prefix.is_empty() {
            len += 1;
        }
        if self.hint_backups != 0 {
            len += 1;
        }
        if self.max_txn_duration.is_some() {
            len += 1;
        }
        if self.min_txn_duration.is_some() {
            len += 1;
        }
        if self.disable {
            len += 1;
        }
        if self.hot_standbys != 0 {
            len += 1;
        }
        if self.labels.is_some() {
            len += 1;
        }
        if self.disable_wait_for_ack {
            len += 1;
        }
        if self.ring_buffer_size != 0 {
            len += 1;
        }
        if self.read_channel_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ShardSpec", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if !self.sources.is_empty() {
            struct_ser.serialize_field("sources", &self.sources)?;
        }
        if !self.recovery_log_prefix.is_empty() {
            struct_ser.serialize_field("recoveryLogPrefix", &self.recovery_log_prefix)?;
        }
        if !self.hint_prefix.is_empty() {
            struct_ser.serialize_field("hintPrefix", &self.hint_prefix)?;
        }
        if self.hint_backups != 0 {
            struct_ser.serialize_field("hintBackups", &self.hint_backups)?;
        }
        if let Some(v) = self.max_txn_duration.as_ref() {
            struct_ser.serialize_field("maxTxnDuration", v)?;
        }
        if let Some(v) = self.min_txn_duration.as_ref() {
            struct_ser.serialize_field("minTxnDuration", v)?;
        }
        if self.disable {
            struct_ser.serialize_field("disable", &self.disable)?;
        }
        if self.hot_standbys != 0 {
            struct_ser.serialize_field("hotStandbys", &self.hot_standbys)?;
        }
        if let Some(v) = self.labels.as_ref() {
            struct_ser.serialize_field("labels", v)?;
        }
        if self.disable_wait_for_ack {
            struct_ser.serialize_field("disableWaitForAck", &self.disable_wait_for_ack)?;
        }
        if self.ring_buffer_size != 0 {
            struct_ser.serialize_field("ringBufferSize", &self.ring_buffer_size)?;
        }
        if self.read_channel_size != 0 {
            struct_ser.serialize_field("readChannelSize", &self.read_channel_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShardSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "sources",
            "recovery_log_prefix",
            "recoveryLogPrefix",
            "hint_prefix",
            "hintPrefix",
            "hint_backups",
            "hintBackups",
            "max_txn_duration",
            "maxTxnDuration",
            "min_txn_duration",
            "minTxnDuration",
            "disable",
            "hot_standbys",
            "hotStandbys",
            "labels",
            "disable_wait_for_ack",
            "disableWaitForAck",
            "ring_buffer_size",
            "ringBufferSize",
            "read_channel_size",
            "readChannelSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Sources,
            RecoveryLogPrefix,
            HintPrefix,
            HintBackups,
            MaxTxnDuration,
            MinTxnDuration,
            Disable,
            HotStandbys,
            Labels,
            DisableWaitForAck,
            RingBufferSize,
            ReadChannelSize,
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
                            "id" => Ok(GeneratedField::Id),
                            "sources" => Ok(GeneratedField::Sources),
                            "recoveryLogPrefix" | "recovery_log_prefix" => Ok(GeneratedField::RecoveryLogPrefix),
                            "hintPrefix" | "hint_prefix" => Ok(GeneratedField::HintPrefix),
                            "hintBackups" | "hint_backups" => Ok(GeneratedField::HintBackups),
                            "maxTxnDuration" | "max_txn_duration" => Ok(GeneratedField::MaxTxnDuration),
                            "minTxnDuration" | "min_txn_duration" => Ok(GeneratedField::MinTxnDuration),
                            "disable" => Ok(GeneratedField::Disable),
                            "hotStandbys" | "hot_standbys" => Ok(GeneratedField::HotStandbys),
                            "labels" => Ok(GeneratedField::Labels),
                            "disableWaitForAck" | "disable_wait_for_ack" => Ok(GeneratedField::DisableWaitForAck),
                            "ringBufferSize" | "ring_buffer_size" => Ok(GeneratedField::RingBufferSize),
                            "readChannelSize" | "read_channel_size" => Ok(GeneratedField::ReadChannelSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShardSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ShardSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ShardSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut sources__ = None;
                let mut recovery_log_prefix__ = None;
                let mut hint_prefix__ = None;
                let mut hint_backups__ = None;
                let mut max_txn_duration__ = None;
                let mut min_txn_duration__ = None;
                let mut disable__ = None;
                let mut hot_standbys__ = None;
                let mut labels__ = None;
                let mut disable_wait_for_ack__ = None;
                let mut ring_buffer_size__ = None;
                let mut read_channel_size__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Sources => {
                            if sources__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sources"));
                            }
                            sources__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RecoveryLogPrefix => {
                            if recovery_log_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recoveryLogPrefix"));
                            }
                            recovery_log_prefix__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HintPrefix => {
                            if hint_prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintPrefix"));
                            }
                            hint_prefix__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HintBackups => {
                            if hint_backups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintBackups"));
                            }
                            hint_backups__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxTxnDuration => {
                            if max_txn_duration__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxTxnDuration"));
                            }
                            max_txn_duration__ = map_.next_value()?;
                        }
                        GeneratedField::MinTxnDuration => {
                            if min_txn_duration__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minTxnDuration"));
                            }
                            min_txn_duration__ = map_.next_value()?;
                        }
                        GeneratedField::Disable => {
                            if disable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("disable"));
                            }
                            disable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HotStandbys => {
                            if hot_standbys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hotStandbys"));
                            }
                            hot_standbys__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Labels => {
                            if labels__.is_some() {
                                return Err(serde::de::Error::duplicate_field("labels"));
                            }
                            labels__ = map_.next_value()?;
                        }
                        GeneratedField::DisableWaitForAck => {
                            if disable_wait_for_ack__.is_some() {
                                return Err(serde::de::Error::duplicate_field("disableWaitForAck"));
                            }
                            disable_wait_for_ack__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RingBufferSize => {
                            if ring_buffer_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ringBufferSize"));
                            }
                            ring_buffer_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ReadChannelSize => {
                            if read_channel_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readChannelSize"));
                            }
                            read_channel_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ShardSpec {
                    id: id__.unwrap_or_default(),
                    sources: sources__.unwrap_or_default(),
                    recovery_log_prefix: recovery_log_prefix__.unwrap_or_default(),
                    hint_prefix: hint_prefix__.unwrap_or_default(),
                    hint_backups: hint_backups__.unwrap_or_default(),
                    max_txn_duration: max_txn_duration__,
                    min_txn_duration: min_txn_duration__,
                    disable: disable__.unwrap_or_default(),
                    hot_standbys: hot_standbys__.unwrap_or_default(),
                    labels: labels__,
                    disable_wait_for_ack: disable_wait_for_ack__.unwrap_or_default(),
                    ring_buffer_size: ring_buffer_size__.unwrap_or_default(),
                    read_channel_size: read_channel_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ShardSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for shard_spec::Source {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.journal.is_empty() {
            len += 1;
        }
        if self.min_offset != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.ShardSpec.Source", len)?;
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.min_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("minOffset", ToString::to_string(&self.min_offset).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for shard_spec::Source {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal",
            "min_offset",
            "minOffset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Journal,
            MinOffset,
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
                            "journal" => Ok(GeneratedField::Journal),
                            "minOffset" | "min_offset" => Ok(GeneratedField::MinOffset),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = shard_spec::Source;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.ShardSpec.Source")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<shard_spec::Source, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal__ = None;
                let mut min_offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MinOffset => {
                            if min_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minOffset"));
                            }
                            min_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(shard_spec::Source {
                    journal: journal__.unwrap_or_default(),
                    min_offset: min_offset__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.ShardSpec.Source", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StatRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.header.is_some() {
            len += 1;
        }
        if !self.shard.is_empty() {
            len += 1;
        }
        if !self.read_through.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.StatRequest", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.shard.is_empty() {
            struct_ser.serialize_field("shard", &self.shard)?;
        }
        if !self.read_through.is_empty() {
            let v: std::collections::HashMap<_, _> = self.read_through.iter()
                .map(|(k, v)| (k, v.to_string())).collect();
            struct_ser.serialize_field("readThrough", &v)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StatRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "shard",
            "read_through",
            "readThrough",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            Shard,
            ReadThrough,
            Extension,
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
                            "header" => Ok(GeneratedField::Header),
                            "shard" => Ok(GeneratedField::Shard),
                            "readThrough" | "read_through" => Ok(GeneratedField::ReadThrough),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StatRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.StatRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StatRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut shard__ = None;
                let mut read_through__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Shard => {
                            if shard__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shard"));
                            }
                            shard__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReadThrough => {
                            if read_through__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readThrough"));
                            }
                            read_through__ = Some(
                                map_.next_value::<std::collections::HashMap<_, ::pbjson::private::NumberDeserialize<i64>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(StatRequest {
                    header: header__,
                    shard: shard__.unwrap_or_default(),
                    read_through: read_through__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.StatRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StatResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if self.header.is_some() {
            len += 1;
        }
        if !self.read_through.is_empty() {
            len += 1;
        }
        if !self.publish_at.is_empty() {
            len += 1;
        }
        if !self.extension.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.StatResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.read_through.is_empty() {
            let v: std::collections::HashMap<_, _> = self.read_through.iter()
                .map(|(k, v)| (k, v.to_string())).collect();
            struct_ser.serialize_field("readThrough", &v)?;
        }
        if !self.publish_at.is_empty() {
            let v: std::collections::HashMap<_, _> = self.publish_at.iter()
                .map(|(k, v)| (k, v.to_string())).collect();
            struct_ser.serialize_field("publishAt", &v)?;
        }
        if !self.extension.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("extension", pbjson::private::base64::encode(&self.extension).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StatResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "read_through",
            "readThrough",
            "publish_at",
            "publishAt",
            "extension",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            ReadThrough,
            PublishAt,
            Extension,
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
                            "status" => Ok(GeneratedField::Status),
                            "header" => Ok(GeneratedField::Header),
                            "readThrough" | "read_through" => Ok(GeneratedField::ReadThrough),
                            "publishAt" | "publish_at" => Ok(GeneratedField::PublishAt),
                            "extension" => Ok(GeneratedField::Extension),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StatResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.StatResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StatResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut read_through__ = None;
                let mut publish_at__ = None;
                let mut extension__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::ReadThrough => {
                            if read_through__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readThrough"));
                            }
                            read_through__ = Some(
                                map_.next_value::<std::collections::HashMap<_, ::pbjson::private::NumberDeserialize<i64>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::PublishAt => {
                            if publish_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publishAt"));
                            }
                            publish_at__ = Some(
                                map_.next_value::<std::collections::HashMap<_, ::pbjson::private::NumberDeserialize<i64>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::Extension => {
                            if extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            extension__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(StatResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    read_through: read_through__.unwrap_or_default(),
                    publish_at: publish_at__.unwrap_or_default(),
                    extension: extension__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.StatResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Status {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Ok => "OK",
            Self::ShardNotFound => "SHARD_NOT_FOUND",
            Self::NoShardPrimary => "NO_SHARD_PRIMARY",
            Self::NotShardPrimary => "NOT_SHARD_PRIMARY",
            Self::EtcdTransactionFailed => "ETCD_TRANSACTION_FAILED",
            Self::ShardStopped => "SHARD_STOPPED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Status {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "OK",
            "SHARD_NOT_FOUND",
            "NO_SHARD_PRIMARY",
            "NOT_SHARD_PRIMARY",
            "ETCD_TRANSACTION_FAILED",
            "SHARD_STOPPED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Status;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "OK" => Ok(Status::Ok),
                    "SHARD_NOT_FOUND" => Ok(Status::ShardNotFound),
                    "NO_SHARD_PRIMARY" => Ok(Status::NoShardPrimary),
                    "NOT_SHARD_PRIMARY" => Ok(Status::NotShardPrimary),
                    "ETCD_TRANSACTION_FAILED" => Ok(Status::EtcdTransactionFailed),
                    "SHARD_STOPPED" => Ok(Status::ShardStopped),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for UnassignRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.shards.is_empty() {
            len += 1;
        }
        if self.only_failed {
            len += 1;
        }
        if self.dry_run {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.UnassignRequest", len)?;
        if !self.shards.is_empty() {
            struct_ser.serialize_field("shards", &self.shards)?;
        }
        if self.only_failed {
            struct_ser.serialize_field("onlyFailed", &self.only_failed)?;
        }
        if self.dry_run {
            struct_ser.serialize_field("dryRun", &self.dry_run)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnassignRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "shards",
            "only_failed",
            "onlyFailed",
            "dry_run",
            "dryRun",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Shards,
            OnlyFailed,
            DryRun,
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
                            "shards" => Ok(GeneratedField::Shards),
                            "onlyFailed" | "only_failed" => Ok(GeneratedField::OnlyFailed),
                            "dryRun" | "dry_run" => Ok(GeneratedField::DryRun),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnassignRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.UnassignRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnassignRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut shards__ = None;
                let mut only_failed__ = None;
                let mut dry_run__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Shards => {
                            if shards__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shards"));
                            }
                            shards__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OnlyFailed => {
                            if only_failed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("onlyFailed"));
                            }
                            only_failed__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DryRun => {
                            if dry_run__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dryRun"));
                            }
                            dry_run__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(UnassignRequest {
                    shards: shards__.unwrap_or_default(),
                    only_failed: only_failed__.unwrap_or_default(),
                    dry_run: dry_run__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.UnassignRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnassignResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status != 0 {
            len += 1;
        }
        if !self.shards.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("consumer.UnassignResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if !self.shards.is_empty() {
            struct_ser.serialize_field("shards", &self.shards)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnassignResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "shards",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Shards,
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
                            "status" => Ok(GeneratedField::Status),
                            "shards" => Ok(GeneratedField::Shards),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnassignResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct consumer.UnassignResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnassignResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut shards__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::Shards => {
                            if shards__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shards"));
                            }
                            shards__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(UnassignResponse {
                    status: status__.unwrap_or_default(),
                    shards: shards__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("consumer.UnassignResponse", FIELDS, GeneratedVisitor)
    }
}
