impl serde::Serialize for AppendRequest {
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
        if !self.journal.is_empty() {
            len += 1;
        }
        if self.do_not_proxy {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if self.check_registers.is_some() {
            len += 1;
        }
        if self.union_registers.is_some() {
            len += 1;
        }
        if self.subtract_registers.is_some() {
            len += 1;
        }
        if self.suspend != 0 {
            len += 1;
        }
        if !self.content.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.AppendRequest", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.do_not_proxy {
            struct_ser.serialize_field("doNotProxy", &self.do_not_proxy)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if let Some(v) = self.check_registers.as_ref() {
            struct_ser.serialize_field("checkRegisters", v)?;
        }
        if let Some(v) = self.union_registers.as_ref() {
            struct_ser.serialize_field("unionRegisters", v)?;
        }
        if let Some(v) = self.subtract_registers.as_ref() {
            struct_ser.serialize_field("subtractRegisters", v)?;
        }
        if self.suspend != 0 {
            let v = append_request::Suspend::try_from(self.suspend)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.suspend)))?;
            struct_ser.serialize_field("suspend", &v)?;
        }
        if !self.content.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("content", pbjson::private::base64::encode(&self.content).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AppendRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "journal",
            "do_not_proxy",
            "doNotProxy",
            "offset",
            "check_registers",
            "checkRegisters",
            "union_registers",
            "unionRegisters",
            "subtract_registers",
            "subtractRegisters",
            "suspend",
            "content",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            Journal,
            DoNotProxy,
            Offset,
            CheckRegisters,
            UnionRegisters,
            SubtractRegisters,
            Suspend,
            Content,
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
                            "journal" => Ok(GeneratedField::Journal),
                            "doNotProxy" | "do_not_proxy" => Ok(GeneratedField::DoNotProxy),
                            "offset" => Ok(GeneratedField::Offset),
                            "checkRegisters" | "check_registers" => Ok(GeneratedField::CheckRegisters),
                            "unionRegisters" | "union_registers" => Ok(GeneratedField::UnionRegisters),
                            "subtractRegisters" | "subtract_registers" => Ok(GeneratedField::SubtractRegisters),
                            "suspend" => Ok(GeneratedField::Suspend),
                            "content" => Ok(GeneratedField::Content),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AppendRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.AppendRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AppendRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut journal__ = None;
                let mut do_not_proxy__ = None;
                let mut offset__ = None;
                let mut check_registers__ = None;
                let mut union_registers__ = None;
                let mut subtract_registers__ = None;
                let mut suspend__ = None;
                let mut content__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DoNotProxy => {
                            if do_not_proxy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doNotProxy"));
                            }
                            do_not_proxy__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::CheckRegisters => {
                            if check_registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checkRegisters"));
                            }
                            check_registers__ = map_.next_value()?;
                        }
                        GeneratedField::UnionRegisters => {
                            if union_registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionRegisters"));
                            }
                            union_registers__ = map_.next_value()?;
                        }
                        GeneratedField::SubtractRegisters => {
                            if subtract_registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subtractRegisters"));
                            }
                            subtract_registers__ = map_.next_value()?;
                        }
                        GeneratedField::Suspend => {
                            if suspend__.is_some() {
                                return Err(serde::de::Error::duplicate_field("suspend"));
                            }
                            suspend__ = Some(map_.next_value::<append_request::Suspend>()? as i32);
                        }
                        GeneratedField::Content => {
                            if content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("content"));
                            }
                            content__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(AppendRequest {
                    header: header__,
                    journal: journal__.unwrap_or_default(),
                    do_not_proxy: do_not_proxy__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                    check_registers: check_registers__,
                    union_registers: union_registers__,
                    subtract_registers: subtract_registers__,
                    suspend: suspend__.unwrap_or_default(),
                    content: content__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.AppendRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for append_request::Suspend {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Resume => "SUSPEND_RESUME",
            Self::NoResume => "SUSPEND_NO_RESUME",
            Self::IfFlushed => "SUSPEND_IF_FLUSHED",
            Self::Now => "SUSPEND_NOW",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for append_request::Suspend {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SUSPEND_RESUME",
            "SUSPEND_NO_RESUME",
            "SUSPEND_IF_FLUSHED",
            "SUSPEND_NOW",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = append_request::Suspend;

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
                    "SUSPEND_RESUME" => Ok(append_request::Suspend::Resume),
                    "SUSPEND_NO_RESUME" => Ok(append_request::Suspend::NoResume),
                    "SUSPEND_IF_FLUSHED" => Ok(append_request::Suspend::IfFlushed),
                    "SUSPEND_NOW" => Ok(append_request::Suspend::Now),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AppendResponse {
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
        if self.commit.is_some() {
            len += 1;
        }
        if self.registers.is_some() {
            len += 1;
        }
        if self.total_chunks != 0 {
            len += 1;
        }
        if self.delayed_chunks != 0 {
            len += 1;
        }
        if !self.store_health_error.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.AppendResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if let Some(v) = self.commit.as_ref() {
            struct_ser.serialize_field("commit", v)?;
        }
        if let Some(v) = self.registers.as_ref() {
            struct_ser.serialize_field("registers", v)?;
        }
        if self.total_chunks != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("totalChunks", ToString::to_string(&self.total_chunks).as_str())?;
        }
        if self.delayed_chunks != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("delayedChunks", ToString::to_string(&self.delayed_chunks).as_str())?;
        }
        if !self.store_health_error.is_empty() {
            struct_ser.serialize_field("storeHealthError", &self.store_health_error)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AppendResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "commit",
            "registers",
            "total_chunks",
            "totalChunks",
            "delayed_chunks",
            "delayedChunks",
            "store_health_error",
            "storeHealthError",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Commit,
            Registers,
            TotalChunks,
            DelayedChunks,
            StoreHealthError,
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
                            "commit" => Ok(GeneratedField::Commit),
                            "registers" => Ok(GeneratedField::Registers),
                            "totalChunks" | "total_chunks" => Ok(GeneratedField::TotalChunks),
                            "delayedChunks" | "delayed_chunks" => Ok(GeneratedField::DelayedChunks),
                            "storeHealthError" | "store_health_error" => Ok(GeneratedField::StoreHealthError),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AppendResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.AppendResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AppendResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut commit__ = None;
                let mut registers__ = None;
                let mut total_chunks__ = None;
                let mut delayed_chunks__ = None;
                let mut store_health_error__ = None;
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
                        GeneratedField::Commit => {
                            if commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("commit"));
                            }
                            commit__ = map_.next_value()?;
                        }
                        GeneratedField::Registers => {
                            if registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("registers"));
                            }
                            registers__ = map_.next_value()?;
                        }
                        GeneratedField::TotalChunks => {
                            if total_chunks__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalChunks"));
                            }
                            total_chunks__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DelayedChunks => {
                            if delayed_chunks__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delayedChunks"));
                            }
                            delayed_chunks__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::StoreHealthError => {
                            if store_health_error__.is_some() {
                                return Err(serde::de::Error::duplicate_field("storeHealthError"));
                            }
                            store_health_error__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AppendResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    commit: commit__,
                    registers: registers__,
                    total_chunks: total_chunks__.unwrap_or_default(),
                    delayed_chunks: delayed_chunks__.unwrap_or_default(),
                    store_health_error: store_health_error__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.AppendResponse", FIELDS, GeneratedVisitor)
    }
}
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
        let mut struct_ser = serializer.serialize_struct("protocol.ApplyRequest", len)?;
        if !self.changes.is_empty() {
            struct_ser.serialize_field("changes", &self.changes)?;
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
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Changes,
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
                formatter.write_str("struct protocol.ApplyRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ApplyRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut changes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Changes => {
                            if changes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("changes"));
                            }
                            changes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ApplyRequest {
                    changes: changes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ApplyRequest", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("protocol.ApplyRequest.Change", len)?;
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
                formatter.write_str("struct protocol.ApplyRequest.Change")
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
        deserializer.deserialize_struct("protocol.ApplyRequest.Change", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("protocol.ApplyResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
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
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
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
                formatter.write_str("struct protocol.ApplyResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ApplyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
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
                    }
                }
                Ok(ApplyResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.ApplyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BrokerSpec {
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
        if self.journal_limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.BrokerSpec", len)?;
        if let Some(v) = self.process_spec.as_ref() {
            struct_ser.serialize_field("processSpec", v)?;
        }
        if self.journal_limit != 0 {
            struct_ser.serialize_field("journalLimit", &self.journal_limit)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BrokerSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "process_spec",
            "processSpec",
            "journal_limit",
            "journalLimit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProcessSpec,
            JournalLimit,
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
                            "journalLimit" | "journal_limit" => Ok(GeneratedField::JournalLimit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BrokerSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.BrokerSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BrokerSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut process_spec__ = None;
                let mut journal_limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ProcessSpec => {
                            if process_spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("processSpec"));
                            }
                            process_spec__ = map_.next_value()?;
                        }
                        GeneratedField::JournalLimit => {
                            if journal_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalLimit"));
                            }
                            journal_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BrokerSpec {
                    process_spec: process_spec__,
                    journal_limit: journal_limit__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.BrokerSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompressionCodec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::None => "NONE",
            Self::Gzip => "GZIP",
            Self::Zstandard => "ZSTANDARD",
            Self::Snappy => "SNAPPY",
            Self::GzipOffloadDecompression => "GZIP_OFFLOAD_DECOMPRESSION",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for CompressionCodec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "NONE",
            "GZIP",
            "ZSTANDARD",
            "SNAPPY",
            "GZIP_OFFLOAD_DECOMPRESSION",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompressionCodec;

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
                    "INVALID" => Ok(CompressionCodec::Invalid),
                    "NONE" => Ok(CompressionCodec::None),
                    "GZIP" => Ok(CompressionCodec::Gzip),
                    "ZSTANDARD" => Ok(CompressionCodec::Zstandard),
                    "SNAPPY" => Ok(CompressionCodec::Snappy),
                    "GZIP_OFFLOAD_DECOMPRESSION" => Ok(CompressionCodec::GzipOffloadDecompression),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Fragment {
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
        if self.begin != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        if self.sum.is_some() {
            len += 1;
        }
        if self.compression_codec != 0 {
            len += 1;
        }
        if !self.backing_store.is_empty() {
            len += 1;
        }
        if self.mod_time != 0 {
            len += 1;
        }
        if !self.path_postfix.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.Fragment", len)?;
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.begin != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("begin", ToString::to_string(&self.begin).as_str())?;
        }
        if self.end != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        if let Some(v) = self.sum.as_ref() {
            struct_ser.serialize_field("sum", v)?;
        }
        if self.compression_codec != 0 {
            let v = CompressionCodec::try_from(self.compression_codec)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression_codec)))?;
            struct_ser.serialize_field("compressionCodec", &v)?;
        }
        if !self.backing_store.is_empty() {
            struct_ser.serialize_field("backingStore", &self.backing_store)?;
        }
        if self.mod_time != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("modTime", ToString::to_string(&self.mod_time).as_str())?;
        }
        if !self.path_postfix.is_empty() {
            struct_ser.serialize_field("pathPostfix", &self.path_postfix)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Fragment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal",
            "begin",
            "end",
            "sum",
            "compression_codec",
            "compressionCodec",
            "backing_store",
            "backingStore",
            "mod_time",
            "modTime",
            "path_postfix",
            "pathPostfix",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Journal,
            Begin,
            End,
            Sum,
            CompressionCodec,
            BackingStore,
            ModTime,
            PathPostfix,
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
                            "begin" => Ok(GeneratedField::Begin),
                            "end" => Ok(GeneratedField::End),
                            "sum" => Ok(GeneratedField::Sum),
                            "compressionCodec" | "compression_codec" => Ok(GeneratedField::CompressionCodec),
                            "backingStore" | "backing_store" => Ok(GeneratedField::BackingStore),
                            "modTime" | "mod_time" => Ok(GeneratedField::ModTime),
                            "pathPostfix" | "path_postfix" => Ok(GeneratedField::PathPostfix),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Fragment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.Fragment")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Fragment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal__ = None;
                let mut begin__ = None;
                let mut end__ = None;
                let mut sum__ = None;
                let mut compression_codec__ = None;
                let mut backing_store__ = None;
                let mut mod_time__ = None;
                let mut path_postfix__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Begin => {
                            if begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("begin"));
                            }
                            begin__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Sum => {
                            if sum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sum"));
                            }
                            sum__ = map_.next_value()?;
                        }
                        GeneratedField::CompressionCodec => {
                            if compression_codec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compressionCodec"));
                            }
                            compression_codec__ = Some(map_.next_value::<CompressionCodec>()? as i32);
                        }
                        GeneratedField::BackingStore => {
                            if backing_store__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backingStore"));
                            }
                            backing_store__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ModTime => {
                            if mod_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modTime"));
                            }
                            mod_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PathPostfix => {
                            if path_postfix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pathPostfix"));
                            }
                            path_postfix__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Fragment {
                    journal: journal__.unwrap_or_default(),
                    begin: begin__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                    sum: sum__,
                    compression_codec: compression_codec__.unwrap_or_default(),
                    backing_store: backing_store__.unwrap_or_default(),
                    mod_time: mod_time__.unwrap_or_default(),
                    path_postfix: path_postfix__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.Fragment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FragmentStoreHealthRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fragment_store.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.FragmentStoreHealthRequest", len)?;
        if !self.fragment_store.is_empty() {
            struct_ser.serialize_field("fragmentStore", &self.fragment_store)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FragmentStoreHealthRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fragment_store",
            "fragmentStore",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FragmentStore,
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
                            "fragmentStore" | "fragment_store" => Ok(GeneratedField::FragmentStore),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FragmentStoreHealthRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.FragmentStoreHealthRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FragmentStoreHealthRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fragment_store__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FragmentStore => {
                            if fragment_store__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragmentStore"));
                            }
                            fragment_store__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FragmentStoreHealthRequest {
                    fragment_store: fragment_store__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.FragmentStoreHealthRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FragmentStoreHealthResponse {
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
        if !self.store_health_error.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.FragmentStoreHealthResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.store_health_error.is_empty() {
            struct_ser.serialize_field("storeHealthError", &self.store_health_error)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FragmentStoreHealthResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "store_health_error",
            "storeHealthError",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            StoreHealthError,
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
                            "storeHealthError" | "store_health_error" => Ok(GeneratedField::StoreHealthError),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FragmentStoreHealthResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.FragmentStoreHealthResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FragmentStoreHealthResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut store_health_error__ = None;
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
                        GeneratedField::StoreHealthError => {
                            if store_health_error__.is_some() {
                                return Err(serde::de::Error::duplicate_field("storeHealthError"));
                            }
                            store_health_error__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FragmentStoreHealthResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    store_health_error: store_health_error__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.FragmentStoreHealthResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FragmentsRequest {
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
        if !self.journal.is_empty() {
            len += 1;
        }
        if self.begin_mod_time != 0 {
            len += 1;
        }
        if self.end_mod_time != 0 {
            len += 1;
        }
        if self.next_page_token != 0 {
            len += 1;
        }
        if self.page_limit != 0 {
            len += 1;
        }
        if self.signature_ttl.is_some() {
            len += 1;
        }
        if self.do_not_proxy {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.FragmentsRequest", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.begin_mod_time != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("beginModTime", ToString::to_string(&self.begin_mod_time).as_str())?;
        }
        if self.end_mod_time != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("endModTime", ToString::to_string(&self.end_mod_time).as_str())?;
        }
        if self.next_page_token != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("nextPageToken", ToString::to_string(&self.next_page_token).as_str())?;
        }
        if self.page_limit != 0 {
            struct_ser.serialize_field("pageLimit", &self.page_limit)?;
        }
        if let Some(v) = self.signature_ttl.as_ref() {
            struct_ser.serialize_field("signatureTTL", v)?;
        }
        if self.do_not_proxy {
            struct_ser.serialize_field("doNotProxy", &self.do_not_proxy)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FragmentsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "journal",
            "begin_mod_time",
            "beginModTime",
            "end_mod_time",
            "endModTime",
            "next_page_token",
            "nextPageToken",
            "page_limit",
            "pageLimit",
            "signatureTTL",
            "do_not_proxy",
            "doNotProxy",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            Journal,
            BeginModTime,
            EndModTime,
            NextPageToken,
            PageLimit,
            SignatureTtl,
            DoNotProxy,
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
                            "journal" => Ok(GeneratedField::Journal),
                            "beginModTime" | "begin_mod_time" => Ok(GeneratedField::BeginModTime),
                            "endModTime" | "end_mod_time" => Ok(GeneratedField::EndModTime),
                            "nextPageToken" | "next_page_token" => Ok(GeneratedField::NextPageToken),
                            "pageLimit" | "page_limit" => Ok(GeneratedField::PageLimit),
                            "signatureTTL" => Ok(GeneratedField::SignatureTtl),
                            "doNotProxy" | "do_not_proxy" => Ok(GeneratedField::DoNotProxy),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FragmentsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.FragmentsRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FragmentsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut journal__ = None;
                let mut begin_mod_time__ = None;
                let mut end_mod_time__ = None;
                let mut next_page_token__ = None;
                let mut page_limit__ = None;
                let mut signature_ttl__ = None;
                let mut do_not_proxy__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BeginModTime => {
                            if begin_mod_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("beginModTime"));
                            }
                            begin_mod_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::EndModTime => {
                            if end_mod_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endModTime"));
                            }
                            end_mod_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NextPageToken => {
                            if next_page_token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nextPageToken"));
                            }
                            next_page_token__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PageLimit => {
                            if page_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pageLimit"));
                            }
                            page_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SignatureTtl => {
                            if signature_ttl__.is_some() {
                                return Err(serde::de::Error::duplicate_field("signatureTTL"));
                            }
                            signature_ttl__ = map_.next_value()?;
                        }
                        GeneratedField::DoNotProxy => {
                            if do_not_proxy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doNotProxy"));
                            }
                            do_not_proxy__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FragmentsRequest {
                    header: header__,
                    journal: journal__.unwrap_or_default(),
                    begin_mod_time: begin_mod_time__.unwrap_or_default(),
                    end_mod_time: end_mod_time__.unwrap_or_default(),
                    next_page_token: next_page_token__.unwrap_or_default(),
                    page_limit: page_limit__.unwrap_or_default(),
                    signature_ttl: signature_ttl__,
                    do_not_proxy: do_not_proxy__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.FragmentsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FragmentsResponse {
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
        if !self.fragments.is_empty() {
            len += 1;
        }
        if self.next_page_token != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.FragmentsResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.fragments.is_empty() {
            struct_ser.serialize_field("fragments", &self.fragments)?;
        }
        if self.next_page_token != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("nextPageToken", ToString::to_string(&self.next_page_token).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FragmentsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "fragments",
            "next_page_token",
            "nextPageToken",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Fragments,
            NextPageToken,
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
                            "fragments" => Ok(GeneratedField::Fragments),
                            "nextPageToken" | "next_page_token" => Ok(GeneratedField::NextPageToken),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FragmentsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.FragmentsResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FragmentsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut fragments__ = None;
                let mut next_page_token__ = None;
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
                        GeneratedField::Fragments => {
                            if fragments__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragments"));
                            }
                            fragments__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NextPageToken => {
                            if next_page_token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nextPageToken"));
                            }
                            next_page_token__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FragmentsResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    fragments: fragments__.unwrap_or_default(),
                    next_page_token: next_page_token__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.FragmentsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for fragments_response::Fragment {
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
        if !self.signed_url.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.FragmentsResponse._Fragment", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if !self.signed_url.is_empty() {
            struct_ser.serialize_field("signedUrl", &self.signed_url)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for fragments_response::Fragment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "signed_url",
            "signedUrl",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            SignedUrl,
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
                            "signedUrl" | "signed_url" => Ok(GeneratedField::SignedUrl),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = fragments_response::Fragment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.FragmentsResponse._Fragment")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<fragments_response::Fragment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut signed_url__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::SignedUrl => {
                            if signed_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("signedUrl"));
                            }
                            signed_url__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(fragments_response::Fragment {
                    spec: spec__,
                    signed_url: signed_url__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.FragmentsResponse._Fragment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Header {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.process_id.is_some() {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        if self.etcd.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.Header", len)?;
        if let Some(v) = self.process_id.as_ref() {
            struct_ser.serialize_field("processId", v)?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        if let Some(v) = self.etcd.as_ref() {
            struct_ser.serialize_field("etcd", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Header {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "process_id",
            "processId",
            "route",
            "etcd",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProcessId,
            Route,
            Etcd,
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
                            "processId" | "process_id" => Ok(GeneratedField::ProcessId),
                            "route" => Ok(GeneratedField::Route),
                            "etcd" => Ok(GeneratedField::Etcd),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Header;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.Header")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Header, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut process_id__ = None;
                let mut route__ = None;
                let mut etcd__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ProcessId => {
                            if process_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("processId"));
                            }
                            process_id__ = map_.next_value()?;
                        }
                        GeneratedField::Route => {
                            if route__.is_some() {
                                return Err(serde::de::Error::duplicate_field("route"));
                            }
                            route__ = map_.next_value()?;
                        }
                        GeneratedField::Etcd => {
                            if etcd__.is_some() {
                                return Err(serde::de::Error::duplicate_field("etcd"));
                            }
                            etcd__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Header {
                    process_id: process_id__,
                    route: route__,
                    etcd: etcd__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.Header", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for header::Etcd {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cluster_id != 0 {
            len += 1;
        }
        if self.member_id != 0 {
            len += 1;
        }
        if self.revision != 0 {
            len += 1;
        }
        if self.raft_term != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.Header.Etcd", len)?;
        if self.cluster_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("clusterId", ToString::to_string(&self.cluster_id).as_str())?;
        }
        if self.member_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("memberId", ToString::to_string(&self.member_id).as_str())?;
        }
        if self.revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("revision", ToString::to_string(&self.revision).as_str())?;
        }
        if self.raft_term != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("raftTerm", ToString::to_string(&self.raft_term).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for header::Etcd {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cluster_id",
            "clusterId",
            "member_id",
            "memberId",
            "revision",
            "raft_term",
            "raftTerm",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ClusterId,
            MemberId,
            Revision,
            RaftTerm,
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
                            "clusterId" | "cluster_id" => Ok(GeneratedField::ClusterId),
                            "memberId" | "member_id" => Ok(GeneratedField::MemberId),
                            "revision" => Ok(GeneratedField::Revision),
                            "raftTerm" | "raft_term" => Ok(GeneratedField::RaftTerm),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = header::Etcd;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.Header.Etcd")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<header::Etcd, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cluster_id__ = None;
                let mut member_id__ = None;
                let mut revision__ = None;
                let mut raft_term__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ClusterId => {
                            if cluster_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clusterId"));
                            }
                            cluster_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MemberId => {
                            if member_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("memberId"));
                            }
                            member_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Revision => {
                            if revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("revision"));
                            }
                            revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RaftTerm => {
                            if raft_term__.is_some() {
                                return Err(serde::de::Error::duplicate_field("raftTerm"));
                            }
                            raft_term__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(header::Etcd {
                    cluster_id: cluster_id__.unwrap_or_default(),
                    member_id: member_id__.unwrap_or_default(),
                    revision: revision__.unwrap_or_default(),
                    raft_term: raft_term__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.Header.Etcd", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JournalSpec {
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
        if self.replication != 0 {
            len += 1;
        }
        if self.labels.is_some() {
            len += 1;
        }
        if self.fragment.is_some() {
            len += 1;
        }
        if self.flags != 0 {
            len += 1;
        }
        if self.max_append_rate != 0 {
            len += 1;
        }
        if self.suspend.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.JournalSpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.replication != 0 {
            struct_ser.serialize_field("replication", &self.replication)?;
        }
        if let Some(v) = self.labels.as_ref() {
            struct_ser.serialize_field("labels", v)?;
        }
        if let Some(v) = self.fragment.as_ref() {
            struct_ser.serialize_field("fragment", v)?;
        }
        if self.flags != 0 {
            struct_ser.serialize_field("flags", &self.flags)?;
        }
        if self.max_append_rate != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxAppendRate", ToString::to_string(&self.max_append_rate).as_str())?;
        }
        if let Some(v) = self.suspend.as_ref() {
            struct_ser.serialize_field("suspend", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JournalSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "replication",
            "labels",
            "fragment",
            "flags",
            "max_append_rate",
            "maxAppendRate",
            "suspend",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Replication,
            Labels,
            Fragment,
            Flags,
            MaxAppendRate,
            Suspend,
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
                            "replication" => Ok(GeneratedField::Replication),
                            "labels" => Ok(GeneratedField::Labels),
                            "fragment" => Ok(GeneratedField::Fragment),
                            "flags" => Ok(GeneratedField::Flags),
                            "maxAppendRate" | "max_append_rate" => Ok(GeneratedField::MaxAppendRate),
                            "suspend" => Ok(GeneratedField::Suspend),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JournalSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.JournalSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JournalSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut replication__ = None;
                let mut labels__ = None;
                let mut fragment__ = None;
                let mut flags__ = None;
                let mut max_append_rate__ = None;
                let mut suspend__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Replication => {
                            if replication__.is_some() {
                                return Err(serde::de::Error::duplicate_field("replication"));
                            }
                            replication__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Labels => {
                            if labels__.is_some() {
                                return Err(serde::de::Error::duplicate_field("labels"));
                            }
                            labels__ = map_.next_value()?;
                        }
                        GeneratedField::Fragment => {
                            if fragment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragment"));
                            }
                            fragment__ = map_.next_value()?;
                        }
                        GeneratedField::Flags => {
                            if flags__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flags"));
                            }
                            flags__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxAppendRate => {
                            if max_append_rate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxAppendRate"));
                            }
                            max_append_rate__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Suspend => {
                            if suspend__.is_some() {
                                return Err(serde::de::Error::duplicate_field("suspend"));
                            }
                            suspend__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JournalSpec {
                    name: name__.unwrap_or_default(),
                    replication: replication__.unwrap_or_default(),
                    labels: labels__,
                    fragment: fragment__,
                    flags: flags__.unwrap_or_default(),
                    max_append_rate: max_append_rate__.unwrap_or_default(),
                    suspend: suspend__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.JournalSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for journal_spec::Flag {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::NotSpecified => "NOT_SPECIFIED",
            Self::ORdonly => "O_RDONLY",
            Self::OWronly => "O_WRONLY",
            Self::ORdwr => "O_RDWR",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for journal_spec::Flag {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NOT_SPECIFIED",
            "O_RDONLY",
            "O_WRONLY",
            "O_RDWR",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = journal_spec::Flag;

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
                    "NOT_SPECIFIED" => Ok(journal_spec::Flag::NotSpecified),
                    "O_RDONLY" => Ok(journal_spec::Flag::ORdonly),
                    "O_WRONLY" => Ok(journal_spec::Flag::OWronly),
                    "O_RDWR" => Ok(journal_spec::Flag::ORdwr),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for journal_spec::Fragment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.length != 0 {
            len += 1;
        }
        if self.compression_codec != 0 {
            len += 1;
        }
        if !self.stores.is_empty() {
            len += 1;
        }
        if self.refresh_interval.is_some() {
            len += 1;
        }
        if self.retention.is_some() {
            len += 1;
        }
        if self.flush_interval.is_some() {
            len += 1;
        }
        if !self.path_postfix_template.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.JournalSpec.Fragment", len)?;
        if self.length != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("length", ToString::to_string(&self.length).as_str())?;
        }
        if self.compression_codec != 0 {
            let v = CompressionCodec::try_from(self.compression_codec)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression_codec)))?;
            struct_ser.serialize_field("compressionCodec", &v)?;
        }
        if !self.stores.is_empty() {
            struct_ser.serialize_field("stores", &self.stores)?;
        }
        if let Some(v) = self.refresh_interval.as_ref() {
            struct_ser.serialize_field("refreshInterval", v)?;
        }
        if let Some(v) = self.retention.as_ref() {
            struct_ser.serialize_field("retention", v)?;
        }
        if let Some(v) = self.flush_interval.as_ref() {
            struct_ser.serialize_field("flushInterval", v)?;
        }
        if !self.path_postfix_template.is_empty() {
            struct_ser.serialize_field("pathPostfixTemplate", &self.path_postfix_template)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for journal_spec::Fragment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "length",
            "compression_codec",
            "compressionCodec",
            "stores",
            "refresh_interval",
            "refreshInterval",
            "retention",
            "flush_interval",
            "flushInterval",
            "path_postfix_template",
            "pathPostfixTemplate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Length,
            CompressionCodec,
            Stores,
            RefreshInterval,
            Retention,
            FlushInterval,
            PathPostfixTemplate,
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
                            "length" => Ok(GeneratedField::Length),
                            "compressionCodec" | "compression_codec" => Ok(GeneratedField::CompressionCodec),
                            "stores" => Ok(GeneratedField::Stores),
                            "refreshInterval" | "refresh_interval" => Ok(GeneratedField::RefreshInterval),
                            "retention" => Ok(GeneratedField::Retention),
                            "flushInterval" | "flush_interval" => Ok(GeneratedField::FlushInterval),
                            "pathPostfixTemplate" | "path_postfix_template" => Ok(GeneratedField::PathPostfixTemplate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = journal_spec::Fragment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.JournalSpec.Fragment")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<journal_spec::Fragment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut length__ = None;
                let mut compression_codec__ = None;
                let mut stores__ = None;
                let mut refresh_interval__ = None;
                let mut retention__ = None;
                let mut flush_interval__ = None;
                let mut path_postfix_template__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::CompressionCodec => {
                            if compression_codec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compressionCodec"));
                            }
                            compression_codec__ = Some(map_.next_value::<CompressionCodec>()? as i32);
                        }
                        GeneratedField::Stores => {
                            if stores__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stores"));
                            }
                            stores__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RefreshInterval => {
                            if refresh_interval__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshInterval"));
                            }
                            refresh_interval__ = map_.next_value()?;
                        }
                        GeneratedField::Retention => {
                            if retention__.is_some() {
                                return Err(serde::de::Error::duplicate_field("retention"));
                            }
                            retention__ = map_.next_value()?;
                        }
                        GeneratedField::FlushInterval => {
                            if flush_interval__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushInterval"));
                            }
                            flush_interval__ = map_.next_value()?;
                        }
                        GeneratedField::PathPostfixTemplate => {
                            if path_postfix_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pathPostfixTemplate"));
                            }
                            path_postfix_template__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(journal_spec::Fragment {
                    length: length__.unwrap_or_default(),
                    compression_codec: compression_codec__.unwrap_or_default(),
                    stores: stores__.unwrap_or_default(),
                    refresh_interval: refresh_interval__,
                    retention: retention__,
                    flush_interval: flush_interval__,
                    path_postfix_template: path_postfix_template__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.JournalSpec.Fragment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for journal_spec::Suspend {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.level != 0 {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.JournalSpec.Suspend", len)?;
        if self.level != 0 {
            let v = journal_spec::suspend::Level::try_from(self.level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.level)))?;
            struct_ser.serialize_field("level", &v)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for journal_spec::Suspend {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "level",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Level,
            Offset,
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
                            "level" => Ok(GeneratedField::Level),
                            "offset" => Ok(GeneratedField::Offset),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = journal_spec::Suspend;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.JournalSpec.Suspend")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<journal_spec::Suspend, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Level => {
                            if level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("level"));
                            }
                            level__ = Some(map_.next_value::<journal_spec::suspend::Level>()? as i32);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(journal_spec::Suspend {
                    level: level__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.JournalSpec.Suspend", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for journal_spec::suspend::Level {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::None => "NONE",
            Self::Partial => "PARTIAL",
            Self::Full => "FULL",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for journal_spec::suspend::Level {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NONE",
            "PARTIAL",
            "FULL",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = journal_spec::suspend::Level;

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
                    "NONE" => Ok(journal_spec::suspend::Level::None),
                    "PARTIAL" => Ok(journal_spec::suspend::Level::Partial),
                    "FULL" => Ok(journal_spec::suspend::Level::Full),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Label {
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
        if !self.value.is_empty() {
            len += 1;
        }
        if self.prefix {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.Label", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.value.is_empty() {
            struct_ser.serialize_field("value", &self.value)?;
        }
        if self.prefix {
            struct_ser.serialize_field("prefix", &self.prefix)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Label {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "value",
            "prefix",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Value,
            Prefix,
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
                            "value" => Ok(GeneratedField::Value),
                            "prefix" => Ok(GeneratedField::Prefix),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Label;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.Label")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Label, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut value__ = None;
                let mut prefix__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Prefix => {
                            if prefix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prefix"));
                            }
                            prefix__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Label {
                    name: name__.unwrap_or_default(),
                    value: value__.unwrap_or_default(),
                    prefix: prefix__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.Label", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LabelSelector {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.include.is_some() {
            len += 1;
        }
        if self.exclude.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.LabelSelector", len)?;
        if let Some(v) = self.include.as_ref() {
            struct_ser.serialize_field("include", v)?;
        }
        if let Some(v) = self.exclude.as_ref() {
            struct_ser.serialize_field("exclude", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LabelSelector {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "include",
            "exclude",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Include,
            Exclude,
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
                            "include" => Ok(GeneratedField::Include),
                            "exclude" => Ok(GeneratedField::Exclude),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LabelSelector;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.LabelSelector")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LabelSelector, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut include__ = None;
                let mut exclude__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Include => {
                            if include__.is_some() {
                                return Err(serde::de::Error::duplicate_field("include"));
                            }
                            include__ = map_.next_value()?;
                        }
                        GeneratedField::Exclude => {
                            if exclude__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exclude"));
                            }
                            exclude__ = map_.next_value()?;
                        }
                    }
                }
                Ok(LabelSelector {
                    include: include__,
                    exclude: exclude__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.LabelSelector", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LabelSet {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.labels.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.LabelSet", len)?;
        if !self.labels.is_empty() {
            struct_ser.serialize_field("labels", &self.labels)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LabelSet {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "labels",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Labels,
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
                            "labels" => Ok(GeneratedField::Labels),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LabelSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.LabelSet")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LabelSet, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut labels__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Labels => {
                            if labels__.is_some() {
                                return Err(serde::de::Error::duplicate_field("labels"));
                            }
                            labels__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(LabelSet {
                    labels: labels__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.LabelSet", FIELDS, GeneratedVisitor)
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
        if self.watch {
            len += 1;
        }
        if self.watch_resume.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ListRequest", len)?;
        if let Some(v) = self.selector.as_ref() {
            struct_ser.serialize_field("selector", v)?;
        }
        if self.watch {
            struct_ser.serialize_field("watch", &self.watch)?;
        }
        if let Some(v) = self.watch_resume.as_ref() {
            struct_ser.serialize_field("watchResume", v)?;
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
            "watch",
            "watch_resume",
            "watchResume",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Selector,
            Watch,
            WatchResume,
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
                            "watch" => Ok(GeneratedField::Watch),
                            "watchResume" | "watch_resume" => Ok(GeneratedField::WatchResume),
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
                formatter.write_str("struct protocol.ListRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut selector__ = None;
                let mut watch__ = None;
                let mut watch_resume__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Selector => {
                            if selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selector"));
                            }
                            selector__ = map_.next_value()?;
                        }
                        GeneratedField::Watch => {
                            if watch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("watch"));
                            }
                            watch__ = Some(map_.next_value()?);
                        }
                        GeneratedField::WatchResume => {
                            if watch_resume__.is_some() {
                                return Err(serde::de::Error::duplicate_field("watchResume"));
                            }
                            watch_resume__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ListRequest {
                    selector: selector__,
                    watch: watch__.unwrap_or_default(),
                    watch_resume: watch_resume__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.ListRequest", FIELDS, GeneratedVisitor)
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
        if !self.journals.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ListResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.journals.is_empty() {
            struct_ser.serialize_field("journals", &self.journals)?;
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
            "journals",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Journals,
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
                            "journals" => Ok(GeneratedField::Journals),
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
                formatter.write_str("struct protocol.ListResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut journals__ = None;
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
                        GeneratedField::Journals => {
                            if journals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journals"));
                            }
                            journals__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ListResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    journals: journals__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ListResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for list_response::Journal {
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
        if self.create_revision != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ListResponse.Journal", len)?;
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
        if self.create_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("createRevision", ToString::to_string(&self.create_revision).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for list_response::Journal {
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
            "create_revision",
            "createRevision",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            ModRevision,
            Route,
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
            type Value = list_response::Journal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ListResponse.Journal")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<list_response::Journal, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut mod_revision__ = None;
                let mut route__ = None;
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
                Ok(list_response::Journal {
                    spec: spec__,
                    mod_revision: mod_revision__.unwrap_or_default(),
                    route: route__,
                    create_revision: create_revision__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ListResponse.Journal", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProcessSpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.id.is_some() {
            len += 1;
        }
        if !self.endpoint.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ProcessSpec", len)?;
        if let Some(v) = self.id.as_ref() {
            struct_ser.serialize_field("id", v)?;
        }
        if !self.endpoint.is_empty() {
            struct_ser.serialize_field("endpoint", &self.endpoint)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProcessSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "endpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Endpoint,
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
                            "endpoint" => Ok(GeneratedField::Endpoint),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProcessSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ProcessSpec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProcessSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut endpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = map_.next_value()?;
                        }
                        GeneratedField::Endpoint => {
                            if endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endpoint"));
                            }
                            endpoint__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ProcessSpec {
                    id: id__,
                    endpoint: endpoint__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ProcessSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for process_spec::Id {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.zone.is_empty() {
            len += 1;
        }
        if !self.suffix.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ProcessSpec.ID", len)?;
        if !self.zone.is_empty() {
            struct_ser.serialize_field("zone", &self.zone)?;
        }
        if !self.suffix.is_empty() {
            struct_ser.serialize_field("suffix", &self.suffix)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for process_spec::Id {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "zone",
            "suffix",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Zone,
            Suffix,
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
                            "zone" => Ok(GeneratedField::Zone),
                            "suffix" => Ok(GeneratedField::Suffix),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = process_spec::Id;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ProcessSpec.ID")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<process_spec::Id, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut zone__ = None;
                let mut suffix__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Zone => {
                            if zone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("zone"));
                            }
                            zone__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Suffix => {
                            if suffix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("suffix"));
                            }
                            suffix__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(process_spec::Id {
                    zone: zone__.unwrap_or_default(),
                    suffix: suffix__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ProcessSpec.ID", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReadRequest {
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
        if !self.journal.is_empty() {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if self.block {
            len += 1;
        }
        if self.do_not_proxy {
            len += 1;
        }
        if self.metadata_only {
            len += 1;
        }
        if self.end_offset != 0 {
            len += 1;
        }
        if self.begin_mod_time != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ReadRequest", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if self.block {
            struct_ser.serialize_field("block", &self.block)?;
        }
        if self.do_not_proxy {
            struct_ser.serialize_field("doNotProxy", &self.do_not_proxy)?;
        }
        if self.metadata_only {
            struct_ser.serialize_field("metadataOnly", &self.metadata_only)?;
        }
        if self.end_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("endOffset", ToString::to_string(&self.end_offset).as_str())?;
        }
        if self.begin_mod_time != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("beginModTime", ToString::to_string(&self.begin_mod_time).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReadRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "journal",
            "offset",
            "block",
            "do_not_proxy",
            "doNotProxy",
            "metadata_only",
            "metadataOnly",
            "end_offset",
            "endOffset",
            "begin_mod_time",
            "beginModTime",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            Journal,
            Offset,
            Block,
            DoNotProxy,
            MetadataOnly,
            EndOffset,
            BeginModTime,
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
                            "journal" => Ok(GeneratedField::Journal),
                            "offset" => Ok(GeneratedField::Offset),
                            "block" => Ok(GeneratedField::Block),
                            "doNotProxy" | "do_not_proxy" => Ok(GeneratedField::DoNotProxy),
                            "metadataOnly" | "metadata_only" => Ok(GeneratedField::MetadataOnly),
                            "endOffset" | "end_offset" => Ok(GeneratedField::EndOffset),
                            "beginModTime" | "begin_mod_time" => Ok(GeneratedField::BeginModTime),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReadRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ReadRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReadRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut journal__ = None;
                let mut offset__ = None;
                let mut block__ = None;
                let mut do_not_proxy__ = None;
                let mut metadata_only__ = None;
                let mut end_offset__ = None;
                let mut begin_mod_time__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Block => {
                            if block__.is_some() {
                                return Err(serde::de::Error::duplicate_field("block"));
                            }
                            block__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DoNotProxy => {
                            if do_not_proxy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doNotProxy"));
                            }
                            do_not_proxy__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MetadataOnly => {
                            if metadata_only__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadataOnly"));
                            }
                            metadata_only__ = Some(map_.next_value()?);
                        }
                        GeneratedField::EndOffset => {
                            if end_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endOffset"));
                            }
                            end_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BeginModTime => {
                            if begin_mod_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("beginModTime"));
                            }
                            begin_mod_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ReadRequest {
                    header: header__,
                    journal: journal__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                    block: block__.unwrap_or_default(),
                    do_not_proxy: do_not_proxy__.unwrap_or_default(),
                    metadata_only: metadata_only__.unwrap_or_default(),
                    end_offset: end_offset__.unwrap_or_default(),
                    begin_mod_time: begin_mod_time__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ReadRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReadResponse {
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
        if self.offset != 0 {
            len += 1;
        }
        if self.write_head != 0 {
            len += 1;
        }
        if self.fragment.is_some() {
            len += 1;
        }
        if !self.fragment_url.is_empty() {
            len += 1;
        }
        if !self.content.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ReadResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if self.write_head != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("writeHead", ToString::to_string(&self.write_head).as_str())?;
        }
        if let Some(v) = self.fragment.as_ref() {
            struct_ser.serialize_field("fragment", v)?;
        }
        if !self.fragment_url.is_empty() {
            struct_ser.serialize_field("fragmentUrl", &self.fragment_url)?;
        }
        if !self.content.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("content", pbjson::private::base64::encode(&self.content).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReadResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "offset",
            "write_head",
            "writeHead",
            "fragment",
            "fragment_url",
            "fragmentUrl",
            "content",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Offset,
            WriteHead,
            Fragment,
            FragmentUrl,
            Content,
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
                            "offset" => Ok(GeneratedField::Offset),
                            "writeHead" | "write_head" => Ok(GeneratedField::WriteHead),
                            "fragment" => Ok(GeneratedField::Fragment),
                            "fragmentUrl" | "fragment_url" => Ok(GeneratedField::FragmentUrl),
                            "content" => Ok(GeneratedField::Content),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReadResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ReadResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReadResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut offset__ = None;
                let mut write_head__ = None;
                let mut fragment__ = None;
                let mut fragment_url__ = None;
                let mut content__ = None;
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
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WriteHead => {
                            if write_head__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writeHead"));
                            }
                            write_head__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Fragment => {
                            if fragment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragment"));
                            }
                            fragment__ = map_.next_value()?;
                        }
                        GeneratedField::FragmentUrl => {
                            if fragment_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragmentUrl"));
                            }
                            fragment_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Content => {
                            if content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("content"));
                            }
                            content__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ReadResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    offset: offset__.unwrap_or_default(),
                    write_head: write_head__.unwrap_or_default(),
                    fragment: fragment__,
                    fragment_url: fragment_url__.unwrap_or_default(),
                    content: content__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ReadResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReplicateRequest {
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
        if self.proposal.is_some() {
            len += 1;
        }
        if self.registers.is_some() {
            len += 1;
        }
        if self.acknowledge {
            len += 1;
        }
        if !self.deprecated_journal.is_empty() {
            len += 1;
        }
        if !self.content.is_empty() {
            len += 1;
        }
        if self.content_delta != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ReplicateRequest", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if let Some(v) = self.proposal.as_ref() {
            struct_ser.serialize_field("proposal", v)?;
        }
        if let Some(v) = self.registers.as_ref() {
            struct_ser.serialize_field("registers", v)?;
        }
        if self.acknowledge {
            struct_ser.serialize_field("acknowledge", &self.acknowledge)?;
        }
        if !self.deprecated_journal.is_empty() {
            struct_ser.serialize_field("deprecatedJournal", &self.deprecated_journal)?;
        }
        if !self.content.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("content", pbjson::private::base64::encode(&self.content).as_str())?;
        }
        if self.content_delta != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("contentDelta", ToString::to_string(&self.content_delta).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReplicateRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "proposal",
            "registers",
            "acknowledge",
            "deprecated_journal",
            "deprecatedJournal",
            "content",
            "content_delta",
            "contentDelta",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            Proposal,
            Registers,
            Acknowledge,
            DeprecatedJournal,
            Content,
            ContentDelta,
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
                            "proposal" => Ok(GeneratedField::Proposal),
                            "registers" => Ok(GeneratedField::Registers),
                            "acknowledge" => Ok(GeneratedField::Acknowledge),
                            "deprecatedJournal" | "deprecated_journal" => Ok(GeneratedField::DeprecatedJournal),
                            "content" => Ok(GeneratedField::Content),
                            "contentDelta" | "content_delta" => Ok(GeneratedField::ContentDelta),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReplicateRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ReplicateRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReplicateRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut proposal__ = None;
                let mut registers__ = None;
                let mut acknowledge__ = None;
                let mut deprecated_journal__ = None;
                let mut content__ = None;
                let mut content_delta__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::Proposal => {
                            if proposal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("proposal"));
                            }
                            proposal__ = map_.next_value()?;
                        }
                        GeneratedField::Registers => {
                            if registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("registers"));
                            }
                            registers__ = map_.next_value()?;
                        }
                        GeneratedField::Acknowledge => {
                            if acknowledge__.is_some() {
                                return Err(serde::de::Error::duplicate_field("acknowledge"));
                            }
                            acknowledge__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DeprecatedJournal => {
                            if deprecated_journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecatedJournal"));
                            }
                            deprecated_journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Content => {
                            if content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("content"));
                            }
                            content__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ContentDelta => {
                            if content_delta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("contentDelta"));
                            }
                            content_delta__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ReplicateRequest {
                    header: header__,
                    proposal: proposal__,
                    registers: registers__,
                    acknowledge: acknowledge__.unwrap_or_default(),
                    deprecated_journal: deprecated_journal__.unwrap_or_default(),
                    content: content__.unwrap_or_default(),
                    content_delta: content_delta__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.ReplicateRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReplicateResponse {
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
        if self.fragment.is_some() {
            len += 1;
        }
        if self.registers.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.ReplicateResponse", len)?;
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if let Some(v) = self.fragment.as_ref() {
            struct_ser.serialize_field("fragment", v)?;
        }
        if let Some(v) = self.registers.as_ref() {
            struct_ser.serialize_field("registers", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReplicateResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "fragment",
            "registers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            Fragment,
            Registers,
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
                            "fragment" => Ok(GeneratedField::Fragment),
                            "registers" => Ok(GeneratedField::Registers),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReplicateResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.ReplicateResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReplicateResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut fragment__ = None;
                let mut registers__ = None;
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
                        GeneratedField::Fragment => {
                            if fragment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragment"));
                            }
                            fragment__ = map_.next_value()?;
                        }
                        GeneratedField::Registers => {
                            if registers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("registers"));
                            }
                            registers__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ReplicateResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    fragment: fragment__,
                    registers: registers__,
                })
            }
        }
        deserializer.deserialize_struct("protocol.ReplicateResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Route {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.members.is_empty() {
            len += 1;
        }
        if self.primary != 0 {
            len += 1;
        }
        if !self.endpoints.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.Route", len)?;
        if !self.members.is_empty() {
            struct_ser.serialize_field("members", &self.members)?;
        }
        if self.primary != 0 {
            struct_ser.serialize_field("primary", &self.primary)?;
        }
        if !self.endpoints.is_empty() {
            struct_ser.serialize_field("endpoints", &self.endpoints)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Route {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "members",
            "primary",
            "endpoints",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Members,
            Primary,
            Endpoints,
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
                            "members" => Ok(GeneratedField::Members),
                            "primary" => Ok(GeneratedField::Primary),
                            "endpoints" => Ok(GeneratedField::Endpoints),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Route;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.Route")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Route, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut members__ = None;
                let mut primary__ = None;
                let mut endpoints__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Members => {
                            if members__.is_some() {
                                return Err(serde::de::Error::duplicate_field("members"));
                            }
                            members__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Primary => {
                            if primary__.is_some() {
                                return Err(serde::de::Error::duplicate_field("primary"));
                            }
                            primary__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Endpoints => {
                            if endpoints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endpoints"));
                            }
                            endpoints__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Route {
                    members: members__.unwrap_or_default(),
                    primary: primary__.unwrap_or_default(),
                    endpoints: endpoints__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.Route", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Sha1Sum {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.part1 != 0 {
            len += 1;
        }
        if self.part2 != 0 {
            len += 1;
        }
        if self.part3 != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("protocol.SHA1Sum", len)?;
        if self.part1 != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("part1", ToString::to_string(&self.part1).as_str())?;
        }
        if self.part2 != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("part2", ToString::to_string(&self.part2).as_str())?;
        }
        if self.part3 != 0 {
            struct_ser.serialize_field("part3", &self.part3)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Sha1Sum {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "part1",
            "part2",
            "part3",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Part1,
            Part2,
            Part3,
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
                            "part1" => Ok(GeneratedField::Part1),
                            "part2" => Ok(GeneratedField::Part2),
                            "part3" => Ok(GeneratedField::Part3),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Sha1Sum;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct protocol.SHA1Sum")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Sha1Sum, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut part1__ = None;
                let mut part2__ = None;
                let mut part3__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Part1 => {
                            if part1__.is_some() {
                                return Err(serde::de::Error::duplicate_field("part1"));
                            }
                            part1__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Part2 => {
                            if part2__.is_some() {
                                return Err(serde::de::Error::duplicate_field("part2"));
                            }
                            part2__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Part3 => {
                            if part3__.is_some() {
                                return Err(serde::de::Error::duplicate_field("part3"));
                            }
                            part3__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Sha1Sum {
                    part1: part1__.unwrap_or_default(),
                    part2: part2__.unwrap_or_default(),
                    part3: part3__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("protocol.SHA1Sum", FIELDS, GeneratedVisitor)
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
            Self::JournalNotFound => "JOURNAL_NOT_FOUND",
            Self::NoJournalPrimaryBroker => "NO_JOURNAL_PRIMARY_BROKER",
            Self::NotJournalPrimaryBroker => "NOT_JOURNAL_PRIMARY_BROKER",
            Self::NotJournalBroker => "NOT_JOURNAL_BROKER",
            Self::InsufficientJournalBrokers => "INSUFFICIENT_JOURNAL_BROKERS",
            Self::OffsetNotYetAvailable => "OFFSET_NOT_YET_AVAILABLE",
            Self::WrongRoute => "WRONG_ROUTE",
            Self::ProposalMismatch => "PROPOSAL_MISMATCH",
            Self::EtcdTransactionFailed => "ETCD_TRANSACTION_FAILED",
            Self::NotAllowed => "NOT_ALLOWED",
            Self::WrongAppendOffset => "WRONG_APPEND_OFFSET",
            Self::IndexHasGreaterOffset => "INDEX_HAS_GREATER_OFFSET",
            Self::RegisterMismatch => "REGISTER_MISMATCH",
            Self::Suspended => "SUSPENDED",
            Self::FragmentStoreUnhealthy => "FRAGMENT_STORE_UNHEALTHY",
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
            "JOURNAL_NOT_FOUND",
            "NO_JOURNAL_PRIMARY_BROKER",
            "NOT_JOURNAL_PRIMARY_BROKER",
            "NOT_JOURNAL_BROKER",
            "INSUFFICIENT_JOURNAL_BROKERS",
            "OFFSET_NOT_YET_AVAILABLE",
            "WRONG_ROUTE",
            "PROPOSAL_MISMATCH",
            "ETCD_TRANSACTION_FAILED",
            "NOT_ALLOWED",
            "WRONG_APPEND_OFFSET",
            "INDEX_HAS_GREATER_OFFSET",
            "REGISTER_MISMATCH",
            "SUSPENDED",
            "FRAGMENT_STORE_UNHEALTHY",
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
                    "JOURNAL_NOT_FOUND" => Ok(Status::JournalNotFound),
                    "NO_JOURNAL_PRIMARY_BROKER" => Ok(Status::NoJournalPrimaryBroker),
                    "NOT_JOURNAL_PRIMARY_BROKER" => Ok(Status::NotJournalPrimaryBroker),
                    "NOT_JOURNAL_BROKER" => Ok(Status::NotJournalBroker),
                    "INSUFFICIENT_JOURNAL_BROKERS" => Ok(Status::InsufficientJournalBrokers),
                    "OFFSET_NOT_YET_AVAILABLE" => Ok(Status::OffsetNotYetAvailable),
                    "WRONG_ROUTE" => Ok(Status::WrongRoute),
                    "PROPOSAL_MISMATCH" => Ok(Status::ProposalMismatch),
                    "ETCD_TRANSACTION_FAILED" => Ok(Status::EtcdTransactionFailed),
                    "NOT_ALLOWED" => Ok(Status::NotAllowed),
                    "WRONG_APPEND_OFFSET" => Ok(Status::WrongAppendOffset),
                    "INDEX_HAS_GREATER_OFFSET" => Ok(Status::IndexHasGreaterOffset),
                    "REGISTER_MISMATCH" => Ok(Status::RegisterMismatch),
                    "SUSPENDED" => Ok(Status::Suspended),
                    "FRAGMENT_STORE_UNHEALTHY" => Ok(Status::FragmentStoreUnhealthy),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
