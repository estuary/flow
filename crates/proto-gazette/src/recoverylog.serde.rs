impl serde::Serialize for FsmHints {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.log.is_empty() {
            len += 1;
        }
        if !self.live_nodes.is_empty() {
            len += 1;
        }
        if !self.properties.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.FSMHints", len)?;
        if !self.log.is_empty() {
            struct_ser.serialize_field("log", &self.log)?;
        }
        if !self.live_nodes.is_empty() {
            struct_ser.serialize_field("liveNodes", &self.live_nodes)?;
        }
        if !self.properties.is_empty() {
            struct_ser.serialize_field("properties", &self.properties)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FsmHints {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log",
            "live_nodes",
            "liveNodes",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Log,
            LiveNodes,
            Properties,
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
                            "log" => Ok(GeneratedField::Log),
                            "liveNodes" | "live_nodes" => Ok(GeneratedField::LiveNodes),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FsmHints;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.FSMHints")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FsmHints, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut log__ = None;
                let mut live_nodes__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Log => {
                            if log__.is_some() {
                                return Err(serde::de::Error::duplicate_field("log"));
                            }
                            log__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LiveNodes => {
                            if live_nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("liveNodes"));
                            }
                            live_nodes__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FsmHints {
                    log: log__.unwrap_or_default(),
                    live_nodes: live_nodes__.unwrap_or_default(),
                    properties: properties__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.FSMHints", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FnodeSegments {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fnode != 0 {
            len += 1;
        }
        if !self.segments.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.FnodeSegments", len)?;
        if self.fnode != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("fnode", ToString::to_string(&self.fnode).as_str())?;
        }
        if !self.segments.is_empty() {
            struct_ser.serialize_field("segments", &self.segments)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FnodeSegments {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fnode",
            "segments",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fnode,
            Segments,
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
                            "fnode" => Ok(GeneratedField::Fnode),
                            "segments" => Ok(GeneratedField::Segments),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FnodeSegments;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.FnodeSegments")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FnodeSegments, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fnode__ = None;
                let mut segments__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Fnode => {
                            if fnode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fnode"));
                            }
                            fnode__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Segments => {
                            if segments__.is_some() {
                                return Err(serde::de::Error::duplicate_field("segments"));
                            }
                            segments__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FnodeSegments {
                    fnode: fnode__.unwrap_or_default(),
                    segments: segments__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.FnodeSegments", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Property {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if !self.content.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.Property", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if !self.content.is_empty() {
            struct_ser.serialize_field("content", &self.content)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Property {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "content",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
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
                            "path" => Ok(GeneratedField::Path),
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
            type Value = Property;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.Property")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Property, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut content__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Content => {
                            if content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("content"));
                            }
                            content__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Property {
                    path: path__.unwrap_or_default(),
                    content: content__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.Property", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RecordedOp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seq_no != 0 {
            len += 1;
        }
        if self.checksum != 0 {
            len += 1;
        }
        if self.author != 0 {
            len += 1;
        }
        if self.first_offset != 0 {
            len += 1;
        }
        if self.last_offset != 0 {
            len += 1;
        }
        if !self.log.is_empty() {
            len += 1;
        }
        if self.create.is_some() {
            len += 1;
        }
        if self.link.is_some() {
            len += 1;
        }
        if self.unlink.is_some() {
            len += 1;
        }
        if self.write.is_some() {
            len += 1;
        }
        if self.property.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.RecordedOp", len)?;
        if self.seq_no != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqNo", ToString::to_string(&self.seq_no).as_str())?;
        }
        if self.checksum != 0 {
            struct_ser.serialize_field("checksum", &self.checksum)?;
        }
        if self.author != 0 {
            struct_ser.serialize_field("author", &self.author)?;
        }
        if self.first_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("firstOffset", ToString::to_string(&self.first_offset).as_str())?;
        }
        if self.last_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastOffset", ToString::to_string(&self.last_offset).as_str())?;
        }
        if !self.log.is_empty() {
            struct_ser.serialize_field("log", &self.log)?;
        }
        if let Some(v) = self.create.as_ref() {
            struct_ser.serialize_field("create", v)?;
        }
        if let Some(v) = self.link.as_ref() {
            struct_ser.serialize_field("link", v)?;
        }
        if let Some(v) = self.unlink.as_ref() {
            struct_ser.serialize_field("unlink", v)?;
        }
        if let Some(v) = self.write.as_ref() {
            struct_ser.serialize_field("write", v)?;
        }
        if let Some(v) = self.property.as_ref() {
            struct_ser.serialize_field("property", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RecordedOp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seq_no",
            "seqNo",
            "checksum",
            "author",
            "first_offset",
            "firstOffset",
            "last_offset",
            "lastOffset",
            "log",
            "create",
            "link",
            "unlink",
            "write",
            "property",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SeqNo,
            Checksum,
            Author,
            FirstOffset,
            LastOffset,
            Log,
            Create,
            Link,
            Unlink,
            Write,
            Property,
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
                            "seqNo" | "seq_no" => Ok(GeneratedField::SeqNo),
                            "checksum" => Ok(GeneratedField::Checksum),
                            "author" => Ok(GeneratedField::Author),
                            "firstOffset" | "first_offset" => Ok(GeneratedField::FirstOffset),
                            "lastOffset" | "last_offset" => Ok(GeneratedField::LastOffset),
                            "log" => Ok(GeneratedField::Log),
                            "create" => Ok(GeneratedField::Create),
                            "link" => Ok(GeneratedField::Link),
                            "unlink" => Ok(GeneratedField::Unlink),
                            "write" => Ok(GeneratedField::Write),
                            "property" => Ok(GeneratedField::Property),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RecordedOp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.RecordedOp")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RecordedOp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seq_no__ = None;
                let mut checksum__ = None;
                let mut author__ = None;
                let mut first_offset__ = None;
                let mut last_offset__ = None;
                let mut log__ = None;
                let mut create__ = None;
                let mut link__ = None;
                let mut unlink__ = None;
                let mut write__ = None;
                let mut property__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SeqNo => {
                            if seq_no__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seqNo"));
                            }
                            seq_no__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Checksum => {
                            if checksum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checksum"));
                            }
                            checksum__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Author => {
                            if author__.is_some() {
                                return Err(serde::de::Error::duplicate_field("author"));
                            }
                            author__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FirstOffset => {
                            if first_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("firstOffset"));
                            }
                            first_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastOffset => {
                            if last_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastOffset"));
                            }
                            last_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Log => {
                            if log__.is_some() {
                                return Err(serde::de::Error::duplicate_field("log"));
                            }
                            log__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Create => {
                            if create__.is_some() {
                                return Err(serde::de::Error::duplicate_field("create"));
                            }
                            create__ = map_.next_value()?;
                        }
                        GeneratedField::Link => {
                            if link__.is_some() {
                                return Err(serde::de::Error::duplicate_field("link"));
                            }
                            link__ = map_.next_value()?;
                        }
                        GeneratedField::Unlink => {
                            if unlink__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unlink"));
                            }
                            unlink__ = map_.next_value()?;
                        }
                        GeneratedField::Write => {
                            if write__.is_some() {
                                return Err(serde::de::Error::duplicate_field("write"));
                            }
                            write__ = map_.next_value()?;
                        }
                        GeneratedField::Property => {
                            if property__.is_some() {
                                return Err(serde::de::Error::duplicate_field("property"));
                            }
                            property__ = map_.next_value()?;
                        }
                    }
                }
                Ok(RecordedOp {
                    seq_no: seq_no__.unwrap_or_default(),
                    checksum: checksum__.unwrap_or_default(),
                    author: author__.unwrap_or_default(),
                    first_offset: first_offset__.unwrap_or_default(),
                    last_offset: last_offset__.unwrap_or_default(),
                    log: log__.unwrap_or_default(),
                    create: create__,
                    link: link__,
                    unlink: unlink__,
                    write: write__,
                    property: property__,
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.RecordedOp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for recorded_op::Create {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.RecordedOp.Create", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for recorded_op::Create {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
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
                            "path" => Ok(GeneratedField::Path),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = recorded_op::Create;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.RecordedOp.Create")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<recorded_op::Create, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(recorded_op::Create {
                    path: path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.RecordedOp.Create", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for recorded_op::Link {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fnode != 0 {
            len += 1;
        }
        if !self.path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.RecordedOp.Link", len)?;
        if self.fnode != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("fnode", ToString::to_string(&self.fnode).as_str())?;
        }
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for recorded_op::Link {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fnode",
            "path",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fnode,
            Path,
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
                            "fnode" => Ok(GeneratedField::Fnode),
                            "path" => Ok(GeneratedField::Path),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = recorded_op::Link;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.RecordedOp.Link")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<recorded_op::Link, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fnode__ = None;
                let mut path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Fnode => {
                            if fnode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fnode"));
                            }
                            fnode__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(recorded_op::Link {
                    fnode: fnode__.unwrap_or_default(),
                    path: path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.RecordedOp.Link", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for recorded_op::Write {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fnode != 0 {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if self.length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.RecordedOp.Write", len)?;
        if self.fnode != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("fnode", ToString::to_string(&self.fnode).as_str())?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if self.length != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("length", ToString::to_string(&self.length).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for recorded_op::Write {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fnode",
            "offset",
            "length",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fnode,
            Offset,
            Length,
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
                            "fnode" => Ok(GeneratedField::Fnode),
                            "offset" => Ok(GeneratedField::Offset),
                            "length" => Ok(GeneratedField::Length),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = recorded_op::Write;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.RecordedOp.Write")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<recorded_op::Write, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fnode__ = None;
                let mut offset__ = None;
                let mut length__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Fnode => {
                            if fnode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fnode"));
                            }
                            fnode__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(recorded_op::Write {
                    fnode: fnode__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                    length: length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.RecordedOp.Write", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Segment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.author != 0 {
            len += 1;
        }
        if self.first_seq_no != 0 {
            len += 1;
        }
        if self.first_offset != 0 {
            len += 1;
        }
        if self.first_checksum != 0 {
            len += 1;
        }
        if self.last_seq_no != 0 {
            len += 1;
        }
        if self.last_offset != 0 {
            len += 1;
        }
        if !self.log.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("recoverylog.Segment", len)?;
        if self.author != 0 {
            struct_ser.serialize_field("author", &self.author)?;
        }
        if self.first_seq_no != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("firstSeqNo", ToString::to_string(&self.first_seq_no).as_str())?;
        }
        if self.first_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("firstOffset", ToString::to_string(&self.first_offset).as_str())?;
        }
        if self.first_checksum != 0 {
            struct_ser.serialize_field("firstChecksum", &self.first_checksum)?;
        }
        if self.last_seq_no != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastSeqNo", ToString::to_string(&self.last_seq_no).as_str())?;
        }
        if self.last_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastOffset", ToString::to_string(&self.last_offset).as_str())?;
        }
        if !self.log.is_empty() {
            struct_ser.serialize_field("log", &self.log)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Segment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "author",
            "first_seq_no",
            "firstSeqNo",
            "first_offset",
            "firstOffset",
            "first_checksum",
            "firstChecksum",
            "last_seq_no",
            "lastSeqNo",
            "last_offset",
            "lastOffset",
            "log",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Author,
            FirstSeqNo,
            FirstOffset,
            FirstChecksum,
            LastSeqNo,
            LastOffset,
            Log,
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
                            "author" => Ok(GeneratedField::Author),
                            "firstSeqNo" | "first_seq_no" => Ok(GeneratedField::FirstSeqNo),
                            "firstOffset" | "first_offset" => Ok(GeneratedField::FirstOffset),
                            "firstChecksum" | "first_checksum" => Ok(GeneratedField::FirstChecksum),
                            "lastSeqNo" | "last_seq_no" => Ok(GeneratedField::LastSeqNo),
                            "lastOffset" | "last_offset" => Ok(GeneratedField::LastOffset),
                            "log" => Ok(GeneratedField::Log),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Segment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct recoverylog.Segment")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Segment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut author__ = None;
                let mut first_seq_no__ = None;
                let mut first_offset__ = None;
                let mut first_checksum__ = None;
                let mut last_seq_no__ = None;
                let mut last_offset__ = None;
                let mut log__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Author => {
                            if author__.is_some() {
                                return Err(serde::de::Error::duplicate_field("author"));
                            }
                            author__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FirstSeqNo => {
                            if first_seq_no__.is_some() {
                                return Err(serde::de::Error::duplicate_field("firstSeqNo"));
                            }
                            first_seq_no__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FirstOffset => {
                            if first_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("firstOffset"));
                            }
                            first_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FirstChecksum => {
                            if first_checksum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("firstChecksum"));
                            }
                            first_checksum__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastSeqNo => {
                            if last_seq_no__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastSeqNo"));
                            }
                            last_seq_no__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastOffset => {
                            if last_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastOffset"));
                            }
                            last_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Log => {
                            if log__.is_some() {
                                return Err(serde::de::Error::duplicate_field("log"));
                            }
                            log__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Segment {
                    author: author__.unwrap_or_default(),
                    first_seq_no: first_seq_no__.unwrap_or_default(),
                    first_offset: first_offset__.unwrap_or_default(),
                    first_checksum: first_checksum__.unwrap_or_default(),
                    last_seq_no: last_seq_no__.unwrap_or_default(),
                    last_offset: last_offset__.unwrap_or_default(),
                    log: log__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("recoverylog.Segment", FIELDS, GeneratedVisitor)
    }
}
