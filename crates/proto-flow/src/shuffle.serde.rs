impl serde::Serialize for CollectionPartitions {
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
        if self.partition_selector.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.CollectionPartitions", len)?;
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if let Some(v) = self.partition_selector.as_ref() {
            struct_ser.serialize_field("partitionSelector", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CollectionPartitions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "collection",
            "partition_selector",
            "partitionSelector",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Collection,
            PartitionSelector,
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
                            "partitionSelector" | "partition_selector" => Ok(GeneratedField::PartitionSelector),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CollectionPartitions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.CollectionPartitions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CollectionPartitions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut collection__ = None;
                let mut partition_selector__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map_.next_value()?;
                        }
                        GeneratedField::PartitionSelector => {
                            if partition_selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionSelector"));
                            }
                            partition_selector__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CollectionPartitions {
                    collection: collection__,
                    partition_selector: partition_selector__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.CollectionPartitions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FrontierChunk {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.journals.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.FrontierChunk", len)?;
        if !self.journals.is_empty() {
            struct_ser.serialize_field("journals", &self.journals)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FrontierChunk {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journals",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = FrontierChunk;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.FrontierChunk")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FrontierChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journals__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Journals => {
                            if journals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journals"));
                            }
                            journals__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FrontierChunk {
                    journals: journals__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.FrontierChunk", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JournalFrontier {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.journal_name_truncate_delta != 0 {
            len += 1;
        }
        if !self.journal_name_suffix.is_empty() {
            len += 1;
        }
        if self.binding != 0 {
            len += 1;
        }
        if !self.producers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.JournalFrontier", len)?;
        if self.journal_name_truncate_delta != 0 {
            struct_ser.serialize_field("journalNameTruncateDelta", &self.journal_name_truncate_delta)?;
        }
        if !self.journal_name_suffix.is_empty() {
            struct_ser.serialize_field("journalNameSuffix", &self.journal_name_suffix)?;
        }
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.producers.is_empty() {
            struct_ser.serialize_field("producers", &self.producers)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JournalFrontier {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal_name_truncate_delta",
            "journalNameTruncateDelta",
            "journal_name_suffix",
            "journalNameSuffix",
            "binding",
            "producers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JournalNameTruncateDelta,
            JournalNameSuffix,
            Binding,
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
                            "journalNameTruncateDelta" | "journal_name_truncate_delta" => Ok(GeneratedField::JournalNameTruncateDelta),
                            "journalNameSuffix" | "journal_name_suffix" => Ok(GeneratedField::JournalNameSuffix),
                            "binding" => Ok(GeneratedField::Binding),
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
            type Value = JournalFrontier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.JournalFrontier")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JournalFrontier, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal_name_truncate_delta__ = None;
                let mut journal_name_suffix__ = None;
                let mut binding__ = None;
                let mut producers__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::JournalNameTruncateDelta => {
                            if journal_name_truncate_delta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalNameTruncateDelta"));
                            }
                            journal_name_truncate_delta__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::JournalNameSuffix => {
                            if journal_name_suffix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalNameSuffix"));
                            }
                            journal_name_suffix__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Binding => {
                            if binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binding"));
                            }
                            binding__ = 
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
                Ok(JournalFrontier {
                    journal_name_truncate_delta: journal_name_truncate_delta__.unwrap_or_default(),
                    journal_name_suffix: journal_name_suffix__.unwrap_or_default(),
                    binding: binding__.unwrap_or_default(),
                    producers: producers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.JournalFrontier", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Member {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.range.is_some() {
            len += 1;
        }
        if !self.endpoint.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.Member", len)?;
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.endpoint.is_empty() {
            struct_ser.serialize_field("endpoint", &self.endpoint)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Member {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "range",
            "endpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Range,
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
                            "range" => Ok(GeneratedField::Range),
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
            type Value = Member;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.Member")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Member, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut range__ = None;
                let mut endpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map_.next_value()?;
                        }
                        GeneratedField::Endpoint => {
                            if endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endpoint"));
                            }
                            endpoint__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Member {
                    range: range__,
                    endpoint: endpoint__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.Member", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProducerFrontier {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.producer != 0 {
            len += 1;
        }
        if self.last_commit != 0 {
            len += 1;
        }
        if self.hinted_commit != 0 {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.ProducerFrontier", len)?;
        if self.producer != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("producer", ToString::to_string(&self.producer).as_str())?;
        }
        if self.last_commit != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastCommit", ToString::to_string(&self.last_commit).as_str())?;
        }
        if self.hinted_commit != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hintedCommit", ToString::to_string(&self.hinted_commit).as_str())?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProducerFrontier {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "producer",
            "last_commit",
            "lastCommit",
            "hinted_commit",
            "hintedCommit",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Producer,
            LastCommit,
            HintedCommit,
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
                            "producer" => Ok(GeneratedField::Producer),
                            "lastCommit" | "last_commit" => Ok(GeneratedField::LastCommit),
                            "hintedCommit" | "hinted_commit" => Ok(GeneratedField::HintedCommit),
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
            type Value = ProducerFrontier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.ProducerFrontier")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProducerFrontier, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut producer__ = None;
                let mut last_commit__ = None;
                let mut hinted_commit__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Producer => {
                            if producer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("producer"));
                            }
                            producer__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastCommit => {
                            if last_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastCommit"));
                            }
                            last_commit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HintedCommit => {
                            if hinted_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintedCommit"));
                            }
                            hinted_commit__ = 
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
                    }
                }
                Ok(ProducerFrontier {
                    producer: producer__.unwrap_or_default(),
                    last_commit: last_commit__.unwrap_or_default(),
                    hinted_commit: hinted_commit__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.ProducerFrontier", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QueueRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.open.is_some() {
            len += 1;
        }
        if self.enqueue.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.enqueue.as_ref() {
            struct_ser.serialize_field("enqueue", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QueueRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "enqueue",
            "flush",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            Enqueue,
            Flush,
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
                            "open" => Ok(GeneratedField::Open),
                            "enqueue" => Ok(GeneratedField::Enqueue),
                            "flush" => Ok(GeneratedField::Flush),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QueueRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<QueueRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut enqueue__ = None;
                let mut flush__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Enqueue => {
                            if enqueue__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enqueue"));
                            }
                            enqueue__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                    }
                }
                Ok(QueueRequest {
                    open: open__,
                    enqueue: enqueue__,
                    flush: flush__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for queue_request::Enqueue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.journal_name_truncate_delta != 0 {
            len += 1;
        }
        if !self.journal_name_suffix.is_empty() {
            len += 1;
        }
        if self.begin_offset != 0 {
            len += 1;
        }
        if self.binding != 0 {
            len += 1;
        }
        if self.priority != 0 {
            len += 1;
        }
        if self.producer != 0 {
            len += 1;
        }
        if self.adjusted_clock != 0 {
            len += 1;
        }
        if !self.packed_key.is_empty() {
            len += 1;
        }
        if !self.doc_archived.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueRequest.Enqueue", len)?;
        if self.journal_name_truncate_delta != 0 {
            struct_ser.serialize_field("journalNameTruncateDelta", &self.journal_name_truncate_delta)?;
        }
        if !self.journal_name_suffix.is_empty() {
            struct_ser.serialize_field("journalNameSuffix", &self.journal_name_suffix)?;
        }
        if self.begin_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("beginOffset", ToString::to_string(&self.begin_offset).as_str())?;
        }
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if self.priority != 0 {
            struct_ser.serialize_field("priority", &self.priority)?;
        }
        if self.producer != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("producer", ToString::to_string(&self.producer).as_str())?;
        }
        if self.adjusted_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("adjustedClock", ToString::to_string(&self.adjusted_clock).as_str())?;
        }
        if !self.packed_key.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("packedKey", pbjson::private::base64::encode(&self.packed_key).as_str())?;
        }
        if !self.doc_archived.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("docArchived", pbjson::private::base64::encode(&self.doc_archived).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for queue_request::Enqueue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal_name_truncate_delta",
            "journalNameTruncateDelta",
            "journal_name_suffix",
            "journalNameSuffix",
            "begin_offset",
            "beginOffset",
            "binding",
            "priority",
            "producer",
            "adjusted_clock",
            "adjustedClock",
            "packed_key",
            "packedKey",
            "doc_archived",
            "docArchived",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JournalNameTruncateDelta,
            JournalNameSuffix,
            BeginOffset,
            Binding,
            Priority,
            Producer,
            AdjustedClock,
            PackedKey,
            DocArchived,
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
                            "journalNameTruncateDelta" | "journal_name_truncate_delta" => Ok(GeneratedField::JournalNameTruncateDelta),
                            "journalNameSuffix" | "journal_name_suffix" => Ok(GeneratedField::JournalNameSuffix),
                            "beginOffset" | "begin_offset" => Ok(GeneratedField::BeginOffset),
                            "binding" => Ok(GeneratedField::Binding),
                            "priority" => Ok(GeneratedField::Priority),
                            "producer" => Ok(GeneratedField::Producer),
                            "adjustedClock" | "adjusted_clock" => Ok(GeneratedField::AdjustedClock),
                            "packedKey" | "packed_key" => Ok(GeneratedField::PackedKey),
                            "docArchived" | "doc_archived" => Ok(GeneratedField::DocArchived),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = queue_request::Enqueue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueRequest.Enqueue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<queue_request::Enqueue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal_name_truncate_delta__ = None;
                let mut journal_name_suffix__ = None;
                let mut begin_offset__ = None;
                let mut binding__ = None;
                let mut priority__ = None;
                let mut producer__ = None;
                let mut adjusted_clock__ = None;
                let mut packed_key__ = None;
                let mut doc_archived__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::JournalNameTruncateDelta => {
                            if journal_name_truncate_delta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalNameTruncateDelta"));
                            }
                            journal_name_truncate_delta__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::JournalNameSuffix => {
                            if journal_name_suffix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalNameSuffix"));
                            }
                            journal_name_suffix__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BeginOffset => {
                            if begin_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("beginOffset"));
                            }
                            begin_offset__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Binding => {
                            if binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binding"));
                            }
                            binding__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Priority => {
                            if priority__.is_some() {
                                return Err(serde::de::Error::duplicate_field("priority"));
                            }
                            priority__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Producer => {
                            if producer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("producer"));
                            }
                            producer__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::AdjustedClock => {
                            if adjusted_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adjustedClock"));
                            }
                            adjusted_clock__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PackedKey => {
                            if packed_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packedKey"));
                            }
                            packed_key__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DocArchived => {
                            if doc_archived__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docArchived"));
                            }
                            doc_archived__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(queue_request::Enqueue {
                    journal_name_truncate_delta: journal_name_truncate_delta__.unwrap_or_default(),
                    journal_name_suffix: journal_name_suffix__.unwrap_or_default(),
                    begin_offset: begin_offset__.unwrap_or_default(),
                    binding: binding__.unwrap_or_default(),
                    priority: priority__.unwrap_or_default(),
                    producer: producer__.unwrap_or_default(),
                    adjusted_clock: adjusted_clock__.unwrap_or_default(),
                    packed_key: packed_key__.unwrap_or_default(),
                    doc_archived: doc_archived__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueRequest.Enqueue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for queue_request::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seq != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueRequest.Flush", len)?;
        if self.seq != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seq", ToString::to_string(&self.seq).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for queue_request::Flush {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seq",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seq,
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
                            "seq" => Ok(GeneratedField::Seq),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = queue_request::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueRequest.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<queue_request::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seq__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Seq => {
                            if seq__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seq"));
                            }
                            seq__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(queue_request::Flush {
                    seq: seq__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueRequest.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for queue_request::Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.session_id != 0 {
            len += 1;
        }
        if !self.members.is_empty() {
            len += 1;
        }
        if self.slice_member_index != 0 {
            len += 1;
        }
        if self.queue_member_index != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueRequest.Open", len)?;
        if self.session_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sessionId", ToString::to_string(&self.session_id).as_str())?;
        }
        if !self.members.is_empty() {
            struct_ser.serialize_field("members", &self.members)?;
        }
        if self.slice_member_index != 0 {
            struct_ser.serialize_field("sliceMemberIndex", &self.slice_member_index)?;
        }
        if self.queue_member_index != 0 {
            struct_ser.serialize_field("queueMemberIndex", &self.queue_member_index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for queue_request::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "session_id",
            "sessionId",
            "members",
            "slice_member_index",
            "sliceMemberIndex",
            "queue_member_index",
            "queueMemberIndex",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SessionId,
            Members,
            SliceMemberIndex,
            QueueMemberIndex,
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
                            "sessionId" | "session_id" => Ok(GeneratedField::SessionId),
                            "members" => Ok(GeneratedField::Members),
                            "sliceMemberIndex" | "slice_member_index" => Ok(GeneratedField::SliceMemberIndex),
                            "queueMemberIndex" | "queue_member_index" => Ok(GeneratedField::QueueMemberIndex),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = queue_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueRequest.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<queue_request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut session_id__ = None;
                let mut members__ = None;
                let mut slice_member_index__ = None;
                let mut queue_member_index__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SessionId => {
                            if session_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionId"));
                            }
                            session_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Members => {
                            if members__.is_some() {
                                return Err(serde::de::Error::duplicate_field("members"));
                            }
                            members__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SliceMemberIndex => {
                            if slice_member_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sliceMemberIndex"));
                            }
                            slice_member_index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::QueueMemberIndex => {
                            if queue_member_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("queueMemberIndex"));
                            }
                            queue_member_index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(queue_request::Open {
                    session_id: session_id__.unwrap_or_default(),
                    members: members__.unwrap_or_default(),
                    slice_member_index: slice_member_index__.unwrap_or_default(),
                    queue_member_index: queue_member_index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for QueueResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.opened.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueResponse", len)?;
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for QueueResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "opened",
            "flushed",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Opened,
            Flushed,
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
                            "opened" => Ok(GeneratedField::Opened),
                            "flushed" => Ok(GeneratedField::Flushed),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = QueueResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<QueueResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut opened__ = None;
                let mut flushed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                    }
                }
                Ok(QueueResponse {
                    opened: opened__,
                    flushed: flushed__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for queue_response::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.seq != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.QueueResponse.Flushed", len)?;
        if self.seq != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seq", ToString::to_string(&self.seq).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for queue_response::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seq",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Seq,
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
                            "seq" => Ok(GeneratedField::Seq),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = queue_response::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueResponse.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<queue_response::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seq__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Seq => {
                            if seq__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seq"));
                            }
                            seq__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(queue_response::Flushed {
                    seq: seq__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueResponse.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for queue_response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.QueueResponse.Opened", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for queue_response::Opened {
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
            type Value = queue_response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.QueueResponse.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<queue_response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(queue_response::Opened {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.QueueResponse.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SessionRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.open.is_some() {
            len += 1;
        }
        if self.resume_checkpoint_chunk.is_some() {
            len += 1;
        }
        if self.next_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SessionRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.resume_checkpoint_chunk.as_ref() {
            struct_ser.serialize_field("resumeCheckpointChunk", v)?;
        }
        if let Some(v) = self.next_checkpoint.as_ref() {
            struct_ser.serialize_field("nextCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SessionRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "resume_checkpoint_chunk",
            "resumeCheckpointChunk",
            "next_checkpoint",
            "nextCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            ResumeCheckpointChunk,
            NextCheckpoint,
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
                            "open" => Ok(GeneratedField::Open),
                            "resumeCheckpointChunk" | "resume_checkpoint_chunk" => Ok(GeneratedField::ResumeCheckpointChunk),
                            "nextCheckpoint" | "next_checkpoint" => Ok(GeneratedField::NextCheckpoint),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SessionRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SessionRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SessionRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut resume_checkpoint_chunk__ = None;
                let mut next_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::ResumeCheckpointChunk => {
                            if resume_checkpoint_chunk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resumeCheckpointChunk"));
                            }
                            resume_checkpoint_chunk__ = map_.next_value()?;
                        }
                        GeneratedField::NextCheckpoint => {
                            if next_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nextCheckpoint"));
                            }
                            next_checkpoint__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SessionRequest {
                    open: open__,
                    resume_checkpoint_chunk: resume_checkpoint_chunk__,
                    next_checkpoint: next_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SessionRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for session_request::NextCheckpoint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.SessionRequest.NextCheckpoint", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for session_request::NextCheckpoint {
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
            type Value = session_request::NextCheckpoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SessionRequest.NextCheckpoint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<session_request::NextCheckpoint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(session_request::NextCheckpoint {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SessionRequest.NextCheckpoint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for session_request::Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.session_id != 0 {
            len += 1;
        }
        if self.task.is_some() {
            len += 1;
        }
        if !self.members.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SessionRequest.Open", len)?;
        if self.session_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sessionId", ToString::to_string(&self.session_id).as_str())?;
        }
        if let Some(v) = self.task.as_ref() {
            struct_ser.serialize_field("task", v)?;
        }
        if !self.members.is_empty() {
            struct_ser.serialize_field("members", &self.members)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for session_request::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "session_id",
            "sessionId",
            "task",
            "members",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SessionId,
            Task,
            Members,
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
                            "sessionId" | "session_id" => Ok(GeneratedField::SessionId),
                            "task" => Ok(GeneratedField::Task),
                            "members" => Ok(GeneratedField::Members),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = session_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SessionRequest.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<session_request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut session_id__ = None;
                let mut task__ = None;
                let mut members__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SessionId => {
                            if session_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionId"));
                            }
                            session_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Task => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("task"));
                            }
                            task__ = map_.next_value()?;
                        }
                        GeneratedField::Members => {
                            if members__.is_some() {
                                return Err(serde::de::Error::duplicate_field("members"));
                            }
                            members__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(session_request::Open {
                    session_id: session_id__.unwrap_or_default(),
                    task: task__,
                    members: members__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SessionRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SessionResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.opened.is_some() {
            len += 1;
        }
        if self.next_checkpoint_chunk.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SessionResponse", len)?;
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.next_checkpoint_chunk.as_ref() {
            struct_ser.serialize_field("nextCheckpointChunk", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SessionResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "opened",
            "next_checkpoint_chunk",
            "nextCheckpointChunk",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Opened,
            NextCheckpointChunk,
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
                            "opened" => Ok(GeneratedField::Opened),
                            "nextCheckpointChunk" | "next_checkpoint_chunk" => Ok(GeneratedField::NextCheckpointChunk),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SessionResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SessionResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SessionResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut opened__ = None;
                let mut next_checkpoint_chunk__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::NextCheckpointChunk => {
                            if next_checkpoint_chunk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nextCheckpointChunk"));
                            }
                            next_checkpoint_chunk__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SessionResponse {
                    opened: opened__,
                    next_checkpoint_chunk: next_checkpoint_chunk__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SessionResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for session_response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.SessionResponse.Opened", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for session_response::Opened {
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
            type Value = session_response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SessionResponse.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<session_response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(session_response::Opened {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SessionResponse.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SliceRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.open.is_some() {
            len += 1;
        }
        if self.start.is_some() {
            len += 1;
        }
        if self.start_read.is_some() {
            len += 1;
        }
        if self.progress.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SliceRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.start_read.as_ref() {
            struct_ser.serialize_field("startRead", v)?;
        }
        if let Some(v) = self.progress.as_ref() {
            struct_ser.serialize_field("progress", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SliceRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "start",
            "start_read",
            "startRead",
            "progress",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            Start,
            StartRead,
            Progress,
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
                            "open" => Ok(GeneratedField::Open),
                            "start" => Ok(GeneratedField::Start),
                            "startRead" | "start_read" => Ok(GeneratedField::StartRead),
                            "progress" => Ok(GeneratedField::Progress),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SliceRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SliceRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut start__ = None;
                let mut start_read__ = None;
                let mut progress__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::StartRead => {
                            if start_read__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startRead"));
                            }
                            start_read__ = map_.next_value()?;
                        }
                        GeneratedField::Progress => {
                            if progress__.is_some() {
                                return Err(serde::de::Error::duplicate_field("progress"));
                            }
                            progress__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SliceRequest {
                    open: open__,
                    start: start__,
                    start_read: start_read__,
                    progress: progress__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_request::Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.session_id != 0 {
            len += 1;
        }
        if self.task.is_some() {
            len += 1;
        }
        if !self.members.is_empty() {
            len += 1;
        }
        if self.member_index != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SliceRequest.Open", len)?;
        if self.session_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sessionId", ToString::to_string(&self.session_id).as_str())?;
        }
        if let Some(v) = self.task.as_ref() {
            struct_ser.serialize_field("task", v)?;
        }
        if !self.members.is_empty() {
            struct_ser.serialize_field("members", &self.members)?;
        }
        if self.member_index != 0 {
            struct_ser.serialize_field("memberIndex", &self.member_index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_request::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "session_id",
            "sessionId",
            "task",
            "members",
            "member_index",
            "memberIndex",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SessionId,
            Task,
            Members,
            MemberIndex,
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
                            "sessionId" | "session_id" => Ok(GeneratedField::SessionId),
                            "task" => Ok(GeneratedField::Task),
                            "members" => Ok(GeneratedField::Members),
                            "memberIndex" | "member_index" => Ok(GeneratedField::MemberIndex),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = slice_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceRequest.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut session_id__ = None;
                let mut task__ = None;
                let mut members__ = None;
                let mut member_index__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SessionId => {
                            if session_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionId"));
                            }
                            session_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Task => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("task"));
                            }
                            task__ = map_.next_value()?;
                        }
                        GeneratedField::Members => {
                            if members__.is_some() {
                                return Err(serde::de::Error::duplicate_field("members"));
                            }
                            members__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MemberIndex => {
                            if member_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("memberIndex"));
                            }
                            member_index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(slice_request::Open {
                    session_id: session_id__.unwrap_or_default(),
                    task: task__,
                    members: members__.unwrap_or_default(),
                    member_index: member_index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_request::Progress {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.SliceRequest.Progress", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_request::Progress {
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
            type Value = slice_request::Progress;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceRequest.Progress")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_request::Progress, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(slice_request::Progress {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceRequest.Progress", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_request::Start {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.SliceRequest.Start", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_request::Start {
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
            type Value = slice_request::Start;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceRequest.Start")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_request::Start, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(slice_request::Start {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceRequest.Start", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_request::StartRead {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.binding != 0 {
            len += 1;
        }
        if self.spec.is_some() {
            len += 1;
        }
        if self.create_revision != 0 {
            len += 1;
        }
        if self.mod_revision != 0 {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        if !self.checkpoint.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SliceRequest.StartRead", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if self.create_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("createRevision", ToString::to_string(&self.create_revision).as_str())?;
        }
        if self.mod_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("modRevision", ToString::to_string(&self.mod_revision).as_str())?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        if !self.checkpoint.is_empty() {
            struct_ser.serialize_field("checkpoint", &self.checkpoint)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_request::StartRead {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "spec",
            "create_revision",
            "createRevision",
            "mod_revision",
            "modRevision",
            "route",
            "checkpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            Spec,
            CreateRevision,
            ModRevision,
            Route,
            Checkpoint,
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
                            "binding" => Ok(GeneratedField::Binding),
                            "spec" => Ok(GeneratedField::Spec),
                            "createRevision" | "create_revision" => Ok(GeneratedField::CreateRevision),
                            "modRevision" | "mod_revision" => Ok(GeneratedField::ModRevision),
                            "route" => Ok(GeneratedField::Route),
                            "checkpoint" => Ok(GeneratedField::Checkpoint),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = slice_request::StartRead;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceRequest.StartRead")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_request::StartRead, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut spec__ = None;
                let mut create_revision__ = None;
                let mut mod_revision__ = None;
                let mut route__ = None;
                let mut checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Binding => {
                            if binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binding"));
                            }
                            binding__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::CreateRevision => {
                            if create_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createRevision"));
                            }
                            create_revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
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
                        GeneratedField::Checkpoint => {
                            if checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checkpoint"));
                            }
                            checkpoint__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(slice_request::StartRead {
                    binding: binding__.unwrap_or_default(),
                    spec: spec__,
                    create_revision: create_revision__.unwrap_or_default(),
                    mod_revision: mod_revision__.unwrap_or_default(),
                    route: route__,
                    checkpoint: checkpoint__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceRequest.StartRead", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SliceResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.opened.is_some() {
            len += 1;
        }
        if self.listing_added.is_some() {
            len += 1;
        }
        if self.progressed.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SliceResponse", len)?;
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.listing_added.as_ref() {
            struct_ser.serialize_field("listingAdded", v)?;
        }
        if let Some(v) = self.progressed.as_ref() {
            struct_ser.serialize_field("progressed", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SliceResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "opened",
            "listing_added",
            "listingAdded",
            "progressed",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Opened,
            ListingAdded,
            Progressed,
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
                            "opened" => Ok(GeneratedField::Opened),
                            "listingAdded" | "listing_added" => Ok(GeneratedField::ListingAdded),
                            "progressed" => Ok(GeneratedField::Progressed),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SliceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SliceResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut opened__ = None;
                let mut listing_added__ = None;
                let mut progressed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::ListingAdded => {
                            if listing_added__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listingAdded"));
                            }
                            listing_added__ = map_.next_value()?;
                        }
                        GeneratedField::Progressed => {
                            if progressed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("progressed"));
                            }
                            progressed__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SliceResponse {
                    opened: opened__,
                    listing_added: listing_added__,
                    progressed: progressed__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_response::ListingAdded {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.binding != 0 {
            len += 1;
        }
        if self.spec.is_some() {
            len += 1;
        }
        if self.create_revision != 0 {
            len += 1;
        }
        if self.mod_revision != 0 {
            len += 1;
        }
        if self.route.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SliceResponse.ListingAdded", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if self.create_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("createRevision", ToString::to_string(&self.create_revision).as_str())?;
        }
        if self.mod_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("modRevision", ToString::to_string(&self.mod_revision).as_str())?;
        }
        if let Some(v) = self.route.as_ref() {
            struct_ser.serialize_field("route", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_response::ListingAdded {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "spec",
            "create_revision",
            "createRevision",
            "mod_revision",
            "modRevision",
            "route",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            Spec,
            CreateRevision,
            ModRevision,
            Route,
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
                            "binding" => Ok(GeneratedField::Binding),
                            "spec" => Ok(GeneratedField::Spec),
                            "createRevision" | "create_revision" => Ok(GeneratedField::CreateRevision),
                            "modRevision" | "mod_revision" => Ok(GeneratedField::ModRevision),
                            "route" => Ok(GeneratedField::Route),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = slice_response::ListingAdded;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceResponse.ListingAdded")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_response::ListingAdded, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut spec__ = None;
                let mut create_revision__ = None;
                let mut mod_revision__ = None;
                let mut route__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Binding => {
                            if binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binding"));
                            }
                            binding__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::CreateRevision => {
                            if create_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createRevision"));
                            }
                            create_revision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
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
                    }
                }
                Ok(slice_response::ListingAdded {
                    binding: binding__.unwrap_or_default(),
                    spec: spec__,
                    create_revision: create_revision__.unwrap_or_default(),
                    mod_revision: mod_revision__.unwrap_or_default(),
                    route: route__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceResponse.ListingAdded", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for slice_response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.SliceResponse.Opened", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for slice_response::Opened {
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
            type Value = slice_response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.SliceResponse.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<slice_response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(slice_response::Opened {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.SliceResponse.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Task {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.Task", len)?;
        if let Some(v) = self.task.as_ref() {
            match v {
                task::Task::CollectionPartitions(v) => {
                    struct_ser.serialize_field("collectionPartitions", v)?;
                }
                task::Task::Derivation(v) => {
                    struct_ser.serialize_field("derivation", v)?;
                }
                task::Task::Materialization(v) => {
                    struct_ser.serialize_field("materialization", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Task {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "collection_partitions",
            "collectionPartitions",
            "derivation",
            "materialization",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CollectionPartitions,
            Derivation,
            Materialization,
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
                            "collectionPartitions" | "collection_partitions" => Ok(GeneratedField::CollectionPartitions),
                            "derivation" => Ok(GeneratedField::Derivation),
                            "materialization" => Ok(GeneratedField::Materialization),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Task;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.Task")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Task, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::CollectionPartitions => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collectionPartitions"));
                            }
                            task__ = map_.next_value::<::std::option::Option<_>>()?.map(task::Task::CollectionPartitions)
;
                        }
                        GeneratedField::Derivation => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("derivation"));
                            }
                            task__ = map_.next_value::<::std::option::Option<_>>()?.map(task::Task::Derivation)
;
                        }
                        GeneratedField::Materialization => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialization"));
                            }
                            task__ = map_.next_value::<::std::option::Option<_>>()?.map(task::Task::Materialization)
;
                        }
                    }
                }
                Ok(Task {
                    task: task__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.Task", FIELDS, GeneratedVisitor)
    }
}
