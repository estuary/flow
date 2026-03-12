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
        if self.disk_backlog_threshold != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.CollectionPartitions", len)?;
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if let Some(v) = self.partition_selector.as_ref() {
            struct_ser.serialize_field("partitionSelector", v)?;
        }
        if self.disk_backlog_threshold != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("diskBacklogThreshold", ToString::to_string(&self.disk_backlog_threshold).as_str())?;
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
            "disk_backlog_threshold",
            "diskBacklogThreshold",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Collection,
            PartitionSelector,
            DiskBacklogThreshold,
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
                            "diskBacklogThreshold" | "disk_backlog_threshold" => Ok(GeneratedField::DiskBacklogThreshold),
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
                let mut disk_backlog_threshold__ = None;
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
                        GeneratedField::DiskBacklogThreshold => {
                            if disk_backlog_threshold__.is_some() {
                                return Err(serde::de::Error::duplicate_field("diskBacklogThreshold"));
                            }
                            disk_backlog_threshold__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CollectionPartitions {
                    collection: collection__,
                    partition_selector: partition_selector__,
                    disk_backlog_threshold: disk_backlog_threshold__.unwrap_or_default(),
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
        if !self.flushed_lsn.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.FrontierChunk", len)?;
        if !self.journals.is_empty() {
            struct_ser.serialize_field("journals", &self.journals)?;
        }
        if !self.flushed_lsn.is_empty() {
            struct_ser.serialize_field("flushedLsn", &self.flushed_lsn.iter().map(ToString::to_string).collect::<Vec<_>>())?;
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
            "flushed_lsn",
            "flushedLsn",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Journals,
            FlushedLsn,
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
                            "flushedLsn" | "flushed_lsn" => Ok(GeneratedField::FlushedLsn),
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
                let mut flushed_lsn__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Journals => {
                            if journals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journals"));
                            }
                            journals__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FlushedLsn => {
                            if flushed_lsn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushedLsn"));
                            }
                            flushed_lsn__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(FrontierChunk {
                    journals: journals__.unwrap_or_default(),
                    flushed_lsn: flushed_lsn__.unwrap_or_default(),
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
        if self.binding != 0 {
            len += 1;
        }
        if self.journal_name_truncate_delta != 0 {
            len += 1;
        }
        if !self.journal_name_suffix.is_empty() {
            len += 1;
        }
        if !self.producers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.JournalFrontier", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if self.journal_name_truncate_delta != 0 {
            struct_ser.serialize_field("journalNameTruncateDelta", &self.journal_name_truncate_delta)?;
        }
        if !self.journal_name_suffix.is_empty() {
            struct_ser.serialize_field("journalNameSuffix", &self.journal_name_suffix)?;
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
            "binding",
            "journal_name_truncate_delta",
            "journalNameTruncateDelta",
            "journal_name_suffix",
            "journalNameSuffix",
            "producers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            JournalNameTruncateDelta,
            JournalNameSuffix,
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
                            "binding" => Ok(GeneratedField::Binding),
                            "journalNameTruncateDelta" | "journal_name_truncate_delta" => Ok(GeneratedField::JournalNameTruncateDelta),
                            "journalNameSuffix" | "journal_name_suffix" => Ok(GeneratedField::JournalNameSuffix),
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
                let mut binding__ = None;
                let mut journal_name_truncate_delta__ = None;
                let mut journal_name_suffix__ = None;
                let mut producers__ = None;
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
                        GeneratedField::Producers => {
                            if producers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("producers"));
                            }
                            producers__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(JournalFrontier {
                    binding: binding__.unwrap_or_default(),
                    journal_name_truncate_delta: journal_name_truncate_delta__.unwrap_or_default(),
                    journal_name_suffix: journal_name_suffix__.unwrap_or_default(),
                    producers: producers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.JournalFrontier", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogRequest {
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
        if self.append.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.LogRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.append.as_ref() {
            struct_ser.serialize_field("append", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "append",
            "flush",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            Append,
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
                            "append" => Ok(GeneratedField::Append),
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
            type Value = LogRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut append__ = None;
                let mut flush__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Append => {
                            if append__.is_some() {
                                return Err(serde::de::Error::duplicate_field("append"));
                            }
                            append__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                    }
                }
                Ok(LogRequest {
                    open: open__,
                    append: append__,
                    flush: flush__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log_request::Append {
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
        if self.priority != 0 {
            len += 1;
        }
        if self.read_delay != 0 {
            len += 1;
        }
        if self.begin_offset != 0 {
            len += 1;
        }
        if self.producer != 0 {
            len += 1;
        }
        if self.clock != 0 {
            len += 1;
        }
        if !self.packed_key.is_empty() {
            len += 1;
        }
        if !self.doc_archived.is_empty() {
            len += 1;
        }
        if self.valid {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.LogRequest.Append", len)?;
        if self.journal_name_truncate_delta != 0 {
            struct_ser.serialize_field("journalNameTruncateDelta", &self.journal_name_truncate_delta)?;
        }
        if !self.journal_name_suffix.is_empty() {
            struct_ser.serialize_field("journalNameSuffix", &self.journal_name_suffix)?;
        }
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if self.priority != 0 {
            struct_ser.serialize_field("priority", &self.priority)?;
        }
        if self.read_delay != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("readDelay", ToString::to_string(&self.read_delay).as_str())?;
        }
        if self.begin_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("beginOffset", ToString::to_string(&self.begin_offset).as_str())?;
        }
        if self.producer != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("producer", ToString::to_string(&self.producer).as_str())?;
        }
        if self.clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("clock", ToString::to_string(&self.clock).as_str())?;
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
        if self.valid {
            struct_ser.serialize_field("valid", &self.valid)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for log_request::Append {
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
            "priority",
            "read_delay",
            "readDelay",
            "begin_offset",
            "beginOffset",
            "producer",
            "clock",
            "packed_key",
            "packedKey",
            "doc_archived",
            "docArchived",
            "valid",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JournalNameTruncateDelta,
            JournalNameSuffix,
            Binding,
            Priority,
            ReadDelay,
            BeginOffset,
            Producer,
            Clock,
            PackedKey,
            DocArchived,
            Valid,
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
                            "priority" => Ok(GeneratedField::Priority),
                            "readDelay" | "read_delay" => Ok(GeneratedField::ReadDelay),
                            "beginOffset" | "begin_offset" => Ok(GeneratedField::BeginOffset),
                            "producer" => Ok(GeneratedField::Producer),
                            "clock" => Ok(GeneratedField::Clock),
                            "packedKey" | "packed_key" => Ok(GeneratedField::PackedKey),
                            "docArchived" | "doc_archived" => Ok(GeneratedField::DocArchived),
                            "valid" => Ok(GeneratedField::Valid),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = log_request::Append;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogRequest.Append")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<log_request::Append, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal_name_truncate_delta__ = None;
                let mut journal_name_suffix__ = None;
                let mut binding__ = None;
                let mut priority__ = None;
                let mut read_delay__ = None;
                let mut begin_offset__ = None;
                let mut producer__ = None;
                let mut clock__ = None;
                let mut packed_key__ = None;
                let mut doc_archived__ = None;
                let mut valid__ = None;
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
                        GeneratedField::Priority => {
                            if priority__.is_some() {
                                return Err(serde::de::Error::duplicate_field("priority"));
                            }
                            priority__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ReadDelay => {
                            if read_delay__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readDelay"));
                            }
                            read_delay__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BeginOffset => {
                            if begin_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("beginOffset"));
                            }
                            begin_offset__ = 
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
                        GeneratedField::Clock => {
                            if clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clock"));
                            }
                            clock__ = 
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
                        GeneratedField::Valid => {
                            if valid__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valid"));
                            }
                            valid__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(log_request::Append {
                    journal_name_truncate_delta: journal_name_truncate_delta__.unwrap_or_default(),
                    journal_name_suffix: journal_name_suffix__.unwrap_or_default(),
                    binding: binding__.unwrap_or_default(),
                    priority: priority__.unwrap_or_default(),
                    read_delay: read_delay__.unwrap_or_default(),
                    begin_offset: begin_offset__.unwrap_or_default(),
                    producer: producer__.unwrap_or_default(),
                    clock: clock__.unwrap_or_default(),
                    packed_key: packed_key__.unwrap_or_default(),
                    doc_archived: doc_archived__.unwrap_or_default(),
                    valid: valid__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogRequest.Append", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log_request::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cycle != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.LogRequest.Flush", len)?;
        if self.cycle != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("cycle", ToString::to_string(&self.cycle).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for log_request::Flush {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cycle",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cycle,
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
                            "cycle" => Ok(GeneratedField::Cycle),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = log_request::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogRequest.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<log_request::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cycle__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Cycle => {
                            if cycle__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cycle"));
                            }
                            cycle__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(log_request::Flush {
                    cycle: cycle__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogRequest.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log_request::Open {
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
        if self.log_member_index != 0 {
            len += 1;
        }
        if self.disk_backlog_threshold != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.LogRequest.Open", len)?;
        if self.session_id != 0 {
            struct_ser.serialize_field("sessionId", &self.session_id)?;
        }
        if !self.members.is_empty() {
            struct_ser.serialize_field("members", &self.members)?;
        }
        if self.slice_member_index != 0 {
            struct_ser.serialize_field("sliceMemberIndex", &self.slice_member_index)?;
        }
        if self.log_member_index != 0 {
            struct_ser.serialize_field("logMemberIndex", &self.log_member_index)?;
        }
        if self.disk_backlog_threshold != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("diskBacklogThreshold", ToString::to_string(&self.disk_backlog_threshold).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for log_request::Open {
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
            "log_member_index",
            "logMemberIndex",
            "disk_backlog_threshold",
            "diskBacklogThreshold",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SessionId,
            Members,
            SliceMemberIndex,
            LogMemberIndex,
            DiskBacklogThreshold,
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
                            "logMemberIndex" | "log_member_index" => Ok(GeneratedField::LogMemberIndex),
                            "diskBacklogThreshold" | "disk_backlog_threshold" => Ok(GeneratedField::DiskBacklogThreshold),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = log_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogRequest.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<log_request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut session_id__ = None;
                let mut members__ = None;
                let mut slice_member_index__ = None;
                let mut log_member_index__ = None;
                let mut disk_backlog_threshold__ = None;
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
                        GeneratedField::LogMemberIndex => {
                            if log_member_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logMemberIndex"));
                            }
                            log_member_index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DiskBacklogThreshold => {
                            if disk_backlog_threshold__.is_some() {
                                return Err(serde::de::Error::duplicate_field("diskBacklogThreshold"));
                            }
                            disk_backlog_threshold__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(log_request::Open {
                    session_id: session_id__.unwrap_or_default(),
                    members: members__.unwrap_or_default(),
                    slice_member_index: slice_member_index__.unwrap_or_default(),
                    log_member_index: log_member_index__.unwrap_or_default(),
                    disk_backlog_threshold: disk_backlog_threshold__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogResponse {
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
        let mut struct_ser = serializer.serialize_struct("shuffle.LogResponse", len)?;
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogResponse {
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
            type Value = LogResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogResponse, V::Error>
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
                Ok(LogResponse {
                    opened: opened__,
                    flushed: flushed__,
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log_response::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cycle != 0 {
            len += 1;
        }
        if self.flushed_lsn != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.LogResponse.Flushed", len)?;
        if self.cycle != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("cycle", ToString::to_string(&self.cycle).as_str())?;
        }
        if self.flushed_lsn != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("flushedLsn", ToString::to_string(&self.flushed_lsn).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for log_response::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cycle",
            "flushed_lsn",
            "flushedLsn",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cycle,
            FlushedLsn,
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
                            "cycle" => Ok(GeneratedField::Cycle),
                            "flushedLsn" | "flushed_lsn" => Ok(GeneratedField::FlushedLsn),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = log_response::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogResponse.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<log_response::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cycle__ = None;
                let mut flushed_lsn__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Cycle => {
                            if cycle__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cycle"));
                            }
                            cycle__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FlushedLsn => {
                            if flushed_lsn__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushedLsn"));
                            }
                            flushed_lsn__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(log_response::Flushed {
                    cycle: cycle__.unwrap_or_default(),
                    flushed_lsn: flushed_lsn__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogResponse.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log_response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("shuffle.LogResponse.Opened", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for log_response::Opened {
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
            type Value = log_response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct shuffle.LogResponse.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<log_response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(log_response::Opened {
                })
            }
        }
        deserializer.deserialize_struct("shuffle.LogResponse.Opened", FIELDS, GeneratedVisitor)
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
        if !self.directory.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.Member", len)?;
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.endpoint.is_empty() {
            struct_ser.serialize_field("endpoint", &self.endpoint)?;
        }
        if !self.directory.is_empty() {
            struct_ser.serialize_field("directory", &self.directory)?;
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
            "directory",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Range,
            Endpoint,
            Directory,
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
                            "directory" => Ok(GeneratedField::Directory),
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
                let mut directory__ = None;
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
                        GeneratedField::Directory => {
                            if directory__.is_some() {
                                return Err(serde::de::Error::duplicate_field("directory"));
                            }
                            directory__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Member {
                    range: range__,
                    endpoint: endpoint__.unwrap_or_default(),
                    directory: directory__.unwrap_or_default(),
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
        if self.task.is_some() {
            len += 1;
        }
        if !self.members.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("shuffle.SessionRequest.Open", len)?;
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
            "task",
            "members",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                let mut task__ = None;
                let mut members__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
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
            struct_ser.serialize_field("sessionId", &self.session_id)?;
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
