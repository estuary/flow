impl serde::Serialize for Log {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.meta.is_some() {
            len += 1;
        }
        if self.shard.is_some() {
            len += 1;
        }
        if self.timestamp.is_some() {
            len += 1;
        }
        if self.level != 0 {
            len += 1;
        }
        if !self.message.is_empty() {
            len += 1;
        }
        if !self.fields_json_map.is_empty() {
            len += 1;
        }
        if !self.spans.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Log", len)?;
        if let Some(v) = self.meta.as_ref() {
            struct_ser.serialize_field("_meta", v)?;
        }
        if let Some(v) = self.shard.as_ref() {
            struct_ser.serialize_field("shard", v)?;
        }
        if let Some(v) = self.timestamp.as_ref() {
            struct_ser.serialize_field("ts", v)?;
        }
        if self.level != 0 {
            let v = log::Level::from_i32(self.level)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.level)))?;
            struct_ser.serialize_field("level", &v)?;
        }
        if !self.message.is_empty() {
            struct_ser.serialize_field("message", &self.message)?;
        }
        if !self.fields_json_map.is_empty() {
            struct_ser.serialize_field("fields", &crate::as_raw_json_map(&self.fields_json_map)?)?;
        }
        if !self.spans.is_empty() {
            struct_ser.serialize_field("spans", &self.spans)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Log {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "meta",
            "_meta",
            "shard",
            "timestamp",
            "ts",
            "level",
            "message",
            "fields_json_map",
            "fields",
            "spans",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Meta,
            Shard,
            Timestamp,
            Level,
            Message,
            FieldsJsonMap,
            Spans,
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
                            "_meta" | "meta" => Ok(GeneratedField::Meta),
                            "shard" => Ok(GeneratedField::Shard),
                            "ts" | "timestamp" => Ok(GeneratedField::Timestamp),
                            "level" => Ok(GeneratedField::Level),
                            "message" => Ok(GeneratedField::Message),
                            "fields" | "fields_json_map" => Ok(GeneratedField::FieldsJsonMap),
                            "spans" => Ok(GeneratedField::Spans),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Log;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Log")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Log, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut meta__ = None;
                let mut shard__ = None;
                let mut timestamp__ = None;
                let mut level__ = None;
                let mut message__ = None;
                let mut fields_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                let mut spans__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Meta => {
                            if meta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("_meta"));
                            }
                            meta__ = map.next_value()?;
                        }
                        GeneratedField::Shard => {
                            if shard__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shard"));
                            }
                            shard__ = map.next_value()?;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ts"));
                            }
                            timestamp__ = map.next_value()?;
                        }
                        GeneratedField::Level => {
                            if level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("level"));
                            }
                            level__ = Some(map.next_value::<log::Level>()? as i32);
                        }
                        GeneratedField::Message => {
                            if message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message__ = Some(map.next_value()?);
                        }
                        GeneratedField::FieldsJsonMap => {
                            if fields_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::Spans => {
                            if spans__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spans"));
                            }
                            spans__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Log {
                    meta: meta__,
                    shard: shard__,
                    timestamp: timestamp__,
                    level: level__.unwrap_or_default(),
                    message: message__.unwrap_or_default(),
                    fields_json_map: fields_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                    spans: spans__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ops.Log", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for log::Level {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::UndefinedLevel => "undefined_level",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for log::Level {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "undefined_level",
            "error",
            "warn",
            "info",
            "debug",
            "trace",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = log::Level;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(log::Level::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(log::Level::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "undefined_level" => Ok(log::Level::UndefinedLevel),
                    "error" => Ok(log::Level::Error),
                    "warn" => Ok(log::Level::Warn),
                    "info" => Ok(log::Level::Info),
                    "debug" => Ok(log::Level::Debug),
                    "trace" => Ok(log::Level::Trace),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Meta {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.uuid.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Meta", len)?;
        if !self.uuid.is_empty() {
            struct_ser.serialize_field("uuid", &self.uuid)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Meta {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "uuid",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Uuid,
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
                            "uuid" => Ok(GeneratedField::Uuid),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Meta;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Meta")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Meta, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut uuid__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Uuid => {
                            if uuid__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuid"));
                            }
                            uuid__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Meta {
                    uuid: uuid__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ops.Meta", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Shard {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind != 0 {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.key_begin.is_empty() {
            len += 1;
        }
        if !self.r_clock_begin.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Shard", len)?;
        if self.kind != 0 {
            let v = shard::Kind::from_i32(self.kind)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.key_begin.is_empty() {
            struct_ser.serialize_field("keyBegin", &self.key_begin)?;
        }
        if !self.r_clock_begin.is_empty() {
            struct_ser.serialize_field("rClockBegin", &self.r_clock_begin)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Shard {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "kind",
            "name",
            "key_begin",
            "keyBegin",
            "r_clock_begin",
            "rClockBegin",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Kind,
            Name,
            KeyBegin,
            RClockBegin,
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
                            "kind" => Ok(GeneratedField::Kind),
                            "name" => Ok(GeneratedField::Name),
                            "keyBegin" | "key_begin" => Ok(GeneratedField::KeyBegin),
                            "rClockBegin" | "r_clock_begin" => Ok(GeneratedField::RClockBegin),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Shard;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Shard")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Shard, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                let mut name__ = None;
                let mut key_begin__ = None;
                let mut r_clock_begin__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map.next_value::<shard::Kind>()? as i32);
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::KeyBegin => {
                            if key_begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyBegin"));
                            }
                            key_begin__ = Some(map.next_value()?);
                        }
                        GeneratedField::RClockBegin => {
                            if r_clock_begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rClockBegin"));
                            }
                            r_clock_begin__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Shard {
                    kind: kind__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    key_begin: key_begin__.unwrap_or_default(),
                    r_clock_begin: r_clock_begin__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ops.Shard", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for shard::Kind {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::InvalidKind => "invalid_kind",
            Self::Capture => "capture",
            Self::Derivation => "derivation",
            Self::Materialization => "materialization",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for shard::Kind {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "invalid_kind",
            "capture",
            "derivation",
            "materialization",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = shard::Kind;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(shard::Kind::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(shard::Kind::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "invalid_kind" => Ok(shard::Kind::InvalidKind),
                    "capture" => Ok(shard::Kind::Capture),
                    "derivation" => Ok(shard::Kind::Derivation),
                    "materialization" => Ok(shard::Kind::Materialization),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Stats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.meta.is_some() {
            len += 1;
        }
        if self.shard.is_some() {
            len += 1;
        }
        if self.timestamp.is_some() {
            len += 1;
        }
        if self.open_seconds_total != 0. {
            len += 1;
        }
        if self.txn_count != 0 {
            len += 1;
        }
        if self.derive.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Stats", len)?;
        if let Some(v) = self.meta.as_ref() {
            struct_ser.serialize_field("_meta", v)?;
        }
        if let Some(v) = self.shard.as_ref() {
            struct_ser.serialize_field("shard", v)?;
        }
        if let Some(v) = self.timestamp.as_ref() {
            struct_ser.serialize_field("ts", v)?;
        }
        if self.open_seconds_total != 0. {
            struct_ser.serialize_field("openSecondsTotal", &self.open_seconds_total)?;
        }
        if self.txn_count != 0 {
            struct_ser.serialize_field("txnCount", &self.txn_count)?;
        }
        if let Some(v) = self.derive.as_ref() {
            struct_ser.serialize_field("derive", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Stats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "meta",
            "_meta",
            "shard",
            "timestamp",
            "ts",
            "open_seconds_total",
            "openSecondsTotal",
            "txn_count",
            "txnCount",
            "derive",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Meta,
            Shard,
            Timestamp,
            OpenSecondsTotal,
            TxnCount,
            Derive,
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
                            "_meta" | "meta" => Ok(GeneratedField::Meta),
                            "shard" => Ok(GeneratedField::Shard),
                            "ts" | "timestamp" => Ok(GeneratedField::Timestamp),
                            "openSecondsTotal" | "open_seconds_total" => Ok(GeneratedField::OpenSecondsTotal),
                            "txnCount" | "txn_count" => Ok(GeneratedField::TxnCount),
                            "derive" => Ok(GeneratedField::Derive),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Stats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Stats")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Stats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut meta__ = None;
                let mut shard__ = None;
                let mut timestamp__ = None;
                let mut open_seconds_total__ = None;
                let mut txn_count__ = None;
                let mut derive__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Meta => {
                            if meta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("_meta"));
                            }
                            meta__ = map.next_value()?;
                        }
                        GeneratedField::Shard => {
                            if shard__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shard"));
                            }
                            shard__ = map.next_value()?;
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ts"));
                            }
                            timestamp__ = map.next_value()?;
                        }
                        GeneratedField::OpenSecondsTotal => {
                            if open_seconds_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("openSecondsTotal"));
                            }
                            open_seconds_total__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::TxnCount => {
                            if txn_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("txnCount"));
                            }
                            txn_count__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Derive => {
                            if derive__.is_some() {
                                return Err(serde::de::Error::duplicate_field("derive"));
                            }
                            derive__ = map.next_value()?;
                        }
                    }
                }
                Ok(Stats {
                    meta: meta__,
                    shard: shard__,
                    timestamp: timestamp__,
                    open_seconds_total: open_seconds_total__.unwrap_or_default(),
                    txn_count: txn_count__.unwrap_or_default(),
                    derive: derive__,
                })
            }
        }
        deserializer.deserialize_struct("ops.Stats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for stats::Derive {
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
        if self.published.is_some() {
            len += 1;
        }
        if self.out.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Stats.Derive", len)?;
        if !self.transforms.is_empty() {
            struct_ser.serialize_field("transforms", &self.transforms)?;
        }
        if let Some(v) = self.published.as_ref() {
            struct_ser.serialize_field("published", v)?;
        }
        if let Some(v) = self.out.as_ref() {
            struct_ser.serialize_field("out", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for stats::Derive {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "transforms",
            "published",
            "out",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Transforms,
            Published,
            Out,
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
                            "published" => Ok(GeneratedField::Published),
                            "out" => Ok(GeneratedField::Out),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = stats::Derive;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Stats.Derive")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<stats::Derive, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut transforms__ = None;
                let mut published__ = None;
                let mut out__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Transforms => {
                            if transforms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transforms"));
                            }
                            transforms__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::Published => {
                            if published__.is_some() {
                                return Err(serde::de::Error::duplicate_field("published"));
                            }
                            published__ = map.next_value()?;
                        }
                        GeneratedField::Out => {
                            if out__.is_some() {
                                return Err(serde::de::Error::duplicate_field("out"));
                            }
                            out__ = map.next_value()?;
                        }
                    }
                }
                Ok(stats::Derive {
                    transforms: transforms__.unwrap_or_default(),
                    published: published__,
                    out: out__,
                })
            }
        }
        deserializer.deserialize_struct("ops.Stats.Derive", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for stats::derive::Transform {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.source.is_empty() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Stats.Derive.Transform", len)?;
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for stats::derive::Transform {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source",
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Source,
            Input,
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
                            "source" => Ok(GeneratedField::Source),
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = stats::derive::Transform;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Stats.Derive.Transform")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<stats::derive::Transform, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source__ = None;
                let mut input__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = Some(map.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                    }
                }
                Ok(stats::derive::Transform {
                    source: source__.unwrap_or_default(),
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("ops.Stats.Derive.Transform", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for stats::DocsAndBytes {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.docs_total != 0 {
            len += 1;
        }
        if self.bytes_total != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ops.Stats.DocsAndBytes", len)?;
        if self.docs_total != 0 {
            struct_ser.serialize_field("docsTotal", &self.docs_total)?;
        }
        if self.bytes_total != 0 {
            struct_ser.serialize_field("bytesTotal", &self.bytes_total)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for stats::DocsAndBytes {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "docs_total",
            "docsTotal",
            "bytes_total",
            "bytesTotal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DocsTotal,
            BytesTotal,
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
                            "docsTotal" | "docs_total" => Ok(GeneratedField::DocsTotal),
                            "bytesTotal" | "bytes_total" => Ok(GeneratedField::BytesTotal),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = stats::DocsAndBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ops.Stats.DocsAndBytes")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<stats::DocsAndBytes, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut docs_total__ = None;
                let mut bytes_total__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DocsTotal => {
                            if docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docsTotal"));
                            }
                            docs_total__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BytesTotal => {
                            if bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytesTotal"));
                            }
                            bytes_total__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(stats::DocsAndBytes {
                    docs_total: docs_total__.unwrap_or_default(),
                    bytes_total: bytes_total__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ops.Stats.DocsAndBytes", FIELDS, GeneratedVisitor)
    }
}
