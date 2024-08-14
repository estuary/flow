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
        if self.discover.is_some() {
            len += 1;
        }
        if self.validate.is_some() {
            len += 1;
        }
        if self.apply.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        if self.acknowledge.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Request", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.discover.as_ref() {
            struct_ser.serialize_field("discover", v)?;
        }
        if let Some(v) = self.validate.as_ref() {
            struct_ser.serialize_field("validate", v)?;
        }
        if let Some(v) = self.apply.as_ref() {
            struct_ser.serialize_field("apply", v)?;
        }
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.acknowledge.as_ref() {
            struct_ser.serialize_field("acknowledge", v)?;
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
            "discover",
            "validate",
            "apply",
            "open",
            "acknowledge",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Discover,
            Validate,
            Apply,
            Open,
            Acknowledge,
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
                            "discover" => Ok(GeneratedField::Discover),
                            "validate" => Ok(GeneratedField::Validate),
                            "apply" => Ok(GeneratedField::Apply),
                            "open" => Ok(GeneratedField::Open),
                            "acknowledge" => Ok(GeneratedField::Acknowledge),
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
                formatter.write_str("struct capture.Request")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Request, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut discover__ = None;
                let mut validate__ = None;
                let mut apply__ = None;
                let mut open__ = None;
                let mut acknowledge__ = None;
                let mut internal__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::Discover => {
                            if discover__.is_some() {
                                return Err(serde::de::Error::duplicate_field("discover"));
                            }
                            discover__ = map_.next_value()?;
                        }
                        GeneratedField::Validate => {
                            if validate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validate"));
                            }
                            validate__ = map_.next_value()?;
                        }
                        GeneratedField::Apply => {
                            if apply__.is_some() {
                                return Err(serde::de::Error::duplicate_field("apply"));
                            }
                            apply__ = map_.next_value()?;
                        }
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Acknowledge => {
                            if acknowledge__.is_some() {
                                return Err(serde::de::Error::duplicate_field("acknowledge"));
                            }
                            acknowledge__ = map_.next_value()?;
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
                    discover: discover__,
                    validate: validate__,
                    apply: apply__,
                    open: open__,
                    acknowledge: acknowledge__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Acknowledge {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.checkpoints != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Request.Acknowledge", len)?;
        if self.checkpoints != 0 {
            struct_ser.serialize_field("checkpoints", &self.checkpoints)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Acknowledge {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "checkpoints",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Checkpoints,
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
                            "checkpoints" => Ok(GeneratedField::Checkpoints),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Acknowledge;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Request.Acknowledge")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Acknowledge, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut checkpoints__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Checkpoints => {
                            if checkpoints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checkpoints"));
                            }
                            checkpoints__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::Acknowledge {
                    checkpoints: checkpoints__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Acknowledge", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Apply {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.capture.is_some() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if self.last_capture.is_some() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Request.Apply", len)?;
        if let Some(v) = self.capture.as_ref() {
            struct_ser.serialize_field("capture", v)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if let Some(v) = self.last_capture.as_ref() {
            struct_ser.serialize_field("lastCapture", v)?;
        }
        if !self.last_version.is_empty() {
            struct_ser.serialize_field("lastVersion", &self.last_version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Apply {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "capture",
            "version",
            "last_capture",
            "lastCapture",
            "last_version",
            "lastVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Capture,
            Version,
            LastCapture,
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
                            "capture" => Ok(GeneratedField::Capture),
                            "version" => Ok(GeneratedField::Version),
                            "lastCapture" | "last_capture" => Ok(GeneratedField::LastCapture),
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
            type Value = request::Apply;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Request.Apply")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Apply, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut capture__ = None;
                let mut version__ = None;
                let mut last_capture__ = None;
                let mut last_version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Capture => {
                            if capture__.is_some() {
                                return Err(serde::de::Error::duplicate_field("capture"));
                            }
                            capture__ = map_.next_value()?;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LastCapture => {
                            if last_capture__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastCapture"));
                            }
                            last_capture__ = map_.next_value()?;
                        }
                        GeneratedField::LastVersion => {
                            if last_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastVersion"));
                            }
                            last_version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Apply {
                    capture: capture__,
                    version: version__.unwrap_or_default(),
                    last_capture: last_capture__,
                    last_version: last_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Apply", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Discover {
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
        let mut struct_ser = serializer.serialize_struct("capture.Request.Discover", len)?;
        if self.connector_type != 0 {
            let v = super::flow::capture_spec::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Discover {
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
            type Value = request::Discover;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Request.Discover")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Discover, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map_.next_value::<super::flow::capture_spec::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Discover {
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Discover", FIELDS, GeneratedVisitor)
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
        if self.capture.is_some() {
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
        let mut struct_ser = serializer.serialize_struct("capture.Request.Open", len)?;
        if let Some(v) = self.capture.as_ref() {
            struct_ser.serialize_field("capture", v)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.state_json.is_empty() {
            struct_ser.serialize_field("state", crate::as_raw_json(&self.state_json)?)?;
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
            "capture",
            "version",
            "range",
            "state_json",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Capture,
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
                            "capture" => Ok(GeneratedField::Capture),
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
                formatter.write_str("struct capture.Request.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut capture__ = None;
                let mut version__ = None;
                let mut range__ = None;
                let mut state_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Capture => {
                            if capture__.is_some() {
                                return Err(serde::de::Error::duplicate_field("capture"));
                            }
                            capture__ = map_.next_value()?;
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
                            state_json__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Open {
                    capture: capture__,
                    version: version__.unwrap_or_default(),
                    range: range__,
                    state_json: state_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Open", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("capture.Request.Spec", len)?;
        if self.connector_type != 0 {
            let v = super::flow::capture_spec::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
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
                formatter.write_str("struct capture.Request.Spec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Spec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map_.next_value::<super::flow::capture_spec::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Spec {
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Spec", FIELDS, GeneratedVisitor)
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
        if !self.name.is_empty() {
            len += 1;
        }
        if self.connector_type != 0 {
            len += 1;
        }
        if !self.config_json.is_empty() {
            len += 1;
        }
        if !self.bindings.is_empty() {
            len += 1;
        }
        if self.last_capture.is_some() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Request.Validate", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.connector_type != 0 {
            let v = super::flow::capture_spec::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if let Some(v) = self.last_capture.as_ref() {
            struct_ser.serialize_field("lastCapture", v)?;
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
            "name",
            "connector_type",
            "connectorType",
            "config_json",
            "config",
            "bindings",
            "last_capture",
            "lastCapture",
            "last_version",
            "lastVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ConnectorType,
            ConfigJson,
            Bindings,
            LastCapture,
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
                            "name" => Ok(GeneratedField::Name),
                            "connectorType" | "connector_type" => Ok(GeneratedField::ConnectorType),
                            "config" | "config_json" => Ok(GeneratedField::ConfigJson),
                            "bindings" => Ok(GeneratedField::Bindings),
                            "lastCapture" | "last_capture" => Ok(GeneratedField::LastCapture),
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
                formatter.write_str("struct capture.Request.Validate")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Validate, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut bindings__ = None;
                let mut last_capture__ = None;
                let mut last_version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map_.next_value::<super::flow::capture_spec::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LastCapture => {
                            if last_capture__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastCapture"));
                            }
                            last_capture__ = map_.next_value()?;
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
                    name: name__.unwrap_or_default(),
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    bindings: bindings__.unwrap_or_default(),
                    last_capture: last_capture__,
                    last_version: last_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Validate", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::validate::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.resource_config_json.is_empty() {
            len += 1;
        }
        if self.collection.is_some() {
            len += 1;
        }
        if self.backfill != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Request.Validate.Binding", len)?;
        if !self.resource_config_json.is_empty() {
            struct_ser.serialize_field("resourceConfig", crate::as_raw_json(&self.resource_config_json)?)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if self.backfill != 0 {
            struct_ser.serialize_field("backfill", &self.backfill)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::validate::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "resource_config_json",
            "resourceConfig",
            "collection",
            "backfill",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ResourceConfigJson,
            Collection,
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
                            "resourceConfig" | "resource_config_json" => Ok(GeneratedField::ResourceConfigJson),
                            "collection" => Ok(GeneratedField::Collection),
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
            type Value = request::validate::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Request.Validate.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::validate::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut collection__ = None;
                let mut backfill__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ResourceConfigJson => {
                            if resource_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfig"));
                            }
                            resource_config_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map_.next_value()?;
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
                Ok(request::validate::Binding {
                    resource_config_json: resource_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    collection: collection__,
                    backfill: backfill__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Request.Validate.Binding", FIELDS, GeneratedVisitor)
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
        if self.discovered.is_some() {
            len += 1;
        }
        if self.validated.is_some() {
            len += 1;
        }
        if self.applied.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.captured.is_some() {
            len += 1;
        }
        if self.checkpoint.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.discovered.as_ref() {
            struct_ser.serialize_field("discovered", v)?;
        }
        if let Some(v) = self.validated.as_ref() {
            struct_ser.serialize_field("validated", v)?;
        }
        if let Some(v) = self.applied.as_ref() {
            struct_ser.serialize_field("applied", v)?;
        }
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.captured.as_ref() {
            struct_ser.serialize_field("captured", v)?;
        }
        if let Some(v) = self.checkpoint.as_ref() {
            struct_ser.serialize_field("checkpoint", v)?;
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
            "discovered",
            "validated",
            "applied",
            "opened",
            "captured",
            "checkpoint",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Discovered,
            Validated,
            Applied,
            Opened,
            Captured,
            Checkpoint,
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
                            "discovered" => Ok(GeneratedField::Discovered),
                            "validated" => Ok(GeneratedField::Validated),
                            "applied" => Ok(GeneratedField::Applied),
                            "opened" => Ok(GeneratedField::Opened),
                            "captured" => Ok(GeneratedField::Captured),
                            "checkpoint" => Ok(GeneratedField::Checkpoint),
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
                formatter.write_str("struct capture.Response")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Response, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut discovered__ = None;
                let mut validated__ = None;
                let mut applied__ = None;
                let mut opened__ = None;
                let mut captured__ = None;
                let mut checkpoint__ = None;
                let mut internal__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::Discovered => {
                            if discovered__.is_some() {
                                return Err(serde::de::Error::duplicate_field("discovered"));
                            }
                            discovered__ = map_.next_value()?;
                        }
                        GeneratedField::Validated => {
                            if validated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validated"));
                            }
                            validated__ = map_.next_value()?;
                        }
                        GeneratedField::Applied => {
                            if applied__.is_some() {
                                return Err(serde::de::Error::duplicate_field("applied"));
                            }
                            applied__ = map_.next_value()?;
                        }
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::Captured => {
                            if captured__.is_some() {
                                return Err(serde::de::Error::duplicate_field("captured"));
                            }
                            captured__ = map_.next_value()?;
                        }
                        GeneratedField::Checkpoint => {
                            if checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("checkpoint"));
                            }
                            checkpoint__ = map_.next_value()?;
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
                    discovered: discovered__,
                    validated: validated__,
                    applied: applied__,
                    opened: opened__,
                    captured: captured__,
                    checkpoint: checkpoint__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Applied {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.action_description.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Applied", len)?;
        if !self.action_description.is_empty() {
            struct_ser.serialize_field("actionDescription", &self.action_description)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Applied {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "action_description",
            "actionDescription",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ActionDescription,
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
                            "actionDescription" | "action_description" => Ok(GeneratedField::ActionDescription),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Applied;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Applied")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Applied, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut action_description__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ActionDescription => {
                            if action_description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("actionDescription"));
                            }
                            action_description__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Applied {
                    action_description: action_description__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Applied", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Captured {
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
        if !self.doc_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Captured", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.doc_json.is_empty() {
            struct_ser.serialize_field("doc", crate::as_raw_json(&self.doc_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Captured {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "doc_json",
            "doc",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
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
                            "binding" => Ok(GeneratedField::Binding),
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
            type Value = response::Captured;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Captured")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Captured, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut doc_json__ : Option<Box<serde_json::value::RawValue>> = None;
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
                        GeneratedField::DocJson => {
                            if doc_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doc"));
                            }
                            doc_json__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Captured {
                    binding: binding__.unwrap_or_default(),
                    doc_json: doc_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Captured", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Checkpoint {
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
        let mut struct_ser = serializer.serialize_struct("capture.Response.Checkpoint", len)?;
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Checkpoint {
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
            type Value = response::Checkpoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Checkpoint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Checkpoint, V::Error>
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
                Ok(response::Checkpoint {
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Checkpoint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Discovered {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.bindings.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Discovered", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Discovered {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bindings",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bindings,
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
                            "bindings" => Ok(GeneratedField::Bindings),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Discovered;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Discovered")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Discovered, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bindings__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Discovered {
                    bindings: bindings__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Discovered", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::discovered::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.recommended_name.is_empty() {
            len += 1;
        }
        if !self.resource_config_json.is_empty() {
            len += 1;
        }
        if !self.document_schema_json.is_empty() {
            len += 1;
        }
        if !self.key.is_empty() {
            len += 1;
        }
        if self.disable {
            len += 1;
        }
        if !self.resource_path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Discovered.Binding", len)?;
        if !self.recommended_name.is_empty() {
            struct_ser.serialize_field("recommendedName", &self.recommended_name)?;
        }
        if !self.resource_config_json.is_empty() {
            struct_ser.serialize_field("resourceConfig", crate::as_raw_json(&self.resource_config_json)?)?;
        }
        if !self.document_schema_json.is_empty() {
            struct_ser.serialize_field("documentSchema", crate::as_raw_json(&self.document_schema_json)?)?;
        }
        if !self.key.is_empty() {
            struct_ser.serialize_field("key", &self.key)?;
        }
        if self.disable {
            struct_ser.serialize_field("disable", &self.disable)?;
        }
        if !self.resource_path.is_empty() {
            struct_ser.serialize_field("resourcePath", &self.resource_path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::discovered::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "recommended_name",
            "recommendedName",
            "resource_config_json",
            "resourceConfig",
            "document_schema_json",
            "documentSchema",
            "key",
            "disable",
            "resource_path",
            "resourcePath",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RecommendedName,
            ResourceConfigJson,
            DocumentSchemaJson,
            Key,
            Disable,
            ResourcePath,
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
                            "recommendedName" | "recommended_name" => Ok(GeneratedField::RecommendedName),
                            "resourceConfig" | "resource_config_json" => Ok(GeneratedField::ResourceConfigJson),
                            "documentSchema" | "document_schema_json" => Ok(GeneratedField::DocumentSchemaJson),
                            "key" => Ok(GeneratedField::Key),
                            "disable" => Ok(GeneratedField::Disable),
                            "resourcePath" | "resource_path" => Ok(GeneratedField::ResourcePath),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::discovered::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Discovered.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::discovered::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut recommended_name__ = None;
                let mut resource_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut document_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut key__ = None;
                let mut disable__ = None;
                let mut resource_path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RecommendedName => {
                            if recommended_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recommendedName"));
                            }
                            recommended_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResourceConfigJson => {
                            if resource_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfig"));
                            }
                            resource_config_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DocumentSchemaJson => {
                            if document_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("documentSchema"));
                            }
                            document_schema_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Disable => {
                            if disable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("disable"));
                            }
                            disable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResourcePath => {
                            if resource_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePath"));
                            }
                            resource_path__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::discovered::Binding {
                    recommended_name: recommended_name__.unwrap_or_default(),
                    resource_config_json: resource_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    document_schema_json: document_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    key: key__.unwrap_or_default(),
                    disable: disable__.unwrap_or_default(),
                    resource_path: resource_path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Discovered.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.explicit_acknowledgements {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Opened", len)?;
        if self.explicit_acknowledgements {
            struct_ser.serialize_field("explicitAcknowledgements", &self.explicit_acknowledgements)?;
        }
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
            "explicit_acknowledgements",
            "explicitAcknowledgements",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExplicitAcknowledgements,
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
                            "explicitAcknowledgements" | "explicit_acknowledgements" => Ok(GeneratedField::ExplicitAcknowledgements),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut explicit_acknowledgements__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ExplicitAcknowledgements => {
                            if explicit_acknowledgements__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explicitAcknowledgements"));
                            }
                            explicit_acknowledgements__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Opened {
                    explicit_acknowledgements: explicit_acknowledgements__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Opened", FIELDS, GeneratedVisitor)
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
        if !self.resource_path_pointers.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Spec", len)?;
        if self.protocol != 0 {
            struct_ser.serialize_field("protocol", &self.protocol)?;
        }
        if !self.config_schema_json.is_empty() {
            struct_ser.serialize_field("configSchema", crate::as_raw_json(&self.config_schema_json)?)?;
        }
        if !self.resource_config_schema_json.is_empty() {
            struct_ser.serialize_field("resourceConfigSchema", crate::as_raw_json(&self.resource_config_schema_json)?)?;
        }
        if !self.documentation_url.is_empty() {
            struct_ser.serialize_field("documentationUrl", &self.documentation_url)?;
        }
        if let Some(v) = self.oauth2.as_ref() {
            struct_ser.serialize_field("oauth2", v)?;
        }
        if !self.resource_path_pointers.is_empty() {
            struct_ser.serialize_field("resourcePathPointers", &self.resource_path_pointers)?;
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
            "resource_path_pointers",
            "resourcePathPointers",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Protocol,
            ConfigSchemaJson,
            ResourceConfigSchemaJson,
            DocumentationUrl,
            Oauth2,
            ResourcePathPointers,
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
                            "resourcePathPointers" | "resource_path_pointers" => Ok(GeneratedField::ResourcePathPointers),
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
                formatter.write_str("struct capture.Response.Spec")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Spec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut protocol__ = None;
                let mut config_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut resource_config_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut documentation_url__ = None;
                let mut oauth2__ = None;
                let mut resource_path_pointers__ = None;
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
                            config_schema_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResourceConfigSchemaJson => {
                            if resource_config_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfigSchema"));
                            }
                            resource_config_schema_json__ = Some(map_.next_value()?);
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
                        GeneratedField::ResourcePathPointers => {
                            if resource_path_pointers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePathPointers"));
                            }
                            resource_path_pointers__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Spec {
                    protocol: protocol__.unwrap_or_default(),
                    config_schema_json: config_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    resource_config_schema_json: resource_config_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    documentation_url: documentation_url__.unwrap_or_default(),
                    oauth2: oauth2__,
                    resource_path_pointers: resource_path_pointers__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Spec", FIELDS, GeneratedVisitor)
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
        if !self.bindings.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Validated", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
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
            "bindings",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bindings,
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
                            "bindings" => Ok(GeneratedField::Bindings),
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
                formatter.write_str("struct capture.Response.Validated")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Validated, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bindings__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::Validated {
                    bindings: bindings__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Validated", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::validated::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.resource_path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("capture.Response.Validated.Binding", len)?;
        if !self.resource_path.is_empty() {
            struct_ser.serialize_field("resourcePath", &self.resource_path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::validated::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "resource_path",
            "resourcePath",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ResourcePath,
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
                            "resourcePath" | "resource_path" => Ok(GeneratedField::ResourcePath),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::validated::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct capture.Response.Validated.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::validated::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource_path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ResourcePath => {
                            if resource_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePath"));
                            }
                            resource_path__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::validated::Binding {
                    resource_path: resource_path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("capture.Response.Validated.Binding", FIELDS, GeneratedVisitor)
    }
}
