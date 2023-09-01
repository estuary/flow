impl serde::Serialize for AdvanceTimeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.advance_seconds != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.AdvanceTimeRequest", len)?;
        if self.advance_seconds != 0 {
            struct_ser.serialize_field("advanceSeconds", ToString::to_string(&self.advance_seconds).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdvanceTimeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "advance_seconds",
            "advanceSeconds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AdvanceSeconds,
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
                            "advanceSeconds" | "advance_seconds" => Ok(GeneratedField::AdvanceSeconds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdvanceTimeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.AdvanceTimeRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AdvanceTimeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut advance_seconds__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AdvanceSeconds => {
                            if advance_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("advanceSeconds"));
                            }
                            advance_seconds__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(AdvanceTimeRequest {
                    advance_seconds: advance_seconds__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.AdvanceTimeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdvanceTimeResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.AdvanceTimeResponse", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdvanceTimeResponse {
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
            type Value = AdvanceTimeResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.AdvanceTimeResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AdvanceTimeResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdvanceTimeResponse {
                })
            }
        }
        deserializer.deserialize_struct("flow.AdvanceTimeResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuildApi {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.BuildAPI", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BuildApi {
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
            type Value = BuildApi;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.BuildAPI")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BuildApi, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(BuildApi {
                })
            }
        }
        deserializer.deserialize_struct("flow.BuildAPI", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for build_api::Config {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.build_id.is_empty() {
            len += 1;
        }
        if !self.build_db.is_empty() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        if self.source_type != 0 {
            len += 1;
        }
        if !self.connector_network.is_empty() {
            len += 1;
        }
        if !self.project_root.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.BuildAPI.Config", len)?;
        if !self.build_id.is_empty() {
            struct_ser.serialize_field("buildId", &self.build_id)?;
        }
        if !self.build_db.is_empty() {
            struct_ser.serialize_field("buildDb", &self.build_db)?;
        }
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if self.source_type != 0 {
            let v = ContentType::from_i32(self.source_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.source_type)))?;
            struct_ser.serialize_field("sourceType", &v)?;
        }
        if !self.connector_network.is_empty() {
            struct_ser.serialize_field("connectorNetwork", &self.connector_network)?;
        }
        if !self.project_root.is_empty() {
            struct_ser.serialize_field("projectRoot", &self.project_root)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for build_api::Config {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "build_id",
            "buildId",
            "build_db",
            "buildDb",
            "source",
            "source_type",
            "sourceType",
            "connector_network",
            "connectorNetwork",
            "project_root",
            "projectRoot",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BuildId,
            BuildDb,
            Source,
            SourceType,
            ConnectorNetwork,
            ProjectRoot,
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
                            "buildId" | "build_id" => Ok(GeneratedField::BuildId),
                            "buildDb" | "build_db" => Ok(GeneratedField::BuildDb),
                            "source" => Ok(GeneratedField::Source),
                            "sourceType" | "source_type" => Ok(GeneratedField::SourceType),
                            "connectorNetwork" | "connector_network" => Ok(GeneratedField::ConnectorNetwork),
                            "projectRoot" | "project_root" => Ok(GeneratedField::ProjectRoot),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = build_api::Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.BuildAPI.Config")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<build_api::Config, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut build_id__ = None;
                let mut build_db__ = None;
                let mut source__ = None;
                let mut source_type__ = None;
                let mut connector_network__ = None;
                let mut project_root__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BuildId => {
                            if build_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("buildId"));
                            }
                            build_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::BuildDb => {
                            if build_db__.is_some() {
                                return Err(serde::de::Error::duplicate_field("buildDb"));
                            }
                            build_db__ = Some(map.next_value()?);
                        }
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = Some(map.next_value()?);
                        }
                        GeneratedField::SourceType => {
                            if source_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceType"));
                            }
                            source_type__ = Some(map.next_value::<ContentType>()? as i32);
                        }
                        GeneratedField::ConnectorNetwork => {
                            if connector_network__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorNetwork"));
                            }
                            connector_network__ = Some(map.next_value()?);
                        }
                        GeneratedField::ProjectRoot => {
                            if project_root__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projectRoot"));
                            }
                            project_root__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(build_api::Config {
                    build_id: build_id__.unwrap_or_default(),
                    build_db: build_db__.unwrap_or_default(),
                    source: source__.unwrap_or_default(),
                    source_type: source_type__.unwrap_or_default(),
                    connector_network: connector_network__.unwrap_or_default(),
                    project_root: project_root__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.BuildAPI.Config", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CaptureSpec {
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
        if self.interval_seconds != 0 {
            len += 1;
        }
        if self.shard_template.is_some() {
            len += 1;
        }
        if self.recovery_log_template.is_some() {
            len += 1;
        }
        if !self.network_ports.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CaptureSpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.connector_type != 0 {
            let v = capture_spec::ConnectorType::from_i32(self.connector_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if self.interval_seconds != 0 {
            struct_ser.serialize_field("intervalSeconds", &self.interval_seconds)?;
        }
        if let Some(v) = self.shard_template.as_ref() {
            struct_ser.serialize_field("shardTemplate", v)?;
        }
        if let Some(v) = self.recovery_log_template.as_ref() {
            struct_ser.serialize_field("recoveryLogTemplate", v)?;
        }
        if !self.network_ports.is_empty() {
            struct_ser.serialize_field("networkPorts", &self.network_ports)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CaptureSpec {
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
            "interval_seconds",
            "intervalSeconds",
            "shard_template",
            "shardTemplate",
            "recovery_log_template",
            "recoveryLogTemplate",
            "network_ports",
            "networkPorts",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ConnectorType,
            ConfigJson,
            Bindings,
            IntervalSeconds,
            ShardTemplate,
            RecoveryLogTemplate,
            NetworkPorts,
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
                            "intervalSeconds" | "interval_seconds" => Ok(GeneratedField::IntervalSeconds),
                            "shardTemplate" | "shard_template" => Ok(GeneratedField::ShardTemplate),
                            "recoveryLogTemplate" | "recovery_log_template" => Ok(GeneratedField::RecoveryLogTemplate),
                            "networkPorts" | "network_ports" => Ok(GeneratedField::NetworkPorts),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CaptureSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CaptureSpec")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CaptureSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut bindings__ = None;
                let mut interval_seconds__ = None;
                let mut shard_template__ = None;
                let mut recovery_log_template__ = None;
                let mut network_ports__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map.next_value::<capture_spec::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map.next_value()?);
                        }
                        GeneratedField::IntervalSeconds => {
                            if interval_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalSeconds"));
                            }
                            interval_seconds__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ShardTemplate => {
                            if shard_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardTemplate"));
                            }
                            shard_template__ = map.next_value()?;
                        }
                        GeneratedField::RecoveryLogTemplate => {
                            if recovery_log_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recoveryLogTemplate"));
                            }
                            recovery_log_template__ = map.next_value()?;
                        }
                        GeneratedField::NetworkPorts => {
                            if network_ports__.is_some() {
                                return Err(serde::de::Error::duplicate_field("networkPorts"));
                            }
                            network_ports__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CaptureSpec {
                    name: name__.unwrap_or_default(),
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    bindings: bindings__.unwrap_or_default(),
                    interval_seconds: interval_seconds__.unwrap_or_default(),
                    shard_template: shard_template__,
                    recovery_log_template: recovery_log_template__,
                    network_ports: network_ports__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.CaptureSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_spec::Binding {
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
        if !self.resource_path.is_empty() {
            len += 1;
        }
        if self.collection.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CaptureSpec.Binding", len)?;
        if !self.resource_config_json.is_empty() {
            struct_ser.serialize_field("resourceConfig", crate::as_raw_json(&self.resource_config_json)?)?;
        }
        if !self.resource_path.is_empty() {
            struct_ser.serialize_field("resourcePath", &self.resource_path)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture_spec::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "resource_config_json",
            "resourceConfig",
            "resource_path",
            "resourcePath",
            "collection",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ResourceConfigJson,
            ResourcePath,
            Collection,
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
                            "resourcePath" | "resource_path" => Ok(GeneratedField::ResourcePath),
                            "collection" => Ok(GeneratedField::Collection),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_spec::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CaptureSpec.Binding")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<capture_spec::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut resource_path__ = None;
                let mut collection__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ResourceConfigJson => {
                            if resource_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfig"));
                            }
                            resource_config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::ResourcePath => {
                            if resource_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePath"));
                            }
                            resource_path__ = Some(map.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map.next_value()?;
                        }
                    }
                }
                Ok(capture_spec::Binding {
                    resource_config_json: resource_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    resource_path: resource_path__.unwrap_or_default(),
                    collection: collection__,
                })
            }
        }
        deserializer.deserialize_struct("flow.CaptureSpec.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_spec::ConnectorType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Image => "IMAGE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for capture_spec::ConnectorType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "IMAGE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_spec::ConnectorType;

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
                    .and_then(capture_spec::ConnectorType::from_i32)
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
                    .and_then(capture_spec::ConnectorType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(capture_spec::ConnectorType::Invalid),
                    "IMAGE" => Ok(capture_spec::ConnectorType::Image),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CollectionSpec {
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
        if !self.write_schema_json.is_empty() {
            len += 1;
        }
        if !self.read_schema_json.is_empty() {
            len += 1;
        }
        if !self.key.is_empty() {
            len += 1;
        }
        if !self.uuid_ptr.is_empty() {
            len += 1;
        }
        if !self.partition_fields.is_empty() {
            len += 1;
        }
        if !self.projections.is_empty() {
            len += 1;
        }
        if !self.ack_template_json.is_empty() {
            len += 1;
        }
        if self.partition_template.is_some() {
            len += 1;
        }
        if self.derivation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CollectionSpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.write_schema_json.is_empty() {
            struct_ser.serialize_field("writeSchema", crate::as_raw_json(&self.write_schema_json)?)?;
        }
        if !self.read_schema_json.is_empty() {
            struct_ser.serialize_field("readSchema", crate::as_raw_json(&self.read_schema_json)?)?;
        }
        if !self.key.is_empty() {
            struct_ser.serialize_field("key", &self.key)?;
        }
        if !self.uuid_ptr.is_empty() {
            struct_ser.serialize_field("uuidPtr", &self.uuid_ptr)?;
        }
        if !self.partition_fields.is_empty() {
            struct_ser.serialize_field("partitionFields", &self.partition_fields)?;
        }
        if !self.projections.is_empty() {
            struct_ser.serialize_field("projections", &self.projections)?;
        }
        if !self.ack_template_json.is_empty() {
            struct_ser.serialize_field("ackTemplate", crate::as_raw_json(&self.ack_template_json)?)?;
        }
        if let Some(v) = self.partition_template.as_ref() {
            struct_ser.serialize_field("partitionTemplate", v)?;
        }
        if let Some(v) = self.derivation.as_ref() {
            struct_ser.serialize_field("derivation", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CollectionSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "write_schema_json",
            "writeSchema",
            "read_schema_json",
            "readSchema",
            "key",
            "uuid_ptr",
            "uuidPtr",
            "partition_fields",
            "partitionFields",
            "projections",
            "ack_template_json",
            "ackTemplate",
            "partition_template",
            "partitionTemplate",
            "derivation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            WriteSchemaJson,
            ReadSchemaJson,
            Key,
            UuidPtr,
            PartitionFields,
            Projections,
            AckTemplateJson,
            PartitionTemplate,
            Derivation,
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
                            "writeSchema" | "write_schema_json" => Ok(GeneratedField::WriteSchemaJson),
                            "readSchema" | "read_schema_json" => Ok(GeneratedField::ReadSchemaJson),
                            "key" => Ok(GeneratedField::Key),
                            "uuidPtr" | "uuid_ptr" => Ok(GeneratedField::UuidPtr),
                            "partitionFields" | "partition_fields" => Ok(GeneratedField::PartitionFields),
                            "projections" => Ok(GeneratedField::Projections),
                            "ackTemplate" | "ack_template_json" => Ok(GeneratedField::AckTemplateJson),
                            "partitionTemplate" | "partition_template" => Ok(GeneratedField::PartitionTemplate),
                            "derivation" => Ok(GeneratedField::Derivation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CollectionSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CollectionSpec")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CollectionSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut write_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut read_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut key__ = None;
                let mut uuid_ptr__ = None;
                let mut partition_fields__ = None;
                let mut projections__ = None;
                let mut ack_template_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut partition_template__ = None;
                let mut derivation__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::WriteSchemaJson => {
                            if write_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writeSchema"));
                            }
                            write_schema_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReadSchemaJson => {
                            if read_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readSchema"));
                            }
                            read_schema_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = Some(map.next_value()?);
                        }
                        GeneratedField::UuidPtr => {
                            if uuid_ptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuidPtr"));
                            }
                            uuid_ptr__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionFields => {
                            if partition_fields__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionFields"));
                            }
                            partition_fields__ = Some(map.next_value()?);
                        }
                        GeneratedField::Projections => {
                            if projections__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projections"));
                            }
                            projections__ = Some(map.next_value()?);
                        }
                        GeneratedField::AckTemplateJson => {
                            if ack_template_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ackTemplate"));
                            }
                            ack_template_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionTemplate => {
                            if partition_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionTemplate"));
                            }
                            partition_template__ = map.next_value()?;
                        }
                        GeneratedField::Derivation => {
                            if derivation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("derivation"));
                            }
                            derivation__ = map.next_value()?;
                        }
                    }
                }
                Ok(CollectionSpec {
                    name: name__.unwrap_or_default(),
                    write_schema_json: write_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    read_schema_json: read_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    key: key__.unwrap_or_default(),
                    uuid_ptr: uuid_ptr__.unwrap_or_default(),
                    partition_fields: partition_fields__.unwrap_or_default(),
                    projections: projections__.unwrap_or_default(),
                    ack_template_json: ack_template_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    partition_template: partition_template__,
                    derivation: derivation__,
                })
            }
        }
        deserializer.deserialize_struct("flow.CollectionSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for collection_spec::Derivation {
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
        if !self.transforms.is_empty() {
            len += 1;
        }
        if !self.shuffle_key_types.is_empty() {
            len += 1;
        }
        if self.shard_template.is_some() {
            len += 1;
        }
        if self.recovery_log_template.is_some() {
            len += 1;
        }
        if !self.network_ports.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CollectionSpec.Derivation", len)?;
        if self.connector_type != 0 {
            let v = collection_spec::derivation::ConnectorType::from_i32(self.connector_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        if !self.transforms.is_empty() {
            struct_ser.serialize_field("transforms", &self.transforms)?;
        }
        if !self.shuffle_key_types.is_empty() {
            let v = self.shuffle_key_types.iter().cloned().map(|v| {
                collection_spec::derivation::ShuffleType::from_i32(v)
                    .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", v)))
                }).collect::<Result<Vec<_>, _>>()?;
            struct_ser.serialize_field("shuffleKeyTypes", &v)?;
        }
        if let Some(v) = self.shard_template.as_ref() {
            struct_ser.serialize_field("shardTemplate", v)?;
        }
        if let Some(v) = self.recovery_log_template.as_ref() {
            struct_ser.serialize_field("recoveryLogTemplate", v)?;
        }
        if !self.network_ports.is_empty() {
            struct_ser.serialize_field("networkPorts", &self.network_ports)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for collection_spec::Derivation {
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
            "transforms",
            "shuffle_key_types",
            "shuffleKeyTypes",
            "shard_template",
            "shardTemplate",
            "recovery_log_template",
            "recoveryLogTemplate",
            "network_ports",
            "networkPorts",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorType,
            ConfigJson,
            Transforms,
            ShuffleKeyTypes,
            ShardTemplate,
            RecoveryLogTemplate,
            NetworkPorts,
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
                            "transforms" => Ok(GeneratedField::Transforms),
                            "shuffleKeyTypes" | "shuffle_key_types" => Ok(GeneratedField::ShuffleKeyTypes),
                            "shardTemplate" | "shard_template" => Ok(GeneratedField::ShardTemplate),
                            "recoveryLogTemplate" | "recovery_log_template" => Ok(GeneratedField::RecoveryLogTemplate),
                            "networkPorts" | "network_ports" => Ok(GeneratedField::NetworkPorts),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = collection_spec::Derivation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CollectionSpec.Derivation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<collection_spec::Derivation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut transforms__ = None;
                let mut shuffle_key_types__ = None;
                let mut shard_template__ = None;
                let mut recovery_log_template__ = None;
                let mut network_ports__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map.next_value::<collection_spec::derivation::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::Transforms => {
                            if transforms__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transforms"));
                            }
                            transforms__ = Some(map.next_value()?);
                        }
                        GeneratedField::ShuffleKeyTypes => {
                            if shuffle_key_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleKeyTypes"));
                            }
                            shuffle_key_types__ = Some(map.next_value::<Vec<collection_spec::derivation::ShuffleType>>()?.into_iter().map(|x| x as i32).collect());
                        }
                        GeneratedField::ShardTemplate => {
                            if shard_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardTemplate"));
                            }
                            shard_template__ = map.next_value()?;
                        }
                        GeneratedField::RecoveryLogTemplate => {
                            if recovery_log_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recoveryLogTemplate"));
                            }
                            recovery_log_template__ = map.next_value()?;
                        }
                        GeneratedField::NetworkPorts => {
                            if network_ports__.is_some() {
                                return Err(serde::de::Error::duplicate_field("networkPorts"));
                            }
                            network_ports__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(collection_spec::Derivation {
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    transforms: transforms__.unwrap_or_default(),
                    shuffle_key_types: shuffle_key_types__.unwrap_or_default(),
                    shard_template: shard_template__,
                    recovery_log_template: recovery_log_template__,
                    network_ports: network_ports__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.CollectionSpec.Derivation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for collection_spec::derivation::ConnectorType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::InvalidConnectorType => "INVALID_CONNECTOR_TYPE",
            Self::Sqlite => "SQLITE",
            Self::Typescript => "TYPESCRIPT",
            Self::Image => "IMAGE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for collection_spec::derivation::ConnectorType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID_CONNECTOR_TYPE",
            "SQLITE",
            "TYPESCRIPT",
            "IMAGE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = collection_spec::derivation::ConnectorType;

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
                    .and_then(collection_spec::derivation::ConnectorType::from_i32)
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
                    .and_then(collection_spec::derivation::ConnectorType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID_CONNECTOR_TYPE" => Ok(collection_spec::derivation::ConnectorType::InvalidConnectorType),
                    "SQLITE" => Ok(collection_spec::derivation::ConnectorType::Sqlite),
                    "TYPESCRIPT" => Ok(collection_spec::derivation::ConnectorType::Typescript),
                    "IMAGE" => Ok(collection_spec::derivation::ConnectorType::Image),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for collection_spec::derivation::ShuffleType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::InvalidShuffleType => "INVALID_SHUFFLE_TYPE",
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::String => "STRING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for collection_spec::derivation::ShuffleType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID_SHUFFLE_TYPE",
            "BOOLEAN",
            "INTEGER",
            "STRING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = collection_spec::derivation::ShuffleType;

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
                    .and_then(collection_spec::derivation::ShuffleType::from_i32)
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
                    .and_then(collection_spec::derivation::ShuffleType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID_SHUFFLE_TYPE" => Ok(collection_spec::derivation::ShuffleType::InvalidShuffleType),
                    "BOOLEAN" => Ok(collection_spec::derivation::ShuffleType::Boolean),
                    "INTEGER" => Ok(collection_spec::derivation::ShuffleType::Integer),
                    "STRING" => Ok(collection_spec::derivation::ShuffleType::String),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for collection_spec::derivation::Transform {
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
        if self.partition_selector.is_some() {
            len += 1;
        }
        if self.priority != 0 {
            len += 1;
        }
        if self.read_delay_seconds != 0 {
            len += 1;
        }
        if !self.shuffle_key.is_empty() {
            len += 1;
        }
        if !self.shuffle_lambda_config_json.is_empty() {
            len += 1;
        }
        if !self.lambda_config_json.is_empty() {
            len += 1;
        }
        if self.read_only {
            len += 1;
        }
        if !self.journal_read_suffix.is_empty() {
            len += 1;
        }
        if self.not_before.is_some() {
            len += 1;
        }
        if self.not_after.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CollectionSpec.Derivation.Transform", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if let Some(v) = self.partition_selector.as_ref() {
            struct_ser.serialize_field("partitionSelector", v)?;
        }
        if self.priority != 0 {
            struct_ser.serialize_field("priority", &self.priority)?;
        }
        if self.read_delay_seconds != 0 {
            struct_ser.serialize_field("readDelaySeconds", &self.read_delay_seconds)?;
        }
        if !self.shuffle_key.is_empty() {
            struct_ser.serialize_field("shuffleKey", &self.shuffle_key)?;
        }
        if !self.shuffle_lambda_config_json.is_empty() {
            struct_ser.serialize_field("shuffleLambdaConfig", crate::as_raw_json(&self.shuffle_lambda_config_json)?)?;
        }
        if !self.lambda_config_json.is_empty() {
            struct_ser.serialize_field("lambdaConfig", crate::as_raw_json(&self.lambda_config_json)?)?;
        }
        if self.read_only {
            struct_ser.serialize_field("readOnly", &self.read_only)?;
        }
        if !self.journal_read_suffix.is_empty() {
            struct_ser.serialize_field("journalReadSuffix", &self.journal_read_suffix)?;
        }
        if let Some(v) = self.not_before.as_ref() {
            struct_ser.serialize_field("notBefore", v)?;
        }
        if let Some(v) = self.not_after.as_ref() {
            struct_ser.serialize_field("notAfter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for collection_spec::derivation::Transform {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "collection",
            "partition_selector",
            "partitionSelector",
            "priority",
            "read_delay_seconds",
            "readDelaySeconds",
            "shuffle_key",
            "shuffleKey",
            "shuffle_lambda_config_json",
            "shuffleLambdaConfig",
            "lambda_config_json",
            "lambdaConfig",
            "read_only",
            "readOnly",
            "journal_read_suffix",
            "journalReadSuffix",
            "not_before",
            "notBefore",
            "not_after",
            "notAfter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Collection,
            PartitionSelector,
            Priority,
            ReadDelaySeconds,
            ShuffleKey,
            ShuffleLambdaConfigJson,
            LambdaConfigJson,
            ReadOnly,
            JournalReadSuffix,
            NotBefore,
            NotAfter,
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
                            "partitionSelector" | "partition_selector" => Ok(GeneratedField::PartitionSelector),
                            "priority" => Ok(GeneratedField::Priority),
                            "readDelaySeconds" | "read_delay_seconds" => Ok(GeneratedField::ReadDelaySeconds),
                            "shuffleKey" | "shuffle_key" => Ok(GeneratedField::ShuffleKey),
                            "shuffleLambdaConfig" | "shuffle_lambda_config_json" => Ok(GeneratedField::ShuffleLambdaConfigJson),
                            "lambdaConfig" | "lambda_config_json" => Ok(GeneratedField::LambdaConfigJson),
                            "readOnly" | "read_only" => Ok(GeneratedField::ReadOnly),
                            "journalReadSuffix" | "journal_read_suffix" => Ok(GeneratedField::JournalReadSuffix),
                            "notBefore" | "not_before" => Ok(GeneratedField::NotBefore),
                            "notAfter" | "not_after" => Ok(GeneratedField::NotAfter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = collection_spec::derivation::Transform;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CollectionSpec.Derivation.Transform")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<collection_spec::derivation::Transform, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut collection__ = None;
                let mut partition_selector__ = None;
                let mut priority__ = None;
                let mut read_delay_seconds__ = None;
                let mut shuffle_key__ = None;
                let mut shuffle_lambda_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut lambda_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut read_only__ = None;
                let mut journal_read_suffix__ = None;
                let mut not_before__ = None;
                let mut not_after__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map.next_value()?;
                        }
                        GeneratedField::PartitionSelector => {
                            if partition_selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionSelector"));
                            }
                            partition_selector__ = map.next_value()?;
                        }
                        GeneratedField::Priority => {
                            if priority__.is_some() {
                                return Err(serde::de::Error::duplicate_field("priority"));
                            }
                            priority__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ReadDelaySeconds => {
                            if read_delay_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readDelaySeconds"));
                            }
                            read_delay_seconds__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ShuffleKey => {
                            if shuffle_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleKey"));
                            }
                            shuffle_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::ShuffleLambdaConfigJson => {
                            if shuffle_lambda_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleLambdaConfig"));
                            }
                            shuffle_lambda_config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::LambdaConfigJson => {
                            if lambda_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lambdaConfig"));
                            }
                            lambda_config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReadOnly => {
                            if read_only__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readOnly"));
                            }
                            read_only__ = Some(map.next_value()?);
                        }
                        GeneratedField::JournalReadSuffix => {
                            if journal_read_suffix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalReadSuffix"));
                            }
                            journal_read_suffix__ = Some(map.next_value()?);
                        }
                        GeneratedField::NotBefore => {
                            if not_before__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notBefore"));
                            }
                            not_before__ = map.next_value()?;
                        }
                        GeneratedField::NotAfter => {
                            if not_after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notAfter"));
                            }
                            not_after__ = map.next_value()?;
                        }
                    }
                }
                Ok(collection_spec::derivation::Transform {
                    name: name__.unwrap_or_default(),
                    collection: collection__,
                    partition_selector: partition_selector__,
                    priority: priority__.unwrap_or_default(),
                    read_delay_seconds: read_delay_seconds__.unwrap_or_default(),
                    shuffle_key: shuffle_key__.unwrap_or_default(),
                    shuffle_lambda_config_json: shuffle_lambda_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    lambda_config_json: lambda_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    read_only: read_only__.unwrap_or_default(),
                    journal_read_suffix: journal_read_suffix__.unwrap_or_default(),
                    not_before: not_before__,
                    not_after: not_after__,
                })
            }
        }
        deserializer.deserialize_struct("flow.CollectionSpec.Derivation.Transform", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CombineApi {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.CombineAPI", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CombineApi {
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
            type Value = CombineApi;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CombineAPI")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CombineApi, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(CombineApi {
                })
            }
        }
        deserializer.deserialize_struct("flow.CombineAPI", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for combine_api::Code {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Configure => "CONFIGURE",
            Self::ReduceLeft => "REDUCE_LEFT",
            Self::CombineRight => "COMBINE_RIGHT",
            Self::DrainChunk => "DRAIN_CHUNK",
            Self::DrainedCombinedDocument => "DRAINED_COMBINED_DOCUMENT",
            Self::DrainedReducedDocument => "DRAINED_REDUCED_DOCUMENT",
            Self::DrainedKey => "DRAINED_KEY",
            Self::DrainedFields => "DRAINED_FIELDS",
            Self::DrainedStats => "DRAINED_STATS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for combine_api::Code {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "CONFIGURE",
            "REDUCE_LEFT",
            "COMBINE_RIGHT",
            "DRAIN_CHUNK",
            "DRAINED_COMBINED_DOCUMENT",
            "DRAINED_REDUCED_DOCUMENT",
            "DRAINED_KEY",
            "DRAINED_FIELDS",
            "DRAINED_STATS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = combine_api::Code;

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
                    .and_then(combine_api::Code::from_i32)
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
                    .and_then(combine_api::Code::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(combine_api::Code::Invalid),
                    "CONFIGURE" => Ok(combine_api::Code::Configure),
                    "REDUCE_LEFT" => Ok(combine_api::Code::ReduceLeft),
                    "COMBINE_RIGHT" => Ok(combine_api::Code::CombineRight),
                    "DRAIN_CHUNK" => Ok(combine_api::Code::DrainChunk),
                    "DRAINED_COMBINED_DOCUMENT" => Ok(combine_api::Code::DrainedCombinedDocument),
                    "DRAINED_REDUCED_DOCUMENT" => Ok(combine_api::Code::DrainedReducedDocument),
                    "DRAINED_KEY" => Ok(combine_api::Code::DrainedKey),
                    "DRAINED_FIELDS" => Ok(combine_api::Code::DrainedFields),
                    "DRAINED_STATS" => Ok(combine_api::Code::DrainedStats),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for combine_api::Config {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schema_json.is_empty() {
            len += 1;
        }
        if !self.key_ptrs.is_empty() {
            len += 1;
        }
        if !self.fields.is_empty() {
            len += 1;
        }
        if !self.uuid_placeholder_ptr.is_empty() {
            len += 1;
        }
        if !self.projections.is_empty() {
            len += 1;
        }
        if !self.collection_name.is_empty() {
            len += 1;
        }
        if !self.infer_schema_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CombineAPI.Config", len)?;
        if !self.schema_json.is_empty() {
            struct_ser.serialize_field("schemaJson", crate::as_raw_json(&self.schema_json)?)?;
        }
        if !self.key_ptrs.is_empty() {
            struct_ser.serialize_field("keyPtrs", &self.key_ptrs)?;
        }
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        if !self.uuid_placeholder_ptr.is_empty() {
            struct_ser.serialize_field("uuidPlaceholderPtr", &self.uuid_placeholder_ptr)?;
        }
        if !self.projections.is_empty() {
            struct_ser.serialize_field("projections", &self.projections)?;
        }
        if !self.collection_name.is_empty() {
            struct_ser.serialize_field("collectionName", &self.collection_name)?;
        }
        if !self.infer_schema_json.is_empty() {
            struct_ser.serialize_field("inferSchemaJson", crate::as_raw_json(&self.infer_schema_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for combine_api::Config {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema_json",
            "schemaJson",
            "key_ptrs",
            "keyPtrs",
            "fields",
            "uuid_placeholder_ptr",
            "uuidPlaceholderPtr",
            "projections",
            "collection_name",
            "collectionName",
            "infer_schema_json",
            "inferSchemaJson",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaJson,
            KeyPtrs,
            Fields,
            UuidPlaceholderPtr,
            Projections,
            CollectionName,
            InferSchemaJson,
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
                            "schemaJson" | "schema_json" => Ok(GeneratedField::SchemaJson),
                            "keyPtrs" | "key_ptrs" => Ok(GeneratedField::KeyPtrs),
                            "fields" => Ok(GeneratedField::Fields),
                            "uuidPlaceholderPtr" | "uuid_placeholder_ptr" => Ok(GeneratedField::UuidPlaceholderPtr),
                            "projections" => Ok(GeneratedField::Projections),
                            "collectionName" | "collection_name" => Ok(GeneratedField::CollectionName),
                            "inferSchemaJson" | "infer_schema_json" => Ok(GeneratedField::InferSchemaJson),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = combine_api::Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CombineAPI.Config")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<combine_api::Config, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut key_ptrs__ = None;
                let mut fields__ = None;
                let mut uuid_placeholder_ptr__ = None;
                let mut projections__ = None;
                let mut collection_name__ = None;
                let mut infer_schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SchemaJson => {
                            if schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaJson"));
                            }
                            schema_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::KeyPtrs => {
                            if key_ptrs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPtrs"));
                            }
                            key_ptrs__ = Some(map.next_value()?);
                        }
                        GeneratedField::Fields => {
                            if fields__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields__ = Some(map.next_value()?);
                        }
                        GeneratedField::UuidPlaceholderPtr => {
                            if uuid_placeholder_ptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuidPlaceholderPtr"));
                            }
                            uuid_placeholder_ptr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Projections => {
                            if projections__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projections"));
                            }
                            projections__ = Some(map.next_value()?);
                        }
                        GeneratedField::CollectionName => {
                            if collection_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collectionName"));
                            }
                            collection_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::InferSchemaJson => {
                            if infer_schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inferSchemaJson"));
                            }
                            infer_schema_json__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(combine_api::Config {
                    schema_json: schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    key_ptrs: key_ptrs__.unwrap_or_default(),
                    fields: fields__.unwrap_or_default(),
                    uuid_placeholder_ptr: uuid_placeholder_ptr__.unwrap_or_default(),
                    projections: projections__.unwrap_or_default(),
                    collection_name: collection_name__.unwrap_or_default(),
                    infer_schema_json: infer_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.CombineAPI.Config", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for combine_api::Stats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if self.out.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.CombineAPI.Stats", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if let Some(v) = self.out.as_ref() {
            struct_ser.serialize_field("out", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for combine_api::Stats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "out",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
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
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
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
            type Value = combine_api::Stats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.CombineAPI.Stats")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<combine_api::Stats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut out__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                        GeneratedField::Out => {
                            if out__.is_some() {
                                return Err(serde::de::Error::duplicate_field("out"));
                            }
                            out__ = map.next_value()?;
                        }
                    }
                }
                Ok(combine_api::Stats {
                    left: left__,
                    right: right__,
                    out: out__,
                })
            }
        }
        deserializer.deserialize_struct("flow.CombineAPI.Stats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectorState {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.updated_json.is_empty() {
            len += 1;
        }
        if self.merge_patch {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.ConnectorState", len)?;
        if !self.updated_json.is_empty() {
            struct_ser.serialize_field("updated", crate::as_raw_json(&self.updated_json)?)?;
        }
        if self.merge_patch {
            struct_ser.serialize_field("mergePatch", &self.merge_patch)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectorState {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "updated_json",
            "updated",
            "merge_patch",
            "mergePatch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UpdatedJson,
            MergePatch,
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
                            "updated" | "updated_json" => Ok(GeneratedField::UpdatedJson),
                            "mergePatch" | "merge_patch" => Ok(GeneratedField::MergePatch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectorState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.ConnectorState")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ConnectorState, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut updated_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut merge_patch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UpdatedJson => {
                            if updated_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("updated"));
                            }
                            updated_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::MergePatch => {
                            if merge_patch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mergePatch"));
                            }
                            merge_patch__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ConnectorState {
                    updated_json: updated_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    merge_patch: merge_patch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.ConnectorState", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ContentType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Catalog => "CATALOG",
            Self::JsonSchema => "JSON_SCHEMA",
            Self::Config => "CONFIG",
            Self::DocumentsFixture => "DOCUMENTS_FIXTURE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ContentType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "CATALOG",
            "JSON_SCHEMA",
            "CONFIG",
            "DOCUMENTS_FIXTURE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ContentType;

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
                    .and_then(ContentType::from_i32)
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
                    .and_then(ContentType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CATALOG" => Ok(ContentType::Catalog),
                    "JSON_SCHEMA" => Ok(ContentType::JsonSchema),
                    "CONFIG" => Ok(ContentType::Config),
                    "DOCUMENTS_FIXTURE" => Ok(ContentType::DocumentsFixture),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for DocsAndBytes {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.docs != 0 {
            len += 1;
        }
        if self.bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.DocsAndBytes", len)?;
        if self.docs != 0 {
            struct_ser.serialize_field("docs", &self.docs)?;
        }
        if self.bytes != 0 {
            struct_ser.serialize_field("bytes", &self.bytes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DocsAndBytes {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "docs",
            "bytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Docs,
            Bytes,
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
                            "docs" => Ok(GeneratedField::Docs),
                            "bytes" => Ok(GeneratedField::Bytes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DocsAndBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.DocsAndBytes")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DocsAndBytes, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut docs__ = None;
                let mut bytes__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Docs => {
                            if docs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docs"));
                            }
                            docs__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Bytes => {
                            if bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bytes"));
                            }
                            bytes__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(DocsAndBytes {
                    docs: docs__.unwrap_or_default(),
                    bytes: bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.DocsAndBytes", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExtractApi {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.ExtractAPI", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExtractApi {
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
            type Value = ExtractApi;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.ExtractAPI")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExtractApi, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ExtractApi {
                })
            }
        }
        deserializer.deserialize_struct("flow.ExtractAPI", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for extract_api::Code {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Configure => "CONFIGURE",
            Self::Extract => "EXTRACT",
            Self::ExtractedUuid => "EXTRACTED_UUID",
            Self::ExtractedFields => "EXTRACTED_FIELDS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for extract_api::Code {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "CONFIGURE",
            "EXTRACT",
            "EXTRACTED_UUID",
            "EXTRACTED_FIELDS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = extract_api::Code;

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
                    .and_then(extract_api::Code::from_i32)
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
                    .and_then(extract_api::Code::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(extract_api::Code::Invalid),
                    "CONFIGURE" => Ok(extract_api::Code::Configure),
                    "EXTRACT" => Ok(extract_api::Code::Extract),
                    "EXTRACTED_UUID" => Ok(extract_api::Code::ExtractedUuid),
                    "EXTRACTED_FIELDS" => Ok(extract_api::Code::ExtractedFields),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for extract_api::Config {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.uuid_ptr.is_empty() {
            len += 1;
        }
        if !self.schema_json.is_empty() {
            len += 1;
        }
        if !self.field_ptrs.is_empty() {
            len += 1;
        }
        if !self.projections.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.ExtractAPI.Config", len)?;
        if !self.uuid_ptr.is_empty() {
            struct_ser.serialize_field("uuidPtr", &self.uuid_ptr)?;
        }
        if !self.schema_json.is_empty() {
            struct_ser.serialize_field("schemaJson", crate::as_raw_json(&self.schema_json)?)?;
        }
        if !self.field_ptrs.is_empty() {
            struct_ser.serialize_field("fieldPtrs", &self.field_ptrs)?;
        }
        if !self.projections.is_empty() {
            struct_ser.serialize_field("projections", &self.projections)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for extract_api::Config {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "uuid_ptr",
            "uuidPtr",
            "schema_json",
            "schemaJson",
            "field_ptrs",
            "fieldPtrs",
            "projections",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UuidPtr,
            SchemaJson,
            FieldPtrs,
            Projections,
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
                            "uuidPtr" | "uuid_ptr" => Ok(GeneratedField::UuidPtr),
                            "schemaJson" | "schema_json" => Ok(GeneratedField::SchemaJson),
                            "fieldPtrs" | "field_ptrs" => Ok(GeneratedField::FieldPtrs),
                            "projections" => Ok(GeneratedField::Projections),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = extract_api::Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.ExtractAPI.Config")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<extract_api::Config, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut uuid_ptr__ = None;
                let mut schema_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut field_ptrs__ = None;
                let mut projections__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UuidPtr => {
                            if uuid_ptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuidPtr"));
                            }
                            uuid_ptr__ = Some(map.next_value()?);
                        }
                        GeneratedField::SchemaJson => {
                            if schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaJson"));
                            }
                            schema_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::FieldPtrs => {
                            if field_ptrs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldPtrs"));
                            }
                            field_ptrs__ = Some(map.next_value()?);
                        }
                        GeneratedField::Projections => {
                            if projections__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projections"));
                            }
                            projections__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(extract_api::Config {
                    uuid_ptr: uuid_ptr__.unwrap_or_default(),
                    schema_json: schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    field_ptrs: field_ptrs__.unwrap_or_default(),
                    projections: projections__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.ExtractAPI.Config", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FieldSelection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.keys.is_empty() {
            len += 1;
        }
        if !self.values.is_empty() {
            len += 1;
        }
        if !self.document.is_empty() {
            len += 1;
        }
        if !self.field_config_json_map.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.FieldSelection", len)?;
        if !self.keys.is_empty() {
            struct_ser.serialize_field("keys", &self.keys)?;
        }
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        if !self.document.is_empty() {
            struct_ser.serialize_field("document", &self.document)?;
        }
        if !self.field_config_json_map.is_empty() {
            struct_ser.serialize_field("fieldConfig", &crate::as_raw_json_map(&self.field_config_json_map)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FieldSelection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "keys",
            "values",
            "document",
            "field_config_json_map",
            "fieldConfig",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Keys,
            Values,
            Document,
            FieldConfigJsonMap,
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
                            "keys" => Ok(GeneratedField::Keys),
                            "values" => Ok(GeneratedField::Values),
                            "document" => Ok(GeneratedField::Document),
                            "fieldConfig" | "field_config_json_map" => Ok(GeneratedField::FieldConfigJsonMap),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FieldSelection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.FieldSelection")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FieldSelection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut keys__ = None;
                let mut values__ = None;
                let mut document__ = None;
                let mut field_config_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Keys => {
                            if keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keys"));
                            }
                            keys__ = Some(map.next_value()?);
                        }
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = Some(map.next_value()?);
                        }
                        GeneratedField::Document => {
                            if document__.is_some() {
                                return Err(serde::de::Error::duplicate_field("document"));
                            }
                            document__ = Some(map.next_value()?);
                        }
                        GeneratedField::FieldConfigJsonMap => {
                            if field_config_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldConfig"));
                            }
                            field_config_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(FieldSelection {
                    keys: keys__.unwrap_or_default(),
                    values: values__.unwrap_or_default(),
                    document: document__.unwrap_or_default(),
                    field_config_json_map: field_config_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                })
            }
        }
        deserializer.deserialize_struct("flow.FieldSelection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Inference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.types.is_empty() {
            len += 1;
        }
        if self.string.is_some() {
            len += 1;
        }
        if !self.title.is_empty() {
            len += 1;
        }
        if !self.description.is_empty() {
            len += 1;
        }
        if !self.default_json.is_empty() {
            len += 1;
        }
        if self.secret {
            len += 1;
        }
        if self.exists != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.Inference", len)?;
        if !self.types.is_empty() {
            struct_ser.serialize_field("types", &self.types)?;
        }
        if let Some(v) = self.string.as_ref() {
            struct_ser.serialize_field("string", v)?;
        }
        if !self.title.is_empty() {
            struct_ser.serialize_field("title", &self.title)?;
        }
        if !self.description.is_empty() {
            struct_ser.serialize_field("description", &self.description)?;
        }
        if !self.default_json.is_empty() {
            struct_ser.serialize_field("default", crate::as_raw_json(&self.default_json)?)?;
        }
        if self.secret {
            struct_ser.serialize_field("secret", &self.secret)?;
        }
        if self.exists != 0 {
            let v = inference::Exists::from_i32(self.exists)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.exists)))?;
            struct_ser.serialize_field("exists", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Inference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "types",
            "string",
            "title",
            "description",
            "default_json",
            "default",
            "secret",
            "exists",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Types,
            String,
            Title,
            Description,
            DefaultJson,
            Secret,
            Exists,
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
                            "types" => Ok(GeneratedField::Types),
                            "string" => Ok(GeneratedField::String),
                            "title" => Ok(GeneratedField::Title),
                            "description" => Ok(GeneratedField::Description),
                            "default" | "default_json" => Ok(GeneratedField::DefaultJson),
                            "secret" => Ok(GeneratedField::Secret),
                            "exists" => Ok(GeneratedField::Exists),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Inference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.Inference")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Inference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut types__ = None;
                let mut string__ = None;
                let mut title__ = None;
                let mut description__ = None;
                let mut default_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut secret__ = None;
                let mut exists__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Types => {
                            if types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("types"));
                            }
                            types__ = Some(map.next_value()?);
                        }
                        GeneratedField::String => {
                            if string__.is_some() {
                                return Err(serde::de::Error::duplicate_field("string"));
                            }
                            string__ = map.next_value()?;
                        }
                        GeneratedField::Title => {
                            if title__.is_some() {
                                return Err(serde::de::Error::duplicate_field("title"));
                            }
                            title__ = Some(map.next_value()?);
                        }
                        GeneratedField::Description => {
                            if description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("description"));
                            }
                            description__ = Some(map.next_value()?);
                        }
                        GeneratedField::DefaultJson => {
                            if default_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("default"));
                            }
                            default_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::Secret => {
                            if secret__.is_some() {
                                return Err(serde::de::Error::duplicate_field("secret"));
                            }
                            secret__ = Some(map.next_value()?);
                        }
                        GeneratedField::Exists => {
                            if exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exists"));
                            }
                            exists__ = Some(map.next_value::<inference::Exists>()? as i32);
                        }
                    }
                }
                Ok(Inference {
                    types: types__.unwrap_or_default(),
                    string: string__,
                    title: title__.unwrap_or_default(),
                    description: description__.unwrap_or_default(),
                    default_json: default_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    secret: secret__.unwrap_or_default(),
                    exists: exists__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.Inference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for inference::Exists {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Must => "MUST",
            Self::May => "MAY",
            Self::Implicit => "IMPLICIT",
            Self::Cannot => "CANNOT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for inference::Exists {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "MUST",
            "MAY",
            "IMPLICIT",
            "CANNOT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = inference::Exists;

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
                    .and_then(inference::Exists::from_i32)
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
                    .and_then(inference::Exists::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(inference::Exists::Invalid),
                    "MUST" => Ok(inference::Exists::Must),
                    "MAY" => Ok(inference::Exists::May),
                    "IMPLICIT" => Ok(inference::Exists::Implicit),
                    "CANNOT" => Ok(inference::Exists::Cannot),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for inference::String {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.content_type.is_empty() {
            len += 1;
        }
        if !self.format.is_empty() {
            len += 1;
        }
        if !self.content_encoding.is_empty() {
            len += 1;
        }
        if self.max_length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.Inference.String", len)?;
        if !self.content_type.is_empty() {
            struct_ser.serialize_field("contentType", &self.content_type)?;
        }
        if !self.format.is_empty() {
            struct_ser.serialize_field("format", &self.format)?;
        }
        if !self.content_encoding.is_empty() {
            struct_ser.serialize_field("contentEncoding", &self.content_encoding)?;
        }
        if self.max_length != 0 {
            struct_ser.serialize_field("maxLength", &self.max_length)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for inference::String {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "content_type",
            "contentType",
            "format",
            "content_encoding",
            "contentEncoding",
            "max_length",
            "maxLength",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContentType,
            Format,
            ContentEncoding,
            MaxLength,
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
                            "contentType" | "content_type" => Ok(GeneratedField::ContentType),
                            "format" => Ok(GeneratedField::Format),
                            "contentEncoding" | "content_encoding" => Ok(GeneratedField::ContentEncoding),
                            "maxLength" | "max_length" => Ok(GeneratedField::MaxLength),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = inference::String;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.Inference.String")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<inference::String, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut content_type__ = None;
                let mut format__ = None;
                let mut content_encoding__ = None;
                let mut max_length__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContentType => {
                            if content_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("contentType"));
                            }
                            content_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::Format => {
                            if format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("format"));
                            }
                            format__ = Some(map.next_value()?);
                        }
                        GeneratedField::ContentEncoding => {
                            if content_encoding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("contentEncoding"));
                            }
                            content_encoding__ = Some(map.next_value()?);
                        }
                        GeneratedField::MaxLength => {
                            if max_length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxLength"));
                            }
                            max_length__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(inference::String {
                    content_type: content_type__.unwrap_or_default(),
                    format: format__.unwrap_or_default(),
                    content_encoding: content_encoding__.unwrap_or_default(),
                    max_length: max_length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.Inference.String", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IngestRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.collection.is_empty() {
            len += 1;
        }
        if !self.build_id.is_empty() {
            len += 1;
        }
        if !self.docs_json_vec.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.IngestRequest", len)?;
        if !self.collection.is_empty() {
            struct_ser.serialize_field("collection", &self.collection)?;
        }
        if !self.build_id.is_empty() {
            struct_ser.serialize_field("buildId", &self.build_id)?;
        }
        if !self.docs_json_vec.is_empty() {
            struct_ser.serialize_field("docs", &crate::as_raw_json_vec(&self.docs_json_vec)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IngestRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "collection",
            "build_id",
            "buildId",
            "docs_json_vec",
            "docs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Collection,
            BuildId,
            DocsJsonVec,
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
                            "buildId" | "build_id" => Ok(GeneratedField::BuildId),
                            "docs" | "docs_json_vec" => Ok(GeneratedField::DocsJsonVec),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IngestRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.IngestRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IngestRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut collection__ = None;
                let mut build_id__ = None;
                let mut docs_json_vec__ : Option<Vec<Box<serde_json::value::RawValue>>> = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = Some(map.next_value()?);
                        }
                        GeneratedField::BuildId => {
                            if build_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("buildId"));
                            }
                            build_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::DocsJsonVec => {
                            if docs_json_vec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docs"));
                            }
                            docs_json_vec__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(IngestRequest {
                    collection: collection__.unwrap_or_default(),
                    build_id: build_id__.unwrap_or_default(),
                    docs_json_vec: docs_json_vec__.unwrap_or_default().into_iter().map(|value| Box::<str>::from(value).into()).collect(),
                })
            }
        }
        deserializer.deserialize_struct("flow.IngestRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IngestResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.journal_write_heads.is_empty() {
            len += 1;
        }
        if self.journal_etcd.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.IngestResponse", len)?;
        if !self.journal_write_heads.is_empty() {
            let v: std::collections::HashMap<_, _> = self.journal_write_heads.iter()
                .map(|(k, v)| (k, v.to_string())).collect();
            struct_ser.serialize_field("journalWriteHeads", &v)?;
        }
        if let Some(v) = self.journal_etcd.as_ref() {
            struct_ser.serialize_field("journalEtcd", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IngestResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal_write_heads",
            "journalWriteHeads",
            "journal_etcd",
            "journalEtcd",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JournalWriteHeads,
            JournalEtcd,
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
                            "journalWriteHeads" | "journal_write_heads" => Ok(GeneratedField::JournalWriteHeads),
                            "journalEtcd" | "journal_etcd" => Ok(GeneratedField::JournalEtcd),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IngestResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.IngestResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IngestResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal_write_heads__ = None;
                let mut journal_etcd__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JournalWriteHeads => {
                            if journal_write_heads__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalWriteHeads"));
                            }
                            journal_write_heads__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, ::pbjson::private::NumberDeserialize<i64>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::JournalEtcd => {
                            if journal_etcd__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalEtcd"));
                            }
                            journal_etcd__ = map.next_value()?;
                        }
                    }
                }
                Ok(IngestResponse {
                    journal_write_heads: journal_write_heads__.unwrap_or_default(),
                    journal_etcd: journal_etcd__,
                })
            }
        }
        deserializer.deserialize_struct("flow.IngestResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaterializationSpec {
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
        if self.shard_template.is_some() {
            len += 1;
        }
        if self.recovery_log_template.is_some() {
            len += 1;
        }
        if !self.network_ports.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.MaterializationSpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.connector_type != 0 {
            let v = materialization_spec::ConnectorType::from_i32(self.connector_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if let Some(v) = self.shard_template.as_ref() {
            struct_ser.serialize_field("shardTemplate", v)?;
        }
        if let Some(v) = self.recovery_log_template.as_ref() {
            struct_ser.serialize_field("recoveryLogTemplate", v)?;
        }
        if !self.network_ports.is_empty() {
            struct_ser.serialize_field("networkPorts", &self.network_ports)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaterializationSpec {
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
            "shard_template",
            "shardTemplate",
            "recovery_log_template",
            "recoveryLogTemplate",
            "network_ports",
            "networkPorts",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ConnectorType,
            ConfigJson,
            Bindings,
            ShardTemplate,
            RecoveryLogTemplate,
            NetworkPorts,
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
                            "shardTemplate" | "shard_template" => Ok(GeneratedField::ShardTemplate),
                            "recoveryLogTemplate" | "recovery_log_template" => Ok(GeneratedField::RecoveryLogTemplate),
                            "networkPorts" | "network_ports" => Ok(GeneratedField::NetworkPorts),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaterializationSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.MaterializationSpec")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MaterializationSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut bindings__ = None;
                let mut shard_template__ = None;
                let mut recovery_log_template__ = None;
                let mut network_ports__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ConnectorType => {
                            if connector_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorType"));
                            }
                            connector_type__ = Some(map.next_value::<materialization_spec::ConnectorType>()? as i32);
                        }
                        GeneratedField::ConfigJson => {
                            if config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map.next_value()?);
                        }
                        GeneratedField::ShardTemplate => {
                            if shard_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardTemplate"));
                            }
                            shard_template__ = map.next_value()?;
                        }
                        GeneratedField::RecoveryLogTemplate => {
                            if recovery_log_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recoveryLogTemplate"));
                            }
                            recovery_log_template__ = map.next_value()?;
                        }
                        GeneratedField::NetworkPorts => {
                            if network_ports__.is_some() {
                                return Err(serde::de::Error::duplicate_field("networkPorts"));
                            }
                            network_ports__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MaterializationSpec {
                    name: name__.unwrap_or_default(),
                    connector_type: connector_type__.unwrap_or_default(),
                    config_json: config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    bindings: bindings__.unwrap_or_default(),
                    shard_template: shard_template__,
                    recovery_log_template: recovery_log_template__,
                    network_ports: network_ports__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.MaterializationSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialization_spec::Binding {
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
        if !self.resource_path.is_empty() {
            len += 1;
        }
        if self.collection.is_some() {
            len += 1;
        }
        if self.partition_selector.is_some() {
            len += 1;
        }
        if self.priority != 0 {
            len += 1;
        }
        if self.field_selection.is_some() {
            len += 1;
        }
        if self.delta_updates {
            len += 1;
        }
        if self.deprecated_shuffle.is_some() {
            len += 1;
        }
        if !self.journal_read_suffix.is_empty() {
            len += 1;
        }
        if self.not_before.is_some() {
            len += 1;
        }
        if self.not_after.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.MaterializationSpec.Binding", len)?;
        if !self.resource_config_json.is_empty() {
            struct_ser.serialize_field("resourceConfig", crate::as_raw_json(&self.resource_config_json)?)?;
        }
        if !self.resource_path.is_empty() {
            struct_ser.serialize_field("resourcePath", &self.resource_path)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if let Some(v) = self.partition_selector.as_ref() {
            struct_ser.serialize_field("partitionSelector", v)?;
        }
        if self.priority != 0 {
            struct_ser.serialize_field("priority", &self.priority)?;
        }
        if let Some(v) = self.field_selection.as_ref() {
            struct_ser.serialize_field("fieldSelection", v)?;
        }
        if self.delta_updates {
            struct_ser.serialize_field("deltaUpdates", &self.delta_updates)?;
        }
        if let Some(v) = self.deprecated_shuffle.as_ref() {
            struct_ser.serialize_field("deprecatedShuffle", v)?;
        }
        if !self.journal_read_suffix.is_empty() {
            struct_ser.serialize_field("journalReadSuffix", &self.journal_read_suffix)?;
        }
        if let Some(v) = self.not_before.as_ref() {
            struct_ser.serialize_field("notBefore", v)?;
        }
        if let Some(v) = self.not_after.as_ref() {
            struct_ser.serialize_field("notAfter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialization_spec::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "resource_config_json",
            "resourceConfig",
            "resource_path",
            "resourcePath",
            "collection",
            "partition_selector",
            "partitionSelector",
            "priority",
            "field_selection",
            "fieldSelection",
            "delta_updates",
            "deltaUpdates",
            "deprecated_shuffle",
            "deprecatedShuffle",
            "journal_read_suffix",
            "journalReadSuffix",
            "not_before",
            "notBefore",
            "not_after",
            "notAfter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ResourceConfigJson,
            ResourcePath,
            Collection,
            PartitionSelector,
            Priority,
            FieldSelection,
            DeltaUpdates,
            DeprecatedShuffle,
            JournalReadSuffix,
            NotBefore,
            NotAfter,
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
                            "resourcePath" | "resource_path" => Ok(GeneratedField::ResourcePath),
                            "collection" => Ok(GeneratedField::Collection),
                            "partitionSelector" | "partition_selector" => Ok(GeneratedField::PartitionSelector),
                            "priority" => Ok(GeneratedField::Priority),
                            "fieldSelection" | "field_selection" => Ok(GeneratedField::FieldSelection),
                            "deltaUpdates" | "delta_updates" => Ok(GeneratedField::DeltaUpdates),
                            "deprecatedShuffle" | "deprecated_shuffle" => Ok(GeneratedField::DeprecatedShuffle),
                            "journalReadSuffix" | "journal_read_suffix" => Ok(GeneratedField::JournalReadSuffix),
                            "notBefore" | "not_before" => Ok(GeneratedField::NotBefore),
                            "notAfter" | "not_after" => Ok(GeneratedField::NotAfter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialization_spec::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.MaterializationSpec.Binding")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<materialization_spec::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut resource_path__ = None;
                let mut collection__ = None;
                let mut partition_selector__ = None;
                let mut priority__ = None;
                let mut field_selection__ = None;
                let mut delta_updates__ = None;
                let mut deprecated_shuffle__ = None;
                let mut journal_read_suffix__ = None;
                let mut not_before__ = None;
                let mut not_after__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ResourceConfigJson => {
                            if resource_config_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceConfig"));
                            }
                            resource_config_json__ = Some(map.next_value()?);
                        }
                        GeneratedField::ResourcePath => {
                            if resource_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePath"));
                            }
                            resource_path__ = Some(map.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = map.next_value()?;
                        }
                        GeneratedField::PartitionSelector => {
                            if partition_selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionSelector"));
                            }
                            partition_selector__ = map.next_value()?;
                        }
                        GeneratedField::Priority => {
                            if priority__.is_some() {
                                return Err(serde::de::Error::duplicate_field("priority"));
                            }
                            priority__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FieldSelection => {
                            if field_selection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldSelection"));
                            }
                            field_selection__ = map.next_value()?;
                        }
                        GeneratedField::DeltaUpdates => {
                            if delta_updates__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deltaUpdates"));
                            }
                            delta_updates__ = Some(map.next_value()?);
                        }
                        GeneratedField::DeprecatedShuffle => {
                            if deprecated_shuffle__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deprecatedShuffle"));
                            }
                            deprecated_shuffle__ = map.next_value()?;
                        }
                        GeneratedField::JournalReadSuffix => {
                            if journal_read_suffix__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journalReadSuffix"));
                            }
                            journal_read_suffix__ = Some(map.next_value()?);
                        }
                        GeneratedField::NotBefore => {
                            if not_before__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notBefore"));
                            }
                            not_before__ = map.next_value()?;
                        }
                        GeneratedField::NotAfter => {
                            if not_after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notAfter"));
                            }
                            not_after__ = map.next_value()?;
                        }
                    }
                }
                Ok(materialization_spec::Binding {
                    resource_config_json: resource_config_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    resource_path: resource_path__.unwrap_or_default(),
                    collection: collection__,
                    partition_selector: partition_selector__,
                    priority: priority__.unwrap_or_default(),
                    field_selection: field_selection__,
                    delta_updates: delta_updates__.unwrap_or_default(),
                    deprecated_shuffle: deprecated_shuffle__,
                    journal_read_suffix: journal_read_suffix__.unwrap_or_default(),
                    not_before: not_before__,
                    not_after: not_after__,
                })
            }
        }
        deserializer.deserialize_struct("flow.MaterializationSpec.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialization_spec::binding::DeprecatedShuffle {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.group_name.is_empty() {
            len += 1;
        }
        if self.partition_selector.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.MaterializationSpec.Binding.DeprecatedShuffle", len)?;
        if !self.group_name.is_empty() {
            struct_ser.serialize_field("groupName", &self.group_name)?;
        }
        if let Some(v) = self.partition_selector.as_ref() {
            struct_ser.serialize_field("partitionSelector", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialization_spec::binding::DeprecatedShuffle {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "group_name",
            "groupName",
            "partition_selector",
            "partitionSelector",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            GroupName,
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
                            "groupName" | "group_name" => Ok(GeneratedField::GroupName),
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
            type Value = materialization_spec::binding::DeprecatedShuffle;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.MaterializationSpec.Binding.DeprecatedShuffle")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<materialization_spec::binding::DeprecatedShuffle, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut group_name__ = None;
                let mut partition_selector__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::GroupName => {
                            if group_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupName"));
                            }
                            group_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionSelector => {
                            if partition_selector__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionSelector"));
                            }
                            partition_selector__ = map.next_value()?;
                        }
                    }
                }
                Ok(materialization_spec::binding::DeprecatedShuffle {
                    group_name: group_name__.unwrap_or_default(),
                    partition_selector: partition_selector__,
                })
            }
        }
        deserializer.deserialize_struct("flow.MaterializationSpec.Binding.DeprecatedShuffle", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialization_spec::ConnectorType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Sqlite => "SQLITE",
            Self::Image => "IMAGE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for materialization_spec::ConnectorType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "SQLITE",
            "IMAGE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialization_spec::ConnectorType;

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
                    .and_then(materialization_spec::ConnectorType::from_i32)
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
                    .and_then(materialization_spec::ConnectorType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(materialization_spec::ConnectorType::Invalid),
                    "SQLITE" => Ok(materialization_spec::ConnectorType::Sqlite),
                    "IMAGE" => Ok(materialization_spec::ConnectorType::Image),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NetworkPort {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.number != 0 {
            len += 1;
        }
        if !self.protocol.is_empty() {
            len += 1;
        }
        if self.public {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.NetworkPort", len)?;
        if self.number != 0 {
            struct_ser.serialize_field("number", &self.number)?;
        }
        if !self.protocol.is_empty() {
            struct_ser.serialize_field("protocol", &self.protocol)?;
        }
        if self.public {
            struct_ser.serialize_field("public", &self.public)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NetworkPort {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "number",
            "protocol",
            "public",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Number,
            Protocol,
            Public,
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
                            "number" => Ok(GeneratedField::Number),
                            "protocol" => Ok(GeneratedField::Protocol),
                            "public" => Ok(GeneratedField::Public),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NetworkPort;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.NetworkPort")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<NetworkPort, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut number__ = None;
                let mut protocol__ = None;
                let mut public__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Number => {
                            if number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("number"));
                            }
                            number__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Protocol => {
                            if protocol__.is_some() {
                                return Err(serde::de::Error::duplicate_field("protocol"));
                            }
                            protocol__ = Some(map.next_value()?);
                        }
                        GeneratedField::Public => {
                            if public__.is_some() {
                                return Err(serde::de::Error::duplicate_field("public"));
                            }
                            public__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(NetworkPort {
                    number: number__.unwrap_or_default(),
                    protocol: protocol__.unwrap_or_default(),
                    public: public__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.NetworkPort", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OAuth2 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.provider.is_empty() {
            len += 1;
        }
        if !self.auth_url_template.is_empty() {
            len += 1;
        }
        if !self.access_token_url_template.is_empty() {
            len += 1;
        }
        if !self.access_token_method.is_empty() {
            len += 1;
        }
        if !self.access_token_body.is_empty() {
            len += 1;
        }
        if !self.access_token_headers_json_map.is_empty() {
            len += 1;
        }
        if !self.access_token_response_json_map.is_empty() {
            len += 1;
        }
        if !self.refresh_token_url_template.is_empty() {
            len += 1;
        }
        if !self.refresh_token_method.is_empty() {
            len += 1;
        }
        if !self.refresh_token_body.is_empty() {
            len += 1;
        }
        if !self.refresh_token_headers_json_map.is_empty() {
            len += 1;
        }
        if !self.refresh_token_response_json_map.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.OAuth2", len)?;
        if !self.provider.is_empty() {
            struct_ser.serialize_field("provider", &self.provider)?;
        }
        if !self.auth_url_template.is_empty() {
            struct_ser.serialize_field("authUrlTemplate", &self.auth_url_template)?;
        }
        if !self.access_token_url_template.is_empty() {
            struct_ser.serialize_field("accessTokenUrlTemplate", &self.access_token_url_template)?;
        }
        if !self.access_token_method.is_empty() {
            struct_ser.serialize_field("accessTokenMethod", &self.access_token_method)?;
        }
        if !self.access_token_body.is_empty() {
            struct_ser.serialize_field("accessTokenBody", &self.access_token_body)?;
        }
        if !self.access_token_headers_json_map.is_empty() {
            struct_ser.serialize_field("accessTokenHeaders", &crate::as_raw_json_map(&self.access_token_headers_json_map)?)?;
        }
        if !self.access_token_response_json_map.is_empty() {
            struct_ser.serialize_field("accessTokenResponseMap", &crate::as_raw_json_map(&self.access_token_response_json_map)?)?;
        }
        if !self.refresh_token_url_template.is_empty() {
            struct_ser.serialize_field("refreshTokenUrlTemplate", &self.refresh_token_url_template)?;
        }
        if !self.refresh_token_method.is_empty() {
            struct_ser.serialize_field("refreshTokenMethod", &self.refresh_token_method)?;
        }
        if !self.refresh_token_body.is_empty() {
            struct_ser.serialize_field("refreshTokenBody", &self.refresh_token_body)?;
        }
        if !self.refresh_token_headers_json_map.is_empty() {
            struct_ser.serialize_field("refreshTokenHeaders", &crate::as_raw_json_map(&self.refresh_token_headers_json_map)?)?;
        }
        if !self.refresh_token_response_json_map.is_empty() {
            struct_ser.serialize_field("refreshTokenResponseMap", &crate::as_raw_json_map(&self.refresh_token_response_json_map)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OAuth2 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "provider",
            "auth_url_template",
            "authUrlTemplate",
            "access_token_url_template",
            "accessTokenUrlTemplate",
            "access_token_method",
            "accessTokenMethod",
            "access_token_body",
            "accessTokenBody",
            "access_token_headers_json_map",
            "accessTokenHeaders",
            "access_token_response_json_map",
            "accessTokenResponseMap",
            "refresh_token_url_template",
            "refreshTokenUrlTemplate",
            "refresh_token_method",
            "refreshTokenMethod",
            "refresh_token_body",
            "refreshTokenBody",
            "refresh_token_headers_json_map",
            "refreshTokenHeaders",
            "refresh_token_response_json_map",
            "refreshTokenResponseMap",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Provider,
            AuthUrlTemplate,
            AccessTokenUrlTemplate,
            AccessTokenMethod,
            AccessTokenBody,
            AccessTokenHeadersJsonMap,
            AccessTokenResponseJsonMap,
            RefreshTokenUrlTemplate,
            RefreshTokenMethod,
            RefreshTokenBody,
            RefreshTokenHeadersJsonMap,
            RefreshTokenResponseJsonMap,
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
                            "provider" => Ok(GeneratedField::Provider),
                            "authUrlTemplate" | "auth_url_template" => Ok(GeneratedField::AuthUrlTemplate),
                            "accessTokenUrlTemplate" | "access_token_url_template" => Ok(GeneratedField::AccessTokenUrlTemplate),
                            "accessTokenMethod" | "access_token_method" => Ok(GeneratedField::AccessTokenMethod),
                            "accessTokenBody" | "access_token_body" => Ok(GeneratedField::AccessTokenBody),
                            "accessTokenHeaders" | "access_token_headers_json_map" => Ok(GeneratedField::AccessTokenHeadersJsonMap),
                            "accessTokenResponseMap" | "access_token_response_json_map" => Ok(GeneratedField::AccessTokenResponseJsonMap),
                            "refreshTokenUrlTemplate" | "refresh_token_url_template" => Ok(GeneratedField::RefreshTokenUrlTemplate),
                            "refreshTokenMethod" | "refresh_token_method" => Ok(GeneratedField::RefreshTokenMethod),
                            "refreshTokenBody" | "refresh_token_body" => Ok(GeneratedField::RefreshTokenBody),
                            "refreshTokenHeaders" | "refresh_token_headers_json_map" => Ok(GeneratedField::RefreshTokenHeadersJsonMap),
                            "refreshTokenResponseMap" | "refresh_token_response_json_map" => Ok(GeneratedField::RefreshTokenResponseJsonMap),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OAuth2;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.OAuth2")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OAuth2, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut provider__ = None;
                let mut auth_url_template__ = None;
                let mut access_token_url_template__ = None;
                let mut access_token_method__ = None;
                let mut access_token_body__ = None;
                let mut access_token_headers_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                let mut access_token_response_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                let mut refresh_token_url_template__ = None;
                let mut refresh_token_method__ = None;
                let mut refresh_token_body__ = None;
                let mut refresh_token_headers_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                let mut refresh_token_response_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Provider => {
                            if provider__.is_some() {
                                return Err(serde::de::Error::duplicate_field("provider"));
                            }
                            provider__ = Some(map.next_value()?);
                        }
                        GeneratedField::AuthUrlTemplate => {
                            if auth_url_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("authUrlTemplate"));
                            }
                            auth_url_template__ = Some(map.next_value()?);
                        }
                        GeneratedField::AccessTokenUrlTemplate => {
                            if access_token_url_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessTokenUrlTemplate"));
                            }
                            access_token_url_template__ = Some(map.next_value()?);
                        }
                        GeneratedField::AccessTokenMethod => {
                            if access_token_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessTokenMethod"));
                            }
                            access_token_method__ = Some(map.next_value()?);
                        }
                        GeneratedField::AccessTokenBody => {
                            if access_token_body__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessTokenBody"));
                            }
                            access_token_body__ = Some(map.next_value()?);
                        }
                        GeneratedField::AccessTokenHeadersJsonMap => {
                            if access_token_headers_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessTokenHeaders"));
                            }
                            access_token_headers_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::AccessTokenResponseJsonMap => {
                            if access_token_response_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("accessTokenResponseMap"));
                            }
                            access_token_response_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::RefreshTokenUrlTemplate => {
                            if refresh_token_url_template__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshTokenUrlTemplate"));
                            }
                            refresh_token_url_template__ = Some(map.next_value()?);
                        }
                        GeneratedField::RefreshTokenMethod => {
                            if refresh_token_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshTokenMethod"));
                            }
                            refresh_token_method__ = Some(map.next_value()?);
                        }
                        GeneratedField::RefreshTokenBody => {
                            if refresh_token_body__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshTokenBody"));
                            }
                            refresh_token_body__ = Some(map.next_value()?);
                        }
                        GeneratedField::RefreshTokenHeadersJsonMap => {
                            if refresh_token_headers_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshTokenHeaders"));
                            }
                            refresh_token_headers_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::RefreshTokenResponseJsonMap => {
                            if refresh_token_response_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refreshTokenResponseMap"));
                            }
                            refresh_token_response_json_map__ = Some(
                                map.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(OAuth2 {
                    provider: provider__.unwrap_or_default(),
                    auth_url_template: auth_url_template__.unwrap_or_default(),
                    access_token_url_template: access_token_url_template__.unwrap_or_default(),
                    access_token_method: access_token_method__.unwrap_or_default(),
                    access_token_body: access_token_body__.unwrap_or_default(),
                    access_token_headers_json_map: access_token_headers_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                    access_token_response_json_map: access_token_response_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                    refresh_token_url_template: refresh_token_url_template__.unwrap_or_default(),
                    refresh_token_method: refresh_token_method__.unwrap_or_default(),
                    refresh_token_body: refresh_token_body__.unwrap_or_default(),
                    refresh_token_headers_json_map: refresh_token_headers_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                    refresh_token_response_json_map: refresh_token_response_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                })
            }
        }
        deserializer.deserialize_struct("flow.OAuth2", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Projection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ptr.is_empty() {
            len += 1;
        }
        if !self.field.is_empty() {
            len += 1;
        }
        if self.explicit {
            len += 1;
        }
        if self.is_partition_key {
            len += 1;
        }
        if self.is_primary_key {
            len += 1;
        }
        if self.inference.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.Projection", len)?;
        if !self.ptr.is_empty() {
            struct_ser.serialize_field("ptr", &self.ptr)?;
        }
        if !self.field.is_empty() {
            struct_ser.serialize_field("field", &self.field)?;
        }
        if self.explicit {
            struct_ser.serialize_field("explicit", &self.explicit)?;
        }
        if self.is_partition_key {
            struct_ser.serialize_field("isPartitionKey", &self.is_partition_key)?;
        }
        if self.is_primary_key {
            struct_ser.serialize_field("isPrimaryKey", &self.is_primary_key)?;
        }
        if let Some(v) = self.inference.as_ref() {
            struct_ser.serialize_field("inference", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Projection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ptr",
            "field",
            "explicit",
            "is_partition_key",
            "isPartitionKey",
            "is_primary_key",
            "isPrimaryKey",
            "inference",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ptr,
            Field,
            Explicit,
            IsPartitionKey,
            IsPrimaryKey,
            Inference,
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
                            "ptr" => Ok(GeneratedField::Ptr),
                            "field" => Ok(GeneratedField::Field),
                            "explicit" => Ok(GeneratedField::Explicit),
                            "isPartitionKey" | "is_partition_key" => Ok(GeneratedField::IsPartitionKey),
                            "isPrimaryKey" | "is_primary_key" => Ok(GeneratedField::IsPrimaryKey),
                            "inference" => Ok(GeneratedField::Inference),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Projection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.Projection")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Projection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ptr__ = None;
                let mut field__ = None;
                let mut explicit__ = None;
                let mut is_partition_key__ = None;
                let mut is_primary_key__ = None;
                let mut inference__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Ptr => {
                            if ptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ptr"));
                            }
                            ptr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = Some(map.next_value()?);
                        }
                        GeneratedField::Explicit => {
                            if explicit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explicit"));
                            }
                            explicit__ = Some(map.next_value()?);
                        }
                        GeneratedField::IsPartitionKey => {
                            if is_partition_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isPartitionKey"));
                            }
                            is_partition_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::IsPrimaryKey => {
                            if is_primary_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isPrimaryKey"));
                            }
                            is_primary_key__ = Some(map.next_value()?);
                        }
                        GeneratedField::Inference => {
                            if inference__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inference"));
                            }
                            inference__ = map.next_value()?;
                        }
                    }
                }
                Ok(Projection {
                    ptr: ptr__.unwrap_or_default(),
                    field: field__.unwrap_or_default(),
                    explicit: explicit__.unwrap_or_default(),
                    is_partition_key: is_partition_key__.unwrap_or_default(),
                    is_primary_key: is_primary_key__.unwrap_or_default(),
                    inference: inference__,
                })
            }
        }
        deserializer.deserialize_struct("flow.Projection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RangeSpec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key_begin != 0 {
            len += 1;
        }
        if self.key_end != 0 {
            len += 1;
        }
        if self.r_clock_begin != 0 {
            len += 1;
        }
        if self.r_clock_end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.RangeSpec", len)?;
        if self.key_begin != 0 {
            struct_ser.serialize_field("keyBegin", &self.key_begin)?;
        }
        if self.key_end != 0 {
            struct_ser.serialize_field("keyEnd", &self.key_end)?;
        }
        if self.r_clock_begin != 0 {
            struct_ser.serialize_field("rClockBegin", &self.r_clock_begin)?;
        }
        if self.r_clock_end != 0 {
            struct_ser.serialize_field("rClockEnd", &self.r_clock_end)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RangeSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key_begin",
            "keyBegin",
            "key_end",
            "keyEnd",
            "r_clock_begin",
            "rClockBegin",
            "r_clock_end",
            "rClockEnd",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            KeyBegin,
            KeyEnd,
            RClockBegin,
            RClockEnd,
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
                            "keyBegin" | "key_begin" => Ok(GeneratedField::KeyBegin),
                            "keyEnd" | "key_end" => Ok(GeneratedField::KeyEnd),
                            "rClockBegin" | "r_clock_begin" => Ok(GeneratedField::RClockBegin),
                            "rClockEnd" | "r_clock_end" => Ok(GeneratedField::RClockEnd),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RangeSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.RangeSpec")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RangeSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key_begin__ = None;
                let mut key_end__ = None;
                let mut r_clock_begin__ = None;
                let mut r_clock_end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::KeyBegin => {
                            if key_begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyBegin"));
                            }
                            key_begin__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::KeyEnd => {
                            if key_end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyEnd"));
                            }
                            key_end__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RClockBegin => {
                            if r_clock_begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rClockBegin"));
                            }
                            r_clock_begin__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RClockEnd => {
                            if r_clock_end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rClockEnd"));
                            }
                            r_clock_end__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(RangeSpec {
                    key_begin: key_begin__.unwrap_or_default(),
                    key_end: key_end__.unwrap_or_default(),
                    r_clock_begin: r_clock_begin__.unwrap_or_default(),
                    r_clock_end: r_clock_end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.RangeSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResetStateRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.ResetStateRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResetStateRequest {
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
            type Value = ResetStateRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.ResetStateRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ResetStateRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ResetStateRequest {
                })
            }
        }
        deserializer.deserialize_struct("flow.ResetStateRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResetStateResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("flow.ResetStateResponse", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResetStateResponse {
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
            type Value = ResetStateResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.ResetStateResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ResetStateResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ResetStateResponse {
                })
            }
        }
        deserializer.deserialize_struct("flow.ResetStateResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Slice {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.begin != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.Slice", len)?;
        if self.begin != 0 {
            struct_ser.serialize_field("begin", &self.begin)?;
        }
        if self.end != 0 {
            struct_ser.serialize_field("end", &self.end)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Slice {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "begin",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Begin,
            End,
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
                            "begin" => Ok(GeneratedField::Begin),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Slice;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.Slice")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Slice, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut begin__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Begin => {
                            if begin__.is_some() {
                                return Err(serde::de::Error::duplicate_field("begin"));
                            }
                            begin__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Slice {
                    begin: begin__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.Slice", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskNetworkProxyRequest {
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
        if !self.data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.TaskNetworkProxyRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if !self.data.is_empty() {
            struct_ser.serialize_field("data", pbjson::private::base64::encode(&self.data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskNetworkProxyRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            Data,
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
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TaskNetworkProxyRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TaskNetworkProxyRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TaskNetworkProxyRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut data__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TaskNetworkProxyRequest {
                    open: open__,
                    data: data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.TaskNetworkProxyRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for task_network_proxy_request::Open {
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
        if !self.shard_id.is_empty() {
            len += 1;
        }
        if self.target_port != 0 {
            len += 1;
        }
        if !self.client_addr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.TaskNetworkProxyRequest.Open", len)?;
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.shard_id.is_empty() {
            struct_ser.serialize_field("shardId", &self.shard_id)?;
        }
        if self.target_port != 0 {
            struct_ser.serialize_field("targetPort", &self.target_port)?;
        }
        if !self.client_addr.is_empty() {
            struct_ser.serialize_field("clientAddr", &self.client_addr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for task_network_proxy_request::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "header",
            "shard_id",
            "shardId",
            "target_port",
            "targetPort",
            "client_addr",
            "clientAddr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Header,
            ShardId,
            TargetPort,
            ClientAddr,
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
                            "shardId" | "shard_id" => Ok(GeneratedField::ShardId),
                            "targetPort" | "target_port" => Ok(GeneratedField::TargetPort),
                            "clientAddr" | "client_addr" => Ok(GeneratedField::ClientAddr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = task_network_proxy_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TaskNetworkProxyRequest.Open")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<task_network_proxy_request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut header__ = None;
                let mut shard_id__ = None;
                let mut target_port__ = None;
                let mut client_addr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map.next_value()?;
                        }
                        GeneratedField::ShardId => {
                            if shard_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardId"));
                            }
                            shard_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::TargetPort => {
                            if target_port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetPort"));
                            }
                            target_port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ClientAddr => {
                            if client_addr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientAddr"));
                            }
                            client_addr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(task_network_proxy_request::Open {
                    header: header__,
                    shard_id: shard_id__.unwrap_or_default(),
                    target_port: target_port__.unwrap_or_default(),
                    client_addr: client_addr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.TaskNetworkProxyRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskNetworkProxyResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.open_response.is_some() {
            len += 1;
        }
        if !self.data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.TaskNetworkProxyResponse", len)?;
        if let Some(v) = self.open_response.as_ref() {
            struct_ser.serialize_field("openResponse", v)?;
        }
        if !self.data.is_empty() {
            struct_ser.serialize_field("data", pbjson::private::base64::encode(&self.data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskNetworkProxyResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open_response",
            "openResponse",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OpenResponse,
            Data,
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
                            "openResponse" | "open_response" => Ok(GeneratedField::OpenResponse),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TaskNetworkProxyResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TaskNetworkProxyResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TaskNetworkProxyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open_response__ = None;
                let mut data__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OpenResponse => {
                            if open_response__.is_some() {
                                return Err(serde::de::Error::duplicate_field("openResponse"));
                            }
                            open_response__ = map.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TaskNetworkProxyResponse {
                    open_response: open_response__,
                    data: data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.TaskNetworkProxyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for task_network_proxy_response::OpenResponse {
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
        let mut struct_ser = serializer.serialize_struct("flow.TaskNetworkProxyResponse.OpenResponse", len)?;
        if self.status != 0 {
            let v = task_network_proxy_response::Status::from_i32(self.status)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for task_network_proxy_response::OpenResponse {
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
            type Value = task_network_proxy_response::OpenResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TaskNetworkProxyResponse.OpenResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<task_network_proxy_response::OpenResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map.next_value::<task_network_proxy_response::Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map.next_value()?;
                        }
                    }
                }
                Ok(task_network_proxy_response::OpenResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                })
            }
        }
        deserializer.deserialize_struct("flow.TaskNetworkProxyResponse.OpenResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for task_network_proxy_response::Status {
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
            Self::InternalError => "INTERNAL_ERROR",
            Self::ShardStopped => "SHARD_STOPPED",
            Self::PortNotAllowed => "PORT_NOT_ALLOWED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for task_network_proxy_response::Status {
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
            "INTERNAL_ERROR",
            "SHARD_STOPPED",
            "PORT_NOT_ALLOWED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = task_network_proxy_response::Status;

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
                    .and_then(task_network_proxy_response::Status::from_i32)
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
                    .and_then(task_network_proxy_response::Status::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "OK" => Ok(task_network_proxy_response::Status::Ok),
                    "SHARD_NOT_FOUND" => Ok(task_network_proxy_response::Status::ShardNotFound),
                    "NO_SHARD_PRIMARY" => Ok(task_network_proxy_response::Status::NoShardPrimary),
                    "NOT_SHARD_PRIMARY" => Ok(task_network_proxy_response::Status::NotShardPrimary),
                    "INTERNAL_ERROR" => Ok(task_network_proxy_response::Status::InternalError),
                    "SHARD_STOPPED" => Ok(task_network_proxy_response::Status::ShardStopped),
                    "PORT_NOT_ALLOWED" => Ok(task_network_proxy_response::Status::PortNotAllowed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for TestSpec {
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
        if !self.steps.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.TestSpec", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.steps.is_empty() {
            struct_ser.serialize_field("steps", &self.steps)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TestSpec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "steps",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Steps,
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
                            "steps" => Ok(GeneratedField::Steps),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TestSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TestSpec")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TestSpec, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut steps__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Steps => {
                            if steps__.is_some() {
                                return Err(serde::de::Error::duplicate_field("steps"));
                            }
                            steps__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(TestSpec {
                    name: name__.unwrap_or_default(),
                    steps: steps__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.TestSpec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for test_spec::Step {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.step_type != 0 {
            len += 1;
        }
        if self.step_index != 0 {
            len += 1;
        }
        if !self.description.is_empty() {
            len += 1;
        }
        if !self.step_scope.is_empty() {
            len += 1;
        }
        if !self.collection.is_empty() {
            len += 1;
        }
        if !self.docs_json_vec.is_empty() {
            len += 1;
        }
        if self.partitions.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.TestSpec.Step", len)?;
        if self.step_type != 0 {
            let v = test_spec::step::Type::from_i32(self.step_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.step_type)))?;
            struct_ser.serialize_field("stepType", &v)?;
        }
        if self.step_index != 0 {
            struct_ser.serialize_field("stepIndex", &self.step_index)?;
        }
        if !self.description.is_empty() {
            struct_ser.serialize_field("description", &self.description)?;
        }
        if !self.step_scope.is_empty() {
            struct_ser.serialize_field("stepScope", &self.step_scope)?;
        }
        if !self.collection.is_empty() {
            struct_ser.serialize_field("collection", &self.collection)?;
        }
        if !self.docs_json_vec.is_empty() {
            struct_ser.serialize_field("docs", &crate::as_raw_json_vec(&self.docs_json_vec)?)?;
        }
        if let Some(v) = self.partitions.as_ref() {
            struct_ser.serialize_field("partitions", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for test_spec::Step {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "step_type",
            "stepType",
            "step_index",
            "stepIndex",
            "description",
            "step_scope",
            "stepScope",
            "collection",
            "docs_json_vec",
            "docs",
            "partitions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StepType,
            StepIndex,
            Description,
            StepScope,
            Collection,
            DocsJsonVec,
            Partitions,
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
                            "stepType" | "step_type" => Ok(GeneratedField::StepType),
                            "stepIndex" | "step_index" => Ok(GeneratedField::StepIndex),
                            "description" => Ok(GeneratedField::Description),
                            "stepScope" | "step_scope" => Ok(GeneratedField::StepScope),
                            "collection" => Ok(GeneratedField::Collection),
                            "docs" | "docs_json_vec" => Ok(GeneratedField::DocsJsonVec),
                            "partitions" => Ok(GeneratedField::Partitions),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = test_spec::Step;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.TestSpec.Step")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<test_spec::Step, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut step_type__ = None;
                let mut step_index__ = None;
                let mut description__ = None;
                let mut step_scope__ = None;
                let mut collection__ = None;
                let mut docs_json_vec__ : Option<Vec<Box<serde_json::value::RawValue>>> = None;
                let mut partitions__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::StepType => {
                            if step_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stepType"));
                            }
                            step_type__ = Some(map.next_value::<test_spec::step::Type>()? as i32);
                        }
                        GeneratedField::StepIndex => {
                            if step_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stepIndex"));
                            }
                            step_index__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Description => {
                            if description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("description"));
                            }
                            description__ = Some(map.next_value()?);
                        }
                        GeneratedField::StepScope => {
                            if step_scope__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stepScope"));
                            }
                            step_scope__ = Some(map.next_value()?);
                        }
                        GeneratedField::Collection => {
                            if collection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collection"));
                            }
                            collection__ = Some(map.next_value()?);
                        }
                        GeneratedField::DocsJsonVec => {
                            if docs_json_vec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docs"));
                            }
                            docs_json_vec__ = Some(map.next_value()?);
                        }
                        GeneratedField::Partitions => {
                            if partitions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitions"));
                            }
                            partitions__ = map.next_value()?;
                        }
                    }
                }
                Ok(test_spec::Step {
                    step_type: step_type__.unwrap_or_default(),
                    step_index: step_index__.unwrap_or_default(),
                    description: description__.unwrap_or_default(),
                    step_scope: step_scope__.unwrap_or_default(),
                    collection: collection__.unwrap_or_default(),
                    docs_json_vec: docs_json_vec__.unwrap_or_default().into_iter().map(|value| Box::<str>::from(value).into()).collect(),
                    partitions: partitions__,
                })
            }
        }
        deserializer.deserialize_struct("flow.TestSpec.Step", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for test_spec::step::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Ingest => "INGEST",
            Self::Verify => "VERIFY",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for test_spec::step::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INGEST",
            "VERIFY",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = test_spec::step::Type;

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
                    .and_then(test_spec::step::Type::from_i32)
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
                    .and_then(test_spec::step::Type::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INGEST" => Ok(test_spec::step::Type::Ingest),
                    "VERIFY" => Ok(test_spec::step::Type::Verify),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for UuidParts {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node != 0 {
            len += 1;
        }
        if self.clock != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("flow.UUIDParts", len)?;
        if self.node != 0 {
            struct_ser.serialize_field("node", ToString::to_string(&self.node).as_str())?;
        }
        if self.clock != 0 {
            struct_ser.serialize_field("clock", ToString::to_string(&self.clock).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UuidParts {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node",
            "clock",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Clock,
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
                            "node" => Ok(GeneratedField::Node),
                            "clock" => Ok(GeneratedField::Clock),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UuidParts;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct flow.UUIDParts")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UuidParts, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node__ = None;
                let mut clock__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Clock => {
                            if clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clock"));
                            }
                            clock__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(UuidParts {
                    node: node__.unwrap_or_default(),
                    clock: clock__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("flow.UUIDParts", FIELDS, GeneratedVisitor)
    }
}
