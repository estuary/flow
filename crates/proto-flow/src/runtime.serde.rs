impl serde::Serialize for Applied {
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
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Applied", len)?;
        if !self.action_description.is_empty() {
            struct_ser.serialize_field("actionDescription", &self.action_description)?;
        }
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Applied {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "action_description",
            "actionDescription",
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ActionDescription,
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Applied;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Applied")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Applied, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut action_description__ = None;
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ActionDescription => {
                            if action_description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("actionDescription"));
                            }
                            action_description__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Applied {
                    action_description: action_description__.unwrap_or_default(),
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Applied", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Apply {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.spec.is_empty() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if !self.last_spec.is_empty() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        if !self.connector_state_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Apply", len)?;
        if !self.spec.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("spec", pbjson::private::base64::encode(&self.spec).as_str())?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if !self.last_spec.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastSpec", pbjson::private::base64::encode(&self.last_spec).as_str())?;
        }
        if !self.last_version.is_empty() {
            struct_ser.serialize_field("lastVersion", &self.last_version)?;
        }
        if !self.connector_state_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorStateJson", &crate::as_raw_json(&self.connector_state_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Apply {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "version",
            "last_spec",
            "lastSpec",
            "last_version",
            "lastVersion",
            "connector_state_json",
            "connectorStateJson",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Version,
            LastSpec,
            LastVersion,
            ConnectorStateJson,
            __SkipField__,
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
                            "version" => Ok(GeneratedField::Version),
                            "lastSpec" | "last_spec" => Ok(GeneratedField::LastSpec),
                            "lastVersion" | "last_version" => Ok(GeneratedField::LastVersion),
                            "connectorStateJson" | "connector_state_json" => Ok(GeneratedField::ConnectorStateJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Apply;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Apply")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Apply, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut version__ = None;
                let mut last_spec__ = None;
                let mut last_version__ = None;
                let mut connector_state_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LastSpec => {
                            if last_spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastSpec"));
                            }
                            last_spec__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastVersion => {
                            if last_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastVersion"));
                            }
                            last_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnectorStateJson => {
                            if connector_state_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorStateJson"));
                            }
                            connector_state_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Apply {
                    spec: spec__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                    last_spec: last_spec__.unwrap_or_default(),
                    last_version: last_version__.unwrap_or_default(),
                    connector_state_json: connector_state_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Apply", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Capture {
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
        if self.spec_response.is_some() {
            len += 1;
        }
        if self.discover.is_some() {
            len += 1;
        }
        if self.discovered.is_some() {
            len += 1;
        }
        if self.validate.is_some() {
            len += 1;
        }
        if self.validated.is_some() {
            len += 1;
        }
        if self.log_level != 0 {
            len += 1;
        }
        if self.session_loop.is_some() {
            len += 1;
        }
        if self.join.is_some() {
            len += 1;
        }
        if self.joined.is_some() {
            len += 1;
        }
        if self.task.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.close_now.is_some() {
            len += 1;
        }
        if self.stop.is_some() {
            len += 1;
        }
        if self.stopped.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Capture", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.spec_response.as_ref() {
            struct_ser.serialize_field("specResponse", v)?;
        }
        if let Some(v) = self.discover.as_ref() {
            struct_ser.serialize_field("discover", v)?;
        }
        if let Some(v) = self.discovered.as_ref() {
            struct_ser.serialize_field("discovered", v)?;
        }
        if let Some(v) = self.validate.as_ref() {
            struct_ser.serialize_field("validate", v)?;
        }
        if let Some(v) = self.validated.as_ref() {
            struct_ser.serialize_field("validated", v)?;
        }
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.session_loop.as_ref() {
            struct_ser.serialize_field("sessionLoop", v)?;
        }
        if let Some(v) = self.join.as_ref() {
            struct_ser.serialize_field("join", v)?;
        }
        if let Some(v) = self.joined.as_ref() {
            struct_ser.serialize_field("joined", v)?;
        }
        if let Some(v) = self.task.as_ref() {
            struct_ser.serialize_field("task", v)?;
        }
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.close_now.as_ref() {
            struct_ser.serialize_field("closeNow", v)?;
        }
        if let Some(v) = self.stop.as_ref() {
            struct_ser.serialize_field("stop", v)?;
        }
        if let Some(v) = self.stopped.as_ref() {
            struct_ser.serialize_field("stopped", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Capture {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "spec_response",
            "specResponse",
            "discover",
            "discovered",
            "validate",
            "validated",
            "log_level",
            "logLevel",
            "session_loop",
            "sessionLoop",
            "join",
            "joined",
            "task",
            "opened",
            "close_now",
            "closeNow",
            "stop",
            "stopped",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            SpecResponse,
            Discover,
            Discovered,
            Validate,
            Validated,
            LogLevel,
            SessionLoop,
            Join,
            Joined,
            Task,
            Opened,
            CloseNow,
            Stop,
            Stopped,
            __SkipField__,
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
                            "specResponse" | "spec_response" => Ok(GeneratedField::SpecResponse),
                            "discover" => Ok(GeneratedField::Discover),
                            "discovered" => Ok(GeneratedField::Discovered),
                            "validate" => Ok(GeneratedField::Validate),
                            "validated" => Ok(GeneratedField::Validated),
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "sessionLoop" | "session_loop" => Ok(GeneratedField::SessionLoop),
                            "join" => Ok(GeneratedField::Join),
                            "joined" => Ok(GeneratedField::Joined),
                            "task" => Ok(GeneratedField::Task),
                            "opened" => Ok(GeneratedField::Opened),
                            "closeNow" | "close_now" => Ok(GeneratedField::CloseNow),
                            "stop" => Ok(GeneratedField::Stop),
                            "stopped" => Ok(GeneratedField::Stopped),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Capture;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Capture")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Capture, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut spec_response__ = None;
                let mut discover__ = None;
                let mut discovered__ = None;
                let mut validate__ = None;
                let mut validated__ = None;
                let mut log_level__ = None;
                let mut session_loop__ = None;
                let mut join__ = None;
                let mut joined__ = None;
                let mut task__ = None;
                let mut opened__ = None;
                let mut close_now__ = None;
                let mut stop__ = None;
                let mut stopped__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::SpecResponse => {
                            if spec_response__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specResponse"));
                            }
                            spec_response__ = map_.next_value()?;
                        }
                        GeneratedField::Discover => {
                            if discover__.is_some() {
                                return Err(serde::de::Error::duplicate_field("discover"));
                            }
                            discover__ = map_.next_value()?;
                        }
                        GeneratedField::Discovered => {
                            if discovered__.is_some() {
                                return Err(serde::de::Error::duplicate_field("discovered"));
                            }
                            discovered__ = map_.next_value()?;
                        }
                        GeneratedField::Validate => {
                            if validate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validate"));
                            }
                            validate__ = map_.next_value()?;
                        }
                        GeneratedField::Validated => {
                            if validated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validated"));
                            }
                            validated__ = map_.next_value()?;
                        }
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::SessionLoop => {
                            if session_loop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionLoop"));
                            }
                            session_loop__ = map_.next_value()?;
                        }
                        GeneratedField::Join => {
                            if join__.is_some() {
                                return Err(serde::de::Error::duplicate_field("join"));
                            }
                            join__ = map_.next_value()?;
                        }
                        GeneratedField::Joined => {
                            if joined__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joined"));
                            }
                            joined__ = map_.next_value()?;
                        }
                        GeneratedField::Task => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("task"));
                            }
                            task__ = map_.next_value()?;
                        }
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::CloseNow => {
                            if close_now__.is_some() {
                                return Err(serde::de::Error::duplicate_field("closeNow"));
                            }
                            close_now__ = map_.next_value()?;
                        }
                        GeneratedField::Stop => {
                            if stop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            stop__ = map_.next_value()?;
                        }
                        GeneratedField::Stopped => {
                            if stopped__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stopped"));
                            }
                            stopped__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Capture {
                    spec: spec__,
                    spec_response: spec_response__,
                    discover: discover__,
                    discovered: discovered__,
                    validate: validate__,
                    validated: validated__,
                    log_level: log_level__.unwrap_or_default(),
                    session_loop: session_loop__,
                    join: join__,
                    joined: joined__,
                    task: task__,
                    opened: opened__,
                    close_now: close_now__,
                    stop: stop__,
                    stopped: stopped__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Capture", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Capture.Opened", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture::Opened {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Capture.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<capture::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(capture::Opened {
                    container: container__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Capture.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CaptureRequestExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.log_level != 0 {
            len += 1;
        }
        if self.rocksdb_descriptor.is_some() {
            len += 1;
        }
        if self.start_commit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureRequestExt", len)?;
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.rocksdb_descriptor.as_ref() {
            struct_ser.serialize_field("rocksdbDescriptor", v)?;
        }
        if let Some(v) = self.start_commit.as_ref() {
            struct_ser.serialize_field("startCommit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CaptureRequestExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log_level",
            "logLevel",
            "rocksdb_descriptor",
            "rocksdbDescriptor",
            "start_commit",
            "startCommit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogLevel,
            RocksdbDescriptor,
            StartCommit,
            __SkipField__,
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
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "rocksdbDescriptor" | "rocksdb_descriptor" => Ok(GeneratedField::RocksdbDescriptor),
                            "startCommit" | "start_commit" => Ok(GeneratedField::StartCommit),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CaptureRequestExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureRequestExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CaptureRequestExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut log_level__ = None;
                let mut rocksdb_descriptor__ = None;
                let mut start_commit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::RocksdbDescriptor => {
                            if rocksdb_descriptor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbDescriptor"));
                            }
                            rocksdb_descriptor__ = map_.next_value()?;
                        }
                        GeneratedField::StartCommit => {
                            if start_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startCommit"));
                            }
                            start_commit__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CaptureRequestExt {
                    log_level: log_level__.unwrap_or_default(),
                    rocksdb_descriptor: rocksdb_descriptor__,
                    start_commit: start_commit__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureRequestExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_request_ext::StartCommit {
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
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureRequestExt.StartCommit", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture_request_ext::StartCommit {
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
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_request_ext::StartCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureRequestExt.StartCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<capture_request_ext::StartCommit, V::Error>
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(capture_request_ext::StartCommit {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureRequestExt.StartCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CaptureResponseExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
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
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureResponseExt", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
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
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CaptureResponseExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "opened",
            "captured",
            "checkpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            Opened,
            Captured,
            Checkpoint,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            "opened" => Ok(GeneratedField::Opened),
                            "captured" => Ok(GeneratedField::Captured),
                            "checkpoint" => Ok(GeneratedField::Checkpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CaptureResponseExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureResponseExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CaptureResponseExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut opened__ = None;
                let mut captured__ = None;
                let mut checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CaptureResponseExt {
                    container: container__,
                    opened: opened__,
                    captured: captured__,
                    checkpoint: checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureResponseExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_response_ext::Captured {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.key_packed.is_empty() {
            len += 1;
        }
        if !self.partitions_packed.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureResponseExt.Captured", len)?;
        if !self.key_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("keyPacked", pbjson::private::base64::encode(&self.key_packed).as_str())?;
        }
        if !self.partitions_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("partitionsPacked", pbjson::private::base64::encode(&self.partitions_packed).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture_response_ext::Captured {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key_packed",
            "keyPacked",
            "partitions_packed",
            "partitionsPacked",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            KeyPacked,
            PartitionsPacked,
            __SkipField__,
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
                            "keyPacked" | "key_packed" => Ok(GeneratedField::KeyPacked),
                            "partitionsPacked" | "partitions_packed" => Ok(GeneratedField::PartitionsPacked),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_response_ext::Captured;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureResponseExt.Captured")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<capture_response_ext::Captured, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key_packed__ = None;
                let mut partitions_packed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::KeyPacked => {
                            if key_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPacked"));
                            }
                            key_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionsPacked => {
                            if partitions_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionsPacked"));
                            }
                            partitions_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(capture_response_ext::Captured {
                    key_packed: key_packed__.unwrap_or_default(),
                    partitions_packed: partitions_packed__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureResponseExt.Captured", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_response_ext::Checkpoint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.stats.is_some() {
            len += 1;
        }
        if self.poll_result != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureResponseExt.Checkpoint", len)?;
        if let Some(v) = self.stats.as_ref() {
            struct_ser.serialize_field("stats", v)?;
        }
        if self.poll_result != 0 {
            let v = capture_response_ext::PollResult::try_from(self.poll_result)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.poll_result)))?;
            struct_ser.serialize_field("pollResult", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture_response_ext::Checkpoint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "stats",
            "poll_result",
            "pollResult",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Stats,
            PollResult,
            __SkipField__,
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
                            "stats" => Ok(GeneratedField::Stats),
                            "pollResult" | "poll_result" => Ok(GeneratedField::PollResult),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_response_ext::Checkpoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureResponseExt.Checkpoint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<capture_response_ext::Checkpoint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stats__ = None;
                let mut poll_result__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = map_.next_value()?;
                        }
                        GeneratedField::PollResult => {
                            if poll_result__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pollResult"));
                            }
                            poll_result__ = Some(map_.next_value::<capture_response_ext::PollResult>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(capture_response_ext::Checkpoint {
                    stats: stats__,
                    poll_result: poll_result__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureResponseExt.Checkpoint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_response_ext::Opened {
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
        let mut struct_ser = serializer.serialize_struct("runtime.CaptureResponseExt.Opened", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for capture_response_ext::Opened {
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
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_response_ext::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CaptureResponseExt.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<capture_response_ext::Opened, V::Error>
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(capture_response_ext::Opened {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.CaptureResponseExt.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for capture_response_ext::PollResult {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Ready => "READY",
            Self::NotReady => "NOT_READY",
            Self::CoolOff => "COOL_OFF",
            Self::Restart => "RESTART",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for capture_response_ext::PollResult {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "READY",
            "NOT_READY",
            "COOL_OFF",
            "RESTART",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = capture_response_ext::PollResult;

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
                    "INVALID" => Ok(capture_response_ext::PollResult::Invalid),
                    "READY" => Ok(capture_response_ext::PollResult::Ready),
                    "NOT_READY" => Ok(capture_response_ext::PollResult::NotReady),
                    "COOL_OFF" => Ok(capture_response_ext::PollResult::CoolOff),
                    "RESTART" => Ok(capture_response_ext::PollResult::Restart),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CloseNow {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.CloseNow", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CloseNow {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CloseNow;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CloseNow")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CloseNow, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(CloseNow {
                })
            }
        }
        deserializer.deserialize_struct("runtime.CloseNow", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CombineRequest {
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
        if self.add.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CombineRequest", len)?;
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.add.as_ref() {
            struct_ser.serialize_field("add", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CombineRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "open",
            "add",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Open,
            Add,
            __SkipField__,
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
                            "add" => Ok(GeneratedField::Add),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CombineRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CombineRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CombineRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut open__ = None;
                let mut add__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Add => {
                            if add__.is_some() {
                                return Err(serde::de::Error::duplicate_field("add"));
                            }
                            add__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CombineRequest {
                    open: open__,
                    add: add__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.CombineRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for combine_request::Add {
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
        if self.front {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CombineRequest.Add", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.doc_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("docJson", &crate::as_raw_json(&self.doc_json)?)?;
        }
        if self.front {
            struct_ser.serialize_field("front", &self.front)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for combine_request::Add {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "doc_json",
            "docJson",
            "front",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            DocJson,
            Front,
            __SkipField__,
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
                            "docJson" | "doc_json" => Ok(GeneratedField::DocJson),
                            "front" => Ok(GeneratedField::Front),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = combine_request::Add;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CombineRequest.Add")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<combine_request::Add, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut doc_json__ = None;
                let mut front__ = None;
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
                                return Err(serde::de::Error::duplicate_field("docJson"));
                            }
                            doc_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::Front => {
                            if front__.is_some() {
                                return Err(serde::de::Error::duplicate_field("front"));
                            }
                            front__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(combine_request::Add {
                    binding: binding__.unwrap_or_default(),
                    doc_json: doc_json__.unwrap_or_default(),
                    front: front__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CombineRequest.Add", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for combine_request::Open {
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
        let mut struct_ser = serializer.serialize_struct("runtime.CombineRequest.Open", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for combine_request::Open {
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
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = combine_request::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CombineRequest.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<combine_request::Open, V::Error>
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(combine_request::Open {
                    bindings: bindings__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CombineRequest.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for combine_request::open::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.full {
            len += 1;
        }
        if !self.key.is_empty() {
            len += 1;
        }
        if !self.projections.is_empty() {
            len += 1;
        }
        if !self.schema_json.is_empty() {
            len += 1;
        }
        if self.ser_policy.is_some() {
            len += 1;
        }
        if !self.uuid_ptr.is_empty() {
            len += 1;
        }
        if !self.values.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CombineRequest.Open.Binding", len)?;
        if self.full {
            struct_ser.serialize_field("full", &self.full)?;
        }
        if !self.key.is_empty() {
            struct_ser.serialize_field("key", &self.key)?;
        }
        if !self.projections.is_empty() {
            struct_ser.serialize_field("projections", &self.projections)?;
        }
        if !self.schema_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("schemaJson", &crate::as_raw_json(&self.schema_json)?)?;
        }
        if let Some(v) = self.ser_policy.as_ref() {
            struct_ser.serialize_field("serPolicy", v)?;
        }
        if !self.uuid_ptr.is_empty() {
            struct_ser.serialize_field("uuidPtr", &self.uuid_ptr)?;
        }
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for combine_request::open::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "full",
            "key",
            "projections",
            "schema_json",
            "schemaJson",
            "ser_policy",
            "serPolicy",
            "uuid_ptr",
            "uuidPtr",
            "values",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Full,
            Key,
            Projections,
            SchemaJson,
            SerPolicy,
            UuidPtr,
            Values,
            __SkipField__,
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
                            "full" => Ok(GeneratedField::Full),
                            "key" => Ok(GeneratedField::Key),
                            "projections" => Ok(GeneratedField::Projections),
                            "schemaJson" | "schema_json" => Ok(GeneratedField::SchemaJson),
                            "serPolicy" | "ser_policy" => Ok(GeneratedField::SerPolicy),
                            "uuidPtr" | "uuid_ptr" => Ok(GeneratedField::UuidPtr),
                            "values" => Ok(GeneratedField::Values),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = combine_request::open::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CombineRequest.Open.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<combine_request::open::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut full__ = None;
                let mut key__ = None;
                let mut projections__ = None;
                let mut schema_json__ = None;
                let mut ser_policy__ = None;
                let mut uuid_ptr__ = None;
                let mut values__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Full => {
                            if full__.is_some() {
                                return Err(serde::de::Error::duplicate_field("full"));
                            }
                            full__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Projections => {
                            if projections__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projections"));
                            }
                            projections__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SchemaJson => {
                            if schema_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaJson"));
                            }
                            schema_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::SerPolicy => {
                            if ser_policy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("serPolicy"));
                            }
                            ser_policy__ = map_.next_value()?;
                        }
                        GeneratedField::UuidPtr => {
                            if uuid_ptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuidPtr"));
                            }
                            uuid_ptr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(combine_request::open::Binding {
                    full: full__.unwrap_or_default(),
                    key: key__.unwrap_or_default(),
                    projections: projections__.unwrap_or_default(),
                    schema_json: schema_json__.unwrap_or_default(),
                    ser_policy: ser_policy__,
                    uuid_ptr: uuid_ptr__.unwrap_or_default(),
                    values: values__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CombineRequest.Open.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CombineResponse {
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
        if self.deleted {
            len += 1;
        }
        if !self.doc_json.is_empty() {
            len += 1;
        }
        if self.front {
            len += 1;
        }
        if !self.key_packed.is_empty() {
            len += 1;
        }
        if !self.values_packed.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.CombineResponse", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if self.deleted {
            struct_ser.serialize_field("deleted", &self.deleted)?;
        }
        if !self.doc_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("docJson", &crate::as_raw_json(&self.doc_json)?)?;
        }
        if self.front {
            struct_ser.serialize_field("front", &self.front)?;
        }
        if !self.key_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("keyPacked", pbjson::private::base64::encode(&self.key_packed).as_str())?;
        }
        if !self.values_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("valuesPacked", pbjson::private::base64::encode(&self.values_packed).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CombineResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "deleted",
            "doc_json",
            "docJson",
            "front",
            "key_packed",
            "keyPacked",
            "values_packed",
            "valuesPacked",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            Deleted,
            DocJson,
            Front,
            KeyPacked,
            ValuesPacked,
            __SkipField__,
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
                            "deleted" => Ok(GeneratedField::Deleted),
                            "docJson" | "doc_json" => Ok(GeneratedField::DocJson),
                            "front" => Ok(GeneratedField::Front),
                            "keyPacked" | "key_packed" => Ok(GeneratedField::KeyPacked),
                            "valuesPacked" | "values_packed" => Ok(GeneratedField::ValuesPacked),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CombineResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.CombineResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CombineResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut deleted__ = None;
                let mut doc_json__ = None;
                let mut front__ = None;
                let mut key_packed__ = None;
                let mut values_packed__ = None;
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
                        GeneratedField::Deleted => {
                            if deleted__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleted"));
                            }
                            deleted__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DocJson => {
                            if doc_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docJson"));
                            }
                            doc_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::Front => {
                            if front__.is_some() {
                                return Err(serde::de::Error::duplicate_field("front"));
                            }
                            front__ = Some(map_.next_value()?);
                        }
                        GeneratedField::KeyPacked => {
                            if key_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPacked"));
                            }
                            key_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ValuesPacked => {
                            if values_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valuesPacked"));
                            }
                            values_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CombineResponse {
                    binding: binding__.unwrap_or_default(),
                    deleted: deleted__.unwrap_or_default(),
                    doc_json: doc_json__.unwrap_or_default(),
                    front: front__.unwrap_or_default(),
                    key_packed: key_packed__.unwrap_or_default(),
                    values_packed: values_packed__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.CombineResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectorProxyRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.ConnectorProxyRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectorProxyRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectorProxyRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.ConnectorProxyRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectorProxyRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ConnectorProxyRequest {
                })
            }
        }
        deserializer.deserialize_struct("runtime.ConnectorProxyRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectorProxyResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.address.is_empty() {
            len += 1;
        }
        if !self.proxy_id.is_empty() {
            len += 1;
        }
        if self.log.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.ConnectorProxyResponse", len)?;
        if !self.address.is_empty() {
            struct_ser.serialize_field("address", &self.address)?;
        }
        if !self.proxy_id.is_empty() {
            struct_ser.serialize_field("proxyId", &self.proxy_id)?;
        }
        if let Some(v) = self.log.as_ref() {
            struct_ser.serialize_field("log", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectorProxyResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "address",
            "proxy_id",
            "proxyId",
            "log",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Address,
            ProxyId,
            Log,
            __SkipField__,
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
                            "address" => Ok(GeneratedField::Address),
                            "proxyId" | "proxy_id" => Ok(GeneratedField::ProxyId),
                            "log" => Ok(GeneratedField::Log),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectorProxyResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.ConnectorProxyResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectorProxyResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut address__ = None;
                let mut proxy_id__ = None;
                let mut log__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Address => {
                            if address__.is_some() {
                                return Err(serde::de::Error::duplicate_field("address"));
                            }
                            address__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ProxyId => {
                            if proxy_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("proxyId"));
                            }
                            proxy_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Log => {
                            if log__.is_some() {
                                return Err(serde::de::Error::duplicate_field("log"));
                            }
                            log__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ConnectorProxyResponse {
                    address: address__.unwrap_or_default(),
                    proxy_id: proxy_id__.unwrap_or_default(),
                    log: log__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.ConnectorProxyResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Container {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ip_addr.is_empty() {
            len += 1;
        }
        if !self.network_ports.is_empty() {
            len += 1;
        }
        if !self.mapped_host_ports.is_empty() {
            len += 1;
        }
        if self.usage_rate != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Container", len)?;
        if !self.ip_addr.is_empty() {
            struct_ser.serialize_field("ipAddr", &self.ip_addr)?;
        }
        if !self.network_ports.is_empty() {
            struct_ser.serialize_field("networkPorts", &self.network_ports)?;
        }
        if !self.mapped_host_ports.is_empty() {
            struct_ser.serialize_field("mappedHostPorts", &self.mapped_host_ports)?;
        }
        if self.usage_rate != 0. {
            struct_ser.serialize_field("usageRate", &self.usage_rate)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Container {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ip_addr",
            "ipAddr",
            "network_ports",
            "networkPorts",
            "mapped_host_ports",
            "mappedHostPorts",
            "usage_rate",
            "usageRate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IpAddr,
            NetworkPorts,
            MappedHostPorts,
            UsageRate,
            __SkipField__,
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
                            "ipAddr" | "ip_addr" => Ok(GeneratedField::IpAddr),
                            "networkPorts" | "network_ports" => Ok(GeneratedField::NetworkPorts),
                            "mappedHostPorts" | "mapped_host_ports" => Ok(GeneratedField::MappedHostPorts),
                            "usageRate" | "usage_rate" => Ok(GeneratedField::UsageRate),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Container;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Container")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Container, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ip_addr__ = None;
                let mut network_ports__ = None;
                let mut mapped_host_ports__ = None;
                let mut usage_rate__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IpAddr => {
                            if ip_addr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ipAddr"));
                            }
                            ip_addr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NetworkPorts => {
                            if network_ports__.is_some() {
                                return Err(serde::de::Error::duplicate_field("networkPorts"));
                            }
                            network_ports__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MappedHostPorts => {
                            if mapped_host_ports__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mappedHostPorts"));
                            }
                            mapped_host_ports__ = Some(
                                map_.next_value::<std::collections::BTreeMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                        GeneratedField::UsageRate => {
                            if usage_rate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("usageRate"));
                            }
                            usage_rate__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Container {
                    ip_addr: ip_addr__.unwrap_or_default(),
                    network_ports: network_ports__.unwrap_or_default(),
                    mapped_host_ports: mapped_host_ports__.unwrap_or_default(),
                    usage_rate: usage_rate__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Container", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Derive {
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
        if self.spec_response.is_some() {
            len += 1;
        }
        if self.validate.is_some() {
            len += 1;
        }
        if self.validated.is_some() {
            len += 1;
        }
        if self.log_level != 0 {
            len += 1;
        }
        if self.session_loop.is_some() {
            len += 1;
        }
        if self.join.is_some() {
            len += 1;
        }
        if self.joined.is_some() {
            len += 1;
        }
        if self.task.is_some() {
            len += 1;
        }
        if self.recover.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.load.is_some() {
            len += 1;
        }
        if self.loaded.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        if self.store.is_some() {
            len += 1;
        }
        if self.stored.is_some() {
            len += 1;
        }
        if self.start_commit.is_some() {
            len += 1;
        }
        if self.started_commit.is_some() {
            len += 1;
        }
        if self.persist.is_some() {
            len += 1;
        }
        if self.persisted.is_some() {
            len += 1;
        }
        if self.close_now.is_some() {
            len += 1;
        }
        if self.stop.is_some() {
            len += 1;
        }
        if self.stopped.is_some() {
            len += 1;
        }
        if self.reset.is_some() {
            len += 1;
        }
        if self.reset_done.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.spec_response.as_ref() {
            struct_ser.serialize_field("specResponse", v)?;
        }
        if let Some(v) = self.validate.as_ref() {
            struct_ser.serialize_field("validate", v)?;
        }
        if let Some(v) = self.validated.as_ref() {
            struct_ser.serialize_field("validated", v)?;
        }
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.session_loop.as_ref() {
            struct_ser.serialize_field("sessionLoop", v)?;
        }
        if let Some(v) = self.join.as_ref() {
            struct_ser.serialize_field("join", v)?;
        }
        if let Some(v) = self.joined.as_ref() {
            struct_ser.serialize_field("joined", v)?;
        }
        if let Some(v) = self.task.as_ref() {
            struct_ser.serialize_field("task", v)?;
        }
        if let Some(v) = self.recover.as_ref() {
            struct_ser.serialize_field("recover", v)?;
        }
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.load.as_ref() {
            struct_ser.serialize_field("load", v)?;
        }
        if let Some(v) = self.loaded.as_ref() {
            struct_ser.serialize_field("loaded", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        if let Some(v) = self.store.as_ref() {
            struct_ser.serialize_field("store", v)?;
        }
        if let Some(v) = self.stored.as_ref() {
            struct_ser.serialize_field("stored", v)?;
        }
        if let Some(v) = self.start_commit.as_ref() {
            struct_ser.serialize_field("startCommit", v)?;
        }
        if let Some(v) = self.started_commit.as_ref() {
            struct_ser.serialize_field("startedCommit", v)?;
        }
        if let Some(v) = self.persist.as_ref() {
            struct_ser.serialize_field("persist", v)?;
        }
        if let Some(v) = self.persisted.as_ref() {
            struct_ser.serialize_field("persisted", v)?;
        }
        if let Some(v) = self.close_now.as_ref() {
            struct_ser.serialize_field("closeNow", v)?;
        }
        if let Some(v) = self.stop.as_ref() {
            struct_ser.serialize_field("stop", v)?;
        }
        if let Some(v) = self.stopped.as_ref() {
            struct_ser.serialize_field("stopped", v)?;
        }
        if let Some(v) = self.reset.as_ref() {
            struct_ser.serialize_field("reset", v)?;
        }
        if let Some(v) = self.reset_done.as_ref() {
            struct_ser.serialize_field("resetDone", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Derive {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "spec_response",
            "specResponse",
            "validate",
            "validated",
            "log_level",
            "logLevel",
            "session_loop",
            "sessionLoop",
            "join",
            "joined",
            "task",
            "recover",
            "open",
            "opened",
            "load",
            "loaded",
            "flush",
            "flushed",
            "store",
            "stored",
            "start_commit",
            "startCommit",
            "started_commit",
            "startedCommit",
            "persist",
            "persisted",
            "close_now",
            "closeNow",
            "stop",
            "stopped",
            "reset",
            "reset_done",
            "resetDone",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            SpecResponse,
            Validate,
            Validated,
            LogLevel,
            SessionLoop,
            Join,
            Joined,
            Task,
            Recover,
            Open,
            Opened,
            Load,
            Loaded,
            Flush,
            Flushed,
            Store,
            Stored,
            StartCommit,
            StartedCommit,
            Persist,
            Persisted,
            CloseNow,
            Stop,
            Stopped,
            Reset,
            ResetDone,
            __SkipField__,
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
                            "specResponse" | "spec_response" => Ok(GeneratedField::SpecResponse),
                            "validate" => Ok(GeneratedField::Validate),
                            "validated" => Ok(GeneratedField::Validated),
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "sessionLoop" | "session_loop" => Ok(GeneratedField::SessionLoop),
                            "join" => Ok(GeneratedField::Join),
                            "joined" => Ok(GeneratedField::Joined),
                            "task" => Ok(GeneratedField::Task),
                            "recover" => Ok(GeneratedField::Recover),
                            "open" => Ok(GeneratedField::Open),
                            "opened" => Ok(GeneratedField::Opened),
                            "load" => Ok(GeneratedField::Load),
                            "loaded" => Ok(GeneratedField::Loaded),
                            "flush" => Ok(GeneratedField::Flush),
                            "flushed" => Ok(GeneratedField::Flushed),
                            "store" => Ok(GeneratedField::Store),
                            "stored" => Ok(GeneratedField::Stored),
                            "startCommit" | "start_commit" => Ok(GeneratedField::StartCommit),
                            "startedCommit" | "started_commit" => Ok(GeneratedField::StartedCommit),
                            "persist" => Ok(GeneratedField::Persist),
                            "persisted" => Ok(GeneratedField::Persisted),
                            "closeNow" | "close_now" => Ok(GeneratedField::CloseNow),
                            "stop" => Ok(GeneratedField::Stop),
                            "stopped" => Ok(GeneratedField::Stopped),
                            "reset" => Ok(GeneratedField::Reset),
                            "resetDone" | "reset_done" => Ok(GeneratedField::ResetDone),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Derive;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Derive, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut spec_response__ = None;
                let mut validate__ = None;
                let mut validated__ = None;
                let mut log_level__ = None;
                let mut session_loop__ = None;
                let mut join__ = None;
                let mut joined__ = None;
                let mut task__ = None;
                let mut recover__ = None;
                let mut open__ = None;
                let mut opened__ = None;
                let mut load__ = None;
                let mut loaded__ = None;
                let mut flush__ = None;
                let mut flushed__ = None;
                let mut store__ = None;
                let mut stored__ = None;
                let mut start_commit__ = None;
                let mut started_commit__ = None;
                let mut persist__ = None;
                let mut persisted__ = None;
                let mut close_now__ = None;
                let mut stop__ = None;
                let mut stopped__ = None;
                let mut reset__ = None;
                let mut reset_done__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::SpecResponse => {
                            if spec_response__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specResponse"));
                            }
                            spec_response__ = map_.next_value()?;
                        }
                        GeneratedField::Validate => {
                            if validate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validate"));
                            }
                            validate__ = map_.next_value()?;
                        }
                        GeneratedField::Validated => {
                            if validated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validated"));
                            }
                            validated__ = map_.next_value()?;
                        }
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::SessionLoop => {
                            if session_loop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionLoop"));
                            }
                            session_loop__ = map_.next_value()?;
                        }
                        GeneratedField::Join => {
                            if join__.is_some() {
                                return Err(serde::de::Error::duplicate_field("join"));
                            }
                            join__ = map_.next_value()?;
                        }
                        GeneratedField::Joined => {
                            if joined__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joined"));
                            }
                            joined__ = map_.next_value()?;
                        }
                        GeneratedField::Task => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("task"));
                            }
                            task__ = map_.next_value()?;
                        }
                        GeneratedField::Recover => {
                            if recover__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recover"));
                            }
                            recover__ = map_.next_value()?;
                        }
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::Load => {
                            if load__.is_some() {
                                return Err(serde::de::Error::duplicate_field("load"));
                            }
                            load__ = map_.next_value()?;
                        }
                        GeneratedField::Loaded => {
                            if loaded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("loaded"));
                            }
                            loaded__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                        GeneratedField::Store => {
                            if store__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store"));
                            }
                            store__ = map_.next_value()?;
                        }
                        GeneratedField::Stored => {
                            if stored__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stored"));
                            }
                            stored__ = map_.next_value()?;
                        }
                        GeneratedField::StartCommit => {
                            if start_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startCommit"));
                            }
                            start_commit__ = map_.next_value()?;
                        }
                        GeneratedField::StartedCommit => {
                            if started_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startedCommit"));
                            }
                            started_commit__ = map_.next_value()?;
                        }
                        GeneratedField::Persist => {
                            if persist__.is_some() {
                                return Err(serde::de::Error::duplicate_field("persist"));
                            }
                            persist__ = map_.next_value()?;
                        }
                        GeneratedField::Persisted => {
                            if persisted__.is_some() {
                                return Err(serde::de::Error::duplicate_field("persisted"));
                            }
                            persisted__ = map_.next_value()?;
                        }
                        GeneratedField::CloseNow => {
                            if close_now__.is_some() {
                                return Err(serde::de::Error::duplicate_field("closeNow"));
                            }
                            close_now__ = map_.next_value()?;
                        }
                        GeneratedField::Stop => {
                            if stop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            stop__ = map_.next_value()?;
                        }
                        GeneratedField::Stopped => {
                            if stopped__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stopped"));
                            }
                            stopped__ = map_.next_value()?;
                        }
                        GeneratedField::Reset => {
                            if reset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reset"));
                            }
                            reset__ = map_.next_value()?;
                        }
                        GeneratedField::ResetDone => {
                            if reset_done__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resetDone"));
                            }
                            reset_done__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Derive {
                    spec: spec__,
                    spec_response: spec_response__,
                    validate: validate__,
                    validated: validated__,
                    log_level: log_level__.unwrap_or_default(),
                    session_loop: session_loop__,
                    join: join__,
                    joined: joined__,
                    task: task__,
                    recover: recover__,
                    open: open__,
                    opened: opened__,
                    load: load__,
                    loaded: loaded__,
                    flush: flush__,
                    flushed: flushed__,
                    store: store__,
                    stored: stored__,
                    start_commit: start_commit__,
                    started_commit: started_commit__,
                    persist: persist__,
                    persisted: persisted__,
                    close_now: close_now__,
                    stop: stop__,
                    stopped: stopped__,
                    reset: reset__,
                    reset_done: reset_done__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Flush", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Flush {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Flush {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        if self.more {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Flushed", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        if self.more {
            struct_ser.serialize_field("more", &self.more)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
            "more",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            More,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            "more" => Ok(GeneratedField::More),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                let mut more__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::More => {
                            if more__.is_some() {
                                return Err(serde::de::Error::duplicate_field("more"));
                            }
                            more__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Flushed {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                    more: more__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Load {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.frontier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Load", len)?;
        if let Some(v) = self.frontier.as_ref() {
            struct_ser.serialize_field("frontier", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Load {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "frontier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Frontier,
            __SkipField__,
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
                            "frontier" => Ok(GeneratedField::Frontier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Load;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Load")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Load, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut frontier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Frontier => {
                            if frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("frontier"));
                            }
                            frontier__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Load {
                    frontier: frontier__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Load", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Loaded {
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
        if self.combiner_usage_bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Loaded", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if self.combiner_usage_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("combinerUsageBytes", ToString::to_string(&self.combiner_usage_bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Loaded {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bindings",
            "combiner_usage_bytes",
            "combinerUsageBytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bindings,
            CombinerUsageBytes,
            __SkipField__,
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
                            "combinerUsageBytes" | "combiner_usage_bytes" => Ok(GeneratedField::CombinerUsageBytes),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Loaded;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Loaded")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Loaded, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bindings__ = None;
                let mut combiner_usage_bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CombinerUsageBytes => {
                            if combiner_usage_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("combinerUsageBytes"));
                            }
                            combiner_usage_bytes__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Loaded {
                    bindings: bindings__.unwrap_or_default(),
                    combiner_usage_bytes: combiner_usage_bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Loaded", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::loaded::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.min_source_clock != 0 {
            len += 1;
        }
        if self.max_source_clock != 0 {
            len += 1;
        }
        if self.sourced_docs_total != 0 {
            len += 1;
        }
        if self.sourced_bytes_total != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Loaded.Binding", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.min_source_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("minSourceClock", ToString::to_string(&self.min_source_clock).as_str())?;
        }
        if self.max_source_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxSourceClock", ToString::to_string(&self.max_source_clock).as_str())?;
        }
        if self.sourced_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sourcedDocsTotal", ToString::to_string(&self.sourced_docs_total).as_str())?;
        }
        if self.sourced_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sourcedBytesTotal", ToString::to_string(&self.sourced_bytes_total).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::loaded::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "min_source_clock",
            "minSourceClock",
            "max_source_clock",
            "maxSourceClock",
            "sourced_docs_total",
            "sourcedDocsTotal",
            "sourced_bytes_total",
            "sourcedBytesTotal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            MinSourceClock,
            MaxSourceClock,
            SourcedDocsTotal,
            SourcedBytesTotal,
            __SkipField__,
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
                            "index" => Ok(GeneratedField::Index),
                            "minSourceClock" | "min_source_clock" => Ok(GeneratedField::MinSourceClock),
                            "maxSourceClock" | "max_source_clock" => Ok(GeneratedField::MaxSourceClock),
                            "sourcedDocsTotal" | "sourced_docs_total" => Ok(GeneratedField::SourcedDocsTotal),
                            "sourcedBytesTotal" | "sourced_bytes_total" => Ok(GeneratedField::SourcedBytesTotal),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::loaded::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Loaded.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::loaded::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut min_source_clock__ = None;
                let mut max_source_clock__ = None;
                let mut sourced_docs_total__ = None;
                let mut sourced_bytes_total__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MinSourceClock => {
                            if min_source_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minSourceClock"));
                            }
                            min_source_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxSourceClock => {
                            if max_source_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxSourceClock"));
                            }
                            max_source_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SourcedDocsTotal => {
                            if sourced_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourcedDocsTotal"));
                            }
                            sourced_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SourcedBytesTotal => {
                            if sourced_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourcedBytesTotal"));
                            }
                            sourced_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::loaded::Binding {
                    index: index__.unwrap_or_default(),
                    min_source_clock: min_source_clock__.unwrap_or_default(),
                    max_source_clock: max_source_clock__.unwrap_or_default(),
                    sourced_docs_total: sourced_docs_total__.unwrap_or_default(),
                    sourced_bytes_total: sourced_bytes_total__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Loaded.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
            len += 1;
        }
        if self.connector_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Opened", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
        }
        if let Some(v) = self.connector_checkpoint.as_ref() {
            struct_ser.serialize_field("connectorCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Opened {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "connector_checkpoint",
            "connectorCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            ConnectorCheckpoint,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            "connectorCheckpoint" | "connector_checkpoint" => Ok(GeneratedField::ConnectorCheckpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut connector_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
                        }
                        GeneratedField::ConnectorCheckpoint => {
                            if connector_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorCheckpoint"));
                            }
                            connector_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Opened {
                    container: container__,
                    connector_checkpoint: connector_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::StartCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.connector_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.StartCommit", len)?;
        if let Some(v) = self.connector_checkpoint.as_ref() {
            struct_ser.serialize_field("connectorCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::StartCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_checkpoint",
            "connectorCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorCheckpoint,
            __SkipField__,
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
                            "connectorCheckpoint" | "connector_checkpoint" => Ok(GeneratedField::ConnectorCheckpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::StartCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.StartCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::StartCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorCheckpoint => {
                            if connector_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorCheckpoint"));
                            }
                            connector_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::StartCommit {
                    connector_checkpoint: connector_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.StartCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::StartedCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Derive.StartedCommit", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::StartedCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::StartedCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.StartedCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::StartedCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(derive::StartedCommit {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.StartedCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Store {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Derive.Store", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Store {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Store;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Store")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Store, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(derive::Store {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Store", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::Stored {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.published_docs_total != 0 {
            len += 1;
        }
        if self.published_bytes_total != 0 {
            len += 1;
        }
        if self.drained_docs_total != 0 {
            len += 1;
        }
        if self.drained_bytes_total != 0 {
            len += 1;
        }
        if self.publisher_commit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Stored", len)?;
        if self.published_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("publishedDocsTotal", ToString::to_string(&self.published_docs_total).as_str())?;
        }
        if self.published_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("publishedBytesTotal", ToString::to_string(&self.published_bytes_total).as_str())?;
        }
        if self.drained_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("drainedDocsTotal", ToString::to_string(&self.drained_docs_total).as_str())?;
        }
        if self.drained_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("drainedBytesTotal", ToString::to_string(&self.drained_bytes_total).as_str())?;
        }
        if let Some(v) = self.publisher_commit.as_ref() {
            struct_ser.serialize_field("publisherCommit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::Stored {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "published_docs_total",
            "publishedDocsTotal",
            "published_bytes_total",
            "publishedBytesTotal",
            "drained_docs_total",
            "drainedDocsTotal",
            "drained_bytes_total",
            "drainedBytesTotal",
            "publisher_commit",
            "publisherCommit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PublishedDocsTotal,
            PublishedBytesTotal,
            DrainedDocsTotal,
            DrainedBytesTotal,
            PublisherCommit,
            __SkipField__,
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
                            "publishedDocsTotal" | "published_docs_total" => Ok(GeneratedField::PublishedDocsTotal),
                            "publishedBytesTotal" | "published_bytes_total" => Ok(GeneratedField::PublishedBytesTotal),
                            "drainedDocsTotal" | "drained_docs_total" => Ok(GeneratedField::DrainedDocsTotal),
                            "drainedBytesTotal" | "drained_bytes_total" => Ok(GeneratedField::DrainedBytesTotal),
                            "publisherCommit" | "publisher_commit" => Ok(GeneratedField::PublisherCommit),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::Stored;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Stored")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::Stored, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut published_docs_total__ = None;
                let mut published_bytes_total__ = None;
                let mut drained_docs_total__ = None;
                let mut drained_bytes_total__ = None;
                let mut publisher_commit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PublishedDocsTotal => {
                            if published_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publishedDocsTotal"));
                            }
                            published_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PublishedBytesTotal => {
                            if published_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publishedBytesTotal"));
                            }
                            published_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DrainedDocsTotal => {
                            if drained_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("drainedDocsTotal"));
                            }
                            drained_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DrainedBytesTotal => {
                            if drained_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("drainedBytesTotal"));
                            }
                            drained_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PublisherCommit => {
                            if publisher_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publisherCommit"));
                            }
                            publisher_commit__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::Stored {
                    published_docs_total: published_docs_total__.unwrap_or_default(),
                    published_bytes_total: published_bytes_total__.unwrap_or_default(),
                    drained_docs_total: drained_docs_total__.unwrap_or_default(),
                    drained_bytes_total: drained_bytes_total__.unwrap_or_default(),
                    publisher_commit: publisher_commit__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Stored", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive::stored::PublisherCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.producer.is_empty() {
            len += 1;
        }
        if self.clock != 0 {
            len += 1;
        }
        if !self.journals.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Derive.Stored.PublisherCommit", len)?;
        if !self.producer.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("producer", pbjson::private::base64::encode(&self.producer).as_str())?;
        }
        if self.clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("clock", ToString::to_string(&self.clock).as_str())?;
        }
        if !self.journals.is_empty() {
            struct_ser.serialize_field("journals", &self.journals)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive::stored::PublisherCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "producer",
            "clock",
            "journals",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Producer,
            Clock,
            Journals,
            __SkipField__,
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
                            "clock" => Ok(GeneratedField::Clock),
                            "journals" => Ok(GeneratedField::Journals),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive::stored::PublisherCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Derive.Stored.PublisherCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive::stored::PublisherCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut producer__ = None;
                let mut clock__ = None;
                let mut journals__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Producer => {
                            if producer__.is_some() {
                                return Err(serde::de::Error::duplicate_field("producer"));
                            }
                            producer__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
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
                        GeneratedField::Journals => {
                            if journals__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journals"));
                            }
                            journals__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive::stored::PublisherCommit {
                    producer: producer__.unwrap_or_default(),
                    clock: clock__.unwrap_or_default(),
                    journals: journals__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Derive.Stored.PublisherCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeriveRequestExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.log_level != 0 {
            len += 1;
        }
        if self.rocksdb_descriptor.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveRequestExt", len)?;
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.rocksdb_descriptor.as_ref() {
            struct_ser.serialize_field("rocksdbDescriptor", v)?;
        }
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeriveRequestExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log_level",
            "logLevel",
            "rocksdb_descriptor",
            "rocksdbDescriptor",
            "open",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogLevel,
            RocksdbDescriptor,
            Open,
            __SkipField__,
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
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "rocksdbDescriptor" | "rocksdb_descriptor" => Ok(GeneratedField::RocksdbDescriptor),
                            "open" => Ok(GeneratedField::Open),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeriveRequestExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveRequestExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeriveRequestExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut log_level__ = None;
                let mut rocksdb_descriptor__ = None;
                let mut open__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::RocksdbDescriptor => {
                            if rocksdb_descriptor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbDescriptor"));
                            }
                            rocksdb_descriptor__ = map_.next_value()?;
                        }
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(DeriveRequestExt {
                    log_level: log_level__.unwrap_or_default(),
                    rocksdb_descriptor: rocksdb_descriptor__,
                    open: open__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveRequestExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive_request_ext::Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sqlite_vfs_uri.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveRequestExt.Open", len)?;
        if !self.sqlite_vfs_uri.is_empty() {
            struct_ser.serialize_field("sqliteVfsUri", &self.sqlite_vfs_uri)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive_request_ext::Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sqlite_vfs_uri",
            "sqliteVfsUri",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SqliteVfsUri,
            __SkipField__,
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
                            "sqliteVfsUri" | "sqlite_vfs_uri" => Ok(GeneratedField::SqliteVfsUri),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive_request_ext::Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveRequestExt.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive_request_ext::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sqlite_vfs_uri__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SqliteVfsUri => {
                            if sqlite_vfs_uri__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sqliteVfsUri"));
                            }
                            sqlite_vfs_uri__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive_request_ext::Open {
                    sqlite_vfs_uri: sqlite_vfs_uri__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveRequestExt.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeriveResponseExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
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
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveResponseExt", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
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
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeriveResponseExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "opened",
            "published",
            "flushed",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            Opened,
            Published,
            Flushed,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            "opened" => Ok(GeneratedField::Opened),
                            "published" => Ok(GeneratedField::Published),
                            "flushed" => Ok(GeneratedField::Flushed),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeriveResponseExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveResponseExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DeriveResponseExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut opened__ = None;
                let mut published__ = None;
                let mut flushed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(DeriveResponseExt {
                    container: container__,
                    opened: opened__,
                    published: published__,
                    flushed: flushed__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveResponseExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive_response_ext::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.stats.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveResponseExt.Flushed", len)?;
        if let Some(v) = self.stats.as_ref() {
            struct_ser.serialize_field("stats", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive_response_ext::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "stats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Stats,
            __SkipField__,
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
                            "stats" => Ok(GeneratedField::Stats),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive_response_ext::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveResponseExt.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive_response_ext::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive_response_ext::Flushed {
                    stats: stats__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveResponseExt.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive_response_ext::Opened {
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
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveResponseExt.Opened", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive_response_ext::Opened {
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
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive_response_ext::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveResponseExt.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive_response_ext::Opened, V::Error>
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive_response_ext::Opened {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveResponseExt.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for derive_response_ext::Published {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.max_clock != 0 {
            len += 1;
        }
        if !self.key_packed.is_empty() {
            len += 1;
        }
        if !self.partitions_packed.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.DeriveResponseExt.Published", len)?;
        if self.max_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxClock", ToString::to_string(&self.max_clock).as_str())?;
        }
        if !self.key_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("keyPacked", pbjson::private::base64::encode(&self.key_packed).as_str())?;
        }
        if !self.partitions_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("partitionsPacked", pbjson::private::base64::encode(&self.partitions_packed).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for derive_response_ext::Published {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "max_clock",
            "maxClock",
            "key_packed",
            "keyPacked",
            "partitions_packed",
            "partitionsPacked",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MaxClock,
            KeyPacked,
            PartitionsPacked,
            __SkipField__,
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
                            "maxClock" | "max_clock" => Ok(GeneratedField::MaxClock),
                            "keyPacked" | "key_packed" => Ok(GeneratedField::KeyPacked),
                            "partitionsPacked" | "partitions_packed" => Ok(GeneratedField::PartitionsPacked),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = derive_response_ext::Published;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.DeriveResponseExt.Published")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<derive_response_ext::Published, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut max_clock__ = None;
                let mut key_packed__ = None;
                let mut partitions_packed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::MaxClock => {
                            if max_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxClock"));
                            }
                            max_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::KeyPacked => {
                            if key_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPacked"));
                            }
                            key_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionsPacked => {
                            if partitions_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionsPacked"));
                            }
                            partitions_packed__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(derive_response_ext::Published {
                    max_clock: max_clock__.unwrap_or_default(),
                    key_packed: key_packed__.unwrap_or_default(),
                    partitions_packed: partitions_packed__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.DeriveResponseExt.Published", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Join {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.etcd_mod_revision != 0 {
            len += 1;
        }
        if !self.shards.is_empty() {
            len += 1;
        }
        if self.shard_index != 0 {
            len += 1;
        }
        if !self.shuffle_directory.is_empty() {
            len += 1;
        }
        if !self.shuffle_endpoint.is_empty() {
            len += 1;
        }
        if !self.leader_endpoint.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Join", len)?;
        if self.etcd_mod_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("etcdModRevision", ToString::to_string(&self.etcd_mod_revision).as_str())?;
        }
        if !self.shards.is_empty() {
            struct_ser.serialize_field("shards", &self.shards)?;
        }
        if self.shard_index != 0 {
            struct_ser.serialize_field("shardIndex", &self.shard_index)?;
        }
        if !self.shuffle_directory.is_empty() {
            struct_ser.serialize_field("shuffleDirectory", &self.shuffle_directory)?;
        }
        if !self.shuffle_endpoint.is_empty() {
            struct_ser.serialize_field("shuffleEndpoint", &self.shuffle_endpoint)?;
        }
        if !self.leader_endpoint.is_empty() {
            struct_ser.serialize_field("leaderEndpoint", &self.leader_endpoint)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Join {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "etcd_mod_revision",
            "etcdModRevision",
            "shards",
            "shard_index",
            "shardIndex",
            "shuffle_directory",
            "shuffleDirectory",
            "shuffle_endpoint",
            "shuffleEndpoint",
            "leader_endpoint",
            "leaderEndpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            EtcdModRevision,
            Shards,
            ShardIndex,
            ShuffleDirectory,
            ShuffleEndpoint,
            LeaderEndpoint,
            __SkipField__,
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
                            "etcdModRevision" | "etcd_mod_revision" => Ok(GeneratedField::EtcdModRevision),
                            "shards" => Ok(GeneratedField::Shards),
                            "shardIndex" | "shard_index" => Ok(GeneratedField::ShardIndex),
                            "shuffleDirectory" | "shuffle_directory" => Ok(GeneratedField::ShuffleDirectory),
                            "shuffleEndpoint" | "shuffle_endpoint" => Ok(GeneratedField::ShuffleEndpoint),
                            "leaderEndpoint" | "leader_endpoint" => Ok(GeneratedField::LeaderEndpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Join;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Join")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Join, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut etcd_mod_revision__ = None;
                let mut shards__ = None;
                let mut shard_index__ = None;
                let mut shuffle_directory__ = None;
                let mut shuffle_endpoint__ = None;
                let mut leader_endpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::EtcdModRevision => {
                            if etcd_mod_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("etcdModRevision"));
                            }
                            etcd_mod_revision__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Shards => {
                            if shards__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shards"));
                            }
                            shards__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ShardIndex => {
                            if shard_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shardIndex"));
                            }
                            shard_index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ShuffleDirectory => {
                            if shuffle_directory__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleDirectory"));
                            }
                            shuffle_directory__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ShuffleEndpoint => {
                            if shuffle_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleEndpoint"));
                            }
                            shuffle_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LeaderEndpoint => {
                            if leader_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leaderEndpoint"));
                            }
                            leader_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Join {
                    etcd_mod_revision: etcd_mod_revision__.unwrap_or_default(),
                    shards: shards__.unwrap_or_default(),
                    shard_index: shard_index__.unwrap_or_default(),
                    shuffle_directory: shuffle_directory__.unwrap_or_default(),
                    shuffle_endpoint: shuffle_endpoint__.unwrap_or_default(),
                    leader_endpoint: leader_endpoint__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Join", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for join::Shard {
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
        if self.labeling.is_some() {
            len += 1;
        }
        if self.reactor.is_some() {
            len += 1;
        }
        if self.etcd_create_revision != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Join.Shard", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if let Some(v) = self.labeling.as_ref() {
            struct_ser.serialize_field("labeling", v)?;
        }
        if let Some(v) = self.reactor.as_ref() {
            struct_ser.serialize_field("reactor", v)?;
        }
        if self.etcd_create_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("etcdCreateRevision", ToString::to_string(&self.etcd_create_revision).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for join::Shard {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "labeling",
            "reactor",
            "etcd_create_revision",
            "etcdCreateRevision",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Labeling,
            Reactor,
            EtcdCreateRevision,
            __SkipField__,
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
                            "labeling" => Ok(GeneratedField::Labeling),
                            "reactor" => Ok(GeneratedField::Reactor),
                            "etcdCreateRevision" | "etcd_create_revision" => Ok(GeneratedField::EtcdCreateRevision),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = join::Shard;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Join.Shard")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<join::Shard, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut labeling__ = None;
                let mut reactor__ = None;
                let mut etcd_create_revision__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Labeling => {
                            if labeling__.is_some() {
                                return Err(serde::de::Error::duplicate_field("labeling"));
                            }
                            labeling__ = map_.next_value()?;
                        }
                        GeneratedField::Reactor => {
                            if reactor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reactor"));
                            }
                            reactor__ = map_.next_value()?;
                        }
                        GeneratedField::EtcdCreateRevision => {
                            if etcd_create_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("etcdCreateRevision"));
                            }
                            etcd_create_revision__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(join::Shard {
                    id: id__.unwrap_or_default(),
                    labeling: labeling__,
                    reactor: reactor__,
                    etcd_create_revision: etcd_create_revision__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Join.Shard", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Joined {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.max_etcd_revision != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Joined", len)?;
        if self.max_etcd_revision != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxEtcdRevision", ToString::to_string(&self.max_etcd_revision).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Joined {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "max_etcd_revision",
            "maxEtcdRevision",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MaxEtcdRevision,
            __SkipField__,
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
                            "maxEtcdRevision" | "max_etcd_revision" => Ok(GeneratedField::MaxEtcdRevision),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Joined;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Joined")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Joined, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut max_etcd_revision__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::MaxEtcdRevision => {
                            if max_etcd_revision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxEtcdRevision"));
                            }
                            max_etcd_revision__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Joined {
                    max_etcd_revision: max_etcd_revision__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Joined", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Materialize {
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
        if self.spec_response.is_some() {
            len += 1;
        }
        if self.validate.is_some() {
            len += 1;
        }
        if self.validated.is_some() {
            len += 1;
        }
        if self.log_level != 0 {
            len += 1;
        }
        if self.session_loop.is_some() {
            len += 1;
        }
        if self.join.is_some() {
            len += 1;
        }
        if self.joined.is_some() {
            len += 1;
        }
        if self.task.is_some() {
            len += 1;
        }
        if self.recover.is_some() {
            len += 1;
        }
        if self.apply.is_some() {
            len += 1;
        }
        if self.applied.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.load.is_some() {
            len += 1;
        }
        if self.loaded.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        if self.store.is_some() {
            len += 1;
        }
        if self.stored.is_some() {
            len += 1;
        }
        if self.start_commit.is_some() {
            len += 1;
        }
        if self.started_commit.is_some() {
            len += 1;
        }
        if self.acknowledge.is_some() {
            len += 1;
        }
        if self.acknowledged.is_some() {
            len += 1;
        }
        if self.persist.is_some() {
            len += 1;
        }
        if self.persisted.is_some() {
            len += 1;
        }
        if self.close_now.is_some() {
            len += 1;
        }
        if self.stop.is_some() {
            len += 1;
        }
        if self.stopped.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
        }
        if let Some(v) = self.spec_response.as_ref() {
            struct_ser.serialize_field("specResponse", v)?;
        }
        if let Some(v) = self.validate.as_ref() {
            struct_ser.serialize_field("validate", v)?;
        }
        if let Some(v) = self.validated.as_ref() {
            struct_ser.serialize_field("validated", v)?;
        }
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.session_loop.as_ref() {
            struct_ser.serialize_field("sessionLoop", v)?;
        }
        if let Some(v) = self.join.as_ref() {
            struct_ser.serialize_field("join", v)?;
        }
        if let Some(v) = self.joined.as_ref() {
            struct_ser.serialize_field("joined", v)?;
        }
        if let Some(v) = self.task.as_ref() {
            struct_ser.serialize_field("task", v)?;
        }
        if let Some(v) = self.recover.as_ref() {
            struct_ser.serialize_field("recover", v)?;
        }
        if let Some(v) = self.apply.as_ref() {
            struct_ser.serialize_field("apply", v)?;
        }
        if let Some(v) = self.applied.as_ref() {
            struct_ser.serialize_field("applied", v)?;
        }
        if let Some(v) = self.open.as_ref() {
            struct_ser.serialize_field("open", v)?;
        }
        if let Some(v) = self.opened.as_ref() {
            struct_ser.serialize_field("opened", v)?;
        }
        if let Some(v) = self.load.as_ref() {
            struct_ser.serialize_field("load", v)?;
        }
        if let Some(v) = self.loaded.as_ref() {
            struct_ser.serialize_field("loaded", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        if let Some(v) = self.store.as_ref() {
            struct_ser.serialize_field("store", v)?;
        }
        if let Some(v) = self.stored.as_ref() {
            struct_ser.serialize_field("stored", v)?;
        }
        if let Some(v) = self.start_commit.as_ref() {
            struct_ser.serialize_field("startCommit", v)?;
        }
        if let Some(v) = self.started_commit.as_ref() {
            struct_ser.serialize_field("startedCommit", v)?;
        }
        if let Some(v) = self.acknowledge.as_ref() {
            struct_ser.serialize_field("acknowledge", v)?;
        }
        if let Some(v) = self.acknowledged.as_ref() {
            struct_ser.serialize_field("acknowledged", v)?;
        }
        if let Some(v) = self.persist.as_ref() {
            struct_ser.serialize_field("persist", v)?;
        }
        if let Some(v) = self.persisted.as_ref() {
            struct_ser.serialize_field("persisted", v)?;
        }
        if let Some(v) = self.close_now.as_ref() {
            struct_ser.serialize_field("closeNow", v)?;
        }
        if let Some(v) = self.stop.as_ref() {
            struct_ser.serialize_field("stop", v)?;
        }
        if let Some(v) = self.stopped.as_ref() {
            struct_ser.serialize_field("stopped", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Materialize {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "spec_response",
            "specResponse",
            "validate",
            "validated",
            "log_level",
            "logLevel",
            "session_loop",
            "sessionLoop",
            "join",
            "joined",
            "task",
            "recover",
            "apply",
            "applied",
            "open",
            "opened",
            "load",
            "loaded",
            "flush",
            "flushed",
            "store",
            "stored",
            "start_commit",
            "startCommit",
            "started_commit",
            "startedCommit",
            "acknowledge",
            "acknowledged",
            "persist",
            "persisted",
            "close_now",
            "closeNow",
            "stop",
            "stopped",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            SpecResponse,
            Validate,
            Validated,
            LogLevel,
            SessionLoop,
            Join,
            Joined,
            Task,
            Recover,
            Apply,
            Applied,
            Open,
            Opened,
            Load,
            Loaded,
            Flush,
            Flushed,
            Store,
            Stored,
            StartCommit,
            StartedCommit,
            Acknowledge,
            Acknowledged,
            Persist,
            Persisted,
            CloseNow,
            Stop,
            Stopped,
            __SkipField__,
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
                            "specResponse" | "spec_response" => Ok(GeneratedField::SpecResponse),
                            "validate" => Ok(GeneratedField::Validate),
                            "validated" => Ok(GeneratedField::Validated),
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "sessionLoop" | "session_loop" => Ok(GeneratedField::SessionLoop),
                            "join" => Ok(GeneratedField::Join),
                            "joined" => Ok(GeneratedField::Joined),
                            "task" => Ok(GeneratedField::Task),
                            "recover" => Ok(GeneratedField::Recover),
                            "apply" => Ok(GeneratedField::Apply),
                            "applied" => Ok(GeneratedField::Applied),
                            "open" => Ok(GeneratedField::Open),
                            "opened" => Ok(GeneratedField::Opened),
                            "load" => Ok(GeneratedField::Load),
                            "loaded" => Ok(GeneratedField::Loaded),
                            "flush" => Ok(GeneratedField::Flush),
                            "flushed" => Ok(GeneratedField::Flushed),
                            "store" => Ok(GeneratedField::Store),
                            "stored" => Ok(GeneratedField::Stored),
                            "startCommit" | "start_commit" => Ok(GeneratedField::StartCommit),
                            "startedCommit" | "started_commit" => Ok(GeneratedField::StartedCommit),
                            "acknowledge" => Ok(GeneratedField::Acknowledge),
                            "acknowledged" => Ok(GeneratedField::Acknowledged),
                            "persist" => Ok(GeneratedField::Persist),
                            "persisted" => Ok(GeneratedField::Persisted),
                            "closeNow" | "close_now" => Ok(GeneratedField::CloseNow),
                            "stop" => Ok(GeneratedField::Stop),
                            "stopped" => Ok(GeneratedField::Stopped),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Materialize;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Materialize, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut spec_response__ = None;
                let mut validate__ = None;
                let mut validated__ = None;
                let mut log_level__ = None;
                let mut session_loop__ = None;
                let mut join__ = None;
                let mut joined__ = None;
                let mut task__ = None;
                let mut recover__ = None;
                let mut apply__ = None;
                let mut applied__ = None;
                let mut open__ = None;
                let mut opened__ = None;
                let mut load__ = None;
                let mut loaded__ = None;
                let mut flush__ = None;
                let mut flushed__ = None;
                let mut store__ = None;
                let mut stored__ = None;
                let mut start_commit__ = None;
                let mut started_commit__ = None;
                let mut acknowledge__ = None;
                let mut acknowledged__ = None;
                let mut persist__ = None;
                let mut persisted__ = None;
                let mut close_now__ = None;
                let mut stop__ = None;
                let mut stopped__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
                        }
                        GeneratedField::SpecResponse => {
                            if spec_response__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specResponse"));
                            }
                            spec_response__ = map_.next_value()?;
                        }
                        GeneratedField::Validate => {
                            if validate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validate"));
                            }
                            validate__ = map_.next_value()?;
                        }
                        GeneratedField::Validated => {
                            if validated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("validated"));
                            }
                            validated__ = map_.next_value()?;
                        }
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::SessionLoop => {
                            if session_loop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sessionLoop"));
                            }
                            session_loop__ = map_.next_value()?;
                        }
                        GeneratedField::Join => {
                            if join__.is_some() {
                                return Err(serde::de::Error::duplicate_field("join"));
                            }
                            join__ = map_.next_value()?;
                        }
                        GeneratedField::Joined => {
                            if joined__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joined"));
                            }
                            joined__ = map_.next_value()?;
                        }
                        GeneratedField::Task => {
                            if task__.is_some() {
                                return Err(serde::de::Error::duplicate_field("task"));
                            }
                            task__ = map_.next_value()?;
                        }
                        GeneratedField::Recover => {
                            if recover__.is_some() {
                                return Err(serde::de::Error::duplicate_field("recover"));
                            }
                            recover__ = map_.next_value()?;
                        }
                        GeneratedField::Apply => {
                            if apply__.is_some() {
                                return Err(serde::de::Error::duplicate_field("apply"));
                            }
                            apply__ = map_.next_value()?;
                        }
                        GeneratedField::Applied => {
                            if applied__.is_some() {
                                return Err(serde::de::Error::duplicate_field("applied"));
                            }
                            applied__ = map_.next_value()?;
                        }
                        GeneratedField::Open => {
                            if open__.is_some() {
                                return Err(serde::de::Error::duplicate_field("open"));
                            }
                            open__ = map_.next_value()?;
                        }
                        GeneratedField::Opened => {
                            if opened__.is_some() {
                                return Err(serde::de::Error::duplicate_field("opened"));
                            }
                            opened__ = map_.next_value()?;
                        }
                        GeneratedField::Load => {
                            if load__.is_some() {
                                return Err(serde::de::Error::duplicate_field("load"));
                            }
                            load__ = map_.next_value()?;
                        }
                        GeneratedField::Loaded => {
                            if loaded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("loaded"));
                            }
                            loaded__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                        GeneratedField::Store => {
                            if store__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store"));
                            }
                            store__ = map_.next_value()?;
                        }
                        GeneratedField::Stored => {
                            if stored__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stored"));
                            }
                            stored__ = map_.next_value()?;
                        }
                        GeneratedField::StartCommit => {
                            if start_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startCommit"));
                            }
                            start_commit__ = map_.next_value()?;
                        }
                        GeneratedField::StartedCommit => {
                            if started_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startedCommit"));
                            }
                            started_commit__ = map_.next_value()?;
                        }
                        GeneratedField::Acknowledge => {
                            if acknowledge__.is_some() {
                                return Err(serde::de::Error::duplicate_field("acknowledge"));
                            }
                            acknowledge__ = map_.next_value()?;
                        }
                        GeneratedField::Acknowledged => {
                            if acknowledged__.is_some() {
                                return Err(serde::de::Error::duplicate_field("acknowledged"));
                            }
                            acknowledged__ = map_.next_value()?;
                        }
                        GeneratedField::Persist => {
                            if persist__.is_some() {
                                return Err(serde::de::Error::duplicate_field("persist"));
                            }
                            persist__ = map_.next_value()?;
                        }
                        GeneratedField::Persisted => {
                            if persisted__.is_some() {
                                return Err(serde::de::Error::duplicate_field("persisted"));
                            }
                            persisted__ = map_.next_value()?;
                        }
                        GeneratedField::CloseNow => {
                            if close_now__.is_some() {
                                return Err(serde::de::Error::duplicate_field("closeNow"));
                            }
                            close_now__ = map_.next_value()?;
                        }
                        GeneratedField::Stop => {
                            if stop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            stop__ = map_.next_value()?;
                        }
                        GeneratedField::Stopped => {
                            if stopped__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stopped"));
                            }
                            stopped__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Materialize {
                    spec: spec__,
                    spec_response: spec_response__,
                    validate: validate__,
                    validated: validated__,
                    log_level: log_level__.unwrap_or_default(),
                    session_loop: session_loop__,
                    join: join__,
                    joined: joined__,
                    task: task__,
                    recover: recover__,
                    apply: apply__,
                    applied: applied__,
                    open: open__,
                    opened: opened__,
                    load: load__,
                    loaded: loaded__,
                    flush: flush__,
                    flushed: flushed__,
                    store: store__,
                    stored: stored__,
                    start_commit: start_commit__,
                    started_commit: started_commit__,
                    acknowledge: acknowledge__,
                    acknowledged: acknowledged__,
                    persist: persist__,
                    persisted: persisted__,
                    close_now: close_now__,
                    stop: stop__,
                    stopped: stopped__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Acknowledge {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Acknowledge", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Acknowledge {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Acknowledge;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Acknowledge")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Acknowledge, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Acknowledge {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Acknowledge", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Acknowledged {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Acknowledged", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Acknowledged {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Acknowledged;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Acknowledged")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Acknowledged, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Acknowledged {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Acknowledged", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Flush", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Flush {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Flush {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Flushed {
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
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Flushed", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bindings",
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bindings,
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bindings__ = None;
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Flushed {
                    bindings: bindings__.unwrap_or_default(),
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::flushed::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.loaded_docs_total != 0 {
            len += 1;
        }
        if self.loaded_bytes_total != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Flushed.Binding", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.loaded_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("loadedDocsTotal", ToString::to_string(&self.loaded_docs_total).as_str())?;
        }
        if self.loaded_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("loadedBytesTotal", ToString::to_string(&self.loaded_bytes_total).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::flushed::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "loaded_docs_total",
            "loadedDocsTotal",
            "loaded_bytes_total",
            "loadedBytesTotal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            LoadedDocsTotal,
            LoadedBytesTotal,
            __SkipField__,
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
                            "index" => Ok(GeneratedField::Index),
                            "loadedDocsTotal" | "loaded_docs_total" => Ok(GeneratedField::LoadedDocsTotal),
                            "loadedBytesTotal" | "loaded_bytes_total" => Ok(GeneratedField::LoadedBytesTotal),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::flushed::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Flushed.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::flushed::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut loaded_docs_total__ = None;
                let mut loaded_bytes_total__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LoadedDocsTotal => {
                            if loaded_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("loadedDocsTotal"));
                            }
                            loaded_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LoadedBytesTotal => {
                            if loaded_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("loadedBytesTotal"));
                            }
                            loaded_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::flushed::Binding {
                    index: index__.unwrap_or_default(),
                    loaded_docs_total: loaded_docs_total__.unwrap_or_default(),
                    loaded_bytes_total: loaded_bytes_total__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Flushed.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Load {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.frontier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Load", len)?;
        if let Some(v) = self.frontier.as_ref() {
            struct_ser.serialize_field("frontier", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Load {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "frontier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Frontier,
            __SkipField__,
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
                            "frontier" => Ok(GeneratedField::Frontier),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Load;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Load")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Load, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut frontier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Frontier => {
                            if frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("frontier"));
                            }
                            frontier__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Load {
                    frontier: frontier__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Load", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Loaded {
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
        if self.combiner_usage_bytes != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Loaded", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if self.combiner_usage_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("combinerUsageBytes", ToString::to_string(&self.combiner_usage_bytes).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Loaded {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bindings",
            "combiner_usage_bytes",
            "combinerUsageBytes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bindings,
            CombinerUsageBytes,
            __SkipField__,
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
                            "combinerUsageBytes" | "combiner_usage_bytes" => Ok(GeneratedField::CombinerUsageBytes),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Loaded;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Loaded")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Loaded, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bindings__ = None;
                let mut combiner_usage_bytes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bindings => {
                            if bindings__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bindings"));
                            }
                            bindings__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CombinerUsageBytes => {
                            if combiner_usage_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("combinerUsageBytes"));
                            }
                            combiner_usage_bytes__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Loaded {
                    bindings: bindings__.unwrap_or_default(),
                    combiner_usage_bytes: combiner_usage_bytes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Loaded", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::loaded::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.min_source_clock != 0 {
            len += 1;
        }
        if self.max_source_clock != 0 {
            len += 1;
        }
        if self.sourced_docs_total != 0 {
            len += 1;
        }
        if self.sourced_bytes_total != 0 {
            len += 1;
        }
        if !self.max_key_delta.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Loaded.Binding", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.min_source_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("minSourceClock", ToString::to_string(&self.min_source_clock).as_str())?;
        }
        if self.max_source_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxSourceClock", ToString::to_string(&self.max_source_clock).as_str())?;
        }
        if self.sourced_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sourcedDocsTotal", ToString::to_string(&self.sourced_docs_total).as_str())?;
        }
        if self.sourced_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("sourcedBytesTotal", ToString::to_string(&self.sourced_bytes_total).as_str())?;
        }
        if !self.max_key_delta.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("maxKeyDelta", pbjson::private::base64::encode(&self.max_key_delta).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::loaded::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "min_source_clock",
            "minSourceClock",
            "max_source_clock",
            "maxSourceClock",
            "sourced_docs_total",
            "sourcedDocsTotal",
            "sourced_bytes_total",
            "sourcedBytesTotal",
            "max_key_delta",
            "maxKeyDelta",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            MinSourceClock,
            MaxSourceClock,
            SourcedDocsTotal,
            SourcedBytesTotal,
            MaxKeyDelta,
            __SkipField__,
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
                            "index" => Ok(GeneratedField::Index),
                            "minSourceClock" | "min_source_clock" => Ok(GeneratedField::MinSourceClock),
                            "maxSourceClock" | "max_source_clock" => Ok(GeneratedField::MaxSourceClock),
                            "sourcedDocsTotal" | "sourced_docs_total" => Ok(GeneratedField::SourcedDocsTotal),
                            "sourcedBytesTotal" | "sourced_bytes_total" => Ok(GeneratedField::SourcedBytesTotal),
                            "maxKeyDelta" | "max_key_delta" => Ok(GeneratedField::MaxKeyDelta),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::loaded::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Loaded.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::loaded::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut min_source_clock__ = None;
                let mut max_source_clock__ = None;
                let mut sourced_docs_total__ = None;
                let mut sourced_bytes_total__ = None;
                let mut max_key_delta__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MinSourceClock => {
                            if min_source_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minSourceClock"));
                            }
                            min_source_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxSourceClock => {
                            if max_source_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxSourceClock"));
                            }
                            max_source_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SourcedDocsTotal => {
                            if sourced_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourcedDocsTotal"));
                            }
                            sourced_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SourcedBytesTotal => {
                            if sourced_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourcedBytesTotal"));
                            }
                            sourced_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxKeyDelta => {
                            if max_key_delta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxKeyDelta"));
                            }
                            max_key_delta__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::loaded::Binding {
                    index: index__.unwrap_or_default(),
                    min_source_clock: min_source_clock__.unwrap_or_default(),
                    max_source_clock: max_source_clock__.unwrap_or_default(),
                    sourced_docs_total: sourced_docs_total__.unwrap_or_default(),
                    sourced_bytes_total: sourced_bytes_total__.unwrap_or_default(),
                    max_key_delta: max_key_delta__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Loaded.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Opened {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
            len += 1;
        }
        if self.connector_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Opened", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
        }
        if let Some(v) = self.connector_checkpoint.as_ref() {
            struct_ser.serialize_field("connectorCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Opened {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "connector_checkpoint",
            "connectorCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            ConnectorCheckpoint,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            "connectorCheckpoint" | "connector_checkpoint" => Ok(GeneratedField::ConnectorCheckpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Opened;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Opened, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut connector_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
                        }
                        GeneratedField::ConnectorCheckpoint => {
                            if connector_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorCheckpoint"));
                            }
                            connector_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Opened {
                    container: container__,
                    connector_checkpoint: connector_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Opened", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::StartCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        if self.connector_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.StartCommit", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        if let Some(v) = self.connector_checkpoint.as_ref() {
            struct_ser.serialize_field("connectorCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::StartCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
            "connector_checkpoint",
            "connectorCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            ConnectorCheckpoint,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            "connectorCheckpoint" | "connector_checkpoint" => Ok(GeneratedField::ConnectorCheckpoint),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::StartCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.StartCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::StartCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                let mut connector_checkpoint__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::ConnectorCheckpoint => {
                            if connector_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorCheckpoint"));
                            }
                            connector_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::StartCommit {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                    connector_checkpoint: connector_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.StartCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::StartedCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.StartedCommit", len)?;
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::StartedCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "connector_patches_json",
            "connectorPatches",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnectorPatchesJson,
            __SkipField__,
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
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::StartedCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.StartedCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::StartedCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut connector_patches_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::StartedCommit {
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.StartedCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Store {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Materialize.Store", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Store {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Store;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Store")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Store, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(materialize::Store {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Store", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::Stored {
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
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Stored", len)?;
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::Stored {
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
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::Stored;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Stored")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::Stored, V::Error>
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::Stored {
                    bindings: bindings__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Stored", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize::stored::Binding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.stored_docs_total != 0 {
            len += 1;
        }
        if self.stored_bytes_total != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Materialize.Stored.Binding", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.stored_docs_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("storedDocsTotal", ToString::to_string(&self.stored_docs_total).as_str())?;
        }
        if self.stored_bytes_total != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("storedBytesTotal", ToString::to_string(&self.stored_bytes_total).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize::stored::Binding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "stored_docs_total",
            "storedDocsTotal",
            "stored_bytes_total",
            "storedBytesTotal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            StoredDocsTotal,
            StoredBytesTotal,
            __SkipField__,
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
                            "index" => Ok(GeneratedField::Index),
                            "storedDocsTotal" | "stored_docs_total" => Ok(GeneratedField::StoredDocsTotal),
                            "storedBytesTotal" | "stored_bytes_total" => Ok(GeneratedField::StoredBytesTotal),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize::stored::Binding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Materialize.Stored.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize::stored::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut stored_docs_total__ = None;
                let mut stored_bytes_total__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::StoredDocsTotal => {
                            if stored_docs_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("storedDocsTotal"));
                            }
                            stored_docs_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::StoredBytesTotal => {
                            if stored_bytes_total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("storedBytesTotal"));
                            }
                            stored_bytes_total__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize::stored::Binding {
                    index: index__.unwrap_or_default(),
                    stored_docs_total: stored_docs_total__.unwrap_or_default(),
                    stored_bytes_total: stored_bytes_total__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Materialize.Stored.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaterializeRequestExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.log_level != 0 {
            len += 1;
        }
        if self.rocksdb_descriptor.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.MaterializeRequestExt", len)?;
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.log_level)))?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if let Some(v) = self.rocksdb_descriptor.as_ref() {
            struct_ser.serialize_field("rocksdbDescriptor", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaterializeRequestExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log_level",
            "logLevel",
            "rocksdb_descriptor",
            "rocksdbDescriptor",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogLevel,
            RocksdbDescriptor,
            __SkipField__,
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
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "rocksdbDescriptor" | "rocksdb_descriptor" => Ok(GeneratedField::RocksdbDescriptor),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaterializeRequestExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.MaterializeRequestExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MaterializeRequestExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut log_level__ = None;
                let mut rocksdb_descriptor__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::RocksdbDescriptor => {
                            if rocksdb_descriptor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbDescriptor"));
                            }
                            rocksdb_descriptor__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(MaterializeRequestExt {
                    log_level: log_level__.unwrap_or_default(),
                    rocksdb_descriptor: rocksdb_descriptor__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.MaterializeRequestExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaterializeResponseExt {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.container.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.MaterializeResponseExt", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaterializeResponseExt {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "flushed",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            Flushed,
            __SkipField__,
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
                            "container" => Ok(GeneratedField::Container),
                            "flushed" => Ok(GeneratedField::Flushed),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaterializeResponseExt;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.MaterializeResponseExt")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MaterializeResponseExt, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut flushed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(MaterializeResponseExt {
                    container: container__,
                    flushed: flushed__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.MaterializeResponseExt", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for materialize_response_ext::Flushed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.stats.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.MaterializeResponseExt.Flushed", len)?;
        if let Some(v) = self.stats.as_ref() {
            struct_ser.serialize_field("stats", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for materialize_response_ext::Flushed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "stats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Stats,
            __SkipField__,
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
                            "stats" => Ok(GeneratedField::Stats),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = materialize_response_ext::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.MaterializeResponseExt.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<materialize_response_ext::Flushed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Stats => {
                            if stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stats"));
                            }
                            stats__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(materialize_response_ext::Flushed {
                    stats: stats__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.MaterializeResponseExt.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Open {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.spec.is_empty() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if !self.connector_state_json.is_empty() {
            len += 1;
        }
        if !self.max_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Open", len)?;
        if !self.spec.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("spec", pbjson::private::base64::encode(&self.spec).as_str())?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.connector_state_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("state", &crate::as_raw_json(&self.connector_state_json)?)?;
        }
        if !self.max_keys.is_empty() {
            let v: std::collections::HashMap<_, _> = self.max_keys.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("maxKeys", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Open {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "spec",
            "version",
            "range",
            "connector_state_json",
            "state",
            "max_keys",
            "maxKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Version,
            Range,
            ConnectorStateJson,
            MaxKeys,
            __SkipField__,
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
                            "version" => Ok(GeneratedField::Version),
                            "range" => Ok(GeneratedField::Range),
                            "state" | "connector_state_json" => Ok(GeneratedField::ConnectorStateJson),
                            "maxKeys" | "max_keys" => Ok(GeneratedField::MaxKeys),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Open;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut version__ = None;
                let mut range__ = None;
                let mut connector_state_json__ = None;
                let mut max_keys__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
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
                        GeneratedField::ConnectorStateJson => {
                            if connector_state_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            connector_state_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::MaxKeys => {
                            if max_keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxKeys"));
                            }
                            max_keys__ = Some(
                                map_.next_value::<std::collections::BTreeMap<::pbjson::private::NumberDeserialize<u32>, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k.0, v.0)).collect()
                            );
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Open {
                    spec: spec__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                    range: range__,
                    connector_state_json: connector_state_json__.unwrap_or_default(),
                    max_keys: max_keys__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Open", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Persist {
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
        if self.delete_ack_intents {
            len += 1;
        }
        if !self.ack_intents.is_empty() {
            len += 1;
        }
        if self.committed_close_clock != 0 {
            len += 1;
        }
        if self.delete_committed_frontier {
            len += 1;
        }
        if self.committed_frontier.is_some() {
            len += 1;
        }
        if !self.connector_patches_json.is_empty() {
            len += 1;
        }
        if self.hinted_close_clock != 0 {
            len += 1;
        }
        if self.delete_hinted_frontier {
            len += 1;
        }
        if self.hinted_frontier.is_some() {
            len += 1;
        }
        if !self.last_applied.is_empty() {
            len += 1;
        }
        if self.delete_legacy_checkpoint {
            len += 1;
        }
        if self.legacy_checkpoint.is_some() {
            len += 1;
        }
        if !self.max_keys.is_empty() {
            len += 1;
        }
        if self.delete_trigger_params {
            len += 1;
        }
        if !self.trigger_params_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Persist", len)?;
        if self.seq_no != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqNo", ToString::to_string(&self.seq_no).as_str())?;
        }
        if self.delete_ack_intents {
            struct_ser.serialize_field("deleteAckIntents", &self.delete_ack_intents)?;
        }
        if !self.ack_intents.is_empty() {
            let v: std::collections::HashMap<_, _> = self.ack_intents.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("ackIntents", &v)?;
        }
        if self.committed_close_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("committedCloseClock", ToString::to_string(&self.committed_close_clock).as_str())?;
        }
        if self.delete_committed_frontier {
            struct_ser.serialize_field("deleteCommittedFrontier", &self.delete_committed_frontier)?;
        }
        if let Some(v) = self.committed_frontier.as_ref() {
            struct_ser.serialize_field("committedFrontier", v)?;
        }
        if !self.connector_patches_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorPatches", &crate::as_raw_json(&self.connector_patches_json)?)?;
        }
        if self.hinted_close_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hintedCloseClock", ToString::to_string(&self.hinted_close_clock).as_str())?;
        }
        if self.delete_hinted_frontier {
            struct_ser.serialize_field("deleteHintedFrontier", &self.delete_hinted_frontier)?;
        }
        if let Some(v) = self.hinted_frontier.as_ref() {
            struct_ser.serialize_field("hintedFrontier", v)?;
        }
        if !self.last_applied.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastApplied", pbjson::private::base64::encode(&self.last_applied).as_str())?;
        }
        if self.delete_legacy_checkpoint {
            struct_ser.serialize_field("deleteLegacyCheckpoint", &self.delete_legacy_checkpoint)?;
        }
        if let Some(v) = self.legacy_checkpoint.as_ref() {
            struct_ser.serialize_field("legacyCheckpoint", v)?;
        }
        if !self.max_keys.is_empty() {
            let v: std::collections::HashMap<_, _> = self.max_keys.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("maxKeys", &v)?;
        }
        if self.delete_trigger_params {
            struct_ser.serialize_field("deleteTriggerParams", &self.delete_trigger_params)?;
        }
        if !self.trigger_params_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("triggerParams", &crate::as_raw_json(&self.trigger_params_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Persist {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seq_no",
            "seqNo",
            "delete_ack_intents",
            "deleteAckIntents",
            "ack_intents",
            "ackIntents",
            "committed_close_clock",
            "committedCloseClock",
            "delete_committed_frontier",
            "deleteCommittedFrontier",
            "committed_frontier",
            "committedFrontier",
            "connector_patches_json",
            "connectorPatches",
            "hinted_close_clock",
            "hintedCloseClock",
            "delete_hinted_frontier",
            "deleteHintedFrontier",
            "hinted_frontier",
            "hintedFrontier",
            "last_applied",
            "lastApplied",
            "delete_legacy_checkpoint",
            "deleteLegacyCheckpoint",
            "legacy_checkpoint",
            "legacyCheckpoint",
            "max_keys",
            "maxKeys",
            "delete_trigger_params",
            "deleteTriggerParams",
            "trigger_params_json",
            "triggerParams",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SeqNo,
            DeleteAckIntents,
            AckIntents,
            CommittedCloseClock,
            DeleteCommittedFrontier,
            CommittedFrontier,
            ConnectorPatchesJson,
            HintedCloseClock,
            DeleteHintedFrontier,
            HintedFrontier,
            LastApplied,
            DeleteLegacyCheckpoint,
            LegacyCheckpoint,
            MaxKeys,
            DeleteTriggerParams,
            TriggerParamsJson,
            __SkipField__,
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
                            "deleteAckIntents" | "delete_ack_intents" => Ok(GeneratedField::DeleteAckIntents),
                            "ackIntents" | "ack_intents" => Ok(GeneratedField::AckIntents),
                            "committedCloseClock" | "committed_close_clock" => Ok(GeneratedField::CommittedCloseClock),
                            "deleteCommittedFrontier" | "delete_committed_frontier" => Ok(GeneratedField::DeleteCommittedFrontier),
                            "committedFrontier" | "committed_frontier" => Ok(GeneratedField::CommittedFrontier),
                            "connectorPatches" | "connector_patches_json" => Ok(GeneratedField::ConnectorPatchesJson),
                            "hintedCloseClock" | "hinted_close_clock" => Ok(GeneratedField::HintedCloseClock),
                            "deleteHintedFrontier" | "delete_hinted_frontier" => Ok(GeneratedField::DeleteHintedFrontier),
                            "hintedFrontier" | "hinted_frontier" => Ok(GeneratedField::HintedFrontier),
                            "lastApplied" | "last_applied" => Ok(GeneratedField::LastApplied),
                            "deleteLegacyCheckpoint" | "delete_legacy_checkpoint" => Ok(GeneratedField::DeleteLegacyCheckpoint),
                            "legacyCheckpoint" | "legacy_checkpoint" => Ok(GeneratedField::LegacyCheckpoint),
                            "maxKeys" | "max_keys" => Ok(GeneratedField::MaxKeys),
                            "deleteTriggerParams" | "delete_trigger_params" => Ok(GeneratedField::DeleteTriggerParams),
                            "triggerParams" | "trigger_params_json" => Ok(GeneratedField::TriggerParamsJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Persist;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Persist")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Persist, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seq_no__ = None;
                let mut delete_ack_intents__ = None;
                let mut ack_intents__ = None;
                let mut committed_close_clock__ = None;
                let mut delete_committed_frontier__ = None;
                let mut committed_frontier__ = None;
                let mut connector_patches_json__ = None;
                let mut hinted_close_clock__ = None;
                let mut delete_hinted_frontier__ = None;
                let mut hinted_frontier__ = None;
                let mut last_applied__ = None;
                let mut delete_legacy_checkpoint__ = None;
                let mut legacy_checkpoint__ = None;
                let mut max_keys__ = None;
                let mut delete_trigger_params__ = None;
                let mut trigger_params_json__ = None;
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
                        GeneratedField::DeleteAckIntents => {
                            if delete_ack_intents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteAckIntents"));
                            }
                            delete_ack_intents__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AckIntents => {
                            if ack_intents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ackIntents"));
                            }
                            ack_intents__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::CommittedCloseClock => {
                            if committed_close_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("committedCloseClock"));
                            }
                            committed_close_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DeleteCommittedFrontier => {
                            if delete_committed_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteCommittedFrontier"));
                            }
                            delete_committed_frontier__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CommittedFrontier => {
                            if committed_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("committedFrontier"));
                            }
                            committed_frontier__ = map_.next_value()?;
                        }
                        GeneratedField::ConnectorPatchesJson => {
                            if connector_patches_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorPatches"));
                            }
                            connector_patches_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::HintedCloseClock => {
                            if hinted_close_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintedCloseClock"));
                            }
                            hinted_close_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DeleteHintedFrontier => {
                            if delete_hinted_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteHintedFrontier"));
                            }
                            delete_hinted_frontier__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HintedFrontier => {
                            if hinted_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintedFrontier"));
                            }
                            hinted_frontier__ = map_.next_value()?;
                        }
                        GeneratedField::LastApplied => {
                            if last_applied__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastApplied"));
                            }
                            last_applied__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DeleteLegacyCheckpoint => {
                            if delete_legacy_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteLegacyCheckpoint"));
                            }
                            delete_legacy_checkpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LegacyCheckpoint => {
                            if legacy_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("legacyCheckpoint"));
                            }
                            legacy_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::MaxKeys => {
                            if max_keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxKeys"));
                            }
                            max_keys__ = Some(
                                map_.next_value::<std::collections::BTreeMap<::pbjson::private::NumberDeserialize<u32>, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k.0, v.0)).collect()
                            );
                        }
                        GeneratedField::DeleteTriggerParams => {
                            if delete_trigger_params__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deleteTriggerParams"));
                            }
                            delete_trigger_params__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TriggerParamsJson => {
                            if trigger_params_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("triggerParams"));
                            }
                            trigger_params_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Persist {
                    seq_no: seq_no__.unwrap_or_default(),
                    delete_ack_intents: delete_ack_intents__.unwrap_or_default(),
                    ack_intents: ack_intents__.unwrap_or_default(),
                    committed_close_clock: committed_close_clock__.unwrap_or_default(),
                    delete_committed_frontier: delete_committed_frontier__.unwrap_or_default(),
                    committed_frontier: committed_frontier__,
                    connector_patches_json: connector_patches_json__.unwrap_or_default(),
                    hinted_close_clock: hinted_close_clock__.unwrap_or_default(),
                    delete_hinted_frontier: delete_hinted_frontier__.unwrap_or_default(),
                    hinted_frontier: hinted_frontier__,
                    last_applied: last_applied__.unwrap_or_default(),
                    delete_legacy_checkpoint: delete_legacy_checkpoint__.unwrap_or_default(),
                    legacy_checkpoint: legacy_checkpoint__,
                    max_keys: max_keys__.unwrap_or_default(),
                    delete_trigger_params: delete_trigger_params__.unwrap_or_default(),
                    trigger_params_json: trigger_params_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Persist", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Persisted {
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
        let mut struct_ser = serializer.serialize_struct("runtime.Persisted", len)?;
        if self.seq_no != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("seqNo", ToString::to_string(&self.seq_no).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Persisted {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "seq_no",
            "seqNo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SeqNo,
            __SkipField__,
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
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Persisted;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Persisted")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Persisted, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut seq_no__ = None;
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
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Persisted {
                    seq_no: seq_no__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Persisted", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Plane {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Public => "PUBLIC",
            Self::Private => "PRIVATE",
            Self::Local => "LOCAL",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Plane {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "PUBLIC",
            "PRIVATE",
            "LOCAL",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Plane;

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
                    "PUBLIC" => Ok(Plane::Public),
                    "PRIVATE" => Ok(Plane::Private),
                    "LOCAL" => Ok(Plane::Local),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Recover {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ack_intents.is_empty() {
            len += 1;
        }
        if self.committed_close_clock != 0 {
            len += 1;
        }
        if self.committed_frontier.is_some() {
            len += 1;
        }
        if !self.connector_state_json.is_empty() {
            len += 1;
        }
        if self.hinted_close_clock != 0 {
            len += 1;
        }
        if self.hinted_frontier.is_some() {
            len += 1;
        }
        if !self.last_applied.is_empty() {
            len += 1;
        }
        if self.legacy_checkpoint.is_some() {
            len += 1;
        }
        if !self.max_keys.is_empty() {
            len += 1;
        }
        if !self.trigger_params_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Recover", len)?;
        if !self.ack_intents.is_empty() {
            let v: std::collections::HashMap<_, _> = self.ack_intents.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("ackIntents", &v)?;
        }
        if self.committed_close_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("committedCloseClock", ToString::to_string(&self.committed_close_clock).as_str())?;
        }
        if let Some(v) = self.committed_frontier.as_ref() {
            struct_ser.serialize_field("committedFrontier", v)?;
        }
        if !self.connector_state_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("connectorState", &crate::as_raw_json(&self.connector_state_json)?)?;
        }
        if self.hinted_close_clock != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hintedCloseClock", ToString::to_string(&self.hinted_close_clock).as_str())?;
        }
        if let Some(v) = self.hinted_frontier.as_ref() {
            struct_ser.serialize_field("hintedFrontier", v)?;
        }
        if !self.last_applied.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lastApplied", pbjson::private::base64::encode(&self.last_applied).as_str())?;
        }
        if let Some(v) = self.legacy_checkpoint.as_ref() {
            struct_ser.serialize_field("legacyCheckpoint", v)?;
        }
        if !self.max_keys.is_empty() {
            let v: std::collections::HashMap<_, _> = self.max_keys.iter()
                .map(|(k, v)| (k, pbjson::private::base64::encode(v))).collect();
            struct_ser.serialize_field("maxKeys", &v)?;
        }
        if !self.trigger_params_json.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("triggerParams", &crate::as_raw_json(&self.trigger_params_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Recover {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ack_intents",
            "ackIntents",
            "committed_close_clock",
            "committedCloseClock",
            "committed_frontier",
            "committedFrontier",
            "connector_state_json",
            "connectorState",
            "hinted_close_clock",
            "hintedCloseClock",
            "hinted_frontier",
            "hintedFrontier",
            "last_applied",
            "lastApplied",
            "legacy_checkpoint",
            "legacyCheckpoint",
            "max_keys",
            "maxKeys",
            "trigger_params_json",
            "triggerParams",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AckIntents,
            CommittedCloseClock,
            CommittedFrontier,
            ConnectorStateJson,
            HintedCloseClock,
            HintedFrontier,
            LastApplied,
            LegacyCheckpoint,
            MaxKeys,
            TriggerParamsJson,
            __SkipField__,
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
                            "ackIntents" | "ack_intents" => Ok(GeneratedField::AckIntents),
                            "committedCloseClock" | "committed_close_clock" => Ok(GeneratedField::CommittedCloseClock),
                            "committedFrontier" | "committed_frontier" => Ok(GeneratedField::CommittedFrontier),
                            "connectorState" | "connector_state_json" => Ok(GeneratedField::ConnectorStateJson),
                            "hintedCloseClock" | "hinted_close_clock" => Ok(GeneratedField::HintedCloseClock),
                            "hintedFrontier" | "hinted_frontier" => Ok(GeneratedField::HintedFrontier),
                            "lastApplied" | "last_applied" => Ok(GeneratedField::LastApplied),
                            "legacyCheckpoint" | "legacy_checkpoint" => Ok(GeneratedField::LegacyCheckpoint),
                            "maxKeys" | "max_keys" => Ok(GeneratedField::MaxKeys),
                            "triggerParams" | "trigger_params_json" => Ok(GeneratedField::TriggerParamsJson),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Recover;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Recover")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Recover, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ack_intents__ = None;
                let mut committed_close_clock__ = None;
                let mut committed_frontier__ = None;
                let mut connector_state_json__ = None;
                let mut hinted_close_clock__ = None;
                let mut hinted_frontier__ = None;
                let mut last_applied__ = None;
                let mut legacy_checkpoint__ = None;
                let mut max_keys__ = None;
                let mut trigger_params_json__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::AckIntents => {
                            if ack_intents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ackIntents"));
                            }
                            ack_intents__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k, v.0)).collect()
                            );
                        }
                        GeneratedField::CommittedCloseClock => {
                            if committed_close_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("committedCloseClock"));
                            }
                            committed_close_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::CommittedFrontier => {
                            if committed_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("committedFrontier"));
                            }
                            committed_frontier__ = map_.next_value()?;
                        }
                        GeneratedField::ConnectorStateJson => {
                            if connector_state_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectorState"));
                            }
                            connector_state_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::HintedCloseClock => {
                            if hinted_close_clock__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintedCloseClock"));
                            }
                            hinted_close_clock__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HintedFrontier => {
                            if hinted_frontier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hintedFrontier"));
                            }
                            hinted_frontier__ = map_.next_value()?;
                        }
                        GeneratedField::LastApplied => {
                            if last_applied__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastApplied"));
                            }
                            last_applied__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LegacyCheckpoint => {
                            if legacy_checkpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("legacyCheckpoint"));
                            }
                            legacy_checkpoint__ = map_.next_value()?;
                        }
                        GeneratedField::MaxKeys => {
                            if max_keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxKeys"));
                            }
                            max_keys__ = Some(
                                map_.next_value::<std::collections::BTreeMap<::pbjson::private::NumberDeserialize<u32>, ::pbjson::private::BytesDeserialize<_>>>()?
                                    .into_iter().map(|(k,v)| (k.0, v.0)).collect()
                            );
                        }
                        GeneratedField::TriggerParamsJson => {
                            if trigger_params_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("triggerParams"));
                            }
                            trigger_params_json__ =
                                Some(map_.next_value::<crate::RawJSONDeserialize>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Recover {
                    ack_intents: ack_intents__.unwrap_or_default(),
                    committed_close_clock: committed_close_clock__.unwrap_or_default(),
                    committed_frontier: committed_frontier__,
                    connector_state_json: connector_state_json__.unwrap_or_default(),
                    hinted_close_clock: hinted_close_clock__.unwrap_or_default(),
                    hinted_frontier: hinted_frontier__,
                    last_applied: last_applied__.unwrap_or_default(),
                    legacy_checkpoint: legacy_checkpoint__,
                    max_keys: max_keys__.unwrap_or_default(),
                    trigger_params_json: trigger_params_json__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Recover", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Reset {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Reset", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Reset {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Reset;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Reset")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Reset, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Reset {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Reset", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResetDone {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.ResetDone", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResetDone {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ResetDone;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.ResetDone")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ResetDone, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ResetDone {
                })
            }
        }
        deserializer.deserialize_struct("runtime.ResetDone", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RocksDbDescriptor {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.rocksdb_env_memptr != 0 {
            len += 1;
        }
        if !self.rocksdb_path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.RocksDBDescriptor", len)?;
        if self.rocksdb_env_memptr != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("rocksdbEnvMemptr", ToString::to_string(&self.rocksdb_env_memptr).as_str())?;
        }
        if !self.rocksdb_path.is_empty() {
            struct_ser.serialize_field("rocksdbPath", &self.rocksdb_path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RocksDbDescriptor {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rocksdb_env_memptr",
            "rocksdbEnvMemptr",
            "rocksdb_path",
            "rocksdbPath",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RocksdbEnvMemptr,
            RocksdbPath,
            __SkipField__,
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
                            "rocksdbEnvMemptr" | "rocksdb_env_memptr" => Ok(GeneratedField::RocksdbEnvMemptr),
                            "rocksdbPath" | "rocksdb_path" => Ok(GeneratedField::RocksdbPath),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RocksDbDescriptor;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.RocksDBDescriptor")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RocksDbDescriptor, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rocksdb_env_memptr__ = None;
                let mut rocksdb_path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RocksdbEnvMemptr => {
                            if rocksdb_env_memptr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbEnvMemptr"));
                            }
                            rocksdb_env_memptr__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RocksdbPath => {
                            if rocksdb_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbPath"));
                            }
                            rocksdb_path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(RocksDbDescriptor {
                    rocksdb_env_memptr: rocksdb_env_memptr__.unwrap_or_default(),
                    rocksdb_path: rocksdb_path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.RocksDBDescriptor", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SessionLoop {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.rocksdb_descriptor.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.SessionLoop", len)?;
        if let Some(v) = self.rocksdb_descriptor.as_ref() {
            struct_ser.serialize_field("rocksdbDescriptor", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SessionLoop {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rocksdb_descriptor",
            "rocksdbDescriptor",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RocksdbDescriptor,
            __SkipField__,
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
                            "rocksdbDescriptor" | "rocksdb_descriptor" => Ok(GeneratedField::RocksdbDescriptor),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SessionLoop;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.SessionLoop")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SessionLoop, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rocksdb_descriptor__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RocksdbDescriptor => {
                            if rocksdb_descriptor__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rocksdbDescriptor"));
                            }
                            rocksdb_descriptor__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SessionLoop {
                    rocksdb_descriptor: rocksdb_descriptor__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.SessionLoop", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShuffleRequest {
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
        if self.replay {
            len += 1;
        }
        if !self.build_id.is_empty() {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if self.end_offset != 0 {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if !self.coordinator.is_empty() {
            len += 1;
        }
        if self.resolution.is_some() {
            len += 1;
        }
        if self.shuffle_index != 0 {
            len += 1;
        }
        if self.derivation.is_some() {
            len += 1;
        }
        if self.materialization.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.ShuffleRequest", len)?;
        if !self.journal.is_empty() {
            struct_ser.serialize_field("journal", &self.journal)?;
        }
        if self.replay {
            struct_ser.serialize_field("replay", &self.replay)?;
        }
        if !self.build_id.is_empty() {
            struct_ser.serialize_field("buildId", &self.build_id)?;
        }
        if self.offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if self.end_offset != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("endOffset", ToString::to_string(&self.end_offset).as_str())?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if !self.coordinator.is_empty() {
            struct_ser.serialize_field("coordinator", &self.coordinator)?;
        }
        if let Some(v) = self.resolution.as_ref() {
            struct_ser.serialize_field("resolution", v)?;
        }
        if self.shuffle_index != 0 {
            struct_ser.serialize_field("shuffleIndex", &self.shuffle_index)?;
        }
        if let Some(v) = self.derivation.as_ref() {
            struct_ser.serialize_field("derivation", v)?;
        }
        if let Some(v) = self.materialization.as_ref() {
            struct_ser.serialize_field("materialization", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShuffleRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "journal",
            "replay",
            "build_id",
            "buildId",
            "offset",
            "end_offset",
            "endOffset",
            "range",
            "coordinator",
            "resolution",
            "shuffle_index",
            "shuffleIndex",
            "derivation",
            "materialization",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Journal,
            Replay,
            BuildId,
            Offset,
            EndOffset,
            Range,
            Coordinator,
            Resolution,
            ShuffleIndex,
            Derivation,
            Materialization,
            __SkipField__,
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
                            "replay" => Ok(GeneratedField::Replay),
                            "buildId" | "build_id" => Ok(GeneratedField::BuildId),
                            "offset" => Ok(GeneratedField::Offset),
                            "endOffset" | "end_offset" => Ok(GeneratedField::EndOffset),
                            "range" => Ok(GeneratedField::Range),
                            "coordinator" => Ok(GeneratedField::Coordinator),
                            "resolution" => Ok(GeneratedField::Resolution),
                            "shuffleIndex" | "shuffle_index" => Ok(GeneratedField::ShuffleIndex),
                            "derivation" => Ok(GeneratedField::Derivation),
                            "materialization" => Ok(GeneratedField::Materialization),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShuffleRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.ShuffleRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ShuffleRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut journal__ = None;
                let mut replay__ = None;
                let mut build_id__ = None;
                let mut offset__ = None;
                let mut end_offset__ = None;
                let mut range__ = None;
                let mut coordinator__ = None;
                let mut resolution__ = None;
                let mut shuffle_index__ = None;
                let mut derivation__ = None;
                let mut materialization__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Journal => {
                            if journal__.is_some() {
                                return Err(serde::de::Error::duplicate_field("journal"));
                            }
                            journal__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Replay => {
                            if replay__.is_some() {
                                return Err(serde::de::Error::duplicate_field("replay"));
                            }
                            replay__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BuildId => {
                            if build_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("buildId"));
                            }
                            build_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::EndOffset => {
                            if end_offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("endOffset"));
                            }
                            end_offset__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map_.next_value()?;
                        }
                        GeneratedField::Coordinator => {
                            if coordinator__.is_some() {
                                return Err(serde::de::Error::duplicate_field("coordinator"));
                            }
                            coordinator__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Resolution => {
                            if resolution__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resolution"));
                            }
                            resolution__ = map_.next_value()?;
                        }
                        GeneratedField::ShuffleIndex => {
                            if shuffle_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("shuffleIndex"));
                            }
                            shuffle_index__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Derivation => {
                            if derivation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("derivation"));
                            }
                            derivation__ = map_.next_value()?;
                        }
                        GeneratedField::Materialization => {
                            if materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialization"));
                            }
                            materialization__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ShuffleRequest {
                    journal: journal__.unwrap_or_default(),
                    replay: replay__.unwrap_or_default(),
                    build_id: build_id__.unwrap_or_default(),
                    offset: offset__.unwrap_or_default(),
                    end_offset: end_offset__.unwrap_or_default(),
                    range: range__,
                    coordinator: coordinator__.unwrap_or_default(),
                    resolution: resolution__,
                    shuffle_index: shuffle_index__.unwrap_or_default(),
                    derivation: derivation__,
                    materialization: materialization__,
                })
            }
        }
        deserializer.deserialize_struct("runtime.ShuffleRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShuffleResponse {
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
        if !self.terminal_error.is_empty() {
            len += 1;
        }
        if self.read_through != 0 {
            len += 1;
        }
        if self.write_head != 0 {
            len += 1;
        }
        if !self.arena.is_empty() {
            len += 1;
        }
        if !self.docs.is_empty() {
            len += 1;
        }
        if !self.offsets.is_empty() {
            len += 1;
        }
        if !self.uuid_parts.is_empty() {
            len += 1;
        }
        if !self.packed_key.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.ShuffleResponse", len)?;
        if self.status != 0 {
            let v = super::consumer::Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        if let Some(v) = self.header.as_ref() {
            struct_ser.serialize_field("header", v)?;
        }
        if !self.terminal_error.is_empty() {
            struct_ser.serialize_field("terminalError", &self.terminal_error)?;
        }
        if self.read_through != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("readThrough", ToString::to_string(&self.read_through).as_str())?;
        }
        if self.write_head != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("writeHead", ToString::to_string(&self.write_head).as_str())?;
        }
        if !self.arena.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("arena", pbjson::private::base64::encode(&self.arena).as_str())?;
        }
        if !self.docs.is_empty() {
            struct_ser.serialize_field("docs", &self.docs)?;
        }
        if !self.offsets.is_empty() {
            struct_ser.serialize_field("offsets", &self.offsets.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if !self.uuid_parts.is_empty() {
            struct_ser.serialize_field("uuidParts", &self.uuid_parts)?;
        }
        if !self.packed_key.is_empty() {
            struct_ser.serialize_field("packedKey", &self.packed_key)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShuffleResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "header",
            "terminal_error",
            "terminalError",
            "read_through",
            "readThrough",
            "write_head",
            "writeHead",
            "arena",
            "docs",
            "offsets",
            "uuid_parts",
            "uuidParts",
            "packed_key",
            "packedKey",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Header,
            TerminalError,
            ReadThrough,
            WriteHead,
            Arena,
            Docs,
            Offsets,
            UuidParts,
            PackedKey,
            __SkipField__,
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
                            "terminalError" | "terminal_error" => Ok(GeneratedField::TerminalError),
                            "readThrough" | "read_through" => Ok(GeneratedField::ReadThrough),
                            "writeHead" | "write_head" => Ok(GeneratedField::WriteHead),
                            "arena" => Ok(GeneratedField::Arena),
                            "docs" => Ok(GeneratedField::Docs),
                            "offsets" => Ok(GeneratedField::Offsets),
                            "uuidParts" | "uuid_parts" => Ok(GeneratedField::UuidParts),
                            "packedKey" | "packed_key" => Ok(GeneratedField::PackedKey),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShuffleResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.ShuffleResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ShuffleResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut header__ = None;
                let mut terminal_error__ = None;
                let mut read_through__ = None;
                let mut write_head__ = None;
                let mut arena__ = None;
                let mut docs__ = None;
                let mut offsets__ = None;
                let mut uuid_parts__ = None;
                let mut packed_key__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<super::consumer::Status>()? as i32);
                        }
                        GeneratedField::Header => {
                            if header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("header"));
                            }
                            header__ = map_.next_value()?;
                        }
                        GeneratedField::TerminalError => {
                            if terminal_error__.is_some() {
                                return Err(serde::de::Error::duplicate_field("terminalError"));
                            }
                            terminal_error__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReadThrough => {
                            if read_through__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readThrough"));
                            }
                            read_through__ =
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
                        GeneratedField::Arena => {
                            if arena__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arena"));
                            }
                            arena__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Docs => {
                            if docs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("docs"));
                            }
                            docs__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Offsets => {
                            if offsets__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offsets"));
                            }
                            offsets__ =
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::UuidParts => {
                            if uuid_parts__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uuidParts"));
                            }
                            uuid_parts__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PackedKey => {
                            if packed_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packedKey"));
                            }
                            packed_key__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ShuffleResponse {
                    status: status__.unwrap_or_default(),
                    header: header__,
                    terminal_error: terminal_error__.unwrap_or_default(),
                    read_through: read_through__.unwrap_or_default(),
                    write_head: write_head__.unwrap_or_default(),
                    arena: arena__.unwrap_or_default(),
                    docs: docs__.unwrap_or_default(),
                    offsets: offsets__.unwrap_or_default(),
                    uuid_parts: uuid_parts__.unwrap_or_default(),
                    packed_key: packed_key__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.ShuffleResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Stop {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Stop", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Stop {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Stop;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Stop")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Stop, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Stop {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Stop", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Stopped {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("runtime.Stopped", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Stopped {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            __SkipField__,
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
                            Ok(GeneratedField::__SkipField__)
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Stopped;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.Stopped")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Stopped, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Stopped {
                })
            }
        }
        deserializer.deserialize_struct("runtime.Stopped", FIELDS, GeneratedVisitor)
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
        if !self.spec.is_empty() {
            len += 1;
        }
        if self.max_transactions != 0 {
            len += 1;
        }
        if !self.sqlite_vfs_uri.is_empty() {
            len += 1;
        }
        if !self.publisher_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.Task", len)?;
        if !self.spec.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("spec", pbjson::private::base64::encode(&self.spec).as_str())?;
        }
        if self.max_transactions != 0 {
            struct_ser.serialize_field("maxTransactions", &self.max_transactions)?;
        }
        if !self.sqlite_vfs_uri.is_empty() {
            struct_ser.serialize_field("sqliteVfsUri", &self.sqlite_vfs_uri)?;
        }
        if !self.publisher_id.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("publisherId", pbjson::private::base64::encode(&self.publisher_id).as_str())?;
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
            "spec",
            "max_transactions",
            "maxTransactions",
            "sqlite_vfs_uri",
            "sqliteVfsUri",
            "publisher_id",
            "publisherId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            MaxTransactions,
            SqliteVfsUri,
            PublisherId,
            __SkipField__,
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
                            "maxTransactions" | "max_transactions" => Ok(GeneratedField::MaxTransactions),
                            "sqliteVfsUri" | "sqlite_vfs_uri" => Ok(GeneratedField::SqliteVfsUri),
                            "publisherId" | "publisher_id" => Ok(GeneratedField::PublisherId),
                            _ => Ok(GeneratedField::__SkipField__),
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
                formatter.write_str("struct runtime.Task")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Task, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut max_transactions__ = None;
                let mut sqlite_vfs_uri__ = None;
                let mut publisher_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxTransactions => {
                            if max_transactions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxTransactions"));
                            }
                            max_transactions__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SqliteVfsUri => {
                            if sqlite_vfs_uri__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sqliteVfsUri"));
                            }
                            sqlite_vfs_uri__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PublisherId => {
                            if publisher_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("publisherId"));
                            }
                            publisher_id__ =
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Task {
                    spec: spec__.unwrap_or_default(),
                    max_transactions: max_transactions__.unwrap_or_default(),
                    sqlite_vfs_uri: sqlite_vfs_uri__.unwrap_or_default(),
                    publisher_id: publisher_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.Task", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskServiceConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.log_file_fd != 0 {
            len += 1;
        }
        if !self.task_name.is_empty() {
            len += 1;
        }
        if !self.uds_path.is_empty() {
            len += 1;
        }
        if !self.container_network.is_empty() {
            len += 1;
        }
        if self.plane != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("runtime.TaskServiceConfig", len)?;
        if self.log_file_fd != 0 {
            struct_ser.serialize_field("logFileFd", &self.log_file_fd)?;
        }
        if !self.task_name.is_empty() {
            struct_ser.serialize_field("taskName", &self.task_name)?;
        }
        if !self.uds_path.is_empty() {
            struct_ser.serialize_field("udsPath", &self.uds_path)?;
        }
        if !self.container_network.is_empty() {
            struct_ser.serialize_field("containerNetwork", &self.container_network)?;
        }
        if self.plane != 0 {
            let v = Plane::try_from(self.plane)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.plane)))?;
            struct_ser.serialize_field("plane", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskServiceConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log_file_fd",
            "logFileFd",
            "task_name",
            "taskName",
            "uds_path",
            "udsPath",
            "container_network",
            "containerNetwork",
            "plane",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogFileFd,
            TaskName,
            UdsPath,
            ContainerNetwork,
            Plane,
            __SkipField__,
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
                            "logFileFd" | "log_file_fd" => Ok(GeneratedField::LogFileFd),
                            "taskName" | "task_name" => Ok(GeneratedField::TaskName),
                            "udsPath" | "uds_path" => Ok(GeneratedField::UdsPath),
                            "containerNetwork" | "container_network" => Ok(GeneratedField::ContainerNetwork),
                            "plane" => Ok(GeneratedField::Plane),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TaskServiceConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct runtime.TaskServiceConfig")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TaskServiceConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut log_file_fd__ = None;
                let mut task_name__ = None;
                let mut uds_path__ = None;
                let mut container_network__ = None;
                let mut plane__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogFileFd => {
                            if log_file_fd__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logFileFd"));
                            }
                            log_file_fd__ =
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::TaskName => {
                            if task_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskName"));
                            }
                            task_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UdsPath => {
                            if uds_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udsPath"));
                            }
                            uds_path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ContainerNetwork => {
                            if container_network__.is_some() {
                                return Err(serde::de::Error::duplicate_field("containerNetwork"));
                            }
                            container_network__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Plane => {
                            if plane__.is_some() {
                                return Err(serde::de::Error::duplicate_field("plane"));
                            }
                            plane__ = Some(map_.next_value::<Plane>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TaskServiceConfig {
                    log_file_fd: log_file_fd__.unwrap_or_default(),
                    task_name: task_name__.unwrap_or_default(),
                    uds_path: uds_path__.unwrap_or_default(),
                    container_network: container_network__.unwrap_or_default(),
                    plane: plane__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("runtime.TaskServiceConfig", FIELDS, GeneratedVisitor)
    }
}
