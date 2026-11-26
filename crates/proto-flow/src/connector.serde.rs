impl serde::Serialize for Request {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if !self.chunk.is_empty() {
            len += 1;
        }
        if self.remaining != 0 {
            len += 1;
        }
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("connector.Request", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if !self.chunk.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field(
                "chunk",
                pbjson::private::base64::encode(&self.chunk).as_str(),
            )?;
        }
        if self.remaining != 0 {
            struct_ser.serialize_field("remaining", &self.remaining)?;
        }
        if let Some(v) = self.kind.as_ref() {
            match v {
                request::Kind::Capture(v) => {
                    struct_ser.serialize_field("capture", v)?;
                }
                request::Kind::Derive(v) => {
                    struct_ser.serialize_field("derive", v)?;
                }
                request::Kind::Materialize(v) => {
                    struct_ser.serialize_field("materialize", v)?;
                }
            }
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
            "start",
            "chunk",
            "remaining",
            "capture",
            "derive",
            "materialize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            Chunk,
            Remaining,
            Capture,
            Derive,
            Materialize,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "chunk" => Ok(GeneratedField::Chunk),
                            "remaining" => Ok(GeneratedField::Remaining),
                            "capture" => Ok(GeneratedField::Capture),
                            "derive" => Ok(GeneratedField::Derive),
                            "materialize" => Ok(GeneratedField::Materialize),
                            _ => Ok(GeneratedField::__SkipField__),
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
                formatter.write_str("struct connector.Request")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Request, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut chunk__ = None;
                let mut remaining__ = None;
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::Chunk => {
                            if chunk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("chunk"));
                            }
                            chunk__ = Some(
                                map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?
                                    .0,
                            );
                        }
                        GeneratedField::Remaining => {
                            if remaining__.is_some() {
                                return Err(serde::de::Error::duplicate_field("remaining"));
                            }
                            remaining__ = Some(
                                map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?
                                    .0,
                            );
                        }
                        GeneratedField::Capture => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("capture"));
                                }
                                kind__ = Some(request::Kind::Capture(v));
                            }
                        }
                        GeneratedField::Derive => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("derive"));
                                }
                                kind__ = Some(request::Kind::Derive(v));
                            }
                        }
                        GeneratedField::Materialize => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("materialize"));
                                }
                                kind__ = Some(request::Kind::Materialize(v));
                            }
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Request {
                    start: start__,
                    chunk: chunk__.unwrap_or_default(),
                    remaining: remaining__.unwrap_or_default(),
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("connector.Request", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Start {
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
        if !self.sqlite_vfs_uri.is_empty() {
            len += 1;
        }
        if self.volume.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("connector.Request.Start", len)?;
        if self.log_level != 0 {
            let v = super::ops::log::Level::try_from(self.log_level).map_err(|_| {
                serde::ser::Error::custom(format!("Invalid variant {}", self.log_level))
            })?;
            struct_ser.serialize_field("logLevel", &v)?;
        }
        if !self.sqlite_vfs_uri.is_empty() {
            struct_ser.serialize_field("sqliteVfsUri", &self.sqlite_vfs_uri)?;
        }
        if let Some(v) = self.volume.as_ref() {
            match v {
                request::start::Volume::HostDir(v) => {
                    struct_ser.serialize_field("hostDir", v)?;
                }
                request::start::Volume::Ephemeral(v) => {
                    struct_ser.serialize_field("ephemeral", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Start {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "log_level",
            "logLevel",
            "sqlite_vfs_uri",
            "sqliteVfsUri",
            "host_dir",
            "hostDir",
            "ephemeral",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogLevel,
            SqliteVfsUri,
            HostDir,
            Ephemeral,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "logLevel" | "log_level" => Ok(GeneratedField::LogLevel),
                            "sqliteVfsUri" | "sqlite_vfs_uri" => Ok(GeneratedField::SqliteVfsUri),
                            "hostDir" | "host_dir" => Ok(GeneratedField::HostDir),
                            "ephemeral" => Ok(GeneratedField::Ephemeral),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Start;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct connector.Request.Start")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Start, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut log_level__ = None;
                let mut sqlite_vfs_uri__ = None;
                let mut volume__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogLevel => {
                            if log_level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logLevel"));
                            }
                            log_level__ = Some(map_.next_value::<super::ops::log::Level>()? as i32);
                        }
                        GeneratedField::SqliteVfsUri => {
                            if sqlite_vfs_uri__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sqliteVfsUri"));
                            }
                            sqlite_vfs_uri__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HostDir => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if volume__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("hostDir"));
                                }
                                volume__ = Some(request::start::Volume::HostDir(v));
                            }
                        }
                        GeneratedField::Ephemeral => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if volume__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("ephemeral"));
                                }
                                volume__ = Some(request::start::Volume::Ephemeral(v));
                            }
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(request::Start {
                    log_level: log_level__.unwrap_or_default(),
                    sqlite_vfs_uri: sqlite_vfs_uri__.unwrap_or_default(),
                    volume: volume__,
                })
            }
        }
        deserializer.deserialize_struct("connector.Request.Start", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::start::Ephemeral {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("connector.Request.Start.Ephemeral", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::start::Ephemeral {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[];

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

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
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
            type Value = request::start::Ephemeral;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct connector.Request.Start.Ephemeral")
            }

            fn visit_map<V>(
                self,
                mut map_: V,
            ) -> std::result::Result<request::start::Ephemeral, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(request::start::Ephemeral {})
            }
        }
        deserializer.deserialize_struct(
            "connector.Request.Start.Ephemeral",
            FIELDS,
            GeneratedVisitor,
        )
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
        if !self.chunk.is_empty() {
            len += 1;
        }
        if self.remaining != 0 {
            len += 1;
        }
        if self.kind.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("connector.Response", len)?;
        if !self.chunk.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field(
                "chunk",
                pbjson::private::base64::encode(&self.chunk).as_str(),
            )?;
        }
        if self.remaining != 0 {
            struct_ser.serialize_field("remaining", &self.remaining)?;
        }
        if let Some(v) = self.kind.as_ref() {
            match v {
                response::Kind::Started(v) => {
                    struct_ser.serialize_field("started", v)?;
                }
                response::Kind::Log(v) => {
                    struct_ser.serialize_field("log", v)?;
                }
                response::Kind::Capture(v) => {
                    struct_ser.serialize_field("capture", v)?;
                }
                response::Kind::Derive(v) => {
                    struct_ser.serialize_field("derive", v)?;
                }
                response::Kind::Materialize(v) => {
                    struct_ser.serialize_field("materialize", v)?;
                }
            }
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
            "chunk",
            "remaining",
            "started",
            "log",
            "capture",
            "derive",
            "materialize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Chunk,
            Remaining,
            Started,
            Log,
            Capture,
            Derive,
            Materialize,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "chunk" => Ok(GeneratedField::Chunk),
                            "remaining" => Ok(GeneratedField::Remaining),
                            "started" => Ok(GeneratedField::Started),
                            "log" => Ok(GeneratedField::Log),
                            "capture" => Ok(GeneratedField::Capture),
                            "derive" => Ok(GeneratedField::Derive),
                            "materialize" => Ok(GeneratedField::Materialize),
                            _ => Ok(GeneratedField::__SkipField__),
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
                formatter.write_str("struct connector.Response")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Response, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut chunk__ = None;
                let mut remaining__ = None;
                let mut kind__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Chunk => {
                            if chunk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("chunk"));
                            }
                            chunk__ = Some(
                                map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?
                                    .0,
                            );
                        }
                        GeneratedField::Remaining => {
                            if remaining__.is_some() {
                                return Err(serde::de::Error::duplicate_field("remaining"));
                            }
                            remaining__ = Some(
                                map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?
                                    .0,
                            );
                        }
                        GeneratedField::Started => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("started"));
                                }
                                kind__ = Some(response::Kind::Started(v));
                            }
                        }
                        GeneratedField::Log => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("log"));
                                }
                                kind__ = Some(response::Kind::Log(v));
                            }
                        }
                        GeneratedField::Capture => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("capture"));
                                }
                                kind__ = Some(response::Kind::Capture(v));
                            }
                        }
                        GeneratedField::Derive => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("derive"));
                                }
                                kind__ = Some(response::Kind::Derive(v));
                            }
                        }
                        GeneratedField::Materialize => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if kind__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("materialize"));
                                }
                                kind__ = Some(response::Kind::Materialize(v));
                            }
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Response {
                    chunk: chunk__.unwrap_or_default(),
                    remaining: remaining__.unwrap_or_default(),
                    kind: kind__,
                })
            }
        }
        deserializer.deserialize_struct("connector.Response", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Started {
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
        if self.codec != 0 {
            len += 1;
        }
        if self.token_restart_at.is_some() {
            len += 1;
        }
        if self.process.is_some() {
            len += 1;
        }
        if self.spec.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("connector.Response.Started", len)?;
        if let Some(v) = self.container.as_ref() {
            struct_ser.serialize_field("container", v)?;
        }
        if self.codec != 0 {
            let v = response::started::Codec::try_from(self.codec).map_err(|_| {
                serde::ser::Error::custom(format!("Invalid variant {}", self.codec))
            })?;
            struct_ser.serialize_field("codec", &v)?;
        }
        if let Some(v) = self.token_restart_at.as_ref() {
            struct_ser.serialize_field("tokenRestartAt", v)?;
        }
        if let Some(v) = self.process.as_ref() {
            struct_ser.serialize_field("process", v)?;
        }
        if let Some(v) = self.spec.as_ref() {
            match v {
                response::started::Spec::Capture(v) => {
                    struct_ser.serialize_field("capture", v)?;
                }
                response::started::Spec::Derive(v) => {
                    struct_ser.serialize_field("derive", v)?;
                }
                response::started::Spec::Materialize(v) => {
                    struct_ser.serialize_field("materialize", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Started {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "container",
            "codec",
            "token_restart_at",
            "tokenRestartAt",
            "process",
            "capture",
            "derive",
            "materialize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Container,
            Codec,
            TokenRestartAt,
            Process,
            Capture,
            Derive,
            Materialize,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "container" => Ok(GeneratedField::Container),
                            "codec" => Ok(GeneratedField::Codec),
                            "tokenRestartAt" | "token_restart_at" => {
                                Ok(GeneratedField::TokenRestartAt)
                            }
                            "process" => Ok(GeneratedField::Process),
                            "capture" => Ok(GeneratedField::Capture),
                            "derive" => Ok(GeneratedField::Derive),
                            "materialize" => Ok(GeneratedField::Materialize),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::Started;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct connector.Response.Started")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Started, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut container__ = None;
                let mut codec__ = None;
                let mut token_restart_at__ = None;
                let mut process__ = None;
                let mut spec__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Container => {
                            if container__.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container__ = map_.next_value()?;
                        }
                        GeneratedField::Codec => {
                            if codec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("codec"));
                            }
                            codec__ = Some(map_.next_value::<response::started::Codec>()? as i32);
                        }
                        GeneratedField::TokenRestartAt => {
                            if token_restart_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tokenRestartAt"));
                            }
                            token_restart_at__ = map_.next_value()?;
                        }
                        GeneratedField::Process => {
                            if process__.is_some() {
                                return Err(serde::de::Error::duplicate_field("process"));
                            }
                            process__ = map_.next_value()?;
                        }
                        GeneratedField::Capture => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if spec__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("capture"));
                                }
                                spec__ = Some(response::started::Spec::Capture(v));
                            }
                        }
                        GeneratedField::Derive => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if spec__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("derive"));
                                }
                                spec__ = Some(response::started::Spec::Derive(v));
                            }
                        }
                        GeneratedField::Materialize => {
                            if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {
                                if spec__.is_some() {
                                    return Err(serde::de::Error::duplicate_field("materialize"));
                                }
                                spec__ = Some(response::started::Spec::Materialize(v));
                            }
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(response::Started {
                    container: container__,
                    codec: codec__.unwrap_or_default(),
                    token_restart_at: token_restart_at__,
                    process: process__,
                    spec: spec__,
                })
            }
        }
        deserializer.deserialize_struct("connector.Response.Started", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::started::Codec {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Proto => "PROTO",
            Self::Json => "JSON",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for response::started::Codec {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["INVALID", "PROTO", "JSON"];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = response::started::Codec;

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
                    "INVALID" => Ok(response::started::Codec::Invalid),
                    "PROTO" => Ok(response::started::Codec::Proto),
                    "JSON" => Ok(response::started::Codec::Json),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
