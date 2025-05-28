impl serde::Serialize for Extra {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("materialize.Extra", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Extra {
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
            type Value = Extra;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Extra")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Extra, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Extra {
                })
            }
        }
        deserializer.deserialize_struct("materialize.Extra", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for extra::ValidateBindingAgainstConstraints {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.binding.is_some() {
            len += 1;
        }
        if !self.constraints.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Extra.ValidateBindingAgainstConstraints", len)?;
        if let Some(v) = self.binding.as_ref() {
            struct_ser.serialize_field("binding", v)?;
        }
        if !self.constraints.is_empty() {
            struct_ser.serialize_field("constraints", &self.constraints)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for extra::ValidateBindingAgainstConstraints {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "constraints",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            Constraints,
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
                            "constraints" => Ok(GeneratedField::Constraints),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = extra::ValidateBindingAgainstConstraints;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Extra.ValidateBindingAgainstConstraints")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<extra::ValidateBindingAgainstConstraints, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut constraints__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Binding => {
                            if binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binding"));
                            }
                            binding__ = map_.next_value()?;
                        }
                        GeneratedField::Constraints => {
                            if constraints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraints"));
                            }
                            constraints__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(extra::ValidateBindingAgainstConstraints {
                    binding: binding__,
                    constraints: constraints__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Extra.ValidateBindingAgainstConstraints", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for extra::ValidateExistingProjectionRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.existing_binding.is_some() {
            len += 1;
        }
        if self.proposed_binding.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Extra.ValidateExistingProjectionRequest", len)?;
        if let Some(v) = self.existing_binding.as_ref() {
            struct_ser.serialize_field("existingBinding", v)?;
        }
        if let Some(v) = self.proposed_binding.as_ref() {
            struct_ser.serialize_field("proposedBinding", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for extra::ValidateExistingProjectionRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "existing_binding",
            "existingBinding",
            "proposed_binding",
            "proposedBinding",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExistingBinding,
            ProposedBinding,
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
                            "existingBinding" | "existing_binding" => Ok(GeneratedField::ExistingBinding),
                            "proposedBinding" | "proposed_binding" => Ok(GeneratedField::ProposedBinding),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = extra::ValidateExistingProjectionRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Extra.ValidateExistingProjectionRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<extra::ValidateExistingProjectionRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut existing_binding__ = None;
                let mut proposed_binding__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ExistingBinding => {
                            if existing_binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("existingBinding"));
                            }
                            existing_binding__ = map_.next_value()?;
                        }
                        GeneratedField::ProposedBinding => {
                            if proposed_binding__.is_some() {
                                return Err(serde::de::Error::duplicate_field("proposedBinding"));
                            }
                            proposed_binding__ = map_.next_value()?;
                        }
                    }
                }
                Ok(extra::ValidateExistingProjectionRequest {
                    existing_binding: existing_binding__,
                    proposed_binding: proposed_binding__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Extra.ValidateExistingProjectionRequest", FIELDS, GeneratedVisitor)
    }
}
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
        if self.validate.is_some() {
            len += 1;
        }
        if self.apply.is_some() {
            len += 1;
        }
        if self.open.is_some() {
            len += 1;
        }
        if self.load.is_some() {
            len += 1;
        }
        if self.flush.is_some() {
            len += 1;
        }
        if self.store.is_some() {
            len += 1;
        }
        if self.start_commit.is_some() {
            len += 1;
        }
        if self.acknowledge.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
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
        if let Some(v) = self.load.as_ref() {
            struct_ser.serialize_field("load", v)?;
        }
        if let Some(v) = self.flush.as_ref() {
            struct_ser.serialize_field("flush", v)?;
        }
        if let Some(v) = self.store.as_ref() {
            struct_ser.serialize_field("store", v)?;
        }
        if let Some(v) = self.start_commit.as_ref() {
            struct_ser.serialize_field("startCommit", v)?;
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
            "validate",
            "apply",
            "open",
            "load",
            "flush",
            "store",
            "start_commit",
            "startCommit",
            "acknowledge",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Validate,
            Apply,
            Open,
            Load,
            Flush,
            Store,
            StartCommit,
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
                            "validate" => Ok(GeneratedField::Validate),
                            "apply" => Ok(GeneratedField::Apply),
                            "open" => Ok(GeneratedField::Open),
                            "load" => Ok(GeneratedField::Load),
                            "flush" => Ok(GeneratedField::Flush),
                            "store" => Ok(GeneratedField::Store),
                            "startCommit" | "start_commit" => Ok(GeneratedField::StartCommit),
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
                formatter.write_str("struct materialize.Request")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Request, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut validate__ = None;
                let mut apply__ = None;
                let mut open__ = None;
                let mut load__ = None;
                let mut flush__ = None;
                let mut store__ = None;
                let mut start_commit__ = None;
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
                        GeneratedField::Load => {
                            if load__.is_some() {
                                return Err(serde::de::Error::duplicate_field("load"));
                            }
                            load__ = map_.next_value()?;
                        }
                        GeneratedField::Flush => {
                            if flush__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flush"));
                            }
                            flush__ = map_.next_value()?;
                        }
                        GeneratedField::Store => {
                            if store__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store"));
                            }
                            store__ = map_.next_value()?;
                        }
                        GeneratedField::StartCommit => {
                            if start_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startCommit"));
                            }
                            start_commit__ = map_.next_value()?;
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
                    validate: validate__,
                    apply: apply__,
                    open: open__,
                    load: load__,
                    flush: flush__,
                    store: store__,
                    start_commit: start_commit__,
                    acknowledge: acknowledge__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Acknowledge {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("materialize.Request.Acknowledge", len)?;
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
            type Value = request::Acknowledge;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.Acknowledge")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Acknowledge, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(request::Acknowledge {
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Acknowledge", FIELDS, GeneratedVisitor)
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
        if self.materialization.is_some() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if self.last_materialization.is_some() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        if !self.state_json.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Apply", len)?;
        if let Some(v) = self.materialization.as_ref() {
            struct_ser.serialize_field("materialization", v)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if let Some(v) = self.last_materialization.as_ref() {
            struct_ser.serialize_field("lastMaterialization", v)?;
        }
        if !self.last_version.is_empty() {
            struct_ser.serialize_field("lastVersion", &self.last_version)?;
        }
        if !self.state_json.is_empty() {
            struct_ser.serialize_field("state", crate::as_raw_json(&self.state_json)?)?;
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
            "materialization",
            "version",
            "last_materialization",
            "lastMaterialization",
            "last_version",
            "lastVersion",
            "state_json",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Materialization,
            Version,
            LastMaterialization,
            LastVersion,
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
                            "materialization" => Ok(GeneratedField::Materialization),
                            "version" => Ok(GeneratedField::Version),
                            "lastMaterialization" | "last_materialization" => Ok(GeneratedField::LastMaterialization),
                            "lastVersion" | "last_version" => Ok(GeneratedField::LastVersion),
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
            type Value = request::Apply;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.Apply")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Apply, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut materialization__ = None;
                let mut version__ = None;
                let mut last_materialization__ = None;
                let mut last_version__ = None;
                let mut state_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Materialization => {
                            if materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialization"));
                            }
                            materialization__ = map_.next_value()?;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LastMaterialization => {
                            if last_materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastMaterialization"));
                            }
                            last_materialization__ = map_.next_value()?;
                        }
                        GeneratedField::LastVersion => {
                            if last_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastVersion"));
                            }
                            last_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::StateJson => {
                            if state_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state_json__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Apply {
                    materialization: materialization__,
                    version: version__.unwrap_or_default(),
                    last_materialization: last_materialization__,
                    last_version: last_version__.unwrap_or_default(),
                    state_json: state_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Apply", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Flush {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("materialize.Request.Flush", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Flush {
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
            type Value = request::Flush;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.Flush")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Flush, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(request::Flush {
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Flush", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Load {
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
        if !self.key_json.is_empty() {
            len += 1;
        }
        if !self.key_packed.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Load", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.key_json.is_empty() {
            struct_ser.serialize_field("key", crate::as_raw_json(&self.key_json)?)?;
        }
        if !self.key_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("keyPacked", pbjson::private::base64::encode(&self.key_packed).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Load {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "key_json",
            "key",
            "key_packed",
            "keyPacked",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            KeyJson,
            KeyPacked,
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
                            "key" | "key_json" => Ok(GeneratedField::KeyJson),
                            "keyPacked" | "key_packed" => Ok(GeneratedField::KeyPacked),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::Load;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.Load")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Load, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut key_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut key_packed__ = None;
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
                        GeneratedField::KeyJson => {
                            if key_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::KeyPacked => {
                            if key_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPacked"));
                            }
                            key_packed__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(request::Load {
                    binding: binding__.unwrap_or_default(),
                    key_json: key_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    key_packed: key_packed__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Load", FIELDS, GeneratedVisitor)
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
        if self.materialization.is_some() {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Open", len)?;
        if let Some(v) = self.materialization.as_ref() {
            struct_ser.serialize_field("materialization", v)?;
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
            "materialization",
            "version",
            "range",
            "state_json",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Materialization,
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
                            "materialization" => Ok(GeneratedField::Materialization),
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
                formatter.write_str("struct materialize.Request.Open")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Open, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut materialization__ = None;
                let mut version__ = None;
                let mut range__ = None;
                let mut state_json__ : Option<Box<serde_json::value::RawValue>> = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Materialization => {
                            if materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialization"));
                            }
                            materialization__ = map_.next_value()?;
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
                    materialization: materialization__,
                    version: version__.unwrap_or_default(),
                    range: range__,
                    state_json: state_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Open", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Spec", len)?;
        if self.connector_type != 0 {
            let v = super::flow::materialization_spec::ConnectorType::try_from(self.connector_type)
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
                formatter.write_str("struct materialize.Request.Spec")
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
                            connector_type__ = Some(map_.next_value::<super::flow::materialization_spec::ConnectorType>()? as i32);
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
        deserializer.deserialize_struct("materialize.Request.Spec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::StartCommit {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Request.StartCommit", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::StartCommit {
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
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = request::StartCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.StartCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::StartCommit, V::Error>
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
                    }
                }
                Ok(request::StartCommit {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.StartCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for request::Store {
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
        if !self.key_json.is_empty() {
            len += 1;
        }
        if !self.key_packed.is_empty() {
            len += 1;
        }
        if !self.values_json.is_empty() {
            len += 1;
        }
        if !self.values_packed.is_empty() {
            len += 1;
        }
        if !self.doc_json.is_empty() {
            len += 1;
        }
        if self.exists {
            len += 1;
        }
        if self.delete {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Store", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.key_json.is_empty() {
            struct_ser.serialize_field("key", crate::as_raw_json(&self.key_json)?)?;
        }
        if !self.key_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("keyPacked", pbjson::private::base64::encode(&self.key_packed).as_str())?;
        }
        if !self.values_json.is_empty() {
            struct_ser.serialize_field("values", crate::as_raw_json(&self.values_json)?)?;
        }
        if !self.values_packed.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("valuesPacked", pbjson::private::base64::encode(&self.values_packed).as_str())?;
        }
        if !self.doc_json.is_empty() {
            struct_ser.serialize_field("doc", crate::as_raw_json(&self.doc_json)?)?;
        }
        if self.exists {
            struct_ser.serialize_field("exists", &self.exists)?;
        }
        if self.delete {
            struct_ser.serialize_field("delete", &self.delete)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for request::Store {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "binding",
            "key_json",
            "key",
            "key_packed",
            "keyPacked",
            "values_json",
            "values",
            "values_packed",
            "valuesPacked",
            "doc_json",
            "doc",
            "exists",
            "delete",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Binding,
            KeyJson,
            KeyPacked,
            ValuesJson,
            ValuesPacked,
            DocJson,
            Exists,
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
                            "binding" => Ok(GeneratedField::Binding),
                            "key" | "key_json" => Ok(GeneratedField::KeyJson),
                            "keyPacked" | "key_packed" => Ok(GeneratedField::KeyPacked),
                            "values" | "values_json" => Ok(GeneratedField::ValuesJson),
                            "valuesPacked" | "values_packed" => Ok(GeneratedField::ValuesPacked),
                            "doc" | "doc_json" => Ok(GeneratedField::DocJson),
                            "exists" => Ok(GeneratedField::Exists),
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
            type Value = request::Store;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Request.Store")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Store, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut binding__ = None;
                let mut key_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut key_packed__ = None;
                let mut values_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut values_packed__ = None;
                let mut doc_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut exists__ = None;
                let mut delete__ = None;
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
                        GeneratedField::KeyJson => {
                            if key_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::KeyPacked => {
                            if key_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyPacked"));
                            }
                            key_packed__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ValuesJson => {
                            if values_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ValuesPacked => {
                            if values_packed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valuesPacked"));
                            }
                            values_packed__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DocJson => {
                            if doc_json__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doc"));
                            }
                            doc_json__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Exists => {
                            if exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exists"));
                            }
                            exists__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Delete => {
                            if delete__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delete"));
                            }
                            delete__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(request::Store {
                    binding: binding__.unwrap_or_default(),
                    key_json: key_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    key_packed: key_packed__.unwrap_or_default(),
                    values_json: values_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    values_packed: values_packed__.unwrap_or_default(),
                    doc_json: doc_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    exists: exists__.unwrap_or_default(),
                    delete: delete__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Store", FIELDS, GeneratedVisitor)
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
        if self.last_materialization.is_some() {
            len += 1;
        }
        if !self.last_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Validate", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.connector_type != 0 {
            let v = super::flow::materialization_spec::ConnectorType::try_from(self.connector_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.connector_type)))?;
            struct_ser.serialize_field("connectorType", &v)?;
        }
        if !self.config_json.is_empty() {
            struct_ser.serialize_field("config", crate::as_raw_json(&self.config_json)?)?;
        }
        if !self.bindings.is_empty() {
            struct_ser.serialize_field("bindings", &self.bindings)?;
        }
        if let Some(v) = self.last_materialization.as_ref() {
            struct_ser.serialize_field("lastMaterialization", v)?;
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
            "last_materialization",
            "lastMaterialization",
            "last_version",
            "lastVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ConnectorType,
            ConfigJson,
            Bindings,
            LastMaterialization,
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
                            "lastMaterialization" | "last_materialization" => Ok(GeneratedField::LastMaterialization),
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
                formatter.write_str("struct materialize.Request.Validate")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::Validate, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut connector_type__ = None;
                let mut config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut bindings__ = None;
                let mut last_materialization__ = None;
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
                            connector_type__ = Some(map_.next_value::<super::flow::materialization_spec::ConnectorType>()? as i32);
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
                        GeneratedField::LastMaterialization => {
                            if last_materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastMaterialization"));
                            }
                            last_materialization__ = map_.next_value()?;
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
                    last_materialization: last_materialization__,
                    last_version: last_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Validate", FIELDS, GeneratedVisitor)
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
        if !self.field_config_json_map.is_empty() {
            len += 1;
        }
        if self.backfill != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Request.Validate.Binding", len)?;
        if !self.resource_config_json.is_empty() {
            struct_ser.serialize_field("resourceConfig", crate::as_raw_json(&self.resource_config_json)?)?;
        }
        if let Some(v) = self.collection.as_ref() {
            struct_ser.serialize_field("collection", v)?;
        }
        if !self.field_config_json_map.is_empty() {
            struct_ser.serialize_field("fieldConfig", &crate::as_raw_json_map(&self.field_config_json_map)?)?;
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
            "field_config_json_map",
            "fieldConfig",
            "backfill",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ResourceConfigJson,
            Collection,
            FieldConfigJsonMap,
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
                            "fieldConfig" | "field_config_json_map" => Ok(GeneratedField::FieldConfigJsonMap),
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
                formatter.write_str("struct materialize.Request.Validate.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<request::validate::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource_config_json__ : Option<Box<serde_json::value::RawValue>> = None;
                let mut collection__ = None;
                let mut field_config_json_map__ : Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> = None;
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
                        GeneratedField::FieldConfigJsonMap => {
                            if field_config_json_map__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldConfig"));
                            }
                            field_config_json_map__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
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
                    field_config_json_map: field_config_json_map__.unwrap_or_default().into_iter().map(|(field, value)| (field, Box::<str>::from(value).into())).collect(),
                    backfill: backfill__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Request.Validate.Binding", FIELDS, GeneratedVisitor)
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
        if self.validated.is_some() {
            len += 1;
        }
        if self.applied.is_some() {
            len += 1;
        }
        if self.opened.is_some() {
            len += 1;
        }
        if self.loaded.is_some() {
            len += 1;
        }
        if self.flushed.is_some() {
            len += 1;
        }
        if self.started_commit.is_some() {
            len += 1;
        }
        if self.acknowledged.is_some() {
            len += 1;
        }
        if !self.internal.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Response", len)?;
        if let Some(v) = self.spec.as_ref() {
            struct_ser.serialize_field("spec", v)?;
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
        if let Some(v) = self.loaded.as_ref() {
            struct_ser.serialize_field("loaded", v)?;
        }
        if let Some(v) = self.flushed.as_ref() {
            struct_ser.serialize_field("flushed", v)?;
        }
        if let Some(v) = self.started_commit.as_ref() {
            struct_ser.serialize_field("startedCommit", v)?;
        }
        if let Some(v) = self.acknowledged.as_ref() {
            struct_ser.serialize_field("acknowledged", v)?;
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
            "validated",
            "applied",
            "opened",
            "loaded",
            "flushed",
            "started_commit",
            "startedCommit",
            "acknowledged",
            "internal",
            "$internal",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Spec,
            Validated,
            Applied,
            Opened,
            Loaded,
            Flushed,
            StartedCommit,
            Acknowledged,
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
                            "validated" => Ok(GeneratedField::Validated),
                            "applied" => Ok(GeneratedField::Applied),
                            "opened" => Ok(GeneratedField::Opened),
                            "loaded" => Ok(GeneratedField::Loaded),
                            "flushed" => Ok(GeneratedField::Flushed),
                            "startedCommit" | "started_commit" => Ok(GeneratedField::StartedCommit),
                            "acknowledged" => Ok(GeneratedField::Acknowledged),
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
                formatter.write_str("struct materialize.Response")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Response, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut spec__ = None;
                let mut validated__ = None;
                let mut applied__ = None;
                let mut opened__ = None;
                let mut loaded__ = None;
                let mut flushed__ = None;
                let mut started_commit__ = None;
                let mut acknowledged__ = None;
                let mut internal__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Spec => {
                            if spec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("spec"));
                            }
                            spec__ = map_.next_value()?;
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
                        GeneratedField::Loaded => {
                            if loaded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("loaded"));
                            }
                            loaded__ = map_.next_value()?;
                        }
                        GeneratedField::Flushed => {
                            if flushed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("flushed"));
                            }
                            flushed__ = map_.next_value()?;
                        }
                        GeneratedField::StartedCommit => {
                            if started_commit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startedCommit"));
                            }
                            started_commit__ = map_.next_value()?;
                        }
                        GeneratedField::Acknowledged => {
                            if acknowledged__.is_some() {
                                return Err(serde::de::Error::duplicate_field("acknowledged"));
                            }
                            acknowledged__ = map_.next_value()?;
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
                    validated: validated__,
                    applied: applied__,
                    opened: opened__,
                    loaded: loaded__,
                    flushed: flushed__,
                    started_commit: started_commit__,
                    acknowledged: acknowledged__,
                    internal: internal__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Acknowledged {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Acknowledged", len)?;
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Acknowledged {
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
            type Value = response::Acknowledged;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.Acknowledged")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Acknowledged, V::Error>
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
                Ok(response::Acknowledged {
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Acknowledged", FIELDS, GeneratedVisitor)
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
        if self.state.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Applied", len)?;
        if !self.action_description.is_empty() {
            struct_ser.serialize_field("actionDescription", &self.action_description)?;
        }
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
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
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ActionDescription,
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
                            "actionDescription" | "action_description" => Ok(GeneratedField::ActionDescription),
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
            type Value = response::Applied;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.Applied")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Applied, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut action_description__ = None;
                let mut state__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ActionDescription => {
                            if action_description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("actionDescription"));
                            }
                            action_description__ = Some(map_.next_value()?);
                        }
                        GeneratedField::State => {
                            if state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state__ = map_.next_value()?;
                        }
                    }
                }
                Ok(response::Applied {
                    action_description: action_description__.unwrap_or_default(),
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Applied", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Flushed {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Flushed", len)?;
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Flushed {
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
            type Value = response::Flushed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.Flushed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Flushed, V::Error>
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
                Ok(response::Flushed {
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Flushed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::Loaded {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Loaded", len)?;
        if self.binding != 0 {
            struct_ser.serialize_field("binding", &self.binding)?;
        }
        if !self.doc_json.is_empty() {
            struct_ser.serialize_field("doc", crate::as_raw_json(&self.doc_json)?)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::Loaded {
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
            type Value = response::Loaded;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.Loaded")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Loaded, V::Error>
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
                Ok(response::Loaded {
                    binding: binding__.unwrap_or_default(),
                    doc_json: doc_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Loaded", FIELDS, GeneratedVisitor)
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
        if self.runtime_checkpoint.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Opened", len)?;
        if let Some(v) = self.runtime_checkpoint.as_ref() {
            struct_ser.serialize_field("runtimeCheckpoint", v)?;
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
            "runtime_checkpoint",
            "runtimeCheckpoint",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RuntimeCheckpoint,
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
                formatter.write_str("struct materialize.Response.Opened")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::Opened, V::Error>
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
                    }
                }
                Ok(response::Opened {
                    runtime_checkpoint: runtime_checkpoint__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Opened", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Spec", len)?;
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
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Protocol,
            ConfigSchemaJson,
            ResourceConfigSchemaJson,
            DocumentationUrl,
            Oauth2,
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
                formatter.write_str("struct materialize.Response.Spec")
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
                    }
                }
                Ok(response::Spec {
                    protocol: protocol__.unwrap_or_default(),
                    config_schema_json: config_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    resource_config_schema_json: resource_config_schema_json__.map(|r| Box::<str>::from(r).into()).unwrap_or_default(),
                    documentation_url: documentation_url__.unwrap_or_default(),
                    oauth2: oauth2__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Spec", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::StartedCommit {
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.StartedCommit", len)?;
        if let Some(v) = self.state.as_ref() {
            struct_ser.serialize_field("state", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::StartedCommit {
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
            type Value = response::StartedCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.StartedCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::StartedCommit, V::Error>
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
                Ok(response::StartedCommit {
                    state: state__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.StartedCommit", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Validated", len)?;
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
                formatter.write_str("struct materialize.Response.Validated")
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
        deserializer.deserialize_struct("materialize.Response.Validated", FIELDS, GeneratedVisitor)
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
        if !self.constraints.is_empty() {
            len += 1;
        }
        if !self.resource_path.is_empty() {
            len += 1;
        }
        if self.delta_updates {
            len += 1;
        }
        if self.ser_policy.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Validated.Binding", len)?;
        if !self.constraints.is_empty() {
            struct_ser.serialize_field("constraints", &self.constraints)?;
        }
        if !self.resource_path.is_empty() {
            struct_ser.serialize_field("resourcePath", &self.resource_path)?;
        }
        if self.delta_updates {
            struct_ser.serialize_field("deltaUpdates", &self.delta_updates)?;
        }
        if let Some(v) = self.ser_policy.as_ref() {
            struct_ser.serialize_field("serPolicy", v)?;
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
            "constraints",
            "resource_path",
            "resourcePath",
            "delta_updates",
            "deltaUpdates",
            "ser_policy",
            "serPolicy",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Constraints,
            ResourcePath,
            DeltaUpdates,
            SerPolicy,
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
                            "constraints" => Ok(GeneratedField::Constraints),
                            "resourcePath" | "resource_path" => Ok(GeneratedField::ResourcePath),
                            "deltaUpdates" | "delta_updates" => Ok(GeneratedField::DeltaUpdates),
                            "serPolicy" | "ser_policy" => Ok(GeneratedField::SerPolicy),
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
                formatter.write_str("struct materialize.Response.Validated.Binding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::validated::Binding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut constraints__ = None;
                let mut resource_path__ = None;
                let mut delta_updates__ = None;
                let mut ser_policy__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Constraints => {
                            if constraints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraints"));
                            }
                            constraints__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::ResourcePath => {
                            if resource_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourcePath"));
                            }
                            resource_path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DeltaUpdates => {
                            if delta_updates__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deltaUpdates"));
                            }
                            delta_updates__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SerPolicy => {
                            if ser_policy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("serPolicy"));
                            }
                            ser_policy__ = map_.next_value()?;
                        }
                    }
                }
                Ok(response::validated::Binding {
                    constraints: constraints__.unwrap_or_default(),
                    resource_path: resource_path__.unwrap_or_default(),
                    delta_updates: delta_updates__.unwrap_or_default(),
                    ser_policy: ser_policy__,
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Validated.Binding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::validated::Constraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.r#type != 0 {
            len += 1;
        }
        if !self.reason.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("materialize.Response.Validated.Constraint", len)?;
        if self.r#type != 0 {
            let v = response::validated::constraint::Type::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if !self.reason.is_empty() {
            struct_ser.serialize_field("reason", &self.reason)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for response::validated::Constraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "reason",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Reason,
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
                            "type" => Ok(GeneratedField::Type),
                            "reason" => Ok(GeneratedField::Reason),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::validated::Constraint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct materialize.Response.Validated.Constraint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<response::validated::Constraint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut reason__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<response::validated::constraint::Type>()? as i32);
                        }
                        GeneratedField::Reason => {
                            if reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reason"));
                            }
                            reason__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(response::validated::Constraint {
                    r#type: r#type__.unwrap_or_default(),
                    reason: reason__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("materialize.Response.Validated.Constraint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for response::validated::constraint::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::FieldRequired => "FIELD_REQUIRED",
            Self::LocationRequired => "LOCATION_REQUIRED",
            Self::LocationRecommended => "LOCATION_RECOMMENDED",
            Self::FieldOptional => "FIELD_OPTIONAL",
            Self::FieldForbidden => "FIELD_FORBIDDEN",
            Self::Unsatisfiable => "UNSATISFIABLE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for response::validated::constraint::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "FIELD_REQUIRED",
            "LOCATION_REQUIRED",
            "LOCATION_RECOMMENDED",
            "FIELD_OPTIONAL",
            "FIELD_FORBIDDEN",
            "UNSATISFIABLE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = response::validated::constraint::Type;

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
                    "INVALID" => Ok(response::validated::constraint::Type::Invalid),
                    "FIELD_REQUIRED" => Ok(response::validated::constraint::Type::FieldRequired),
                    "LOCATION_REQUIRED" => Ok(response::validated::constraint::Type::LocationRequired),
                    "LOCATION_RECOMMENDED" => Ok(response::validated::constraint::Type::LocationRecommended),
                    "FIELD_OPTIONAL" => Ok(response::validated::constraint::Type::FieldOptional),
                    "FIELD_FORBIDDEN" => Ok(response::validated::constraint::Type::FieldForbidden),
                    "UNSATISFIABLE" => Ok(response::validated::constraint::Type::Unsatisfiable),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
