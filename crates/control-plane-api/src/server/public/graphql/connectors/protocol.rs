/// The type of task that the connector is used for. Note that derivation
/// connectors do exist, but aren't yet represented in `connector_tags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
#[graphql(rename_items = "lowercase")]
pub enum ConnectorProto {
    Capture,
    Materialization,
}

impl AsRef<str> for ConnectorProto {
    fn as_ref(&self) -> &str {
        match self {
            ConnectorProto::Capture => "capture",
            ConnectorProto::Materialization => "materialization",
        }
    }
}

impl sqlx::Type<sqlx::Postgres> for ConnectorProto {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        <&'static str as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ConnectorProto {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let name: &str = self.as_ref();
        sqlx::Encode::<'q, sqlx::Postgres>::encode_by_ref(&name, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ConnectorProto {
    fn decode(
        value: <sqlx::Postgres as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let str_val = <&'_ str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        match str_val {
            "capture" => Ok(ConnectorProto::Capture),
            "materialization" => Ok(ConnectorProto::Materialization),
            other => Err(Box::new(ProtoErr(other.to_string()))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown ConnectorProto value: '{0}'")]
pub struct ProtoErr(String);
