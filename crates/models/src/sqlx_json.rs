/// Implements `sqlx::{Type, Decode, Encode}` for a given type, allowing sqlx to
/// treat it as a plain `json` column (not `jsonb`). The type given to this
/// macro must implement `serde::Serialize` and `serde::Deserialize`.
macro_rules! sqlx_json {
    ($rust_type:ty) => {

        #[cfg(feature = "sqlx-support")]
        impl sqlx::Type<sqlx::Postgres> for $rust_type {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                sqlx::postgres::PgTypeInfo::with_name("JSON")
            }
            // TODO: is `compatible` impl necessary?
            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                *ty == Self::type_info() // Not compatible with JSONB.
            }
        }

        #[cfg(feature = "sqlx-support")]
        impl<'a> sqlx::Decode<'a, sqlx::postgres::Postgres> for $rust_type {
            fn decode(value: sqlx::postgres::PgValueRef<'a>) -> Result<Self, sqlx::error::BoxDynError> {
                <sqlx::types::Json<$rust_type> as sqlx::Decode<'a, sqlx::postgres::Postgres>>::decode(value)
                    .map(|t| t.0)
            }
        }

        #[cfg(feature = "sqlx-support")]
        impl<'q> sqlx::Encode<'q, sqlx::postgres::Postgres> for $rust_type {
            fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
                <sqlx::types::Json<&Self> as sqlx::Encode<'q, sqlx::postgres::Postgres>>::encode(
                    sqlx::types::Json(self),
                    buf,
                )
            }
        }
    };
}

pub(crate) use sqlx_json;
