use serde::{Deserialize, Serialize};
use sqlx::postgres;
use sqlx::{Decode, Encode, Type};

/// TextJson is a newtype wrapper like sqlx::types::Json,
/// but will only encode itself using the postgres JSON protocol
/// encoding, which is the unmodified textual encoding of the document.
/// Crucially JSONB is not used, which ensures that spacing and
/// ordering of document properties are preserved.
#[derive(Copy, Clone, Debug, Serialize)]
pub struct TextJson<T>(pub T);

impl<T> std::ops::Deref for TextJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Type<postgres::Postgres> for TextJson<T> {
    fn type_info() -> postgres::PgTypeInfo {
        postgres::PgTypeInfo::with_name("JSON")
    }
    fn compatible(ty: &postgres::PgTypeInfo) -> bool {
        *ty == Self::type_info() // Not compatible with JSONB.
    }
}

impl<T> sqlx::postgres::PgHasArrayType for TextJson<T> {
    fn array_type_info() -> postgres::PgTypeInfo {
        postgres::PgTypeInfo::with_name("_JSON")
    }
}

impl<T: Serialize> Encode<'_, postgres::Postgres> for TextJson<T> {
    fn encode_by_ref(&self, buf: &mut postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        buf.push(b' '); // Send as JSON (not JSONB).

        serde_json::to_writer(&mut **buf, &self.0)
            .expect("failed to serialize OrderedJson for transmission to database");

        sqlx::encode::IsNull::No
    }
}

// Decode passes-through to the sqlx::types::Json implementation,
// but is restricted to the JSON (and not JSONB) postgres types.
impl<'r, T: 'r> Decode<'r, postgres::Postgres> for TextJson<T>
where
    T: Deserialize<'r>,
{
    fn decode(value: postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        <sqlx::types::Json<T> as Decode<'r, postgres::Postgres>>::decode(value).map(|t| Self(t.0))
    }
}
