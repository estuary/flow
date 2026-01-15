/// Wraps a `chrono::TimeDelta` to allow it to be used as a Postgres `interval`
/// type. This is necessary because `chrono::TimeDelta` does not implement
/// `Decode`. Note that converting a `chrono::TimeDelta` to an `interval` may
/// fail if the duration cannot be faithfully represented as an interval. This
/// would be the case if it uses nanosecond precision, for example. Thus if we
/// ever need to support inserting an `Interval`, we should add explicit
/// conversion functions from `chrono::TimeDelta`.
pub struct Interval(chrono::TimeDelta);

impl sqlx::Type<sqlx::postgres::Postgres> for Interval {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <chrono::TimeDelta as sqlx::Type<sqlx::postgres::Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <chrono::TimeDelta as sqlx::Type<sqlx::postgres::Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::postgres::Postgres> for Interval {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.0.encode_by_ref(buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::postgres::Postgres> for Interval {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let pg_int = <sqlx::postgres::types::PgInterval as sqlx::Decode<
            'r,
            sqlx::postgres::Postgres,
        >>::decode(value)?;

        let d = chrono::TimeDelta::microseconds(pg_int.microseconds);
        Ok(Interval(d))
    }
}

impl std::ops::Deref for Interval {
    type Target = chrono::TimeDelta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Interval> for chrono::TimeDelta {
    fn from(interval: Interval) -> Self {
        interval.0
    }
}
