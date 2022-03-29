use bytes::BufMut;
use tokio_postgres::types as pgtypes;

/// Id is the Rust equivalent of the Postgres `flowid` type domain.
/// It's a fixed 8-byte payload which is represented in hexadecimal notation.
/// TODO(johnny): Introduce a TypedId wrapper using PhantomData?
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 8]);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", i64::from_be_bytes(self.0))
    }
}
impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

/// Within Postgres, the `flowid` domain type uses macaddr8 storage.
/// Implement direct conversion from the macaddr8 wire type to an Id.
impl<'a> pgtypes::FromSql<'a> for Id {
    fn from_sql(
        _: &pgtypes::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 8 {
            return Err("invalid message length: macaddr8 length mismatch".into());
        }

        let mut inner = [0; 8];
        inner.copy_from_slice(raw);
        Ok(Id(inner))
    }

    pgtypes::accepts!(MACADDR8);
}

/// Implement direct conversion from an Id to the the macaddr8 wire type.
impl pgtypes::ToSql for Id {
    fn to_sql(
        &self,
        _: &pgtypes::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<pgtypes::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        w.put_slice(&self.0);
        Ok(pgtypes::IsNull::No)
    }

    pgtypes::accepts!(MACADDR8);
    pgtypes::to_sql_checked!();
}
