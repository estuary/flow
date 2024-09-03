#[cfg(feature = "persist")]
use itertools::Itertools;

/// Column is a column of a table.
pub trait Column: std::fmt::Debug {
    // column_fmt is a debugging view over a column type.
    // It conforms closely to how types are natively represented in sqlite
    // for historical reasons, though they're no longer tightly coupled.
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Debug>::fmt(self, f)
    }
}

#[cfg(feature = "persist")]
/// SqlColumn is a Column which can persist to and from sqlite.
pub trait SqlColumn: Sized + Column {
    /// SQL type of this TableColumn.
    fn sql_type() -> &'static str;
    /// Convert this TableColumn to SQL.
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>>;
    /// Convert this TableColumn from SQL.
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self>;
}

/// Row is a row of a Table.
pub trait Row: std::fmt::Debug + Sized {
    type Key: std::cmp::Ord + Clone;

    fn cmp_key(&self, other: &Self::Key) -> std::cmp::Ordering;
    fn cmp_row(&self, other: &Self) -> std::cmp::Ordering;
}

impl<'a, T: Row> Row for &'a T {
    type Key = T::Key;

    fn cmp_key(&self, other: &Self::Key) -> std::cmp::Ordering {
        T::cmp_key(*self, other)
    }

    fn cmp_row(&self, other: &Self) -> std::cmp::Ordering {
        T::cmp_row(*self, other)
    }
}

#[cfg(feature = "persist")]
/// SqlRow is a Row which can persist to and from sqlite.
pub trait SqlRow: Row {
    fn sql_table_name() -> &'static str;
    fn sql_columns() -> Vec<(&'static str, &'static str)>;

    /// Persist this row, using a Statement previously prepared from Table::insert_sql().
    fn persist<'stmt>(&self, stmt: &mut rusqlite::Statement<'stmt>) -> rusqlite::Result<()>;
    /// Scan an instance from a Row shape queried via Table::select_sql().
    fn scan<'stmt>(row: &rusqlite::Row<'stmt>) -> rusqlite::Result<Self>;
}

#[cfg(feature = "persist")]
/// SqlTableObj is the object-safe portion of a concrete Table's SQL support.
pub trait SqlTableObj {
    /// SQL name for this Table.
    fn sql_name(&self) -> &'static str;
    /// SQL for creating this Table schema.
    fn create_table_sql(&self) -> String;
    /// Persist all rows of this Table into the database.
    fn persist_all(&self, db: &rusqlite::Connection) -> rusqlite::Result<()>;
    /// Load all rows from the database into this Table.
    fn load_all(&mut self, db: &rusqlite::Connection) -> rusqlite::Result<()>;
    /// Load rows from the database matching a WHERE clause and parameters.
    fn load_where(
        &mut self,
        db: &rusqlite::Connection,
        filter: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> rusqlite::Result<()>;
}

/// Table is a collection of Rows.
pub struct Table<R: Row>(Vec<R>);

impl<R: Row> Table<R> {
    /// New returns an empty Table.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Insert a new ordered Row into the Table.
    pub fn insert(&mut self, row: R) {
        use superslice::Ext;
        let index = self.0.upper_bound_by(|_l| _l.cmp_row(&row));
        self.0.insert(index, row);
    }

    /// Extend the Table from the given Iterator.
    pub fn extend(&mut self, it: impl Iterator<Item = R>) {
        self.0.extend(it);
        self.reindex();
    }

    /// Get a Row having the given key from the table.
    /// If multiple rows match the key, an arbitrary one is returned.
    pub fn get_key<'s>(&'s self, key: &R::Key) -> Option<&'s R> {
        match self.0.binary_search_by(|r| r.cmp_key(key)) {
            Ok(index) => Some(&self.0[index]),
            Err(_) => None,
        }
    }

    pub fn outer_join<'s, I, IK, IV, M, O>(&'s self, it: I, join: M) -> impl Iterator<Item = O> + 's
    where
        I: Iterator<Item = (IK, IV)> + 's,
        IK: std::borrow::Borrow<R::Key>,
        M: FnMut(itertools::EitherOrBoth<&'s R, (IK, IV)>) -> Option<O> + 's,
    {
        itertools::merge_join_by(self.iter(), it, |l, (rk, _rv)| l.cmp_key(rk.borrow()))
            .filter_map(join)
    }

    pub fn inner_join<'s, I, IK, IV, M, O>(
        &'s self,
        it: I,
        mut join: M,
    ) -> impl Iterator<Item = O> + 's
    where
        I: Iterator<Item = (IK, IV)> + 's,
        IK: std::borrow::Borrow<R::Key>,
        M: FnMut(&'s R, IK, IV) -> Option<O> + 's,
    {
        self.outer_join(it, move |eob| match eob {
            itertools::EitherOrBoth::Both(row, (k, v)) => join(row, k, v),
            _ => None,
        })
    }

    pub fn into_outer_join<I, IK, IV, M, O>(self, it: I, join: M) -> impl Iterator<Item = O>
    where
        I: Iterator<Item = (IK, IV)>,
        IK: std::borrow::Borrow<R::Key>,
        M: FnMut(itertools::EitherOrBoth<R, (IK, IV)>) -> Option<O>,
    {
        itertools::merge_join_by(self.into_iter(), it, |l, (rk, _rv)| l.cmp_key(rk.borrow()))
            .filter_map(join)
    }

    pub fn into_inner_join<I, IK, IV, M, O>(self, it: I, mut join: M) -> impl Iterator<Item = O>
    where
        I: Iterator<Item = (IK, IV)>,
        IK: std::borrow::Borrow<R::Key>,
        M: FnMut(R, IK, IV) -> Option<O>,
    {
        self.into_outer_join(it, move |eob| match eob {
            itertools::EitherOrBoth::Both(row, (k, v)) => join(row, k, v),
            _ => None,
        })
    }

    pub fn outer_join_mut<'s, I, IK, IV, M, O>(
        &'s mut self,
        it: I,
        join: M,
    ) -> impl Iterator<Item = O> + 's
    where
        I: Iterator<Item = (IK, IV)> + 's,
        IK: std::borrow::Borrow<R::Key>,
        M: FnMut(itertools::EitherOrBoth<&'s mut R, (IK, IV)>) -> Option<O> + 's,
    {
        itertools::merge_join_by(self.iter_mut(), it, |l, (rk, _rv)| l.cmp_key(rk.borrow()))
            .filter_map(join)
    }

    pub fn get_by_key(&self, key: &R::Key) -> Option<&R> {
        self.0
            .binary_search_by(|r| r.cmp_key(key))
            .ok()
            .map(|i| &self.0[i])
    }

    pub fn get_mut_by_key(&mut self, key: &R::Key) -> Option<&mut R> {
        self.0
            .binary_search_by(|r| r.cmp_key(key))
            .ok()
            .map(move |i| &mut self.0[i])
    }

    pub fn get_or_insert_with<F>(&mut self, key: &R::Key, make_new: F) -> &mut R
    where
        F: FnOnce() -> R,
    {
        match self.0.binary_search_by(|r| r.cmp_key(key)) {
            Ok(i) => &mut self.0[i],
            Err(i) => {
                self.0.insert(i, make_new());
                &mut self.0[i]
            }
        }
    }

    pub fn upsert<F>(&mut self, row: R, mut merge: F)
    where
        F: FnMut(&mut R, Option<R>),
    {
        match self.0.binary_search_by(|r| r.cmp_row(&row)) {
            Ok(i) => {
                // TODO: finish upsert and use it in test_util
                let prev = std::mem::replace(&mut self.0[i], row);
                merge(&mut self.0[i], Some(prev));
            }
            Err(i) => {
                self.0.insert(i, row);
            }
        };
    }

    pub fn upsert_overwrite(&mut self, row: R) {
        self.upsert(row, |_, _| {});
    }

    // Re-index the Table as a bulk operation.
    fn reindex(&mut self) {
        self.0.sort_by(|l, r| l.cmp_row(r));
    }
}

impl<R: Row> Default for Table<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Row> std::ops::Deref for Table<R> {
    type Target = Vec<R>;
    fn deref(&self) -> &Vec<R> {
        &self.0
    }
}

impl<R: Row> std::ops::DerefMut for Table<R> {
    fn deref_mut(&mut self) -> &mut Vec<R> {
        &mut self.0
    }
}

impl<R: Row> std::iter::FromIterator<R> for Table<R> {
    fn from_iter<I: IntoIterator<Item = R>>(iter: I) -> Self {
        let mut c = Self::new();
        c.extend(iter.into_iter());
        c
    }
}

impl<R: Row> std::iter::IntoIterator for Table<R> {
    type Item = R;
    type IntoIter = std::vec::IntoIter<R>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'t, R: Row> std::iter::IntoIterator for &'t Table<R> {
    type Item = &'t R;
    type IntoIter = std::slice::Iter<'t, R>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<R: Row> std::fmt::Debug for Table<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}

#[cfg(feature = "persist")]
impl<R: SqlRow> Table<R> {
    /// SQL for inserting table rows.
    fn insert_sql() -> String {
        [
            "INSERT INTO ",
            R::sql_table_name(),
            " ( ",
            R::sql_columns()
                .iter()
                .map(|(sql_name, _sql_type)| *sql_name)
                .join(", ")
                .as_str(),
            " ) VALUES ( ",
            R::sql_columns().iter().map(|_| "?").join(", ").as_str(),
            " );",
        ]
        .concat()
    }

    /// SQL for querying table rows.
    /// Filtering WHERE clauses may be appended to the returned string.
    fn select_sql() -> String {
        [
            "SELECT ",
            R::sql_columns()
                .iter()
                .map(|(sql_name, _sql_type)| *sql_name)
                .join(", ")
                .as_str(),
            " FROM ",
            R::sql_table_name(),
            // Closing ';' is omitted so that WHERE clauses may be chained.
            // rusqlite is okay with a non-closed statement.
        ]
        .concat()
    }
}

#[cfg(feature = "persist")]
impl<R: SqlRow> SqlTableObj for Table<R> {
    fn sql_name(&self) -> &'static str {
        R::sql_table_name()
    }

    fn create_table_sql(&self) -> String {
        [
            "CREATE TABLE IF NOT EXISTS ",
            R::sql_table_name(),
            " ( ",
            R::sql_columns()
                .iter()
                .map(|(name, typ)| format!("{name} {typ}"))
                .join(", ")
                .as_str(),
            " );",
        ]
        .concat()
    }

    fn persist_all(&self, db: &rusqlite::Connection) -> rusqlite::Result<()> {
        let mut stmt = db.prepare(&Self::insert_sql())?;

        for row in &self.0 {
            row.persist(&mut stmt)?;
        }
        Ok(())
    }

    fn load_all(&mut self, db: &rusqlite::Connection) -> rusqlite::Result<()> {
        let mut stmt = db.prepare(&Self::select_sql())?;
        self.extend(
            stmt.query_map([], R::scan)?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter(),
        );
        Ok(())
    }

    fn load_where(
        &mut self,
        db: &rusqlite::Connection,
        filter: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> rusqlite::Result<()> {
        let mut stmt = db.prepare(&format!("{} WHERE {}", Self::select_sql(), filter))?;
        self.extend(
            stmt.query_map(params, R::scan)?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter(),
        );
        Ok(())
    }
}

/// Trait for accepting arguments which may be owned, or can be cloned.
pub trait OwnOrClone<T> {
    fn own_or_clone(self) -> T;
}

impl<T> OwnOrClone<T> for T {
    fn own_or_clone(self) -> T {
        self
    }
}

impl<'a, T: Clone> OwnOrClone<T> for &'a T {
    fn own_or_clone(self) -> T {
        self.clone()
    }
}

/// Wrapper impl which makes any T: Column have a Option<T> Column.
impl<T: Column> Column for Option<T> {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Some(some) => some.column_fmt(f),
            None => f.write_str("NULL"),
        }
    }
}

#[cfg(feature = "persist")]
/// Wrapper impl which makes any T: SqlColumn have a Option<T> SqlColumn.
impl<T: SqlColumn> SqlColumn for Option<T> {
    fn sql_type() -> &'static str {
        T::sql_type()
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            None => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Null,
            )),
            Some(inner) => inner.to_sql(),
        }
    }
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        if let rusqlite::types::ValueRef::Null = value {
            Ok(None)
        } else {
            T::column_result(value).map(Option::Some)
        }
    }
}

#[cfg(feature = "persist")]
/// Persist a dynamic set of tables to the database, creating their table schema
/// if they don't yet exist, and writing all row records.
pub fn persist_tables(
    db: &rusqlite::Connection,
    tables: &[&dyn SqlTableObj],
) -> rusqlite::Result<()> {
    db.execute_batch("BEGIN IMMEDIATE;")?;
    for table in tables {
        db.execute_batch(&table.create_table_sql())?;
        table.persist_all(db)?;
    }
    db.execute_batch("COMMIT;")?;
    Ok(())
}

#[cfg(feature = "persist")]
/// Load all rows of a dynamic set of tables from the database.
pub fn load_tables(
    db: &rusqlite::Connection,
    tables: &mut [&mut dyn SqlTableObj],
) -> rusqlite::Result<()> {
    db.execute_batch("BEGIN;")?;
    for table in tables {
        table.load_all(db)?;
    }
    db.execute_batch("COMMIT;")?;
    Ok(())
}

/// primitive_sql_types establishes TableColumn implementations for
/// types already having rusqlite FromSql / ToSql implementations.
macro_rules! primitive_sql_types {
    ($($rust_type:ty => $sql_type:literal,)*) => {
        $(

        #[cfg(feature = "persist")]
        impl SqlColumn for $rust_type {
            fn sql_type() -> &'static str {
                $sql_type
            }
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                <Self as rusqlite::types::ToSql>::to_sql(self)
            }
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                <Self as rusqlite::types::FromSql>::column_result(value)
            }
        }

        )*
    };
}

/// string_wrapper_types establishes TableColumn implementations for
/// newtype String wrappers.
macro_rules! string_wrapper_types {
    ($($rust_type:ty,)*) => {
        $(
        impl Column for $rust_type {
            fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_ref())
            }
        }

        #[cfg(feature = "persist")]
        impl SqlColumn for $rust_type {
            fn sql_type() -> &'static str {
                "TEXT"
            }
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                <str as rusqlite::types::ToSql>::to_sql(self)
            }
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                Ok(Self::new(<String as rusqlite::types::FromSql>::column_result(value)?))
            }
        }
        )*
    };
}

/// json_sql_types establishes TableColumn implementations for
/// Serialize & Deserialize types which encode as JSON.
macro_rules! json_sql_types {
    ($($rust_type:ty,)*) => {
        $(
        impl Column for $rust_type {
            fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let s = serde_json::to_string_pretty(&self).unwrap();
                f.write_str(&s)
            }
        }

        #[cfg(feature = "persist")]
        impl SqlColumn for $rust_type {
            fn sql_type() -> &'static str {
                "TEXT"
            }
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                // "pretty" encoding is not strictly required, but it makes database
                // more pleasant to examine with SQLite GUI tooling.
                let s = serde_json::to_string_pretty(self)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(err.into()))?;
                Ok(s.into())
            }
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                Ok(serde_json::from_str(value.as_str()?)
                   .map_err(|err| rusqlite::types::FromSqlError::Other(err.into()))?)
            }
        }
        )*
    };
}

/// proto_sql_types establishes TableColumn implementations for
/// ToProto / FromProto types which encode as protobuf.
macro_rules! proto_sql_types {
    ($($rust_type:ty,)*) => {
        $(
        impl Column for $rust_type {}

        #[cfg(feature = "persist")]
        impl SqlColumn for $rust_type {
            fn sql_type() -> &'static str {
                "BLOB"
            }
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                let mut b = Vec::with_capacity(self.encoded_len());
                self.encode(&mut b).unwrap();
                Ok(b.into())
            }
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                Ok(Self::decode(value.as_blob()?)
                   .map_err(|err| rusqlite::types::FromSqlError::Other(err.into()))?)
            }
        }
        )*
    };
}

/// Define row & table structures and related implementations.
macro_rules! tables {
    ($(
        table $table:ident ( row $( #[$rowattrs:meta] )* $row:ident, sql $sql_name:literal ) {
            $(key $key:ident: $key_type:ty,)*
            $(val $val:ident: $val_type:ty,)*
        }
    )*) => {
        $(

        $( #[$rowattrs] )*
        pub struct $row {
            $(pub $key: $key_type,)*
            $(pub $val: $val_type,)*
        }

        /// Type alias for a Table of this Row.
        pub type $table = Table<$row>;

        table_impl_row!($table, $row, [ $($key: $key_type,)* ]);

        impl std::fmt::Debug for $row {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut f = f.debug_struct(stringify!($row));
                $(
                let f = f.field(stringify!($key), &crate::macros::ColumnDebugWrapper(&self.$key));
                )*
                $(
                let f = f.field(stringify!($val), &crate::macros::ColumnDebugWrapper(&self.$val));
                )*
                f.finish()
            }
        }

        #[cfg(feature = "persist")]
        impl SqlRow for $row {
            fn sql_table_name() -> &'static str { $sql_name }

            fn sql_columns() -> Vec<(&'static str, &'static str)> {
                vec![
                    $( (stringify!($key), <$key_type>::sql_type()), )*
                    $( (stringify!($val), <$val_type>::sql_type()), )*
                ]
            }

            fn persist(&self, stmt: &mut rusqlite::Statement<'_>) -> rusqlite::Result<()> {
                stmt.execute(rusqlite::params![
                    $( <$key_type as SqlColumn>::to_sql(&self.$key)?, )*
                    $( <$val_type as SqlColumn>::to_sql(&self.$val)?, )*
                ])?;
                Ok(())
            }

            fn scan<'stmt>(row: &rusqlite::Row<'stmt>) -> rusqlite::Result<Self> {
                let mut _idx = 0;
                $(
                let $key = <$key_type as SqlColumn>::column_result(row.get_ref_unwrap(_idx))?;
                _idx += 1;
                )*
                $(
                let $val = <$val_type as SqlColumn>::column_result(row.get_ref_unwrap(_idx))?;
                _idx += 1;
                )*

                Ok($row { $( $key, )* $( $val, )* })
            }
        }

        impl Table<$row> {
            /// Insert a new ordered Row into the Table.
            /// Arguments match the positional order of the table's definition.
            #[allow(dead_code)]
            pub fn insert_row(&mut self, $( $key: impl OwnOrClone<$key_type>, )* $( $val: impl OwnOrClone<$val_type>, )*) {
                self.insert($row {
                    $($key: $key.own_or_clone(),)*
                    $($val: $val.own_or_clone(),)*
                });
            }
        }

        )*
    }
}

macro_rules! table_impl_row {
    // Key N=0
    ($table:ident, $row:ident, [ ] ) => {
        impl Row for $row {
            type Key = ();

            fn cmp_key(&self, _other: &Self::Key) -> std::cmp::Ordering { std::cmp::Ordering::Equal }
            fn cmp_row(&self, _other: &Self) -> std::cmp::Ordering { std::cmp::Ordering::Equal }
        }
    };
    // Key N=1
    ($table:ident, $row:ident, [ $key:ident: $key_type:ty, ] ) => {
        impl Row for $row {
            type Key = $key_type;

            fn cmp_key(&self, other: &Self::Key) -> std::cmp::Ordering { self.$key.cmp(other) }
            fn cmp_row(&self, other: &Self) -> std::cmp::Ordering { self.$key.cmp(&other.$key) }
        }
    };
    // Key N=2
    ($table:ident, $row:ident, [ $key1:ident: $key1_type:ty, $key2:ident: $key2_type:ty, ] ) => {
        impl Row for $row {
            type Key = ($key1_type, $key2_type);

            fn cmp_key(&self, other: &Self::Key) -> std::cmp::Ordering {
                (&self.$key1, &self.$key2).cmp(&(&other.0, &other.1))
            }
            fn cmp_row(&self, other: &Self) -> std::cmp::Ordering {
                (&self.$key1, &self.$key2).cmp(&(&other.$key1, &other.$key2))
            }
        }
    };
    // Other N's are not implemented yet.
    ($table:ident, $row:ident, [ $($key:ident: $key_type:ty,)* ] ) => {
        impl Row for $row {
            type Key = ( $($key_type,)* );

            fn cmp_key(&self, _other: &Self::Key) -> std::cmp::Ordering {
                todo!("cmp_key must be implemented for keys of this length")
            }
            fn cmp_row(&self, _other: &Self) -> std::cmp::Ordering {
                todo!("cmp_row must be implemented for keys of this length")
            }
        }
    };
}

pub struct ColumnDebugWrapper<'a>(pub &'a dyn Column);

impl<'a> std::fmt::Debug for ColumnDebugWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.column_fmt(f)
    }
}
