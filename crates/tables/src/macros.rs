use crate::Action;
use crate::Id;

/// Column is a column of a table.
pub trait Column: std::fmt::Debug {
    // column_fmt is a debugging view over a column type.
    // It conforms closely to how types are natively represented in sqlite
    // for historical reasons, though they're no longer tightly coupled.
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Debug>::fmt(self, f)
    }
}

/// Row is a row of a Table.
pub trait Row: Sized {
    type Table: Table;
}

/// Table is a collection of Rows.
pub trait Table: Sized {
    type Row: Row;

    fn iter(&self) -> std::slice::Iter<Self::Row>;
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

#[cfg(feature = "persist")]
/// SqlRow is a Row which can persist to and from sqlite.
pub trait SqlRow: Row {
    type SqlTable: SqlTable;

    /// Persist this row, using a Statement previously prepared from Table::insert_sql().
    fn persist<'stmt>(&self, stmt: &mut rusqlite::Statement<'stmt>) -> rusqlite::Result<()>;
    /// Scan an instance from a Row shape queried via Table::select_sql().
    fn scan<'stmt>(row: &rusqlite::Row<'stmt>) -> rusqlite::Result<Self>;
}

#[cfg(feature = "persist")]
/// SqlTable is a Table which can persist to and from sqlite.
pub trait SqlTable: Table + SqlTableObj {
    type SqlRow: SqlRow;

    /// SQL for inserting table rows.
    fn insert_sql() -> String;
    /// SQL for querying table rows.
    /// Filtering WHERE clauses may be appended to the returned string.
    fn select_sql() -> String;
}

#[cfg(feature = "persist")]
/// SqlTableObj is the object-safe portion of a SqlTable.
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

#[cfg(feature = "persist")]
// Helper for swapping a token tree with another expression.
macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

pub trait SpecRow<T>: std::fmt::Debug {
    fn get_name(&self) -> &str;
    fn get_action(&self) -> Option<Action>;
    fn draft_update(&mut self) -> &mut T;
    fn get_draft_spec(&self) -> Option<&T>;
    fn get_live_spec(&self) -> Option<&T>;
    fn get_last_pub_id(&self) -> Option<Id>;
    fn get_expect_pub_id(&self) -> Option<Id>;

    fn get_final_spec(&self) -> &T {
        let Some(ds) = self.get_draft_spec() else {
            let Some(ls) = self.get_live_spec() else {
                panic!("invalid SpecRow has no draft or live spec in {self:?}");
            };
            return ls;
        };
        ds
    }
}

macro_rules! spec_row {
    ($row_type:ty, $spec_type:ty, $name_field:ident) => {
        impl SpecRow<$spec_type> for $row_type {
            fn get_name(&self) -> &str {
                self.$name_field.as_str()
            }

            fn get_action(&self) -> Option<Action> {
                self.action
            }

            fn draft_update(&mut self) -> &mut $spec_type {
                if self.drafted.is_none() {
                    self.drafted = self.live_spec.clone();
                }
                if self.last_pub_id.is_some() {
                    self.expect_pub_id = self.last_pub_id;
                }
                self.action = Some(Action::Update);
                self.drafted
                    .as_mut()
                    .expect("draft_update requires live_spec or drafted must be Some")
            }

            fn get_draft_spec(&self) -> Option<&$spec_type> {
                self.drafted.as_ref()
            }

            fn get_live_spec(&self) -> Option<&$spec_type> {
                self.live_spec.as_ref()
            }

            fn get_last_pub_id(&self) -> Option<Id> {
                self.last_pub_id
            }

            fn get_expect_pub_id(&self) -> Option<Id> {
                self.expect_pub_id
            }
        }

        impl NamedRow for $row_type {
            fn name(&self) -> &str {
                self.get_name()
            }
        }
    };
}

macro_rules! with_catalog_name {
    ($table:ident, $row:ident, $name:ident) => {
        impl NamedRow for $row {
            fn name(&self) -> &str {
                self.$name.as_str()
            }
        }

        impl $table {
            pub fn get_by_name(&self, name: &str) -> Option<&$row> {
                let Some(idx) = self.index_of_named(name) else {
                    return None;
                };
                self.0.get(idx)
            }

            pub fn get_mut_by_name(&mut self, name: &str) -> Option<&mut $row> {
                let Some(idx) = self.index_of_named(name) else {
                    return None;
                };
                self.0.get_mut(idx)
            }

            pub fn contains(&self, catalog_name: &str) -> bool {
                self.index_of_named(catalog_name).is_some()
            }

            fn index_of_named(&self, name: &str) -> Option<usize> {
                self.0.binary_search_by(|row| row.name().cmp(name)).ok()
            }
        }
    };
}

/// Define row & table structures and related implementations.
macro_rules! tables {
    ($(
        table $table:ident ( row $( #[$rowattrs:meta] )? $row:ident, order_by [ $($order_by:ident)* ], sql $sql_name:literal ) {
            $($field:ident: $rust_type:ty,)*
        }
    )*) => {
        $(

        $( #[$rowattrs] )?
        pub struct $row {
            $(pub $field: $rust_type,)*
        }

        /// New-type wrapper of a Row vector.
        #[derive(Default)]
        pub struct $table(Vec<$row>);

        impl $table {
            /// New returns an empty Table.
            pub fn new() -> Self { Self(Vec::new()) }
            /// Insert a new ordered Row into the Table.
            /// Arguments match the positional order of the table's definition.
            #[allow(dead_code)]
            pub fn insert_row(&mut self, $( $field: impl OwnOrClone<$rust_type>, )*) {
                self.insert($row {
                    $($field: $field.own_or_clone(),)*
                });
            }
            /// Insert a new ordered Row into the Table.
            #[allow(dead_code)]
            pub fn insert(&mut self, row: $row) {
                use superslice::Ext;

                let r = ($(&row.$order_by,)*);

                let index = self.0.upper_bound_by(move |_l| {
                    let l = ($(&_l.$order_by,)*);
                    l.cmp(&r)
                });
                self.0.insert(index, row);
            }

            // TODO: maybe move upsert functions into with_catalog_name?
            /// Insert a new row, or update an existing one if a row with the same key already exists.
            /// Be warned: the Table must be ordered, and the results are unspecified if it is not.
            #[allow(dead_code)]
            pub fn upsert<F>(&mut self, row: $row, mut merge: F)
            where F: FnMut(&$row, &mut $row) {
                let r = ($(&row.$order_by,)*);

                // idk why, but rustc things l_row is unused when it clearly is used
                let idx = self.0.binary_search_by(|#[allow(unused_variables)] l_row| {
                    let l = ($(&l_row.$order_by,)*);
                    l.cmp(&r)
                });
                match idx {
                    Ok(i) => {
                        let mut next = row;
                        merge(&self.0[i], &mut next);
                        let pos = self.0.get_mut(i).unwrap();
                        let _ = std::mem::replace(pos, next);
                    }
                    Err(i) => {
                        self.0.insert(i, row);
                    }
                }
            }

            pub fn upsert_all<F>(&mut self, rows: impl IntoIterator<Item=$row>, mut merge: F)
            where F: FnMut(&$row, &mut $row) {
                // TODO: optimize this to avoid doing n binary searches
                for row in rows {
                    self.upsert(row, &mut merge);
                }
            }

            pub fn upsert_overwrite(&mut self, row: $row) {
                self.upsert(row, |_, _| {})
            }

            /// Convert the Table into an Iterator.
            #[allow(dead_code)]
            pub fn into_iter(self) -> impl Iterator<Item=$row> {
                self.0.into_iter()
            }
            /// Extend the Table from the given Iterator.
            #[allow(dead_code)]
            pub fn extend(&mut self, it: impl Iterator<Item=$row>) {
                self.0.extend(it);
                self.reindex();
            }
            // Re-index the Table as a bulk operation.
            fn reindex(&mut self) {
                self.0.sort_by(|_l, _r| {
                    let l = ($(&_l.$order_by,)*);
                    let r = ($(&_r.$order_by,)*);
                    l.cmp(&r)
                });
            }

        }

        impl Table for $table {
            type Row = $row;

            fn iter(&self) -> std::slice::Iter<Self::Row> {
                self.0.iter()
            }
        }
        impl Row for $row {
            type Table = $table;
        }

        impl std::ops::Deref for $table {
            type Target = Vec<$row>;
            fn deref(&self) -> &Vec<$row> { &self.0 }
        }

        impl std::ops::DerefMut for $table {
            fn deref_mut(&mut self) -> &mut Vec<$row> { &mut self.0 }
        }

        impl std::iter::FromIterator<$row> for $table {
            fn from_iter<I: IntoIterator<Item=$row>>(iter: I) -> Self {
                let mut c = $table::new();
                c.extend(iter.into_iter());
                c
            }
        }
        impl IntoIterator for $table {
            type Item = $row;
            type IntoIter = std::vec::IntoIter<$row>;

            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        }

        impl std::fmt::Debug for $table {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.as_slice().fmt(f)
            }
        }

        impl std::fmt::Debug for $row {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut f = f.debug_struct(stringify!($row));
                $(
                let f = f.field(stringify!($field), &crate::macros::ColumnDebugWrapper(&self.$field));
                )*
                f.finish()
            }
        }

        #[cfg(feature = "persist")]
        impl SqlTable for $table {
            type SqlRow = $row;

            fn insert_sql() -> String {
                [
                    "INSERT INTO ",
                    $sql_name,
                    " ( ",
                    [ $( stringify!($field), )* ].join(", ").as_str(),
                    " ) VALUES ( ",
                    [ $( replace_expr!($field "?"), )* ].join(", ").as_str(),
                    " );"
                ].concat()
            }

            fn select_sql() -> String {
                [
                    "SELECT ",
                    [ $( stringify!($field), )* ].join(", ").as_str(),
                    " FROM ",
                    $sql_name,
                    // Closing ';' is omitted so that WHERE clauses may be chained.
                    // rusqlite is okay with a non-closed statement.
                ].concat()
            }
        }

        #[cfg(feature = "persist")]
        impl SqlTableObj for $table {
            fn sql_name(&self) -> &'static str { $sql_name }

            fn create_table_sql(&self) -> String {
                [
                    "CREATE TABLE IF NOT EXISTS ",
                    $sql_name,
                    " ( ",
                    [ $(
                        [
                            stringify!($field),
                            " ",
                            <$rust_type as SqlColumn>::sql_type(),
                        ].concat(),
                    )* ].join(", ").as_str(),
                    " ); "
                ].concat()
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
                self.extend(stmt.query_map([], $row::scan)?
                              .collect::<Result<Vec<_>, _>>()?.into_iter());
                Ok(())
            }

            fn load_where(&mut self, db: &rusqlite::Connection, filter: &str, params: &[&dyn rusqlite::types::ToSql]) -> rusqlite::Result<()> {
                let mut stmt = db.prepare(&format!("{} WHERE {}", Self::select_sql(), filter))?;
                self.extend(stmt.query_map(params, $row::scan)?
                              .collect::<Result<Vec<_>, _>>()?.into_iter());
                Ok(())
            }
        }

        #[cfg(feature = "persist")]
        impl SqlRow for $row {
            type SqlTable = $table;

            fn persist(&self, stmt: &mut rusqlite::Statement<'_>) -> rusqlite::Result<()> {
                stmt.execute(rusqlite::params![ $(
                    <$rust_type as SqlColumn>::to_sql(&self.$field)?,
                )* ])?;
                Ok(())
            }

            fn scan<'stmt>(row: &rusqlite::Row<'stmt>) -> rusqlite::Result<Self> {
                let mut _idx = 0;
                $(
                let $field = <$rust_type as SqlColumn>::column_result(row.get_ref_unwrap(_idx))?;
                _idx += 1;
                )*

                Ok($row { $( $field, )* })
            }
        }

        )*
    }
}

pub struct ColumnDebugWrapper<'a>(pub &'a dyn Column);

impl<'a> std::fmt::Debug for ColumnDebugWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.column_fmt(f)
    }
}
