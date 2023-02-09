use std::cell::Cell;
use std::rc::Rc;

/// Projection describes a document location that's projected into a SQLite table column.
#[derive(Debug, Clone)]
pub struct Projection {
    // Name of the table column within SQLite.
    pub field: String,
    // JSON pointer location of this field within documents.
    pub ptr: doc::Pointer,
    // Location uses string format: "integer" ?
    pub is_format_integer: bool,
    // Location uses string format: "number" ?
    pub is_format_number: bool,
    // Location uses string contentEncoding: "base64" ?
    pub is_content_encoding_base64: bool,
}

impl Projection {
    pub fn new(
        field: &str,
        ptr: &str,
        is_format_integer: bool,
        is_format_number: bool,
        is_content_encoding_base64: bool,
    ) -> Self {
        Self {
            field: field.to_string(),
            ptr: doc::Pointer::from_str(ptr),
            is_format_integer,
            is_format_number,
            is_content_encoding_base64,
        }
    }
}

/// Lambda is a SQLite execution context which is able to repeatedly invoke a
/// given query over novel source and register documents, returning an array
/// of document results.
pub struct Lambda<N: doc::AsNode + 'static> {
    db: Option<rusqlite::Connection>,
    stmt: Option<rusqlite::Statement<'static>>,
    output_columns: Vec<String>,

    /// Cells which hold documents which will be taken by the next vtab::Cursor.
    source: Rc<Cell<Option<&'static N>>>,
    register: Rc<Cell<Option<&'static N>>>,
    previous_register: Rc<Cell<Option<&'static N>>>,
}

impl<N: doc::AsNode + 'static> Lambda<N> {
    /// Create a new Lambda which executes the given `query`. The execution
    /// context will include a table `source` having the given `source_columns`.
    ///
    /// If `register_columns` is non-empty, it will additionally have tables
    /// `register` and `previous_register` having the given `register_columns`.
    ///
    /// Each row returned by `query` is mapped into a JSON document, with each
    /// output column becoming a top-level document property. SQLite types are
    /// mapped to corresponding JSON types null, integer, float, and string.
    ///
    /// Nested JSON arrays and objects are also supported: As SQLite doesn't have
    /// a bespoke JSON value type, this implementation looks for a leading / trailing
    /// pair of '{','}' or '[',']' and, if found, will attempt to parse the string
    /// as a JSON document. If parsing fails, the raw text is passed through as a
    /// regular JSON string.
    ///
    /// As a special case, if the query has a single output column named `flow_document`
    /// then this column is directly mapped into the returned output document.
    /// This can be used to implement lambdas with dynamic top-level properties.
    pub fn new(
        query: &str,
        source_columns: &[Projection],
        register_columns: &[Projection],
    ) -> rusqlite::Result<Self> {
        let db = rusqlite::Connection::open_in_memory()?;

        let source = vtab::Table::<N>::install(&db, "source", source_columns)?;
        let (register, previous_register) = if !register_columns.is_empty() {
            (
                vtab::Table::<N>::install(&db, "register", register_columns)?,
                vtab::Table::<N>::install(&db, "previous_register", register_columns)?,
            )
        } else {
            Default::default()
        };

        let stmt = db.prepare(query)?;

        // Extract output columns from the prepared statement.
        let output_columns = if stmt.column_name(0)? == "flow_document" && stmt.column_count() == 1
        {
            Vec::new()
        } else {
            (0..stmt.column_count())
                .map(|i| stmt.column_name(i).unwrap().to_string())
                .collect()
        };

        // `stmt` borrows and embeds the lifetime of `db`, but we wish to move `db`
        // and store `stmt` alongside it (sharing its lifetime), and thus must transmute to 'static.
        // SAFETY: we implement Drop to destroy `stmt` and `db` correctly.
        let stmt = unsafe { std::mem::transmute::<_, rusqlite::Statement<'static>>(stmt) };

        Ok(Self {
            db: Some(db),
            stmt: Some(stmt),
            output_columns,
            source,
            register,
            previous_register,
        })
    }

    pub fn invoke(
        &mut self,
        source: &N,
        register: Option<&N>,
        previous_register: Option<&N>,
    ) -> rusqlite::Result<Vec<serde_json::Value>> {
        // Our invocation documents have anonymous lifetimes, but the Cell
        // through which we communicate with our vtab::Table is &'static.
        // SAFETY: we call query() and fully consume its result set (or error) before returning.
        // query(), in turn, creates a vtab::Cursor which reads from our Cell, and we fully
        // process its result rows before returning. After this function call,
        // we will not attempt to access these particular argument documents again.
        self.source.set(Some(unsafe {
            std::mem::transmute::<_, &'static N>(source)
        }));
        self.register
            .set(unsafe { std::mem::transmute::<_, Option<&'static N>>(register) });
        self.previous_register
            .set(unsafe { std::mem::transmute::<_, Option<&'static N>>(previous_register) });

        let result = self
            .stmt
            .as_mut()
            .unwrap()
            .query([])?
            .mapped(|row| Ok(Self::row_to_json(&self.output_columns, row)))
            .map(|v| v.unwrap())
            .collect();

        Ok(result)
    }

    fn row_to_json(columns: &[String], row: &rusqlite::Row<'_>) -> serde_json::Value {
        if columns.is_empty() {
            // SELECT json_object(...) as flow_document from ...
            Self::convert_value_ref(row.get_ref(0).unwrap())
        } else {
            // SELECT 1 as foo, 'two' as bar from ...
            serde_json::Value::Object(
                columns
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        (
                            name.to_owned(),
                            Self::convert_value_ref(row.get_ref(index).unwrap()),
                        )
                    })
                    .collect(),
            )
        }
    }

    fn convert_value_ref(value: rusqlite::types::ValueRef<'_>) -> serde_json::Value {
        use rusqlite::types::ValueRef;
        use serde_json::{Number, Value};

        match value {
            ValueRef::Text(s) => {
                if matches!(
                    (s.first(), s.last()),
                    (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']'))
                ) {
                    if let Ok(v) = serde_json::from_slice(s) {
                        return v;
                    }
                }
                serde_json::Value::String(String::from_utf8(s.to_vec()).unwrap())
            }
            ValueRef::Blob(b) => Value::String(base64::encode(b)),
            ValueRef::Integer(i) => Value::Number(Number::from(i)),
            ValueRef::Real(f) => match Number::from_f64(f) {
                Some(n) => Value::Number(n),
                _ => Value::String(format!("{f}")),
            },
            ValueRef::Null => Value::Null,
        }
    }
}

impl<N: doc::AsNode + 'static> Drop for Lambda<N> {
    fn drop(&mut self) {
        let Self {
            db,
            stmt,
            output_columns: _,
            source,
            register,
            previous_register,
        } = self;

        let db = db.take().unwrap();
        let stmt = stmt.take().unwrap();

        source.set(None);
        register.set(None);
        previous_register.set(None);

        // SQLite is a bit particular about VTables and borrows.
        // Attempting to finalize `stmt` before explicitly dropping these tables
        // can sometimes cause a panic due to an outstanding RefCell borrow.

        if let Err(err) = db.execute_batch(
            r#"
            drop table source;
            drop table if exists register;
            drop table if exists previous_register;
        "#,
        ) {
            eprintln!("failed to drop sqlite lambda vtab: {err:?}");
            panic!("unexpected failure closing sqlite lambda db");
        }

        // These panic!'s may cause a double-panic and program abort, so log first
        // to capture any debugging info should they happen.
        if let Err(err) = stmt.finalize() {
            eprintln!("failed to finalize sqlite statement: {err:?}");
            panic!("unexpected failure finalizing sqlite lambda statement");
        } else if let Err((_, err)) = db.close() {
            eprintln!("failed to close sqlite database: {err:?}");
            panic!("unexpected failure closing sqlite lambda db");
        }
    }
}

pub mod vtab;

// TODO(johnny): I don't love the dependency on proto_flow within this crate,
// but at the moment it's important to have easy conversion from a proto Projection.
// Seek to remove this over time.
impl From<&proto_flow::flow::Projection> for Projection {
    fn from(p: &proto_flow::flow::Projection) -> Self {
        use proto_flow::flow::Inference;

        Self::new(
            &p.field,
            &p.ptr,
            matches!(&p.inference, Some(Inference{string: Some(str), ..}) if str.format == "integer"),
            matches!(&p.inference, Some(Inference{string: Some(str), ..}) if str.format == "number"),
            matches!(&p.inference, Some(Inference{string: Some(str), ..}) if str.content_encoding == "base64"),
        )
    }
}
