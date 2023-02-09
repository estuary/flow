use super::Projection;
use rusqlite::vtab::{
    read_only_module, sqlite3_vtab, sqlite3_vtab_cursor, Context, CreateVTab, IndexInfo, VTab,
    VTabConnection, VTabCursor, VTabKind, Values,
};
use rusqlite::Result;
use std::cell::Cell;
use std::os::raw::c_int;
use std::rc::Rc;

#[repr(C)]
pub struct Table<N>
where
    N: doc::AsNode + 'static,
{
    // Must be first.
    base: sqlite3_vtab,
    // Columns of the virtual table.
    columns: Vec<Projection>,
    // Next document to return on SELECT.
    next_document: Rc<Cell<Option<&'static N>>>,
}

impl<N> Table<N>
where
    N: doc::AsNode + 'static,
{
    pub fn install(
        db: &rusqlite::Connection,
        name: &str,
        columns: &[Projection],
    ) -> rusqlite::Result<Rc<Cell<Option<&'static N>>>> {
        let module = read_only_module::<Self>();

        let next_document = Rc::new(Cell::from(None));
        let aux = Some((columns.to_vec(), next_document.clone()));
        db.create_module::<Self>(name, module, aux)?;

        if let Err(err) = db.execute(&format!("CREATE VIRTUAL TABLE {name} using {name}"), []) {
            let columns = columns.iter().map(|p| &p.field).collect::<Vec<_>>();
            Err(rusqlite::Error::ModuleError(format!("failed to prepare sqlite execution context for table {name:?} with columns {columns:?}.\n\tEnsure all projected columns have unique, case-insensitive names\n\t({err})")))
        } else {
            Ok(next_document)
        }
    }
}

impl<N> CreateVTab<'_> for Table<N>
where
    N: doc::AsNode + 'static,
{
    const KIND: VTabKind = VTabKind::Default;
}

#[repr(C)]
pub struct Cursor<'vtab, N>
where
    N: doc::AsNode + 'static,
{
    // Must be first.
    base: sqlite3_vtab_cursor,
    // Columns of the virtual table.
    columns: &'vtab [Projection],
    // Document exposed by this Cursor.
    document: Option<&'static N>,
}

unsafe impl<'vtab, N> VTab<'vtab> for Table<N>
where
    N: doc::AsNode,
{
    type Aux = (Vec<Projection>, Rc<Cell<Option<&'static N>>>);
    type Cursor = Cursor<'vtab, N>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> Result<(String, Self)> {
        let (columns, document) = aux.unwrap();

        let schema = format!(
            "CREATE TABLE x({})",
            columns
                .iter()
                .map(|Projection { field, .. }| format!(
                    "\"{}\"",
                    rusqlite::vtab::escape_double_quote(field)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let vtab = Self {
            base: sqlite3_vtab::default(),
            columns: columns.clone(),
            next_document: document.clone(),
        };
        Ok((schema, vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        let document = self.next_document.get();

        Ok(Cursor {
            base: sqlite3_vtab_cursor::default(),
            columns: &&self.columns,
            document,
        })
    }
}

unsafe impl<N> VTabCursor for Cursor<'_, N>
where
    N: doc::AsNode,
{
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> Result<()> {
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.document = None;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.document.is_none()
    }

    fn column(&self, ctx: &mut Context, index: c_int) -> Result<()> {
        let doc = self.document.unwrap();
        let Projection {
            field: _,
            ptr,
            is_format_integer,
            is_format_number,
            is_content_encoding_base64,
        } = &self.columns[index as usize];

        use doc::Node;
        match ptr.query(doc).map(doc::AsNode::as_node) {
            None | Some(Node::Null) => ctx.set_result(&None::<bool>),
            Some(Node::Bool(b)) => ctx.set_result(&b),
            Some(Node::String(s)) => {
                if *is_format_integer {
                    if let Ok(i) = s.parse::<i64>() {
                        return ctx.set_result(&i);
                    }
                }
                if *is_format_number {
                    if let Ok(f) = s.parse::<f64>() {
                        return ctx.set_result(&f);
                    }
                }
                if *is_content_encoding_base64 {
                    if let Ok(b) = base64::decode(s) {
                        return ctx.set_result(&b);
                    }
                }
                ctx.set_result(&s)
            }
            Some(Node::Bytes(b)) => ctx.set_result(&b),
            Some(Node::Number(json::Number::Float(f))) => ctx.set_result(&f),
            Some(Node::Number(json::Number::Signed(s))) => ctx.set_result(&s),
            Some(Node::Number(json::Number::Unsigned(u))) => ctx.set_result(&u),
            Some(n @ Node::Array(_)) => ctx.set_result(&serde_json::to_string(&n).unwrap()),
            Some(n @ Node::Object(_)) => ctx.set_result(&serde_json::to_string(&n).unwrap()),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}
