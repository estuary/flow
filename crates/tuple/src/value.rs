use super::{TupleDepth, TuplePack, VersionstampOffset};
use serde_json::Value;
use std::io;

impl TuplePack for Value {
    fn pack<W: io::Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> io::Result<VersionstampOffset> {
        match self {
            Value::Null => Option::<()>::None.pack(w, tuple_depth),
            Value::Bool(b) => b.pack(w, tuple_depth),
            Value::Number(n) => {
                if let Some(n) = n.as_u64() {
                    n.pack(w, tuple_depth)
                } else if let Some(n) = n.as_i64() {
                    n.pack(w, tuple_depth)
                } else {
                    n.as_f64().unwrap().pack(w, tuple_depth)
                }
            }
            Value::String(s) => s.pack(w, tuple_depth),
            Value::Array(_) | Value::Object(_) => {
                serde_json::to_vec(self).unwrap().pack(w, tuple_depth)
            }
        }
    }
}
