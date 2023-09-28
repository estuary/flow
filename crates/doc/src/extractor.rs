use crate::{compare::compare, AsNode, LazyNode, Node, OwnedNode, Pointer};
use bytes::BufMut;
use std::borrow::Cow;
use tuple::TuplePack;

/// Extractor extracts locations from documents, and encapsulates various
/// details of precisely how that's done.
#[derive(Debug, Clone)]
pub struct Extractor {
    ptr: Pointer,
    default: serde_json::Value,
    is_uuid_v1_date_time: bool,
}

impl Extractor {
    /// Build an Extractor for the JSON pointer.
    /// If the location doesn't exist, then `null` is extracted.
    pub fn new(ptr: &str) -> Self {
        Self {
            ptr: Pointer::from(ptr),
            default: serde_json::Value::Null,
            is_uuid_v1_date_time: false,
        }
    }

    /// Build an extractor for the JSON pointer.
    /// If the location doesn't exist, the provided value is extracted instead.
    pub fn with_default(ptr: &str, default: serde_json::Value) -> Self {
        Self {
            ptr: Pointer::from(ptr),
            default,
            is_uuid_v1_date_time: false,
        }
    }

    /// Build an extractor for the JSON pointer, which is a v1 UUID.
    pub fn for_uuid_v1_date_time(ptr: &str) -> Self {
        Self {
            ptr: Pointer::from(ptr),
            default: serde_json::Value::Null,
            is_uuid_v1_date_time: true,
        }
    }

    /// Query the value extracted from the document.
    /// The result is Ok if a literal document node is extracted,
    /// or Err if the document value doesn't exist and an alternative
    /// serde_json::Value was extracted.
    ///
    /// Most commonly this is a default value for the location
    /// and a borrowed Value is returned.
    ///
    /// Or, it may be a dynamic Value extracted from a UUID timestamp.
    pub fn query<'s, 'n, N: AsNode>(
        &'s self,
        doc: &'n N,
    ) -> Result<&'n N, Cow<'s, serde_json::Value>> {
        let Some(node) = self.ptr.query(doc) else {
            return Err(Cow::Borrowed(&self.default));
        };

        if self.is_uuid_v1_date_time {
            if let Some(date_time) = match node.as_node() {
                Node::String(s) => Some(s),
                _ => None,
            }
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .and_then(|u| u.get_timestamp())
            .and_then(|t| {
                let (seconds, nanos) = t.to_unix();
                time::OffsetDateTime::from_unix_timestamp_nanos(
                    seconds as i128 * 1_000_000_000 + nanos as i128,
                )
                .ok()
            }) {
                return Err(Cow::Owned(serde_json::Value::String(
                    date_time
                        .format(&time::format_description::well_known::Rfc3339)
                        .expect("rfc3339 format always succeeds"),
                )));
            }
        }

        Ok(node)
    }

    /// Extract a packed tuple representation from an instance of doc::AsNode.
    pub fn extract_all<N: AsNode>(
        doc: &N,
        extractors: &[Self],
        out: &mut bytes::BytesMut,
    ) -> bytes::Bytes {
        let mut w = out.writer();

        for ex in extractors {
            // Unwrap because Write is infallible for BytesMut.
            ex.extract(doc, &mut w).unwrap();
        }
        out.split().freeze()
    }

    /// Extract a packed tuple representation from an instance of doc::OwnedNode.
    pub fn extract_all_owned<'alloc>(
        doc: &OwnedNode,
        extractors: &[Self],
        out: &mut bytes::BytesMut,
    ) -> bytes::Bytes {
        match doc {
            OwnedNode::Heap(n) => Self::extract_all(n.get(), extractors, out),
            OwnedNode::Archived(n) => Self::extract_all(n.get(), extractors, out),
        }
    }

    /// Extract from an instance of doc::AsNode, writing a packed encoding into the writer.
    pub fn extract<N: AsNode, W: std::io::Write>(&self, doc: &N, w: &mut W) -> std::io::Result<()> {
        match self.query(doc) {
            Ok(v) => v.as_node().pack(w, tuple::TupleDepth::new().increment())?,
            Err(v) => v.pack(w, tuple::TupleDepth::new().increment())?,
        };
        Ok(())
    }

    /// Compare the deep ordering of `lhs` and `rhs` with respect to a composite key.
    pub fn compare_key<L: AsNode, R: AsNode>(key: &[Self], lhs: &L, rhs: &R) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        key.iter()
            .map(|ex| match (ex.query(lhs), ex.query(rhs)) {
                (Ok(lhs), Ok(rhs)) => compare(lhs, rhs),
                (Err(lhs), Ok(rhs)) => compare(lhs.as_ref(), rhs),
                (Ok(lhs), Err(rhs)) => compare(lhs, rhs.as_ref()),
                (Err(lhs), Err(rhs)) => compare(lhs.as_ref(), rhs.as_ref()),
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    }

    /// Compare the deep ordering of `lhs` and `rhs` with respect to a composite key.
    pub fn compare_key_lazy<'alloc, 'l, 'r, L: AsNode, R: AsNode>(
        key: &[Self],
        lhs: &LazyNode<'alloc, 'l, L>,
        rhs: &LazyNode<'alloc, 'r, R>,
    ) -> std::cmp::Ordering {
        match (lhs, rhs) {
            (LazyNode::Heap(lhs), LazyNode::Heap(rhs)) => Self::compare_key(key, lhs, rhs),
            (LazyNode::Heap(lhs), LazyNode::Node(rhs)) => Self::compare_key(key, lhs, *rhs),
            (LazyNode::Node(lhs), LazyNode::Heap(rhs)) => Self::compare_key(key, *lhs, rhs),
            (LazyNode::Node(lhs), LazyNode::Node(rhs)) => Self::compare_key(key, *lhs, *rhs),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::cmp::Ordering;

    #[test]
    fn test_extracted_tuple() {
        let v1 = serde_json::json!({
            "a": "value",
            "obj": {"tru": true, "other": "value"},
            "fals": false,
            "arr": ["foo"],
            "doub": 1.3,
            "unsi": 2,
            "sign": -30,
            "uuid-ts": [
                "85bad119-15f2-11ee-8401-43f05f562888",
                "1878923d-162a-11ee-8401-43f05f562888",
                "6d304974-1631-11ee-8401-whoops"
            ]
        });

        let extractors = vec![
            Extractor::new("/missing"),
            Extractor::with_default("/missing-default", json!("default")),
            Extractor::new("/obj/true"),
            Extractor::new("/fals"),
            Extractor::new("/arr/0"),
            Extractor::new("/unsi"),
            Extractor::new("/doub"),
            Extractor::new("/sign"),
            Extractor::new("/obj"),
            Extractor::new("/arr"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/0"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/1"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/2"),
            Extractor::for_uuid_v1_date_time("/missing"),
            Extractor::for_uuid_v1_date_time("/fals"),
        ];

        let mut buffer = bytes::BytesMut::new();
        let packed = Extractor::extract_all(&v1, &extractors, &mut buffer);
        let unpacked: Vec<tuple::Element> = tuple::unpack(&packed).unwrap();

        insta::assert_debug_snapshot!(unpacked, @r###"
        [
            Nil,
            String(
                "default",
            ),
            Nil,
            Bool(
                false,
            ),
            String(
                "foo",
            ),
            Int(
                2,
            ),
            Double(
                1.3,
            ),
            Int(
                -30,
            ),
            Bytes(
                b"\x7b\x22other\x22\x3a\x22value\x22\x2c\x22tru\x22\x3atrue\x7d",
            ),
            Bytes(
                b"\x5b\x22foo\x22\x5d",
            ),
            String(
                "2023-06-28T20:29:46.4945945Z",
            ),
            String(
                "2023-06-29T03:07:35.0056509Z",
            ),
            String(
                "6d304974-1631-11ee-8401-whoops",
            ),
            Nil,
            Bool(
                false,
            ),
        ]
        "###);
    }

    #[test]
    fn test_compare_objects() {
        let d1 = &json!({"a": 1, "b": 2, "c": 3});
        let d2 = &json!({"a": 2, "b": 1});

        let empty = || Extractor::new("");
        let a = || Extractor::new("/a");
        let b = || Extractor::new("/b");
        let c = || Extractor::with_default("/c", json!(3));

        // No pointers => always equal.
        assert_eq!(
            Extractor::compare_key(&[] as &[Extractor], d1, d2),
            Ordering::Equal
        );
        // Deep compare of document roots.
        assert_eq!(Extractor::compare_key(&[empty()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(Extractor::compare_key(&[a()], d1, d2), Ordering::Less);
        assert_eq!(Extractor::compare_key(&[b()], d1, d2), Ordering::Greater);
        assert_eq!(Extractor::compare_key(&[c()], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(Extractor::compare_key(&[c(), a()], d1, d2), Ordering::Less);
        assert_eq!(
            Extractor::compare_key(&[c(), b()], d1, d2),
            Ordering::Greater
        );
        assert_eq!(Extractor::compare_key(&[c(), c()], d1, d2), Ordering::Equal);
        assert_eq!(
            Extractor::compare_key(&[c(), c(), c(), a()], d1, d2),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_arrays() {
        let d1 = &json!([1, 2, 3]);
        let d2 = &json!([2, 1]);

        let empty = || Extractor::new("");
        let zero = || Extractor::new("/0");
        let one = || Extractor::new("/1");
        let two = || Extractor::with_default("/2", json!(3));

        // No pointers => always equal.
        assert_eq!(
            Extractor::compare_key(&[] as &[Extractor], d1, d2),
            Ordering::Equal
        );
        // Deep compare of document roots.
        assert_eq!(Extractor::compare_key(&[empty()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(Extractor::compare_key(&[zero()], d1, d2), Ordering::Less);
        assert_eq!(Extractor::compare_key(&[one()], d1, d2), Ordering::Greater);
        assert_eq!(Extractor::compare_key(&[two()], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(
            Extractor::compare_key(&[two(), zero()], d1, d2),
            Ordering::Less
        );
        assert_eq!(
            Extractor::compare_key(&[two(), one()], d1, d2),
            Ordering::Greater
        );
        assert_eq!(
            Extractor::compare_key(&[two(), two()], d1, d2),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_missing() {
        let d1 = &json!({"a": null, "c": 3});
        let d2 = &json!({"b": 2});

        let missing = || Extractor::new("/does/not/exist");
        let a = || Extractor::new("/a");
        let b = || Extractor::new("/b");
        let c = || Extractor::new("/c");

        assert_eq!(
            Extractor::compare_key(&[missing()], d1, d2),
            Ordering::Equal
        );
        // Key exists at |d1| but not |d2|. |d2| value is implicitly null.
        assert_eq!(Extractor::compare_key(&[c()], d1, d2), Ordering::Greater);
        // Key exists at |d2| but not |d1|. |d1| value is implicitly null.
        assert_eq!(Extractor::compare_key(&[b()], d1, d2), Ordering::Less);
        // Key exists at |d1| but not |d2|. Both are null (implicit and explicit).
        assert_eq!(Extractor::compare_key(&[a()], d1, d2), Ordering::Equal);
    }
}
