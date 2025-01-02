use crate::{compare::compare, AsNode, Node, OwnedNode, Pointer, SerPolicy};
use bytes::BufMut;
use std::{
    borrow::Cow,
    sync::atomic::{AtomicBool, Ordering},
};
use tuple::TuplePack;

/// JSON pointer of the synthetic projection of the truncation indicator.
pub const TRUNCATION_INDICATOR_PTR: &str = "/_meta/flow_truncated";

/// Extractor extracts locations from documents, and encapsulates various
/// details of precisely how that's done.
#[derive(Debug, Clone)]
pub struct Extractor {
    ptr: Pointer,
    policy: SerPolicy,
    default: serde_json::Value,
    magic: Option<Magic>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Magic {
    UuidV1DateTime,
    TruncationIndicator,
}

impl Extractor {
    /// Build an Extractor for the JSON pointer.
    /// If the location doesn't exist, then `null` is extracted.
    pub fn new<P: Into<Pointer>>(ptr: P, policy: &SerPolicy) -> Self {
        Self {
            ptr: ptr.into(),
            policy: policy.clone(),
            default: serde_json::Value::Null,
            magic: None,
        }
    }

    /// Build an extractor for the JSON pointer.
    /// If the location doesn't exist, the provided value is extracted instead.
    pub fn with_default(ptr: &str, policy: &SerPolicy, default: serde_json::Value) -> Self {
        Self {
            ptr: Pointer::from(ptr),
            policy: policy.clone(),
            default,
            magic: None,
        }
    }

    /// Build an extractor for the JSON pointer, which is a v1 UUID.
    pub fn for_uuid_v1_date_time(ptr: &str) -> Self {
        Self {
            ptr: Pointer::from(ptr),
            policy: SerPolicy::noop(),
            default: serde_json::Value::Null,
            magic: Some(Magic::UuidV1DateTime),
        }
    }

    pub fn for_truncation_indicator() -> Self {
        Self {
            ptr: Pointer::empty(),
            policy: SerPolicy::noop(),
            default: serde_json::Value::Null,
            magic: Some(Magic::TruncationIndicator),
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

        match self.magic {
            None => { /* sorry, kid, I guess your parents aren't coming back */ }
            Some(Magic::UuidV1DateTime) => {
                if let Some(date_time) = match node.as_node() {
                    Node::String(s) => Some(s),
                    _ => None,
                }
                .and_then(|s| proto_gazette::uuid::parse_str(s).ok())
                .and_then(|(_producer, clock, _flags)| {
                    let (seconds, nanos) = clock.to_unix();
                    time::OffsetDateTime::from_unix_timestamp_nanos(
                        seconds as i128 * 1_000_000_000 + nanos as i128,
                    )
                    .ok()
                }) {
                    // Use a custom format and not time::format_description::well_known::Rfc3339,
                    // because date-times must be right-padded out to the nanosecond to ensure
                    // that lexicographic ordering matches temporal ordering.
                    let format = time::macros::format_description!(
                        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z"
                    );
                    return Err(Cow::Owned(serde_json::Value::String(
                        date_time
                            .format(format)
                            .expect("rfc3339 format always succeeds"),
                    )));
                }
            }
            Some(Magic::TruncationIndicator) => {
                // The real magic is behind some _other_ curtain.
                // We just set a constant false value here.
                // If we end up pruning some part of the document, we'll go back and change this
                // value retroactively as part of `extract_all_indicate_truncation`.
                return Err(Cow::Owned(serde_json::Value::Bool(false)));
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
        let indicator = &AtomicBool::new(false);
        Extractor::extract_all_indicate_truncation(doc, extractors, out, indicator)
    }

    /// Extract a packed tuple representation from an instance of doc::AsNode.
    pub fn extract_all_indicate_truncation<N: AsNode>(
        doc: &N,
        extractors: &[Self],
        out: &mut bytes::BytesMut,
        indicator: &AtomicBool,
    ) -> bytes::Bytes {
        let mut w = out.writer();

        // Truncation indicators are handled by having the extractor always write
        // a `false` value (0x26). We remember the byte offset of this value in the
        // encoded tuple, and will change it to `true` at the end if any value was
        // truncated.
        let mut projected_indicator_pos: Option<usize> = None;
        for ex in extractors {
            if ex.magic == Some(Magic::TruncationIndicator) {
                debug_assert!(
                    projected_indicator_pos.is_none(),
                    "extractors have multiple projections of truncation indicator"
                );
                projected_indicator_pos = Some(w.get_ref().len());
            }
            // Unwrap because Write is infallible for BytesMut.
            ex.extract_indicate_truncation(doc, &mut w, indicator)
                .unwrap();
        }

        let write_indicator = projected_indicator_pos.filter(|_| indicator.load(Ordering::SeqCst));
        if let Some(pos) = write_indicator {
            out[pos] = 0x27; // this is the Foundation tuple byte value of `true`
        }
        out.split().freeze()
    }

    pub fn extract_all_owned<'alloc>(
        doc: &OwnedNode,
        extractors: &[Self],
        out: &mut bytes::BytesMut,
    ) -> bytes::Bytes {
        let indicator = &AtomicBool::new(false);
        Extractor::extract_all_owned_indicate_truncation(doc, extractors, out, indicator)
    }

    /// Extract a packed tuple representation from an instance of doc::OwnedNode.
    pub fn extract_all_owned_indicate_truncation<'alloc>(
        doc: &OwnedNode,
        extractors: &[Self],
        out: &mut bytes::BytesMut,
        indicator: &AtomicBool,
    ) -> bytes::Bytes {
        match doc {
            OwnedNode::Heap(n) => {
                Self::extract_all_indicate_truncation(n.get(), extractors, out, indicator)
            }
            OwnedNode::Archived(n) => {
                Self::extract_all_indicate_truncation(n.get(), extractors, out, indicator)
            }
        }
    }

    /// Extract from an instance of doc::AsNode, writing a packed encoding into the writer.
    pub fn extract_indicate_truncation<N: AsNode, W: std::io::Write>(
        &self,
        doc: &N,
        w: &mut W,
        indicator: &AtomicBool,
    ) -> std::io::Result<()> {
        match self.query(doc) {
            Ok(v) => self
                .policy
                .with_truncation_indicator(v, indicator)
                .pack(w, tuple::TupleDepth::new().increment())?,
            Err(v) => self
                .policy
                .with_truncation_indicator(v.as_ref(), indicator)
                .pack(w, tuple::TupleDepth::new().increment())?,
        };
        Ok(())
    }

    /// Extract from an instance of doc::AsNode, writing a packed encoding into the writer.
    pub fn extract<N: AsNode, W: std::io::Write>(&self, doc: &N, w: &mut W) -> std::io::Result<()> {
        let indicator = &AtomicBool::new(false);
        self.extract_indicate_truncation(doc, w, indicator)
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
                // These differ only in counter bits.
                "66285dbc-543d-11ef-8001-69ef5bf77016", // Counter is zero.
                "66285dbc-543d-11ef-8401-69ef5bf77016",
                "66285dbc-543d-11ef-8801-69ef5bf77016",
                // Does not parse.
                "6d304974-1631-11ee-8401-whoops",
            ],
            "long-str": "very very very very very very very very very very very very long",
        });
        let policy = SerPolicy::truncate_strings(32);

        let extractors = vec![
            Extractor::new("/missing", &policy),
            Extractor::with_default("/missing-default", &policy, json!("default")),
            Extractor::new("/obj/true", &policy),
            Extractor::new("/fals", &policy),
            Extractor::new("/arr/0", &policy),
            Extractor::new("/unsi", &policy),
            Extractor::new("/doub", &policy),
            Extractor::new("/sign", &policy),
            Extractor::new("/obj", &policy),
            Extractor::new("/arr", &policy),
            Extractor::for_uuid_v1_date_time("/uuid-ts/0"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/1"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/2"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/3"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/4"),
            Extractor::for_uuid_v1_date_time("/uuid-ts/5"),
            Extractor::for_uuid_v1_date_time("/missing"),
            Extractor::for_uuid_v1_date_time("/fals"),
            Extractor::new("/long-str", &policy),
        ];

        let mut buffer = bytes::BytesMut::new();
        let indicator = AtomicBool::new(false);
        let packed =
            Extractor::extract_all_indicate_truncation(&v1, &extractors, &mut buffer, &indicator);
        assert!(indicator.load(std::sync::atomic::Ordering::SeqCst));
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
                "2023-06-28T20:29:46.494594504Z",
            ),
            String(
                "2023-06-29T03:07:35.005650904Z",
            ),
            String(
                "2024-08-06T21:46:55.543442800Z",
            ),
            String(
                "2024-08-06T21:46:55.543442804Z",
            ),
            String(
                "2024-08-06T21:46:55.543442808Z",
            ),
            String(
                "6d304974-1631-11ee-8401-whoops",
            ),
            Nil,
            Bool(
                false,
            ),
            String(
                "very very very very very very ve",
            ),
        ]
        "###);
    }

    #[test]
    fn test_setting_truncation_indicator() {
        let policy = SerPolicy {
            str_truncate_after: 7,
            ..SerPolicy::noop()
        };

        let doc = json!({
            "nested": {
                "big": "a string that will be truncated",
            },
            "smol": "notuchy",
        });

        let mut extractors = vec![
            Extractor::for_truncation_indicator(),
            Extractor::new("/smol", &policy),
        ];

        // Assert that the truncation indicator is false when no extracted
        // fields were affected by the SerPolicy.
        let mut buffer = bytes::BytesMut::new();
        let prune_indicator = AtomicBool::new(false);
        let packed = Extractor::extract_all_indicate_truncation(
            &doc,
            &extractors,
            &mut buffer,
            &prune_indicator,
        );
        assert!(!prune_indicator.load(std::sync::atomic::Ordering::SeqCst));
        let unpacked: Vec<tuple::Element> = tuple::unpack(&packed).unwrap();
        assert_eq!(tuple::Element::Bool(false), unpacked[0]);

        // Add an extractor for the root document, and assert that the truncation indicator
        // gets set to true.
        extractors.push(Extractor::new("", &policy));
        let mut buffer = bytes::BytesMut::new();
        let prune_indicator = AtomicBool::new(false);
        let packed = Extractor::extract_all_indicate_truncation(
            &doc,
            &extractors,
            &mut buffer,
            &prune_indicator,
        );
        assert!(prune_indicator.load(std::sync::atomic::Ordering::SeqCst));
        let unpacked: Vec<tuple::Element> = tuple::unpack(&packed).unwrap();
        assert_eq!(tuple::Element::Bool(true), unpacked[0]);
    }

    #[test]
    fn test_compare_objects() {
        let d1 = &json!({"a": 1, "b": 2, "c": 3});
        let d2 = &json!({"a": 2, "b": 1});

        let policy = SerPolicy::noop();
        let empty = || Extractor::new("", &policy);
        let a = || Extractor::new("/a", &policy);
        let b = || Extractor::new("/b", &policy);
        let c = || Extractor::with_default("/c", &policy, json!(3));

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

        let policy = SerPolicy::noop();
        let empty = || Extractor::new("", &policy);
        let zero = || Extractor::new("/0", &policy);
        let one = || Extractor::new("/1", &policy);
        let two = || Extractor::with_default("/2", &policy, json!(3));

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

        let policy = SerPolicy::noop();
        let missing = || Extractor::new("/does/not/exist", &policy);
        let a = || Extractor::new("/a", &policy);
        let b = || Extractor::new("/b", &policy);
        let c = || Extractor::new("/c", &policy);

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
