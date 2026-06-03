use crate::{OwnedNode, SerPolicy};
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
    pub(crate) ptr: json::Pointer,
    policy: SerPolicy,
    default: serde_json::Value,
    magic: Option<Magic>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Magic {
    UuidV1DateTime,
    TruncationIndicator,
}

/// Encoding of an extracted composite key or value.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Encoding {
    /// FoundationDB packed tuple of self-delimiting elements. Opaque bytes:
    /// nested objects and arrays collapse to tuple bytes. Read by protobuf
    /// connectors, and used for all internal key handling (ordering, hashing).
    Packed,
    /// JSON array `[v0, v1, ...]`, preserving nested objects and arrays as
    /// JSON. Read by connectors that speak JSON rather than protobuf.
    Json,
}

/// Planner-facing classification of an extractor.
#[derive(Debug, Copy, Clone)]
pub(crate) enum PlanKind<'a> {
    /// Sibling-leaf extractor. A candidate for inclusion in a planner block.
    MergeJoinLeaf {
        parent: &'a [json::ptr::Token],
        name: &'a str,
    },
    /// Truncation indicator. Must run in place through the reference path so
    /// its placeholder byte lands at the right tuple position.
    TruncationIndicator,
    /// Root document (empty pointer) or array-index terminal. Cannot be
    /// included in a planner block.
    Other,
}

impl Extractor {
    /// Build an Extractor for the JSON pointer.
    /// If the location doesn't exist, then `null` is extracted.
    pub fn new<P: Into<json::Pointer>>(ptr: P, policy: &SerPolicy) -> Self {
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
            ptr: json::Pointer::from(ptr),
            policy: policy.clone(),
            default,
            magic: None,
        }
    }

    /// Build an extractor for the JSON pointer, which is a v1 UUID.
    pub fn for_uuid_v1_date_time(ptr: &str) -> Self {
        Self {
            ptr: json::Pointer::from(ptr),
            policy: SerPolicy::noop(),
            default: serde_json::Value::Null,
            magic: Some(Magic::UuidV1DateTime),
        }
    }

    pub fn for_truncation_indicator() -> Self {
        Self {
            ptr: json::Pointer::empty(),
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
    pub fn query<'s, 'n, N: json::AsNode>(
        &'s self,
        doc: &'n N,
    ) -> Result<&'n N, Cow<'s, serde_json::Value>> {
        self.value_from_resolved(self.ptr.query(doc))
    }

    /// Extract a composite key or value from an instance of json::AsNode into
    /// `out`, in `encoding`. Pass `Some(indicator)` to track truncation across
    /// this and prior extractions (see [`Encoding`] and the truncation-indicator
    /// extractor); pass `None` when the extractors never truncate (e.g. keys,
    /// which use a no-op SerPolicy).
    pub fn extract_all<N: json::AsNode>(
        doc: &N,
        extractors: &[Self],
        encoding: Encoding,
        out: &mut bytes::BytesMut,
        indicator: Option<&AtomicBool>,
    ) {
        write_encoded(encoding, out, indicator, ResolveSlice { extractors, doc });
    }

    /// Extract a composite key or value from a doc::OwnedNode. See [`Self::extract_all`].
    pub fn extract_all_owned(
        doc: &OwnedNode,
        extractors: &[Self],
        encoding: Encoding,
        out: &mut bytes::BytesMut,
        indicator: Option<&AtomicBool>,
    ) {
        match doc {
            OwnedNode::Heap(n) => match n.access() {
                Ok(heap_node) => {
                    Self::extract_all(&heap_node, extractors, encoding, out, indicator)
                }
                Err(embedded) => {
                    Self::extract_all(embedded.get(), extractors, encoding, out, indicator)
                }
            },
            OwnedNode::Archived(n) => {
                Self::extract_all(n.get(), extractors, encoding, out, indicator)
            }
        }
    }

    /// Extract this single extractor into `w`, in `encoding` — one packed-tuple
    /// element or one JSON value, with no surrounding composite framing.
    /// Optionally track truncation across this and prior extractions via
    /// `indicator` (pass `None` when tracking isn't required).
    pub fn extract<N: json::AsNode, W: std::io::Write>(
        &self,
        doc: &N,
        encoding: Encoding,
        w: &mut W,
        indicator: Option<&AtomicBool>,
    ) -> std::io::Result<()> {
        // A lone element is never backpatched, so a substituted indicator is
        // only ever written to (recording a truncation we then discard).
        let throwaway = AtomicBool::new(false);
        let indicator = indicator.unwrap_or(&throwaway);
        let resolved = self.ptr.query(doc);

        match encoding {
            Encoding::Packed => self.write_packed_from_resolved(resolved, w, indicator),
            Encoding::Json => self.write_json_from_resolved(resolved, w, indicator),
        }
    }

    /// Compare the deep ordering of `lhs` and `rhs` with respect to a composite key.
    pub fn compare_key<L: json::AsNode, R: json::AsNode>(
        key: &[Self],
        lhs: &L,
        rhs: &R,
    ) -> std::cmp::Ordering {
        use json::node::compare;
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

    /// Return the canonical hash of a packed tuple encoding, as produced by extract_all().
    ///
    /// The hash is the top 32 bits of a HighwayHash over the tuple's bytes
    /// using a fixed public (non-cryptographic) key.
    ///
    /// This routine's results are identical to Go's flow.PackedKeyHash_HH64.
    pub fn packed_hash(packed_key: &[u8]) -> u32 {
        use highway::HighwayHash;

        let mut hasher = highway::HighwayHasher::new(HIGHWAY_KEY);
        hasher.append(packed_key);
        (hasher.finalize64() >> 32) as u32
    }

    /// Determine whether a packed-key `prefix` fully contains a key of
    /// `num_components` components, returning `Some(len)` with the key's byte
    /// length when it does (the key is then `prefix[..len]`), or `None` when
    /// the key may have been truncated and must be re-extracted from its
    /// source document.
    ///
    /// Callers having (only) a key prefix should use this to skip re-extraction
    /// whenever the prefix suffices.
    pub fn packed_key_prefix_len(prefix: &[u8], num_components: usize) -> Option<usize> {
        let mut rest = prefix;
        for _ in 0..num_components {
            // Depth 1 matches how each component is packed by `extract_all` and
            // decodes a single element; depth 0 would instead fold the trailing
            // zero-padding into a tuple of spurious Nil elements.
            let (next, _elem) = <tuple::Element as tuple::TupleUnpack>::unpack(
                rest,
                tuple::TupleDepth::new().increment(),
            )
            .ok()?;
            rest = next;
        }
        // A decode finishing strictly before the prefix's end is whole, while
        // one that consumes the entire prefix is conservatively reported as
        // `None` (possibly-truncated), costing a redundant re-extraction.
        let consumed = prefix.len() - rest.len();
        (consumed < prefix.len()).then_some(consumed)
    }

    pub(crate) fn is_truncation_indicator(&self) -> bool {
        matches!(self.magic, Some(Magic::TruncationIndicator))
    }

    pub(crate) fn plan_kind(&self) -> PlanKind<'_> {
        if self.is_truncation_indicator() {
            return PlanKind::TruncationIndicator;
        }

        match self.ptr.0.split_last() {
            Some((json::ptr::Token::Property(name), parent)) => {
                PlanKind::MergeJoinLeaf { parent, name }
            }
            // Root document (empty pointer) or array-index terminal (e.g.
            // /arr/0) — neither can merge-join against an object's fields,
            // so the plan runs them as singles through the reference path.
            _ => PlanKind::Other,
        }
    }

    fn value_from_resolved<'s, 'n, N: json::AsNode>(
        &'s self,
        resolved: Option<&'n N>,
    ) -> Result<&'n N, Cow<'s, serde_json::Value>> {
        let Some(node) = resolved else {
            return Err(Cow::Borrowed(&self.default));
        };

        match self.magic {
            None => { /* sorry, kid, I guess your parents aren't coming back */ }
            Some(Magic::UuidV1DateTime) => {
                if let Some(date_time) = match node.as_node() {
                    json::Node::String(s) => Some(s),
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
                // value retroactively in `write_encoded` (see the indicator backpatch).
                return Err(Cow::Owned(serde_json::Value::Bool(false)));
            }
        }

        Ok(node)
    }

    /// Write the already-resolved node (or its default / magic value) as one
    /// packed-tuple element into `w`. The packed analog of
    /// [`Self::write_json_from_resolved`], used when a leaf node has already been
    /// found, skipping the per-extractor `Pointer::query`.
    pub(crate) fn write_packed_from_resolved<N: json::AsNode, W: std::io::Write>(
        &self,
        resolved: Option<&N>,
        w: &mut W,
        indicator: &AtomicBool,
    ) -> std::io::Result<()> {
        match self.value_from_resolved(resolved) {
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

    /// JSON analog of [`Self::write_packed_from_resolved`]: write the element for
    /// an already-resolved node (or its default / magic value) as one JSON value
    /// into `w`. Any composite framing (`[` / `,` / `]`) and the
    /// truncation-indicator backpatch are the caller's; a truncation indicator
    /// resolves to a `false` literal here.
    pub(crate) fn write_json_from_resolved<N: json::AsNode, W: std::io::Write>(
        &self,
        resolved: Option<&N>,
        w: &mut W,
        indicator: &AtomicBool,
    ) -> std::io::Result<()> {
        // Our SerNode values never produce a serde serialization error, so any
        // failure is an IO error surfaced from `w`.
        match self.value_from_resolved(resolved) {
            Ok(node) => serde_json::to_writer(
                &mut *w,
                &self.policy.with_truncation_indicator(node, indicator),
            ),
            Err(value) => serde_json::to_writer(
                &mut *w,
                &self
                    .policy
                    .with_truncation_indicator(value.as_ref(), indicator),
            ),
        }
        .map_err(std::io::Error::other)
    }
}

/// Drives in-order enumeration of `(extractor, resolved-node)` pairs for
/// [`write_encoded`], where `resolved` is the node at the extractor's pointer
/// (`None` when absent), prior to default / magic resolution. Implemented by
/// the flat slice path ([`ResolveSlice`]) and the block-merge-join plan path
/// (`ExtractorPlan`). The generic `for_each` keeps the per-element callback
/// statically dispatched through the hot extraction loop.
pub(crate) trait Resolve<'n, N: json::AsNode> {
    fn for_each(self, emit: impl FnMut(&Extractor, Option<&'n N>));
}

/// [`Resolve`] over a flat extractor slice: each extractor is resolved through
/// the per-extractor reference path (`Pointer::query`).
pub(crate) struct ResolveSlice<'a, 'n, N> {
    pub extractors: &'a [Extractor],
    pub doc: &'n N,
}

impl<'a, 'n, N: json::AsNode> Resolve<'n, N> for ResolveSlice<'a, 'n, N> {
    #[inline]
    fn for_each(self, mut emit: impl FnMut(&Extractor, Option<&'n N>)) {
        for ex in self.extractors {
            emit(ex, ex.ptr.query(self.doc));
        }
    }
}

/// Write a composite tuple into `out` in the requested `encoding`, resolving
/// elements via `resolver` (a flat slice or a compiled plan).
///
/// A truncation-indicator extractor writes a `false` placeholder whose byte
/// offset is remembered and retroactively flipped to `true` if `indicator` was
/// set — by an element extracted here, or by the caller before this call.
/// `None` substitutes a throwaway indicator, appropriate when the extractors
/// never truncate.
pub(crate) fn write_encoded<'n, N, R>(
    encoding: Encoding,
    out: &mut bytes::BytesMut,
    indicator: Option<&AtomicBool>,
    resolver: R,
) where
    N: json::AsNode,
    R: Resolve<'n, N>,
{
    // Substitute a throwaway indicator when the caller doesn't track truncation.
    // It's never read back, so a truncation it records is simply discarded.
    let standin;
    let indicator = match indicator {
        Some(indicator) => indicator,
        None => {
            standin = AtomicBool::new(false);
            &standin
        }
    };

    // Truncation indicators are handled by writing a `false` placeholder and
    // remembering its byte offset, to be flipped to `true` once the final
    // indicator state is known (see `finalize_*_truncation_indicator`).
    let mut indicator_pos: Option<usize> = None;

    match encoding {
        Encoding::Packed => {
            {
                let mut w = (&mut *out).writer();
                resolver.for_each(|ex, resolved| {
                    if ex.is_truncation_indicator() {
                        debug_assert!(
                            indicator_pos.is_none(),
                            "extractors have multiple projections of truncation indicator"
                        );
                        indicator_pos = Some(w.get_ref().len());
                    }
                    // Writing to BytesMut is infallible.
                    ex.write_packed_from_resolved(resolved, &mut w, indicator)
                        .unwrap();
                });
            }
            finalize_packed_truncation_indicator(out, indicator_pos, indicator);
        }
        Encoding::Json => {
            // We frame the array by hand (rather than serde's SerializeSeq) so
            // we can note the byte offset of a truncation-indicator element and
            // flip it once the final indicator state is known.
            out.put_u8(b'[');
            let mut first = true;
            resolver.for_each(|ex, resolved| {
                if !first {
                    out.put_u8(b',');
                }
                first = false;

                if ex.is_truncation_indicator() {
                    debug_assert!(
                        indicator_pos.is_none(),
                        "extractors have multiple projections of truncation indicator"
                    );
                    indicator_pos = Some(out.len());
                }
                let mut w = (&mut *out).writer();
                // Writing into a BytesMut is infallible.
                ex.write_json_from_resolved(resolved, &mut w, indicator)
                    .expect("serialization into BytesMut is infallible");
            });
            out.put_u8(b']');
            finalize_json_truncation_indicator(out, indicator_pos, indicator);
        }
    }
}

/// Retroactively flip the packed truncation-indicator placeholder to `true` if
/// anything was truncated during the enclosing extract.
pub(crate) fn finalize_packed_truncation_indicator(
    out: &mut bytes::BytesMut,
    position: Option<usize>,
    indicator: &AtomicBool,
) {
    let write_indicator = position.filter(|_| indicator.load(Ordering::SeqCst));
    if let Some(pos) = write_indicator {
        out[pos] = 0x27; // this is the Foundation tuple byte value of `true`
    }
}

/// JSON analog of [`finalize_packed_truncation_indicator`]: flip a truncation
/// indicator's `false` placeholder (5 bytes) to `true ` if anything truncated.
/// The trailing space keeps the byte length stable for an in-place edit and is
/// insignificant JSON whitespace.
pub(crate) fn finalize_json_truncation_indicator(
    out: &mut bytes::BytesMut,
    position: Option<usize>,
    indicator: &AtomicBool,
) {
    if let Some(pos) = position.filter(|_| indicator.load(Ordering::SeqCst)) {
        out[pos..pos + 5].copy_from_slice(b"true ");
    }
}

/// Fixed 32-byte key used with HighwayHash to derive a canonical hash from
/// a packed tuple encoding. Matches the Go implementation in go/flow/mapping.go.
pub const HIGHWAY_KEY: highway::Key = highway::Key([
    u64::from_le_bytes([0xba, 0x73, 0x7e, 0x89, 0x15, 0x52, 0x38, 0xd4]),
    u64::from_le_bytes([0x7d, 0x80, 0x67, 0xc3, 0x5a, 0xad, 0x4d, 0x25]),
    u64::from_le_bytes([0xec, 0xdd, 0x1c, 0x34, 0x88, 0x22, 0x7e, 0x01]),
    u64::from_le_bytes([0x1f, 0xfa, 0x48, 0x0c, 0x02, 0x2b, 0xd3, 0xba]),
]);

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
        Extractor::extract_all(
            &v1,
            &extractors,
            Encoding::Packed,
            &mut buffer,
            Some(&indicator),
        );
        let packed = buffer.split().freeze();
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
    fn test_extract_all_to_json() {
        // Same fixture and extractors as `test_extracted_tuple`, but emitted
        // as a JSON array rather than a packed tuple. Note that objects and
        // arrays survive as JSON (vs. opaque bytes in the packed form), and
        // that the long string is still truncated by the SerPolicy.
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
                "66285dbc-543d-11ef-8401-69ef5bf77016",
                "6d304974-1631-11ee-8401-whoops", // Does not parse.
            ],
            "long-str": "very very very very very very very very very very very very long",
        });
        let policy = SerPolicy::truncate_strings(32);

        let extractors = vec![
            Extractor::new("/missing", &policy),
            Extractor::with_default("/missing-default", &policy, json!("default")),
            Extractor::new("/obj/tru", &policy),
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
            Extractor::new("/long-str", &policy),
        ];

        let mut buffer = bytes::BytesMut::new();
        Extractor::extract_all(&v1, &extractors, Encoding::Json, &mut buffer, None);
        let out = String::from_utf8(buffer.to_vec()).unwrap();

        insta::assert_snapshot!(out, @r###"[null,"default",true,false,"foo",2,1.3,-30,{"other":"value","tru":true},["foo"],"2023-06-28T20:29:46.494594504Z","2024-08-06T21:46:55.543442804Z","6d304974-1631-11ee-8401-whoops","very very very very very very ve"]"###);
    }

    #[test]
    fn test_extract_all_to_json_truncation() {
        let policy = SerPolicy {
            str_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let doc = json!({"a": "xxxxxx", "b": "ok"});

        // The indicator precedes the truncating value `/a`, exercising the
        // retroactive flip: the `false` placeholder becomes `true ` (note the
        // byte-preserving trailing space) once `/a` truncates.
        let extractors = vec![
            Extractor::new("/b", &policy),
            Extractor::for_truncation_indicator(),
            Extractor::new("/a", &policy),
        ];
        let mut buffer = bytes::BytesMut::new();
        let indicator = AtomicBool::new(false);
        Extractor::extract_all(
            &doc,
            &extractors,
            Encoding::Json,
            &mut buffer,
            Some(&indicator),
        );
        assert!(indicator.load(std::sync::atomic::Ordering::SeqCst));
        let out = String::from_utf8(buffer.to_vec()).unwrap();
        insta::assert_snapshot!(out, @r###"["ok",true ,"xxx"]"###);
        // The inserted whitespace keeps the output valid JSON.
        serde_json::from_str::<serde_json::Value>(&out).unwrap();

        // Nothing truncates: the placeholder stays `false`, and an indicator
        // pre-set by an earlier (document) extraction still flips it to true.
        let doc = json!({"a": "ok", "b": "ok"});
        for preset in [false, true] {
            let mut buffer = bytes::BytesMut::new();
            let indicator = AtomicBool::new(preset);
            Extractor::extract_all(
                &doc,
                &extractors,
                Encoding::Json,
                &mut buffer,
                Some(&indicator),
            );
            let out = String::from_utf8(buffer.to_vec()).unwrap();
            if preset {
                assert_eq!(out, r#"["ok",true ,"ok"]"#);
            } else {
                assert_eq!(out, r#"["ok",false,"ok"]"#);
            }
        }
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
        Extractor::extract_all(
            &doc,
            &extractors,
            Encoding::Packed,
            &mut buffer,
            Some(&prune_indicator),
        );
        let packed = buffer.split().freeze();
        assert!(!prune_indicator.load(std::sync::atomic::Ordering::SeqCst));
        let unpacked: Vec<tuple::Element> = tuple::unpack(&packed).unwrap();
        assert_eq!(tuple::Element::Bool(false), unpacked[0]);

        // Add an extractor for the root document, and assert that the truncation indicator
        // gets set to true.
        extractors.push(Extractor::new("", &policy));
        let mut buffer = bytes::BytesMut::new();
        let prune_indicator = AtomicBool::new(false);
        Extractor::extract_all(
            &doc,
            &extractors,
            Encoding::Packed,
            &mut buffer,
            Some(&prune_indicator),
        );
        let packed = buffer.split().freeze();
        assert!(prune_indicator.load(std::sync::atomic::Ordering::SeqCst));
        let unpacked: Vec<tuple::Element> = tuple::unpack(&packed).unwrap();
        assert_eq!(tuple::Element::Bool(true), unpacked[0]);
    }

    #[test]
    fn test_extract_single_element() {
        let policy = SerPolicy::noop();
        let doc = json!({"obj": {"b": 2}, "s": "hi"});

        for (ptr, want_json) in [
            ("/s", r#""hi""#),
            ("/obj", r#"{"b":2}"#),
            ("/missing", "null"),
        ] {
            let ex = Extractor::new(ptr, &policy);

            // Packed `extract` of one element equals the one-element composite:
            // the packed encoding has no array framing to differ on.
            let mut single = Vec::new();
            ex.extract(&doc, Encoding::Packed, &mut single, None)
                .unwrap();
            let mut composite = bytes::BytesMut::new();
            Extractor::extract_all(
                &doc,
                std::slice::from_ref(&ex),
                Encoding::Packed,
                &mut composite,
                None,
            );
            assert_eq!(&single[..], &composite[..], "packed {ptr}");

            // JSON `extract` writes the bare value, without the `[...]` framing
            // that `extract_all` adds around a composite.
            let mut json_out = Vec::new();
            ex.extract(&doc, Encoding::Json, &mut json_out, None)
                .unwrap();
            assert_eq!(
                String::from_utf8(json_out).unwrap(),
                want_json,
                "json {ptr}"
            );
        }

        // A truncating value still sets the shared indicator as a side effect,
        // even though a lone element is never backpatched.
        let trunc_policy = SerPolicy {
            str_truncate_after: 1,
            ..SerPolicy::noop()
        };
        let ex = Extractor::new("/s", &trunc_policy);
        let indicator = AtomicBool::new(false);
        let mut json_out = Vec::new();
        ex.extract(&doc, Encoding::Json, &mut json_out, Some(&indicator))
            .unwrap();
        assert_eq!(String::from_utf8(json_out).unwrap(), r#""h""#);
        assert!(indicator.load(std::sync::atomic::Ordering::SeqCst));
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
    fn test_packed_hash_regression() {
        // Regression test matching Go's TestHighwayHashRegression in go/flow/mapping_test.go.
        // Expect that small (e.g. single bit) changes to the input wildly change the output.
        use tuple::TuplePack;

        let cases: Vec<(u32, Vec<u8>)> = vec![
            (0xb9f08d38, (true,).pack_to_vec()),
            (0x1505e3cb, (false,).pack_to_vec()),
            (0x6ae719f3, ("foo", "bar").pack_to_vec()),
            (0x8adddd61, ("foobar",).pack_to_vec()),
            (0x7273e587, ("foobas",).pack_to_vec()),
            (0xf4ec4d33, ("1",).pack_to_vec()),
            (0x1e023d95, ("2",).pack_to_vec()),
            (0x38a34efe, ("3",).pack_to_vec()),
            (0x17751bae, ("10",).pack_to_vec()),
            (0x87d93806, ("11",).pack_to_vec()),
            (0x3c90c1d9, (1i64,).pack_to_vec()),
            (0x97901bac, (2i64,).pack_to_vec()),
            (0xcbc7f1e2, (3i64,).pack_to_vec()),
            (0xd1d3f3eb, (10i64,).pack_to_vec()),
        ];
        for (expect, packed) in &cases {
            assert_eq!(
                *expect,
                Extractor::packed_hash(packed),
                "packed key: {packed:?}"
            );
        }
    }

    #[test]
    fn test_packed_key_prefix_len() {
        let policy = SerPolicy::noop();

        // Build the packed key for `extractors` over `doc`, then derive the
        // 16-byte prefix exactly as the shuffle log writer does: copy at most
        // 16 bytes, zero-padding a shorter key. Returns (full_key, prefix).
        fn build(doc: &serde_json::Value, extractors: &[Extractor]) -> (bytes::Bytes, [u8; 16]) {
            let mut buf = bytes::BytesMut::new();
            Extractor::extract_all(doc, extractors, Encoding::Packed, &mut buf, None);
            let full = buf.split().freeze();

            let mut prefix = [0u8; 16];
            let copy = full.len().min(16);
            prefix[..copy].copy_from_slice(&full[..copy]);
            (full, prefix)
        }

        let key = |ptrs: &[&str]| -> Vec<Extractor> {
            ptrs.iter().map(|p| Extractor::new(*p, &policy)).collect()
        };

        // A key that fits with room to spare resolves to its exact length, and
        // the prefix slice equals a fresh extraction.
        let ex = key(&["/a", "/b"]);
        let (full, prefix) = build(&json!({"a": 1, "b": 2}), &ex);
        let len = Extractor::packed_key_prefix_len(&prefix, ex.len()).unwrap();
        assert_eq!(&prefix[..len], &full[..]);

        // Zero components is always complete and empty.
        assert_eq!(Extractor::packed_key_prefix_len(&prefix, 0), Some(0));

        // A trailing null component is decoded (not mistaken for padding), and
        // padding past the key never extends a string's terminator scan.
        let ex = key(&["/s", "/n"]);
        let (full, prefix) = build(&json!({"s": "hi", "n": null}), &ex);
        let len = Extractor::packed_key_prefix_len(&prefix, ex.len()).unwrap();
        assert_eq!(&prefix[..len], &full[..]);

        // An embedded NUL inside a string is escaped (0x00 0xFF) and must not
        // be read as the terminator.
        let ex = key(&["/s"]);
        let (full, prefix) = build(&json!({"s": "a\u{0}b"}), &ex);
        assert!(full.len() < 16);
        let len = Extractor::packed_key_prefix_len(&prefix, ex.len()).unwrap();
        assert_eq!(&prefix[..len], &full[..]);

        // A string key longer than 16 bytes is truncated in the prefix, and is
        // reported as possibly-truncated.
        let ex = key(&["/s"]);
        let (full, prefix) = build(
            &json!({"s": "this string is definitely longer than sixteen bytes"}),
            &ex,
        );
        assert!(full.len() > 16);
        assert_eq!(Extractor::packed_key_prefix_len(&prefix, ex.len()), None);

        // A key packing to exactly 16 bytes hits the conservative boundary and
        // is reported as possibly-truncated even though it's whole.
        let ex = key(&["/s"]);
        let (full, prefix) = build(&json!({"s": "fourteen chars"}), &ex); // 0x02 + 14 + 0x00 = 16
        assert_eq!(full.len(), 16);
        assert_eq!(Extractor::packed_key_prefix_len(&prefix, ex.len()), None);
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
