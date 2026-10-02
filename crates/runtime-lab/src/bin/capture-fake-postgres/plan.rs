//! Row plans: a bound collection's top-level projections, compiled into the
//! generators of its documents. Documents are written as JSON by hand, into a
//! reused buffer, as fast as the runtime could ever read them.
//!
//! Fork points: the generators of `Gen` (value shapes and lengths), and
//! `NULL_PERCENT`.

use anyhow::Context;
use proto_flow::flow;

/// Share of a nullable field's values which are `null`.
const NULL_PERCENT: u64 = 10;
/// Radix of each non-leading component of a composite key (see `key_parts`).
const KEY_RADIX: u64 = 1000;
/// Unix seconds of the synthetic epoch, 2024-01-01T00:00:00Z. Generated times
/// fall within a few years of it, never the wall clock, so output repeats.
pub const EPOCH: u64 = 1_704_067_200;

/// A wyrand PRNG: tiny, fast, and stable forever, unlike `rand`'s small RNGs,
/// whose streams may change between versions.
pub struct Rng(u64);

impl Rng {
    /// A generator of the unit of work `(seed, a, b)`, such as one backfill
    /// chunk or one transaction, so a resumed capture regenerates exactly.
    pub fn new(seed: u64, a: u64, b: u64) -> Self {
        let mut rng = Self(seed ^ a.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        rng.0 ^= rng.next().wrapping_add(b);
        rng
    }

    #[inline]
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0xa076_1d64_78bd_642f);
        let t = (self.0 as u128).wrapping_mul((self.0 ^ 0xe703_7ed1_a0b4_28db) as u128);
        ((t >> 64) ^ t) as u64
    }

    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        ((self.next() as u128 * n as u128) >> 64) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Gen {
    Bool,
    Int,
    Float,
    Numeric,
    Text { max_length: u32 },
    Bytea,
    Date,
    Time,
    Timestamp,
    Uuid,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyGen {
    Int,
    Text,
}

#[derive(Debug)]
enum Kind {
    /// Component `index` of the collection key.
    Key(usize, KeyGen),
    Value(Gen, bool),
    /// The connector's `_meta`.
    Meta,
}

#[derive(Debug)]
struct Field {
    /// `"name":`, escaped.
    prefix: Vec<u8>,
    kind: Kind,
}

/// The document plan of one binding.
#[derive(Debug)]
pub struct RowPlan {
    /// Fields sorted by name, as source-postgres orders them.
    fields: Vec<Field>,
    /// Generators of the collection key's components, in key order.
    pub key: Vec<KeyGen>,
    /// `"schema":"...","table":"..."`, of `_meta.source`.
    source: Vec<u8>,
}

/// Where a document came from, which its `_meta` describes.
pub enum Meta {
    /// A backfill row at `offset` of its table's backfill.
    Backfill { offset: u64 },
    /// A replication change, of `op` (`c`, `u`, `d`).
    Change {
        op: u8,
        loc: [u64; 3],
        ts_ms: u64,
        txid: u64,
    },
}

/// Compile the plan of `collection`, failing on any top-level projection or
/// key the generators don't produce.
pub fn compile(
    collection: &flow::CollectionSpec,
    table: &crate::config::Table,
) -> anyhow::Result<RowPlan> {
    let mut fields: Vec<(String, Kind)> = vec![("_meta".to_string(), Kind::Meta)];

    for projection in &collection.projections {
        let Some(name) = top_level(&projection.ptr) else {
            continue;
        };
        if name == "_meta" || fields.iter().any(|(n, _)| *n == name) {
            continue;
        }
        let inference = projection
            .inference
            .as_ref()
            .with_context(|| format!("projection {} has no inference", projection.ptr))?;
        let (gen_, nullable) = gen_of(inference).with_context(|| {
            format!(
                "field {name:?} of {} (types {:?}, format {:?}) can't be generated",
                collection.name,
                inference.types,
                inference
                    .string
                    .as_ref()
                    .map(|s| s.format.as_str())
                    .unwrap_or_default(),
            )
        })?;
        fields.push((name, Kind::Value(gen_, nullable)));
    }

    let mut key = Vec::new();
    for (index, ptr) in collection.key.iter().enumerate() {
        let name = top_level(ptr)
            .with_context(|| format!("key {ptr} of {} isn't a top-level field", collection.name))?;
        let (_, kind) = fields
            .iter_mut()
            .find(|(n, _)| *n == name)
            .with_context(|| format!("key {ptr} of {} has no projection", collection.name))?;

        let key_gen = match kind {
            Kind::Value(Gen::Int, false) => KeyGen::Int,
            Kind::Value(Gen::Text { .. }, false) => KeyGen::Text,
            other => anyhow::bail!(
                "key {ptr} of {} must be a non-null integer or plain string, not {other:?}",
                collection.name
            ),
        };
        *kind = Kind::Key(index, key_gen);
        key.push(key_gen);
    }

    fields.sort_by(|(a, _), (b, _)| a.cmp(b));
    let fields = fields
        .into_iter()
        .map(|(name, kind)| {
            let mut prefix = serde_json::to_vec(&name).unwrap();
            prefix.push(b':');
            Field { prefix, kind }
        })
        .collect();

    let source = format!(
        "\"schema\":{},\"table\":{}",
        serde_json::to_string(&table.namespace).unwrap(),
        serde_json::to_string(&table.stream).unwrap(),
    )
    .into_bytes();

    Ok(RowPlan {
        fields,
        key,
        source,
    })
}

/// The field name of a top-level JSON pointer, or None if it's the root or
/// nested.
fn top_level(ptr: &str) -> Option<String> {
    let rest = ptr.strip_prefix('/')?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest.replace("~1", "/").replace("~0", "~"))
}

fn gen_of(inference: &flow::Inference) -> Option<(Gen, bool)> {
    let has = |t: &str| inference.types.iter().any(|x| x == t);
    let nullable = has("null");
    let mut types: Vec<&str> = inference
        .types
        .iter()
        .map(String::as_str)
        .filter(|t| *t != "null")
        .collect();
    types.sort();

    let string = inference.string.as_ref();
    let format = string.map(|s| s.format.as_str()).unwrap_or_default();

    // An untyped field (as `jsonb`) admits every type.
    if has("object") && has("array") && has("string") && has("number") && has("boolean") {
        return Some((Gen::Json, nullable));
    }
    let gen_ = match (types.as_slice(), format) {
        (["boolean"], _) => Gen::Bool,
        (["integer"], _) => Gen::Int,
        (["number"], _) | (["number", "string"], "number") => Gen::Float,
        (["string"], "number") => Gen::Numeric,
        (["string"], "date-time") => Gen::Timestamp,
        (["string"], "date") => Gen::Date,
        (["string"], "time") => Gen::Time,
        (["string"], "uuid") => Gen::Uuid,
        (["string"], "") if string.is_some_and(|s| s.content_encoding == "base64") => Gen::Bytea,
        (["string"], "") => Gen::Text {
            max_length: string
                .map(|s| s.max_length)
                .filter(|n| *n != 0)
                .unwrap_or(40),
        },
        _ => return None,
    };
    Some((gen_, nullable))
}

/// The key components of row `ordinal`. A composite key splits the ordinal in
/// mixed radix (the last component cycles fastest), so that ascending
/// ordinals are ascending keys, as a backfill reads them.
pub fn key_parts(ordinal: u64, components: usize, out: &mut [u64; 8]) {
    let mut n = ordinal;
    for i in (1..components).rev() {
        out[i] = n % KEY_RADIX;
        n /= KEY_RADIX;
    }
    out[0] = n;
}

impl RowPlan {
    /// Write the document of row `ordinal` into `out`. A delete has only its
    /// key and `_meta`, as with Postgres's default replica identity.
    pub fn write_doc(&self, out: &mut Vec<u8>, rng: &mut Rng, ordinal: u64, meta: &Meta) {
        let mut parts = [0u64; 8];
        key_parts(ordinal, self.key.len(), &mut parts);
        let delete = matches!(meta, Meta::Change { op: b'd', .. });

        out.push(b'{');
        let mut first = true;
        for field in &self.fields {
            if delete && matches!(field.kind, Kind::Value(..)) {
                continue;
            }
            if !first {
                out.push(b',');
            }
            first = false;
            out.extend_from_slice(&field.prefix);

            match field.kind {
                Kind::Key(index, KeyGen::Int) => write_u64(out, parts[index]),
                Kind::Key(index, KeyGen::Text) => write_text_key(out, parts[index]),
                Kind::Value(gen_, nullable) => {
                    if nullable && rng.below(100) < NULL_PERCENT {
                        out.extend_from_slice(b"null");
                    } else {
                        write_value(out, rng, gen_);
                    }
                }
                Kind::Meta => self.write_meta(out, meta),
            }
        }
        out.push(b'}');
    }

    fn write_meta(&self, out: &mut Vec<u8>, meta: &Meta) {
        match *meta {
            Meta::Backfill { offset } => {
                out.extend_from_slice(b"{\"op\":\"c\",\"source\":{");
                out.extend_from_slice(&self.source);
                out.extend_from_slice(b",\"snapshot\":true,\"loc\":[-1,");
                write_u64(out, offset);
                out.extend_from_slice(b",0]}}");
            }
            Meta::Change {
                op,
                loc,
                ts_ms,
                txid,
            } => {
                out.extend_from_slice(b"{\"op\":\"");
                out.push(op);
                out.extend_from_slice(b"\",\"source\":{\"ts_ms\":");
                write_u64(out, ts_ms);
                out.push(b',');
                out.extend_from_slice(&self.source);
                out.extend_from_slice(b",\"loc\":[");
                write_u64(out, loc[0]);
                out.push(b',');
                write_u64(out, loc[1]);
                out.push(b',');
                write_u64(out, loc[2]);
                out.extend_from_slice(b"],\"txid\":");
                write_u64(out, txid);
                out.extend_from_slice(b"}}");
            }
        }
    }

    /// The key of row `ordinal`, packed as an FDB tuple, as sqlcapture
    /// encodes a backfill's `scanned` cursor.
    pub fn packed_key(&self, ordinal: u64) -> Vec<u8> {
        let mut parts = [0u64; 8];
        key_parts(ordinal, self.key.len(), &mut parts);
        let elements: Vec<tuple::Element> = self
            .key
            .iter()
            .zip(parts)
            .map(|(key_gen, part)| match key_gen {
                KeyGen::Int => tuple::Element::Int(part as i64),
                KeyGen::Text => {
                    let mut text = Vec::new();
                    write_text_key(&mut text, part);
                    let text = String::from_utf8(text[1..text.len() - 1].to_vec()).unwrap();
                    tuple::Element::String(text.into())
                }
            })
            .collect();
        tuple::pack(&elements)
    }
}

const ALPHABET: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -";
const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const HEX: &[u8; 16] = b"0123456789abcdef";

fn write_value(out: &mut Vec<u8>, rng: &mut Rng, gen_: Gen) {
    match gen_ {
        Gen::Bool => out.extend_from_slice(if rng.next() & 1 == 0 {
            b"false"
        } else {
            b"true"
        }),
        Gen::Int => write_u64(out, rng.below(1_000_000_000)),
        Gen::Float => write_cents(out, rng.below(10_000_000)),
        Gen::Numeric => {
            out.push(b'"');
            write_cents(out, rng.below(100_000_000));
            out.push(b'"');
        }
        Gen::Text { max_length } => {
            let max = max_length.min(40) as u64;
            let len = max.min(8) + rng.below(max - max.min(8) + 1);
            write_chars(out, rng, len, ALPHABET);
        }
        Gen::Bytea => write_chars(out, rng, 32, BASE64),
        Gen::Date => {
            let (y, m, d) = civil(EPOCH / 86400 + rng.below(3 * 365));
            out.push(b'"');
            write_date(out, y, m, d);
            out.push(b'"');
        }
        Gen::Time => {
            let s = rng.below(86400);
            out.push(b'"');
            write_hms(out, s);
            out.push(b'"');
        }
        Gen::Timestamp => {
            let secs = EPOCH + rng.below(3 * 365 * 86400);
            let (y, m, d) = civil(secs / 86400);
            out.push(b'"');
            write_date(out, y, m, d);
            out.push(b'T');
            write_hms(out, secs % 86400);
            out.push(b'.');
            write_padded(out, rng.below(1_000_000), 6);
            out.extend_from_slice(b"Z\"");
        }
        Gen::Uuid => {
            let (a, b) = (rng.next(), rng.next());
            out.push(b'"');
            for (i, nibble) in (0..32).map(|i| {
                let word = if i < 16 { a } else { b };
                (i, (word >> ((i % 16) * 4)) & 0xf)
            }) {
                if matches!(i, 8 | 12 | 16 | 20) {
                    out.push(b'-');
                }
                out.push(HEX[nibble as usize]);
            }
            out.push(b'"');
        }
        Gen::Json => {
            out.extend_from_slice(b"{\"k\":");
            write_u64(out, rng.below(1000));
            out.extend_from_slice(b",\"v\":");
            write_chars(out, rng, 8, ALPHABET);
            out.push(b'}');
        }
    }
}

/// A quoted string of `len` characters of `alphabet`.
fn write_chars(out: &mut Vec<u8>, rng: &mut Rng, len: u64, alphabet: &[u8; 64]) {
    out.push(b'"');
    let mut bits = 0u64;
    for i in 0..len {
        if i % 10 == 0 {
            bits = rng.next();
        }
        out.push(alphabet[(bits & 63) as usize]);
        bits >>= 6;
    }
    out.push(b'"');
}

fn write_text_key(out: &mut Vec<u8>, part: u64) {
    out.extend_from_slice(b"\"r");
    write_padded(out, part, 12);
    out.push(b'"');
}

pub fn write_u64(out: &mut Vec<u8>, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

fn write_padded(out: &mut Vec<u8>, n: u64, width: usize) {
    let mut buf = [b'0'; 20];
    let mut n = n;
    for i in (20 - width..20).rev() {
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out.extend_from_slice(&buf[20 - width..]);
}

/// `cents` as a decimal with two fractional digits.
fn write_cents(out: &mut Vec<u8>, cents: u64) {
    write_u64(out, cents / 100);
    out.push(b'.');
    write_padded(out, cents % 100, 2);
}

fn write_date(out: &mut Vec<u8>, y: u64, m: u64, d: u64) {
    write_padded(out, y, 4);
    out.push(b'-');
    write_padded(out, m, 2);
    out.push(b'-');
    write_padded(out, d, 2);
}

fn write_hms(out: &mut Vec<u8>, s: u64) {
    write_padded(out, s / 3600, 2);
    out.push(b':');
    write_padded(out, s / 60 % 60, 2);
    out.push(b':');
    write_padded(out, s % 60, 2);
}

/// The (year, month, day) of a count of days since the Unix epoch
/// (Howard Hinnant's `civil_from_days`, for days after 1970).
fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn civil_dates() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(EPOCH / 86400), (2024, 1, 1));
        assert_eq!(civil(EPOCH / 86400 + 59), (2024, 2, 29));
    }

    #[test]
    fn composite_keys_ascend() {
        let mut prior = [0u64; 8];
        for ordinal in 0..5000 {
            let mut parts = [0u64; 8];
            key_parts(ordinal, 2, &mut parts);
            assert!(ordinal == 0 || parts[..2] > prior[..2]);
            prior = parts;
        }
        let mut parts = [0u64; 8];
        key_parts(12_345, 2, &mut parts);
        assert_eq!(&parts[..2], &[12, 345]);
    }

    #[test]
    fn generated_values_are_json() {
        let mut rng = Rng::new(1, 2, 3);
        for gen_ in [
            Gen::Bool,
            Gen::Int,
            Gen::Float,
            Gen::Numeric,
            Gen::Text { max_length: 16 },
            Gen::Bytea,
            Gen::Date,
            Gen::Time,
            Gen::Timestamp,
            Gen::Uuid,
            Gen::Json,
        ] {
            let mut out = Vec::new();
            write_value(&mut out, &mut rng, gen_);
            serde_json::from_slice::<serde_json::Value>(&out)
                .unwrap_or_else(|err| panic!("{gen_:?} {}: {err}", String::from_utf8_lossy(&out)));
        }
    }
}
