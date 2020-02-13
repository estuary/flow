use crate::{LocatedItem, LocatedProperty, Location, Number, Span, Walker};
use fxhash::{hash64, FxHasher64};
use serde::de;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Walk deserializes a document, invoking callbacks of the provided Walker.
pub fn walk<'de, D, W>(deserializer: D, walker: &mut W) -> Result<Span, D::Error>
where
    D: de::Deserializer<'de>,
    W: Walker,
{
    let root_visitor = Visitor {
        walker,
        location: Location::Root,
        span_begin: 0,
    };
    deserializer.deserialize_any(root_visitor)
}

/// Visitor wraps a Walker with context of the specific JSON value being visited.
struct Visitor<'lc, 'w, W: Walker> {
    walker: &'w mut W,
    location: Location<'lc>,
    span_begin: usize,
}

/// SeqWrapper wraps a Visitor to instrument visitations of sequence items.
/// Notably, is dispatches calls to Walker::push_item immediately prior to
/// delegating value deserialization to the wrapped Visitor.
struct SeqWrapper<'lc, 'w, W: Walker> {
    wrapped: Visitor<'lc, 'w, W>,
    span: Span,
    count: usize,
    hasher: FxHasher64,
}

impl<'de, 'lc, 'w, W: Walker> de::Visitor<'de> for Visitor<'lc, 'w, W> {
    type Value = Span;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let span = Span::new(
            self.span_begin,
            if v { BOOL_TRUE_HASH } else { BOOL_FALSE_HASH },
        );
        self.walker.pop_bool(&span, &self.location, v);
        Ok(span)
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let span = Span::new(self.span_begin, hash64(&v));
        self.walker
            .pop_numeric(&span, &self.location, Number::Signed(v));
        Ok(span)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let span = Span::new(self.span_begin, hash64(&v));
        self.walker
            .pop_numeric(&span, &self.location, Number::Unsigned(v));
        Ok(span)
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Separately hash integral and fractional hash parts to maintain equality
        // between integer f64 values and u64/i64 types.
        let vt = v.trunc();
        let h = hash64(&(vt as i64)) ^ hash64(&(v - vt).to_bits());

        let span = Span::new(self.span_begin, h);
        self.walker
            .pop_numeric(&span, &self.location, Number::Float(v));
        Ok(span)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let span = Span::new(self.span_begin, STRING_SEED ^ hash64(v));
        self.walker.pop_str(&span, &self.location, v);
        Ok(span)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let span = Span::new(self.span_begin, UNIT_HASH);
        self.walker.pop_null(&span, &self.location);
        Ok(span)
    }

    fn visit_seq<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let mut sw = SeqWrapper {
            span: Span::new(self.span_begin, ARRAY_SEED),
            count: 0,
            wrapped: self,
            hasher: FxHasher64::default(),
        };

        while let Some(()) = v.next_element_seed(&mut sw)? {}
        // Hash of this span is the result of hashing over the ordered
        // hashes of each sub-span.
        sw.span.hashed ^= sw.hasher.finish();

        sw.wrapped
            .walker
            .pop_array(&sw.span, &sw.wrapped.location, sw.count);
        Ok(sw.span)
    }

    fn visit_map<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let mut span = Span::new(self.span_begin, OBJECT_SEED);
        let mut count = 0 as usize;

        while let Some(prop) = v.next_key::<&str>()? {
            // Tell the walker of this property (and its upcoming value).
            let prop_span = Span::new(span.end, hash64(prop));
            let prop_loc = LocatedProperty {
                parent: &self.location,
                name: prop,
                index: count,
            };
            self.walker.push_property(&prop_span, &prop_loc);

            let vv = Visitor {
                walker: self.walker,
                span_begin: span.end,
                location: Location::Property(prop_loc),
            };
            let sub_span = v.next_value_seed(vv)?;

            span.end = sub_span.end;
            count += 1;

            // Update the hash of our span by XOR'ing in a composed hash
            // of the property name and sub-span value. The XOR is required
            // in order to produce hash values which are invariant to the
            // order in which properties are enumerated.
            let mut h = FxHasher64::default();
            prop_span.hashed.hash(&mut h);
            sub_span.hashed.hash(&mut h);
            span.hashed ^= h.finish();
        }

        self.walker.pop_object(&span, &self.location, count);
        Ok(span)
    }
}

impl<'de, 'lc, 'w, W: Walker> de::DeserializeSeed<'de> for Visitor<'lc, 'w, W> {
    type Value = Span;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de, 'lc, 'w, W: Walker> de::DeserializeSeed<'de> for &mut SeqWrapper<'lc, 'w, W> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let item_span = Span::new(self.span.end, 0);
        let item_loc = LocatedItem {
            parent: &self.wrapped.location,
            index: self.count,
        };
        self.wrapped.walker.push_item(&item_span, &item_loc);

        let vv = Visitor {
            walker: self.wrapped.walker,
            span_begin: self.span.end,
            location: Location::Item(item_loc),
        };
        let sub_span = deserializer.deserialize_any(vv)?;

        self.hasher.write_u64(sub_span.hashed);
        self.span.end = sub_span.end;
        self.count += 1;

        Ok(())
    }
}

// Seeds to distinguish zero-valued types from one another.
// Numbers are not seeded.
const UNIT_HASH: u64 = 0xe0fe5d21a7c19aeb;
const BOOL_TRUE_HASH: u64 = 0x3bd83018139b2c4d;
const BOOL_FALSE_HASH: u64 = 0x4cd6d6c279e081c0;
const ARRAY_SEED: u64 = 0xca910bdc0b6441dd;
const OBJECT_SEED: u64 = 0x76662a22f0a45102;
const STRING_SEED: u64 = 0x5570bb24d6cdeee2;
