use std::borrow::Cow;

use super::{BumpStr, BumpVec, HeapField, HeapNode};

use serde::de;

impl<'alloc> HeapNode<'alloc> {
    /// from_serde deserializes a HeapNode from a serde Deserializer, using a
    /// provided bump allocator and string de-duplicator.
    ///
    /// ```
    /// use doc::HeapNode;
    ///
    /// let alloc = HeapNode::new_allocator();
    /// let mut deser = serde_json::Deserializer::from_str(r#"{"hello": "world", "one": 2}"#);
    /// HeapNode::from_serde(&mut deser, &alloc).unwrap();
    /// ```
    pub fn from_serde<'de, 'dedup, D>(
        deser: D,
        alloc: &'alloc bumpalo::Bump,
    ) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deser.deserialize_any(HeapDocVisitor { alloc })
    }
}

struct HeapDocVisitor<'alloc> {
    alloc: &'alloc bumpalo::Bump,
}

impl<'alloc, 'de> de::Visitor<'de> for HeapDocVisitor<'alloc> {
    type Value = HeapNode<'alloc>;

    fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(HeapNode::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v < 0 {
            Ok(HeapNode::NegInt(v))
        } else {
            Ok(HeapNode::PosInt(v as u64))
        }
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(HeapNode::PosInt(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(HeapNode::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Self { alloc } = self;
        Ok(HeapNode::String(BumpStr::from_str(v, alloc)))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(HeapNode::Null)
    }

    fn visit_seq<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let Self { alloc } = self;
        let mut arr = BumpVec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);

        while let Some(child) = v.next_element_seed(HeapDocVisitor { alloc })? {
            arr.push(child, alloc);
        }
        Ok(HeapNode::Array(arr))
    }

    fn visit_map<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let Self { alloc } = self;

        let mut fields = BumpVec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);
        let mut not_sorted = false;

        // Using a `Cow` here is necessary to handle keys that contain escape sequences.
        // In that case, serde will not be able to pass us a borrowed string
        // because it needs to allocate in order to process the escapes.
        while let Some(property) = v.next_key::<Cow<'_, str>>()? {
            let property = BumpStr::from_str(property.as_ref(), alloc);
            let value = v.next_value_seed(HeapDocVisitor { alloc })?;

            not_sorted = not_sorted
                || matches!(fields.last(), Some(HeapField{property: prev, ..}) if prev.as_str() > property.as_str());

            fields.push(HeapField { property, value }, alloc);
        }

        if not_sorted {
            fields.sort_by(|lhs, rhs| lhs.property.cmp(&rhs.property));
        }
        Ok(HeapNode::Object(fields))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let Self { alloc, .. } = self;
        Ok(HeapNode::Bytes(BumpVec::from_slice(v, alloc)))
    }
}

impl<'alloc, 'de> de::DeserializeSeed<'de> for HeapDocVisitor<'alloc> {
    type Value = HeapNode<'alloc>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}
