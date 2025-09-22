use super::{BumpStr, BumpVec, HeapField, HeapNode};
use serde::de;
use std::borrow::Cow;

impl<'alloc> HeapNode<'alloc> {
    /// from_serde deserializes a HeapNode from a serde Deserializer
    /// using the provided bump allocator.
    ///
    /// ```
    /// use doc::HeapNode;
    ///
    /// let alloc = HeapNode::new_allocator();
    /// let mut deser = serde_json::Deserializer::from_str(r#"{"hello": "world", "one": 2}"#);
    /// let node = HeapNode::from_serde(&mut deser, &alloc).unwrap();
    /// ```
    #[inline]
    pub fn from_serde<'de, 'dedup, D>(
        deser: D,
        alloc: &'alloc bumpalo::Bump,
    ) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deser
            .deserialize_any(HeapDocVisitor { alloc })
            .map(|(node, _)| node)
    }
}

struct HeapDocVisitor<'alloc> {
    alloc: &'alloc bumpalo::Bump,
}

impl<'alloc, 'de> de::Visitor<'de> for HeapDocVisitor<'alloc> {
    type Value = (HeapNode<'alloc>, i32);

    fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok((HeapNode::Bool(v), 1))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok((
            if v < 0 {
                HeapNode::NegInt(v)
            } else {
                HeapNode::PosInt(v as u64)
            },
            1,
        ))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok((HeapNode::PosInt(v), 1))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok((HeapNode::Float(v), 1))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Self { alloc } = self;
        Ok((HeapNode::String(BumpStr::from_str(v, alloc)), 1))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok((HeapNode::Null, 1))
    }

    fn visit_seq<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let Self { alloc } = self;
        let mut arr = BumpVec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);
        let mut built_length = 1;

        while let Some((value, child_delta)) = v.next_element_seed(HeapDocVisitor { alloc })? {
            built_length += child_delta;
            arr.push(value, alloc);
        }
        Ok((HeapNode::Array(built_length, arr), built_length))
    }

    fn visit_map<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let Self { alloc } = self;

        let mut fields = BumpVec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);
        let mut not_sorted = false;
        let mut built_length = 1;

        // Using a `Cow` here is necessary to handle keys that contain escape sequences.
        // In that case, serde will not be able to pass us a borrowed string
        // because it needs to allocate in order to process the escapes.
        while let Some(property) = v.next_key::<Cow<'_, str>>()? {
            let property = BumpStr::from_str(property.as_ref(), alloc);
            let (value, child_delta) = v.next_value_seed(HeapDocVisitor { alloc })?;
            built_length += child_delta;

            not_sorted = not_sorted
                || matches!(fields.last(), Some(HeapField{property: prev, ..}) if prev.as_str() > property.as_str());

            fields.push(HeapField { property, value }, alloc);
        }

        if not_sorted {
            fields.sort_by(|lhs, rhs| lhs.property.cmp(&rhs.property));
        }
        Ok((HeapNode::Object(built_length, fields), built_length))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let Self { alloc, .. } = self;
        Ok((HeapNode::Bytes(BumpVec::from_slice(v, alloc)), 1))
    }
}

impl<'alloc, 'de> de::DeserializeSeed<'de> for HeapDocVisitor<'alloc> {
    type Value = (HeapNode<'alloc>, i32);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}
