use super::dedup::Deduper;
use super::heap::{BumpVec, HeapField, HeapNode};

use serde::de;

impl<'alloc> HeapNode<'alloc> {
    /// from_serde deserializes a HeapNode from a serde Deserializer, using a
    /// provided bump allocator and string de-duplicator.
    ///
    /// ```
    /// use doc_poc::HeapNode;
    ///
    /// let alloc = HeapNode::new_allocator();
    /// let dedup = HeapNode::new_deduper(&alloc);
    /// let mut deser = serde_json::Deserializer::from_str(r#"{"hello": "world", "one": 2}"#);
    /// HeapNode::from_serde(&mut deser, &alloc, &dedup).unwrap();
    /// ```
    pub fn from_serde<'de, 'dedup, D>(
        deser: D,
        alloc: &'alloc bumpalo::Bump,
        dedup: &'dedup Deduper<'alloc>,
    ) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deser.deserialize_any(HeapDocVisitor { alloc, dedup })
    }
}

struct HeapDocVisitor<'alloc, 'dedup> {
    alloc: &'alloc bumpalo::Bump,
    dedup: &'dedup Deduper<'alloc>,
}

impl<'alloc, 'de, 'b> de::Visitor<'de> for HeapDocVisitor<'alloc, 'b> {
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
        let Self { dedup, .. } = self;
        Ok(dedup.alloc_string(v))
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
        let Self { alloc, dedup } = self;
        let mut arr =
            bumpalo::collections::Vec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);

        while let Some(child) = v.next_element_seed(HeapDocVisitor { alloc, dedup })? {
            arr.push(child);
        }
        Ok(HeapNode::Array(BumpVec(arr)))
    }

    fn visit_map<V>(self, mut v: V) -> Result<Self::Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let Self { alloc, dedup } = self;

        let mut fields =
            bumpalo::collections::Vec::with_capacity_in(v.size_hint().unwrap_or_default(), alloc);
        let mut not_sorted = false;

        while let Some(property) = v.next_key::<&str>()? {
            let property = dedup.alloc_shared_string(property);
            let value = v.next_value_seed(HeapDocVisitor { alloc, dedup })?;

            not_sorted = not_sorted
                || matches!(fields.last(), Some(HeapField{property: prev, ..}) if prev > &property);

            fields.push(HeapField { property, value });
        }

        if not_sorted {
            fields.sort_by(|lhs, rhs| lhs.property.cmp(&rhs.property));
        }
        Ok(HeapNode::Object(BumpVec(fields)))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let Self { alloc, .. } = self;
        let mut vec = bumpalo::collections::Vec::with_capacity_in(v.len(), alloc);
        vec.extend_from_slice(v);
        Ok(HeapNode::Bytes(BumpVec(vec)))
    }
}

impl<'alloc, 'de, 'b> de::DeserializeSeed<'de> for HeapDocVisitor<'alloc, 'b> {
    type Value = HeapNode<'alloc>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}
