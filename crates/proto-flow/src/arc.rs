/// A shared, immutable message which is cheap to clone.
///
/// `Arc` wraps a `std::sync::Arc` so that it can implement `prost::Message`,
/// which the orphan rule forbids for the standard type. Generated bindings use
/// it for every `flow::CollectionSpec` field (see `build.rs`), so that a spec
/// is shared rather than copied as it moves between table rows, bindings,
/// and `linked_collections`.
///
/// There is deliberately no `DerefMut`. Mutation goes through
/// [`Arc::make_mut`], which deep-copies the message if it's shared.
pub struct Arc<M>(std::sync::Arc<M>);

impl<M> Arc<M> {
    pub fn new(message: M) -> Self {
        Self(std::sync::Arc::new(message))
    }

    /// Mutably borrow the message, first deep-copying it if it's shared.
    pub fn make_mut(this: &mut Self) -> &mut M
    where
        M: Clone,
    {
        std::sync::Arc::make_mut(&mut this.0)
    }

    /// Take the message, deep-copying it if it's shared.
    pub fn unwrap_or_clone(this: Self) -> M
    where
        M: Clone,
    {
        std::sync::Arc::unwrap_or_clone(this.0)
    }

    /// Whether `this` and `other` share one allocation.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&this.0, &other.0)
    }

    pub fn into_inner(this: Self) -> std::sync::Arc<M> {
        this.0
    }
}

impl<M> Clone for Arc<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: Default> Default for Arc<M> {
    fn default() -> Self {
        Self::new(M::default())
    }
}

impl<M> std::ops::Deref for Arc<M> {
    type Target = M;

    fn deref(&self) -> &M {
        &self.0
    }
}

impl<M> AsRef<M> for Arc<M> {
    fn as_ref(&self) -> &M {
        &self.0
    }
}

impl<M> From<M> for Arc<M> {
    fn from(message: M) -> Self {
        Self::new(message)
    }
}

impl<M> From<std::sync::Arc<M>> for Arc<M> {
    fn from(inner: std::sync::Arc<M>) -> Self {
        Self(inner)
    }
}

// Transparent, so that Debug snapshots don't depend on how a message is held.
impl<M: std::fmt::Debug> std::fmt::Debug for Arc<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

// Shared messages are equal without a walk of their contents.
// Unlike `std::sync::Arc`, this doesn't require `M: Eq`: a message holding a
// NaN is equal to itself here, which no caller distinguishes.
impl<M: PartialEq> PartialEq for Arc<M> {
    fn eq(&self, other: &Self) -> bool {
        Self::ptr_eq(self, other) || **self == **other
    }
}

impl<M: Eq> Eq for Arc<M> {}

impl<M: std::hash::Hash> std::hash::Hash for Arc<M> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<M> prost::Message for Arc<M>
where
    M: prost::Message + Clone + Default,
{
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        (**self).encode_raw(buf)
    }
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        // A message being decoded is uniquely held, so this copies only when
        // merging into a message that's already shared.
        Self::make_mut(self).merge_field(tag, wire_type, buf, ctx)
    }
    fn encoded_len(&self) -> usize {
        (**self).encoded_len()
    }
    fn clear(&mut self) {
        *self = Self::default()
    }
}

impl<M: serde::Serialize> serde::Serialize for Arc<M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (**self).serialize(serializer)
    }
}

impl<'de, M: serde::Deserialize<'de>> serde::Deserialize<'de> for Arc<M> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        M::deserialize(deserializer).map(Self::new)
    }
}

#[cfg(test)]
mod test {
    use super::Arc;
    use crate::flow;
    use prost::Message;

    fn fixture() -> flow::capture_spec::Binding {
        flow::capture_spec::Binding {
            collection: Some(Arc::new(flow::CollectionSpec {
                name: "acmeCo/widgets".to_string(),
                key: vec!["/id".to_string()],
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn round_trips_match_the_wrapped_message() {
        let binding = fixture();

        let decoded = flow::capture_spec::Binding::decode(binding.encode_to_vec().as_slice())
            .expect("decodes");
        assert_eq!(decoded, binding);

        let json = serde_json::to_string(&binding).unwrap();
        assert_eq!(
            json,
            r#"{"collection":{"name":"acmeCo/widgets","key":["/id"]}}"#
        );
        let decoded: flow::capture_spec::Binding = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, binding);

        assert_eq!(
            format!("{:?}", binding.collection.as_ref().unwrap()),
            format!("{:?}", &**binding.collection.as_ref().unwrap()),
        );
    }

    #[test]
    fn make_mut_copies_only_when_shared() {
        let mut unique = Arc::new(flow::CollectionSpec::default());
        let before = &*unique as *const flow::CollectionSpec;
        Arc::make_mut(&mut unique).name = "acmeCo/one".to_string();
        assert_eq!(before, &*unique as *const flow::CollectionSpec);

        let shared = unique.clone();
        assert!(Arc::ptr_eq(&unique, &shared));

        Arc::make_mut(&mut unique).name = "acmeCo/two".to_string();
        assert!(!Arc::ptr_eq(&unique, &shared));
        assert_eq!(shared.name, "acmeCo/one");
        assert_eq!(unique.name, "acmeCo/two");
    }

    #[test]
    fn equality_is_by_pointer_then_value() {
        let one = Arc::new(flow::CollectionSpec {
            name: "acmeCo/one".to_string(),
            ..Default::default()
        });
        let same_value = Arc::new((*one).clone());
        let other = Arc::new(flow::CollectionSpec::default());

        assert_eq!(one, one.clone());
        assert!(!Arc::ptr_eq(&one, &same_value));
        assert_eq!(one, same_value);
        assert_ne!(one, other);
    }
}
