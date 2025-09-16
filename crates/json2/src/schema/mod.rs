pub mod build;
pub mod formats;
mod frozen;
pub mod index;
pub mod keywords;
pub mod types;

pub use frozen::{FrozenSlice, FrozenString};
pub use index::Index;
pub use keywords::{Annotation, CoreAnnotation, Keyword};

#[derive(Debug)]
pub struct Schema<A>
where
    A: Annotation,
{
    // Keywords of the Schema.
    pub kw: FrozenSlice<Keyword<A>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sizes() {
        assert_eq!(std::mem::size_of::<FrozenString>(), 12);
        assert_eq!(std::mem::size_of::<FrozenSlice<serde_json::Value>>(), 12);
        assert_eq!(std::mem::size_of::<Keyword<CoreAnnotation>>(), 16);
        assert_eq!(std::mem::size_of::<Schema<CoreAnnotation>>(), 12);
        assert_eq!(
            std::mem::size_of::<(FrozenString, Schema<CoreAnnotation>)>(),
            24
        );
    }
}
