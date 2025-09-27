pub mod build;
pub mod formats;
mod frozen;
pub mod index;
pub mod keywords;
pub mod types;

pub use build::build_schema as build;
pub use frozen::{FrozenSlice, FrozenString};
pub use index::Index;
pub use keywords::{Annotation, CoreAnnotation, Keyword};

#[derive(Debug)]
pub struct Schema<A>
where
    A: Annotation,
{
    // Keywords of the Schema.
    pub keywords: FrozenSlice<Keyword<A>>,
}

impl<A> Schema<A>
where
    A: Annotation,
{
    pub fn curi(&self) -> url::Url {
        url::Url::parse(get_curi(&self.keywords)).unwrap()
    }
}

#[inline]
pub fn get_curi<'s, A: Annotation>(keywords: &'s [Keyword<A>]) -> &'s FrozenString {
    let Some(Keyword::Id { curi, .. }) = keywords.first() else {
        panic!("Keyword::Id must be first Schema keyword");
    };
    curi
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

        // Used by Keyword::Properties.
        assert_eq!(
            std::mem::size_of::<(FrozenString, Schema<CoreAnnotation>)>(),
            24
        );
        // Used by schema::Index.
        assert_eq!(
            std::mem::size_of::<(super::FrozenString, bool, &Schema<CoreAnnotation>)>(),
            24
        )
    }
}
