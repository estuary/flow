use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::Serialize;

use crate::models::id::Id;

/// We often want to act as a passthrough to a connector, forwarding a response
/// exactly as it was sent to/from the connector. We use `RawValue` to avoid
/// serde actually parsing the contents.
///
/// If/when we want to parse/validate/modify payloads on the within the API,
/// we'll remove these usages.
pub type RawJson = Box<serde_json::value::RawValue>;

/// A single JSON:API resource. Data structures representing specific API
/// resources will be wrapped up according to the spec.
#[derive(Debug, Serialize)]
pub struct Resource<T> {
    pub id: Id<T>,
    pub r#type: &'static str,
    pub attributes: T,
    #[serde(skip_serializing_if = "Links::is_empty")]
    pub links: Links,
}

/// JSON:API documents can either hold a `Resource`, or a `Vec<Resource>`. This
/// is defined with a trait, rather than an enum, to allow for differentiating
/// this cardinality difference in function signatures.
pub trait DocumentContents<T>: private::Sealed {}
impl<T> DocumentContents<T> for Resource<T> {}
impl<T> DocumentContents<T> for Vec<Resource<T>> {}

/// A JSON:API document containing one or many `Resource`s, along with other
/// metadata about the response.
#[derive(Debug, Serialize)]
pub struct DocumentData<D, C: DocumentContents<D>> {
    data: C,
    #[serde(skip_serializing_if = "Links::is_empty")]
    links: Links,
    #[serde(skip)]
    _inner_data_type: PhantomData<D>,
}

impl<D, C: DocumentContents<D>> DocumentData<D, C> {
    pub fn new(data: C, links: Links) -> Self {
        Self {
            data,
            links,
            _inner_data_type: PhantomData,
        }
    }
}

/// A JSON:API document containing a singular `Resource`.
pub type One<T> = DocumentData<T, Resource<T>>;
/// A JSON:API document containing a list of `Resource`s.
pub type Many<T> = DocumentData<T, Vec<Resource<T>>>;

/// Detailed information about an error message.
// TODO: Remove/rework how error details are used/constructed after initial
// development phase. This is currently the full error message details. This
// level of detail is not appropriate for end users, but is probably helpful for
// developers in the short term.
#[derive(Debug, Serialize)]
pub struct ProblemDetails {
    pub title: String,
    pub detail: Option<String>,
}

/// A JSON:API document containing a list of errors, along with other metadata
/// about the response.
#[derive(Debug, Serialize)]
pub struct PayloadError {
    pub errors: Vec<ProblemDetails>,
    #[serde(skip_serializing_if = "Links::is_empty")]
    pub links: Links,
}

impl PayloadError {
    pub fn new(error: ProblemDetails) -> Self {
        Self {
            errors: vec![error],
            links: Links::default(),
        }
    }
}

/// A set of simple links. These render as `(relation, href)` pairs in responses.
///
/// Usage: `Links::default().put("self", "/foo/bar/123").put("owner", "/bar/456")`
///
/// Rendered JSON: `{"self": "/foo/bar/123", "owner": "/bar/456"}`
#[derive(Debug, Default, Serialize)]
pub struct Links(BTreeMap<String, String>);

impl Links {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Add a link into the set of links. If a relation of the same name already
    /// existed, it is overwritten.
    pub fn put(mut self, relation: impl Into<String>, href: impl Into<String>) -> Self {
        use std::collections::btree_map::Entry;

        match self.0.entry(relation.into()) {
            Entry::Occupied(mut e) => {
                e.insert(href.into());
            }
            Entry::Vacant(e) => {
                e.insert(href.into());
            }
        };

        self
    }
}

mod private {
    /// This prevents other types from being stuck into a `DocumentData`, as this
    /// would generate an invalid payload.
    pub trait Sealed {}
    impl<T> Sealed for super::Resource<T> {}
    impl<T> Sealed for Vec<super::Resource<T>> {}
}
