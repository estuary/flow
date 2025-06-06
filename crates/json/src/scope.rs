use crate::Location;
use url::Url;

/// Scope is a stack-based mechanism for tracking fine-grained location
/// context of a resource currently being processed.
#[derive(Copy, Clone)]
pub struct Scope<'a> {
    /// Parent of this Scope, or None if this is the Context root.
    pub parent: Option<&'a Scope<'a>>,
    /// Resource of this Context, which is !None if and only if this Scope roots
    /// processing of a new resource (as opposed to being scoped to a path therein).
    pub resource: Option<&'a Url>,
    /// Location within the current resource.
    pub location: Location<'a>,
}

impl<'a> Scope<'a> {
    /// Create a new scope rooted at the given resource.
    pub fn new(resource: &'a Url) -> Scope<'a> {
        Scope {
            parent: None,
            resource: Some(resource),
            location: Location::Root,
        }
    }
    /// Push a resource onto the current Scope, returning a new Scope.
    pub fn push_resource(&'a self, resource: &'a Url) -> Scope<'a> {
        if resource.fragment().is_some() {
            panic!("resource cannot have fragment");
        }
        Scope {
            parent: Some(self),
            resource: Some(resource),
            location: Location::Root,
        }
    }
    /// Push a property onto the current Scope, returning a new Scope.
    pub fn push_prop(&'a self, name: &'a str) -> Scope<'a> {
        Scope {
            parent: Some(self),
            resource: None,
            location: self.location.push_prop(name),
        }
    }
    /// Push an item index onto the current Scope, returning a new Scope.
    pub fn push_item(&'a self, index: usize) -> Scope<'a> {
        Scope {
            parent: Some(self),
            resource: None,
            location: self.location.push_item(index),
        }
    }

    /// Returns the resource being processed by this Scope.
    pub fn resource(self) -> &'a url::Url {
        match self.resource {
            Some(r) => r,
            None => self.parent.unwrap().resource(),
        }
    }

    /// Returns the depth of the stack of resources which are in the Scope.
    pub fn resource_depth(self) -> usize {
        self.parent.map_or(0, |p| p.resource_depth()) + self.resource.map_or(0, |_| 1)
    }

    /// Flatten the scope into its current resource URI, extended with a
    /// URL fragment-encoded JSON pointer of the current location.
    pub fn flatten(self) -> Url {
        let mut f = self.resource().clone();

        if !matches!(self.location, Location::Root) {
            f.set_fragment(Some(&format!(
                "{}{}",
                f.fragment().unwrap_or(""),
                self.location.pointer_str().to_string()
            )));
        }
        f
    }
}

#[cfg(test)]
mod test {
    use super::{Scope, Url};

    #[test]
    fn test_scope_errors() {
        let ra = Url::parse("http://example/A#/path/prefix").unwrap();
        let rb = Url::parse("http://example/B").unwrap();

        let s1 = Scope::new(&ra);
        let s2 = s1.push_prop("foo");
        let s3 = s2.push_item(32);
        let s4 = s3.push_resource(&rb);
        let s5 = s4.push_prop("something");

        assert_eq!(s1.flatten().as_str(), "http://example/A#/path/prefix");
        assert_eq!(
            s3.flatten().as_str(),
            "http://example/A#/path/prefix/foo/32"
        );
        assert_eq!(s5.flatten().as_str(), "http://example/B#/something");

        assert_eq!(s1.resource_depth(), 1);
        assert_eq!(s3.resource_depth(), 1);
        assert_eq!(s4.resource_depth(), 2);
        assert_eq!(s5.resource_depth(), 2);
    }
}
