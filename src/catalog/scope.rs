use super::{Error, Resource, Result, DB};
use estuary_json::Location;

/// Scope holds context about the current catalog build process. It's used
/// whenever the we need to know which Resource we're processing,
/// or the fine-grained Location path we've traversed within that Resource.
/// This tracking is particularly helpful for generating error messages.
#[derive(Copy, Clone)]
pub struct Scope<'a> {
    /// The database connection to use during the build process.
    pub db: &'a DB,
    // Parent of this Context, or None if this is the Context root.
    pub parent: Option<&'a Scope<'a>>,
    // Resource of this Context, which is !None if and only if this Context roots
    // processing of a new resource (as opposed to being scoped to a path therein).
    pub resource: Option<Resource>,
    // Location within the current resource.
    pub location: Location<'a>,
}

impl<'a> Scope<'a> {
    /// Empty create an empty Scope.
    pub fn empty(db: &'a rusqlite::Connection) -> Scope<'a> {
        Scope {
            db,
            parent: None,
            resource: None,
            location: Location::Root,
        }
    }

    /// Push a resource onto the current Scope, returning a new Scope.
    pub fn push_resource(&'a self, resource: Resource) -> Scope<'a> {
        Scope {
            db: self.db,
            parent: Some(self),
            resource: Some(resource),
            location: Location::Root,
        }
    }

    /// Push a property onto the current Scope, returning a new Scope.
    pub fn push_prop_with_index(&'a self, name: &'a str, index: usize) -> Scope<'a> {
        Scope {
            db: self.db,
            parent: Some(self),
            resource: None,
            location: self.location.push_prop_with_index(name, index),
        }
    }

    /// Push unordered property is like push_property, but doesn't capture an enumeration
    /// order index (eg, because we're invoking from a deserialized Rust struct without
    /// a defined visitation order).
    pub fn push_prop(&'a self, name: &'a str) -> Scope<'a> {
        self.push_prop_with_index(name, usize::MAX)
    }

    /// Push an item index onto the current Scope, returning a new Scope.
    pub fn push_item(&'a self, index: usize) -> Scope<'a> {
        Scope {
            db: self.db,
            parent: Some(self),
            resource: None,
            location: self.location.push_item(index),
        }
    }

    /// Returns the resource being processed by this Scope, which cannot be
    /// an empty Scope (eg must have been produced by at least one
    /// push_resource() invocation).
    pub fn resource(self) -> Resource {
        match self.resource {
            Some(r) => r,
            None => self
                .parent
                .expect("scope must have at least one resource")
                .resource(),
        }
    }

    /// Execute the provided closure, passing self in as the closure's Scope.
    /// If the closure returns an error, it's wrapped with the scope's location.
    pub fn then<F, T>(self, f: F) -> Result<T>
    where
        F: FnOnce(Scope) -> Result<T>,
    {
        f(self).map_err(|e| self.locate(e))
    }

    /// Locate an error by wrapping it with details of the effective Scope.
    /// If |err| already has a location, it's returned unmodified.
    pub fn locate(self, err: Error) -> Error {
        match err {
            Error::At { .. } => err,
            _ => self.locate_inner(err),
        }
    }

    fn locate_inner(self, mut err: Error) -> Error {
        if self.parent.is_none() {
            return err; // We're the terminal, empty Scope.
        }
        let url = match self.resource().primary_url(self.db) {
            Err(e) => return e,
            Ok(url) => url,
        };
        err = Error::At {
            loc: format!("{}#{}", url, self.location.url_escaped()),
            detail: Box::new(err),
        };

        // Unwrap until we reach the _parent_ of the Scope which captures
        // the current resource.
        let mut next = self;
        loop {
            let done = next.resource.is_some();
            next = *next.parent.unwrap();

            if done {
                break;
            }
        }
        next.locate_inner(err)
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{init_db_schema, open, sql_params, ContentType},
        *,
    };

    #[test]
    fn test_scope_errors() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        db.execute(
            "INSERT INTO resources
            (resource_id, content_type, content, is_processed) VALUES
            (10, 'application/schema+yaml', 'doc-a', FALSE),
            (20, 'application/schema+yaml', 'doc-b', FALSE);",
            sql_params![],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (10, 'file:///dev/null/a?query', TRUE),
                (20, 'file:///dev/null/other/b', TRUE);",
            sql_params![],
        )?;

        let s0 = Scope::empty(&db);
        let s1 = s0.push_resource(Resource { id: 10 });
        let s2 = s1.push_prop("foo");
        let s3 = s2.push_item(32);
        let s4 = s3.push_resource(Resource { id: 20 });
        let s5 = s4.push_prop("something");

        let out = s5.locate(Error::ContentTypeMismatch {
            next: ContentType::Schema,
            prev: ContentType::CatalogSpec,
        });

        assert_eq!(
            format!("{}", out),
            r#"at file:///dev/null/a?query#/foo/32:
at file:///dev/null/other/b#/something:
resource has content-type application/schema+yaml, but is already registered with type application/vnd.estuary.dev-catalog-spec+yaml"#
        );

        Ok(())
    }
}
