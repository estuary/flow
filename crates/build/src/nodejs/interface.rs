use super::camel_case;
use itertools::Itertools;
use models::{names, tables};
use std::fmt::Write;
use std::path;
use url::Url;

#[derive(Debug)]
pub enum MethodType {
    Shuffle,
    Publish,
    Update,
}

impl MethodType {
    pub fn method_name(&self, transform: &names::Transform) -> String {
        let mut w = camel_case(transform, false);
        write!(w, "{:?}", self).unwrap();
        w
    }

    pub fn signature(&self, transform: &tables::Transform) -> Vec<String> {
        let tables::Transform {
            derivation,
            source_collection,
            source_schema,
            transform,
            ..
        } = transform;

        let src = match source_schema {
            Some(_) => format!(
                "transforms.{}{}Source",
                camel_case(derivation, true),
                camel_case(transform, false)
            ),
            None => format!("collections.{}", camel_case(source_collection, true)),
        };
        let tgt = camel_case(derivation, true);

        let mut lines = Vec::new();
        lines.push(format!("{}(", self.method_name(transform)));
        lines.push(format!("    source: {},", src));

        match self {
            MethodType::Shuffle => {
                lines.push("): unknown[]>".to_string());
            }
            MethodType::Update => {
                lines.push(format!("): registers.{}[]", tgt));
            }
            MethodType::Publish => {
                lines.push(format!("    register: registers.{},", tgt));
                lines.push(format!("    previous: registers.{},", tgt));
                lines.push(format!("): collections.{}[]", tgt));
            }
        }

        lines
    }
}

pub struct Method<'a> {
    pub derivation: &'a tables::Derivation,
    pub transform: &'a tables::Transform,
    pub type_: MethodType,
}

impl<'a> Method<'a> {
    pub fn signature(&self) -> Vec<String> {
        self.type_.signature(&self.transform)
    }
}

pub struct Interface<'a> {
    pub derivation: &'a tables::Derivation,
    pub module: Module,
    pub methods: Vec<Method<'a>>,
}

impl<'a> Interface<'a> {
    pub fn extract_all(
        package_dir: &path::Path,
        derivations: &'a [tables::Derivation],
        transforms: &'a [tables::Transform],
    ) -> Vec<Interface<'a>> {
        let mut methods = Vec::new();

        for transform in transforms.iter() {
            // Map transform through the corresponding derivation row.
            let derivation = match derivations
                .iter()
                .find(|d| d.derivation == transform.derivation)
            {
                Some(d) => d,
                None => continue,
            };

            // Pattern-match against the shuffle, update, and publish lambda locations.
            if matches!(transform.shuffle_lambda, Some(names::Lambda::Typescript)) {
                methods.push(Method {
                    derivation,
                    transform,
                    type_: MethodType::Shuffle,
                });
            }
            if matches!(transform.update_lambda, Some(names::Lambda::Typescript)) {
                methods.push(Method {
                    derivation,
                    transform,
                    type_: MethodType::Update,
                });
            }
            if matches!(transform.publish_lambda, Some(names::Lambda::Typescript)) {
                methods.push(Method {
                    derivation,
                    transform,
                    type_: MethodType::Publish,
                });
            }
        }

        methods.sort_by_key(|m| (&m.transform.derivation, &m.transform.transform));

        methods
            .into_iter()
            .group_by(|m| &m.transform.derivation)
            .into_iter()
            .map(|(_, methods)| {
                let methods = methods.collect::<Vec<_>>();
                let derivation = &methods[0].derivation;

                // Map to the TypeScript module URL which must live alongside the
                // derivation's resource spec, and which must implement this interface.
                let mut module = derivation.scope.clone();
                let mut path = path::PathBuf::from(derivation.scope.path());
                path.set_extension("ts");

                module.set_path(path.to_str().unwrap()); // Still UTF-8.
                module.set_fragment(None);
                module.set_query(None);

                Interface {
                    derivation,
                    methods,
                    module: Module::new(&module, package_dir),
                }
            })
            .collect()
    }
}

pub struct Module {
    url: url::Url,
    relative: Option<String>,
}

impl Module {
    pub fn new(url: &Url, package_dir: &path::Path) -> Module {
        assert!(package_dir.is_absolute());

        let relative = package_dir
            .to_str()
            .filter(|_| url.scheme() == "file")
            .and_then(|d| url.path().strip_prefix(d))
            .map(|p| &p[1..]) // Trim leading '/', which remains after stripping directory.
            .map(str::to_string);

        Module {
            url: url.clone(),
            relative,
        }
    }

    pub fn is_relative(&self) -> bool {
        matches!(self.relative, Some(_))
    }

    pub fn absolute_url(&self) -> &url::Url {
        &self.url
    }

    /// Return the Module as a join-able relative URL, complete with query and fragment.
    /// The URL is only relative if the Module is rooted by the package directory.
    /// Otherwise, an absolute URL is returned.
    pub fn relative_url(&self) -> String {
        match &self.relative {
            Some(relative) => {
                let mut relative = relative.clone();

                // Re-attach trailing query & fragment components.
                if let Some(query) = self.url.query() {
                    relative.push('?');
                    relative.push_str(query);
                }

                if let Some(fragment) = self.url.fragment() {
                    relative.push('#');
                    relative.push_str(fragment);
                }
                relative
            }
            None => self.url.to_string(),
        }
    }

    /// Return the Module as a path relative to the package directory.
    pub fn relative_path(&self) -> String {
        assert!(self.url.query().is_none());
        assert!(self.url.fragment().is_none());
        assert!(!self.url.cannot_be_a_base());

        if let Some(relative) = &self.relative {
            return relative.clone();
        }
        let mut parts = vec!["flow_generated", "external"];

        if let Some(d) = self.url.domain() {
            parts.push(d)
        }
        parts.extend(
            self.url
                .path_segments()
                .unwrap()
                .filter(|segment| !segment.is_empty()),
        );
        parts.join("/")
    }
}
