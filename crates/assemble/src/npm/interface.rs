use super::camel_case;
use std::fmt::Write;
use std::path;
use superslice::Ext;

#[derive(Debug)]
pub enum MethodType {
    Shuffle,
    Publish,
    Update,
}

impl MethodType {
    fn method_name(&self, transform: &tables::Transform) -> String {
        let mut w = camel_case(&transform.transform, false);
        write!(w, "{:?}", self).unwrap();
        w
    }

    fn signature(&self, transform: &tables::Transform, underscore: bool) -> Vec<String> {
        let src = format!("{}Source", camel_case(&transform.transform, true));

        let mut lines = Vec::new();
        let underscore = if underscore { "_" } else { "" };

        lines.push(format!("{}(", self.method_name(transform)));
        lines.push(format!("    {underscore}source: {src},"));

        match self {
            MethodType::Shuffle => {
                lines.push("): unknown[]>".to_string());
            }
            MethodType::Update => {
                lines.push(format!("): Register[]"));
            }
            MethodType::Publish => {
                lines.push(format!("    {underscore}register: Register,"));
                lines.push(format!("    {underscore}previous: Register,"));
                lines.push(format!("): OutputDocument[]"));
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
    pub fn signature(&self, underscore: bool) -> Vec<String> {
        self.type_.signature(&self.transform, underscore)
    }

    pub fn method_name(&self) -> String {
        self.type_.method_name(&self.transform)
    }

    pub fn transforms(v: &[Self]) -> Vec<&'a tables::Transform> {
        let mut out: Vec<_> = v
            .iter()
            .map(|Method { transform, .. }| *transform)
            .collect();

        out.sort_by_key(|t| &t.transform);
        out.dedup_by_key(|t| &t.transform);

        out
    }
}

pub struct Interface<'a> {
    // Derivation which defines this interface.
    pub derivation: &'a tables::Derivation,
    // User TypeScript module which must implement the interface.
    pub typescript_module: &'a url::Url,
    // Is the typescript module relative to the package directory
    // which can be directly imported, or an external module which
    // must be copied to the local filesystem?
    pub module_is_relative: bool,
    // Relative import path of the module within the package directory.
    pub module_import_path: String,
    // Methods of the interface.
    pub methods: Vec<Method<'a>>,
}

impl<'a> Interface<'a> {
    // Extract all collections joined with optional Interfaces of its TypeScript derivation.
    pub fn extract_all(
        package_dir: &path::Path,
        collections: &'a [tables::Collection],
        derivations: &'a [tables::Derivation],
        transforms: &'a [tables::Transform],
    ) -> Vec<(&'a tables::Collection, Option<Self>)> {
        assert!(package_dir.is_absolute());
        let mut out = Vec::new();

        for collection in collections {
            let (derivation, typescript_module) = match derivations
                .binary_search_by_key(&&collection.collection, |derivation| &derivation.derivation)
                .ok()
                .map(|ind| &derivations[ind])
            {
                Some(
                    derivation @ tables::Derivation {
                        typescript_module: Some(typescript_module),
                        ..
                    },
                ) => (derivation, typescript_module),

                _ => {
                    // Collection is not a derivation, or has no TypeScript module (and thus no Interface).
                    out.push((collection, None));
                    continue;
                }
            };

            let transforms = &transforms[transforms
                .equal_range_by_key(&&derivation.derivation, |transform| &transform.derivation)];

            let mut methods = Vec::new();
            for transform in transforms {
                // Pattern-match against the shuffle, update, and publish lambda locations.
                if matches!(
                    transform.spec.shuffle,
                    Some(models::Shuffle::Lambda(models::Lambda::Typescript))
                ) {
                    methods.push(Method {
                        derivation,
                        transform,
                        type_: MethodType::Shuffle,
                    });
                }
                if matches!(
                    transform.spec.update,
                    Some(models::Update {
                        lambda: models::Lambda::Typescript
                    })
                ) {
                    methods.push(Method {
                        derivation,
                        transform,
                        type_: MethodType::Update,
                    });
                }
                if matches!(
                    transform.spec.publish,
                    Some(models::Publish {
                        lambda: models::Lambda::Typescript
                    })
                ) {
                    methods.push(Method {
                        derivation,
                        transform,
                        type_: MethodType::Publish,
                    });
                }
            }

            // If `typescript_module` is a regular file (has no query or fragment)
            // rooted by the `package_dir`, derive its import relative to that root.
            let module_import_path = package_dir
                .to_str()
                .filter(|_| {
                    typescript_module.query().is_none()
                        && typescript_module.fragment().is_none()
                        && typescript_module.scheme() == "file"
                })
                .and_then(|d| typescript_module.path().strip_prefix(d))
                .map(|p| &p[1..]) // Trim leading '/', which remains after stripping directory.
                .map(str::to_string);

            let module_is_relative = module_import_path.is_some();

            // If the module is _not_ relative, derive the location where we'll
            // write its content to *make* it relative.
            // We name these files by the derivation they implement.
            let module_import_path = match module_import_path {
                Some(p) => p,
                None => {
                    let mut parts = vec!["flow_generated", "external"];
                    parts.extend(derivation.derivation.split("/"));
                    format!("{}.ts", parts.join("/"))
                }
            };

            out.push((
                collection,
                Some(Self {
                    derivation,
                    typescript_module,
                    module_is_relative,
                    module_import_path,
                    methods,
                }),
            ));
        }

        out
    }
}
