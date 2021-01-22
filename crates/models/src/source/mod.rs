use crate::tables;

mod loader;
pub mod scenarios;
mod scope;
pub mod specs;
pub mod wrappers;

pub use loader::{FetchResult, LoadError, Loader};
pub use scope::Scope;

pub use specs::*;
pub use wrappers::*;

#[derive(Default, Debug)]
pub struct Tables {
    pub captures: tables::Captures,
    pub collections: tables::Collections,
    pub derivations: tables::Derivations,
    pub endpoints: tables::Endpoints,
    pub errors: tables::Errors,
    pub fetches: tables::Fetches,
    pub imports: tables::Imports,
    pub materializations: tables::Materializations,
    pub nodejs_dependencies: tables::NodeJSDependencies,
    pub projections: tables::Projections,
    pub resources: tables::Resources,
    pub schema_docs: tables::SchemaDocs,
    pub test_steps: tables::TestSteps,
    pub transforms: tables::Transforms,
}

impl Tables {
    // Access Tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn tables::TableObj> {
        // This de-structure ensures we can't fail to update if fields change.
        let Tables {
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            materializations,
            nodejs_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        } = self;

        vec![
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            materializations,
            nodejs_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        ]
    }

    // Access Tables as an array of mutable dynamic TableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn tables::TableObj> {
        let Tables {
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            materializations,
            nodejs_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        } = self;

        vec![
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            materializations,
            nodejs_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        ]
    }
}
