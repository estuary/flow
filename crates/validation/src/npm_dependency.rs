use super::Error;
use itertools::Itertools;

pub fn walk_all_npm_dependencies(
    npm_dependencies: &[tables::NPMDependency],
    errors: &mut tables::Errors,
) {
    for (lhs, rhs) in npm_dependencies
        .iter()
        .sorted_by_key(|p| (&p.package, &p.version))
        .tuple_windows()
    {
        if lhs.package != rhs.package {
            continue;
        } else if lhs.version != rhs.version {
            Error::NPMVersionsIncompatible {
                package: lhs.package.clone(),
                lhs_version: lhs.version.clone(),
                rhs_version: rhs.version.clone(),
                rhs_scope: rhs.scope.clone(),
            }
            .push(&lhs.scope, errors);
        }
    }
}
