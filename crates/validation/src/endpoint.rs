use super::indexed;
use models::tables;

pub fn walk_all_endpoints(endpoints: &[tables::Endpoint], errors: &mut tables::Errors) {
    for tables::Endpoint {
        scope,
        endpoint: name,
        endpoint_type: _,
        base_spec: _,
    } in endpoints
    {
        indexed::walk_name(scope, "endpoint", name, &indexed::ENDPOINT_RE, errors);
    }

    indexed::walk_duplicates(
        "endpoint",
        endpoints.iter().map(|ep| (&ep.endpoint, &ep.scope)),
        errors,
    );
}
