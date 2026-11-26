use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

pub mod alert_subscriptions;
pub mod alerts;
pub mod billing;
pub mod catalog_stats;
pub mod connector_tags;
pub mod connectors;
pub mod controllers;
pub mod data_plane;
pub mod discovers;
pub mod draft;
mod envelope;
pub mod grants;
mod interval;
pub mod jobs;
pub mod live_specs;
pub mod logs;
pub mod publications;
pub mod sandboxes;
pub mod secrets;
pub mod server;
pub mod sprites;
pub mod storage_mappings;
pub mod tenants;
mod text_json;

#[cfg(test)]
pub(crate) mod test_server;

/// TextJson encodes JSON for Postgres while preserving property ordering.
pub use text_json::TextJson;

/// TODO(johnny): Could we use sqlx's native PgInterval type?
pub use interval::Interval;

/// ControlClaims are claims encoded within control-plane access tokens.
type ControlClaims = models::authorizations::ControlClaims;

/// DataClaims are claims encoded within data-plane access tokens.
pub type DataClaims = proto_gazette::Claims;

/// AuthZResult is the result of an authorization policy evaluation,
/// designed to be used with Envelope::authorization_outcome.
///
/// Its Ok variant contains an optional `cordon_at` DateTime which denotes when
/// the authorization will become invalid due to cordoning, which (when present)
/// upper-bounds the expiry of a derived authorization.
pub type AuthZResult<Ok> = tonic::Result<(Option<tokens::DateTime>, Ok)>;

/// Envelope is common fields and parameters of every API request.
pub use envelope::{Envelope, Locale, MaybeControlClaims};

// TODO(johnny): These types are all fundamental to this crate, and should be
// hoisted from the `server` module. For now, just re-export to minimize churn.
pub(crate) use server::evaluate_names_authorization;
pub use server::{
    ApiError, App, AuthZRetry, build_router,
    snapshot::{self, Snapshot},
};

// TODO(johnny): Move to publications module?
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "flow_type")]
#[sqlx(rename_all = "snake_case")]
pub enum FlowType {
    Capture,
    Collection,
    Materialization,
    Test,
    SourceCapture,
}

impl From<models::CatalogType> for FlowType {
    fn from(c: models::CatalogType) -> Self {
        use models::CatalogType;
        match c {
            CatalogType::Capture => FlowType::Capture,
            CatalogType::Collection => FlowType::Collection,
            CatalogType::Materialization => FlowType::Materialization,
            CatalogType::Test => FlowType::Test,
        }
    }
}

/// Returns the user ID for the given email address, or an error if the email address is not found.
pub async fn get_user_id_for_email(email: &str, db: &sqlx::PgPool) -> sqlx::Result<Uuid> {
    sqlx::query_scalar!(
        r#"
        SELECT id
        FROM auth.users
        WHERE email = $1
        "#,
        email
    )
    .fetch_one(db)
    .await
}

/// Select a page of `sorted`, which must be in ascending order of `key`.
/// `after` and `before` are exclusive bounds. Within them, `first` keeps the
/// leading rows, then `last` keeps the trailing rows of those. Returns the page,
/// whether rows precede it, and whether rows follow it.
pub(crate) fn paginate<T, K: Ord>(
    mut sorted: Vec<T>,
    key: impl Fn(&T) -> K,
    after: Option<K>,
    before: Option<K>,
    first: Option<usize>,
    last: Option<usize>,
) -> (Vec<T>, bool, bool) {
    let total = sorted.len();
    let mut start = after.map_or(0, |after| sorted.partition_point(|row| key(row) <= after));
    let mut end = before
        .map_or(total, |before| {
            sorted.partition_point(|row| key(row) < before)
        })
        .max(start);
    if let Some(first) = first {
        end = end.min(start.saturating_add(first));
    }
    if let Some(last) = last {
        start = start.max(end.saturating_sub(last));
    }
    (sorted.drain(start..end).collect(), start > 0, end < total)
}

fn status_into_response(mut status: tonic::Status) -> axum::response::Response {
    let http_code = tokens::rest::grpc_status_code_to_http(status.code());
    let http_code = axum::http::StatusCode::from_u16(http_code).unwrap();
    let mut builder = axum::response::Response::builder().status(http_code);

    // Map Status Metadata into HTTP headers.
    let mut headers = std::mem::take(status.metadata_mut()).into_headers();
    std::mem::swap(builder.headers_mut().unwrap(), &mut headers);

    let body = axum::body::Body::from(status.message().to_string());
    builder.body(body).unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn paginates() {
        fn page(
            after: Option<&str>,
            before: Option<&str>,
            first: Option<usize>,
            last: Option<usize>,
        ) -> (Vec<&'static str>, bool, bool) {
            super::paginate(
                vec!["a", "b", "c", "d"],
                |n| n.to_string(),
                after.map(String::from),
                before.map(String::from),
                first,
                last,
            )
        }

        // Forward pagination: (rows, has_previous, has_next).
        assert_eq!(
            page(None, None, Some(2), None),
            (vec!["a", "b"], false, true)
        );
        assert_eq!(
            page(Some("b"), None, Some(2), None),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(
            page(Some("b"), None, Some(10), None),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(page(Some("d"), None, Some(2), None), (vec![], true, false));

        // Backward pagination.
        assert_eq!(
            page(None, None, None, Some(2)),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(
            page(None, Some("d"), None, Some(2)),
            (vec!["b", "c"], true, true)
        );
        assert_eq!(
            page(None, Some("c"), None, Some(10)),
            (vec!["a", "b"], false, true)
        );

        // Zero and absent limits.
        assert_eq!(page(None, None, Some(0), None), (vec![], false, true));
        assert_eq!(page(None, None, None, Some(0)), (vec![], true, false));
        assert_eq!(
            page(None, None, None, None),
            (vec!["a", "b", "c", "d"], false, false)
        );

        // Both bounds, then both limits.
        assert_eq!(
            page(Some("a"), Some("d"), None, None),
            (vec!["b", "c"], true, true)
        );
        assert_eq!(
            page(Some("a"), Some("d"), Some(1), None),
            (vec!["b"], true, true)
        );
        assert_eq!(
            page(None, None, Some(3), Some(2)),
            (vec!["b", "c"], true, true)
        );

        // Cursors outside the rows, then crossed cursors.
        assert_eq!(
            page(Some(""), None, Some(10), None),
            (vec!["a", "b", "c", "d"], false, false)
        );
        assert_eq!(
            page(Some("0"), None, Some(2), None),
            (vec!["a", "b"], false, true)
        );
        assert_eq!(
            page(None, Some("z"), None, Some(2)),
            (vec!["c", "d"], true, false)
        );
        assert_eq!(page(None, Some(""), None, Some(2)), (vec![], false, true));
        assert_eq!(page(Some("c"), Some("b"), None, None), (vec![], true, true));
    }
}
