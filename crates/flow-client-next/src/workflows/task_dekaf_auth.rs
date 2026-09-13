use tokens::TimeDelta;

/// Minimum interval between `/authorize/dekaf` requests which this task
/// originates.
///
/// Sessions revoke a [`DekafAuth`] whenever they observe evidence that it's
/// stale, and a successful re-authorization carries no server-side pacing.
/// This cool-off is what keeps a task's sessions from turning that into a
/// request storm. It's slept inside `build_request`, so the Watch's last
/// good token stays visible throughout.
///
/// It does not apply to a server-directed retry (`retry_millis`): the
/// control plane sized that delay to land after its own snapshot refresh,
/// and holding it back further only delays a definitive answer. A retry
/// is told apart by its `started`, which the Watch holds constant across
/// retries of one logical operation and renews for each new one.
const MIN_REFRESH_INTERVAL: TimeDelta = TimeDelta::seconds(20);

/// Redirect responses carry no token to draw a cadence from.
const REDIRECT_REFRESH_INTERVAL: TimeDelta = TimeDelta::minutes(5);

/// DekafAuth is a `/authorize/dekaf` response paired with a handle which a
/// bearer may cancel to force a prompt re-fetch, bounded by the cool-off.
pub struct DekafAuth {
    pub response: models::authorizations::DekafAuthResponse,
    /// Cancel to ask that this authorization be refreshed now, because its
    /// bearer has seen something which only a newer response can explain.
    /// A cool-off bounds how promptly that can actually happen.
    pub revoke: tokens::CancellationToken,
}

/// TaskDekafAuth is a tokens::Source for Dekaf tasks requesting their
/// MaterializationSpec and access to persisted AVRO schemas.
pub struct TaskDekafAuth {
    client: crate::rest::Client,
    /// SignedSource for authorization request claims.
    signed_source: tokens::jwt::SignedSource<proto_gazette::Claims>,
    /// Upper bound on the refresh cadence of an authorized response.
    /// The control plane's token typically outlives it, and this bound is
    /// what picks up non-error changes: a rotated password, a removed binding.
    max_refresh: TimeDelta,
    /// DateTime at which this Source last signed a request.
    last_request: tokens::DateTime,
    /// `started` of the request this Source last signed. A request bearing
    /// the same `started` is a server-directed retry of that operation.
    last_started: tokens::DateTime,
}

impl TaskDekafAuth {
    pub fn new(
        client: crate::rest::Client,
        signed_source: tokens::jwt::SignedSource<proto_gazette::Claims>,
        max_refresh: TimeDelta,
    ) -> Self {
        Self {
            client,
            signed_source,
            max_refresh,
            last_request: tokens::DateTime::UNIX_EPOCH,
            last_started: tokens::DateTime::UNIX_EPOCH,
        }
    }
}

/// Build a SignedSource for authoring TaskDekafAuth request tokens scoping
/// the requesting data-plane & task.
///
/// `task_name` is the catalog name of the requesting subject Dekaf task.
///
/// `data_plane_fqdn` is the FQDN of the data-plane hosting the task.
///
/// `data_plane_signing_key` is the secret data-plane signing key
/// corresponding to the data-plane FQDN.
///
pub fn new_signed_source(
    task_name: String,
    data_plane_fqdn: String,
    data_plane_signing_key: tokens::jwt::EncodingKey,
) -> tokens::jwt::SignedSource<proto_gazette::Claims> {
    let claims = proto_gazette::Claims {
        cap: proto_flow::capability::AUTHORIZE,
        exp: 0,
        iat: 0,
        iss: data_plane_fqdn,
        sel: Default::default(),
        sub: task_name,
    };

    tokens::jwt::SignedSource {
        claims,
        set_time_claims: Box::new(|claims, _iat, exp| {
            // claims.iat is explicitly set to the start time of the logical request.
            claims.exp = exp.timestamp() as u64;
        }),
        duration: TimeDelta::minutes(1),
        key: data_plane_signing_key,
    }
}

impl tokens::RestSource for TaskDekafAuth {
    type Model = models::authorizations::DekafAuthResponse;
    type Token = DekafAuth;
    type Revoke = tokens::WaitForCancellationFutureOwned;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
        let cool_off = (self.last_request + MIN_REFRESH_INTERVAL) - tokens::now();
        if started != self.last_started
            && let Ok(cool_off) = cool_off.to_std()
        {
            // A new operation within the cool-off: we must wait.
            () = tokio::time::sleep(cool_off).await;
        }
        self.last_request = tokens::now();
        self.last_started = started;

        self.signed_source.claims.iat = started.timestamp() as u64;

        let request = models::authorizations::TaskAuthorizationRequest {
            token: self.signed_source.sign()?,
        };
        Ok(self.client.post("/authorize/dekaf", &request, None))
    }

    fn extract(
        &self,
        model: Self::Model,
    ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
        if model.retry_millis != 0 {
            return Ok(Err(TimeDelta::milliseconds(model.retry_millis as i64)));
        }

        // Redirects don't include a token. Use a fixed periodic refresh.
        let refresh_after = if model.redirect_dataplane_fqdn.is_some() {
            REDIRECT_REFRESH_INTERVAL
        } else {
            let unverified =
                tokens::jwt::parse_unverified::<serde::de::IgnoredAny>(model.token.as_bytes())?;

            std::cmp::min(
                tokens::refresh_before_expiry(unverified.valid_for()),
                self.max_refresh,
            )
        };

        let revoke = tokens::CancellationToken::new();
        let revoked = revoke.clone().cancelled_owned();

        Ok(Ok((
            DekafAuth {
                response: model,
                revoke,
            },
            refresh_after,
            revoked,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{MIN_REFRESH_INTERVAL, TaskDekafAuth, new_signed_source};
    use tokens::{RestSource, TimeDelta};

    fn source(max_refresh: TimeDelta) -> TaskDekafAuth {
        TaskDekafAuth::new(
            crate::rest::Client::new(
                &url::Url::parse("https://agent.example.com/").unwrap(),
                "test",
            ),
            new_signed_source(
                "acmeCo/dekaf-pineapple".to_string(),
                "fqdn.example.com".to_string(),
                tokens::jwt::EncodingKey::from_secret(b"secret"),
            ),
            max_refresh,
        )
    }

    /// A response bearing a token valid for `valid_for`.
    fn authorized(valid_for: TimeDelta) -> models::authorizations::DekafAuthResponse {
        #[derive(serde::Serialize)]
        struct Claims {
            exp: i64,
        }
        let token = tokens::jwt::sign(
            Claims {
                exp: (tokens::now() + valid_for).timestamp(),
            },
            &tokens::jwt::EncodingKey::from_secret(b"secret"),
        )
        .unwrap();

        models::authorizations::DekafAuthResponse {
            token,
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_extract_cadence() {
        let source = source(TimeDelta::minutes(5));

        // A long-lived token is clamped to `max_refresh`.
        let (_token, refresh_after, _revoke) = source
            .extract(authorized(TimeDelta::hours(1)))
            .unwrap()
            .unwrap();
        assert_eq!(refresh_after, TimeDelta::minutes(5));

        // A token expiring sooner than `max_refresh` sets the cadence itself,
        // refreshed two minutes early. A JWT `exp` is whole seconds, so the
        // round-trip through one loses sub-second precision.
        let (_token, refresh_after, _revoke) = source
            .extract(authorized(TimeDelta::minutes(4)))
            .unwrap()
            .unwrap();
        assert!(
            (refresh_after - TimeDelta::minutes(2)).abs() < TimeDelta::seconds(1),
            "{refresh_after:?}"
        );

        // A server-directed retry is neither.
        let Ok(Err(retry_after)) = source.extract(models::authorizations::DekafAuthResponse {
            retry_millis: 5_000,
            ..Default::default()
        }) else {
            panic!("expected a server-directed retry")
        };
        assert_eq!(retry_after, TimeDelta::seconds(5));

        // A redirect carries no token, and refreshes on its own cadence.
        let (_token, refresh_after, _revoke) = source
            .extract(models::authorizations::DekafAuthResponse {
                redirect_dataplane_fqdn: Some("other.example.com".to_string()),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(refresh_after, TimeDelta::minutes(5));
    }

    #[tokio::test(start_paused = true)]
    async fn test_revoke_resolves_on_cancel() {
        let source = source(TimeDelta::minutes(5));

        let (token, _refresh_after, revoke) = source
            .extract(authorized(TimeDelta::hours(1)))
            .unwrap()
            .unwrap();
        tokio::pin!(revoke);

        assert!(futures::poll!(revoke.as_mut()).is_pending());
        token.revoke.cancel();
        assert!(futures::poll!(revoke).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn test_build_request_cools_off() {
        let mut source = source(TimeDelta::minutes(5));
        let cool_off = MIN_REFRESH_INTERVAL.to_std().unwrap();

        // The first request is immediate: `last_request` is the epoch.
        let entered = tokio::time::Instant::now();
        let started = tokens::now();
        _ = source.build_request(started).await.unwrap();
        assert_eq!(tokio::time::Instant::now(), entered);

        // A server-directed retry of the same operation is also immediate:
        // the control plane already paced it, and `started` says so.
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        let retried = tokio::time::Instant::now();
        _ = source.build_request(started).await.unwrap();
        assert_eq!(tokio::time::Instant::now(), retried);

        // A new operation waits out the cool-off, measured from the retry.
        let started = tokens::now();
        _ = source.build_request(started).await.unwrap();
        assert_eq!(tokio::time::Instant::now() - retried, cool_off);

        // Once the cool-off has passed, a new operation is immediate again.
        tokio::time::advance(cool_off).await;
        let entered = tokio::time::Instant::now();
        let started = tokens::now();
        _ = source.build_request(started).await.unwrap();
        assert_eq!(tokio::time::Instant::now(), entered);
    }
}
