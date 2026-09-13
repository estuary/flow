use tokens::TimeDelta;

/// Minimum age of a [`DekafAuth`] before its revocation takes effect.
///
/// Sessions revoke a DekafAuth whenever they observe evidence that it's
/// stale, and a successful re-authorization carries no server-side pacing.
/// This cool-off is what keeps a task's sessions from turning that into a
/// request storm: a revoke of a response younger than this is honored, but
/// only once the response has aged into it. The Watch's last good token
/// stays visible throughout, and a server-directed retry (`retry_millis`)
/// is unaffected because it never passes through the revocation path.
const MIN_REFRESH_INTERVAL: TimeDelta = TimeDelta::seconds(20);

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
    /// Upper bound on the refresh cadence of a response. An authorized
    /// response's control-plane token typically outlives it, and a redirect
    /// carries no token at all, so this bound is what picks up changes which
    /// produce no error: a rotated password, a removed binding, a task which
    /// has migrated back.
    max_refresh: TimeDelta,
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
    type Revoke = futures::future::BoxFuture<'static, ()>;

    async fn build_request(
        &mut self,
        started: tokens::DateTime,
    ) -> tonic::Result<reqwest::RequestBuilder> {
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

        // A redirect carries no token to draw a cadence from.
        let refresh_after = if model.redirect_dataplane_fqdn.is_some() {
            self.max_refresh
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

        // The cool-off is the revocation's own delay: it resolves no sooner
        // than MIN_REFRESH_INTERVAL after this response was extracted, however
        // early it was cancelled. Measuring from extraction, rather than from
        // the cancel, means a revoke of an already-aged response is immediate.
        let cool_off = tokio::time::Instant::now() + MIN_REFRESH_INTERVAL.to_std().unwrap();
        let revoked = Box::pin(async move {
            () = revoked.await;
            () = tokio::time::sleep_until(cool_off).await;
        });

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

        // A redirect carries no token, and refreshes on `max_refresh` alone.
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
    async fn test_revoke_cools_off() {
        let source = source(TimeDelta::minutes(5));
        let cool_off = MIN_REFRESH_INTERVAL.to_std().unwrap();

        // A revoke of a fresh response is honored, but only once the response
        // has aged into the cool-off.
        let extracted = tokio::time::Instant::now();
        let (token, _refresh_after, revoke) = source
            .extract(authorized(TimeDelta::hours(1)))
            .unwrap()
            .unwrap();
        tokio::pin!(revoke);

        assert!(futures::poll!(revoke.as_mut()).is_pending());
        token.revoke.cancel();
        assert!(futures::poll!(revoke.as_mut()).is_pending());

        () = revoke.as_mut().await;
        assert_eq!(tokio::time::Instant::now() - extracted, cool_off);

        // A revoke of a response older than the cool-off is immediate.
        let (token, _refresh_after, revoke) = source
            .extract(authorized(TimeDelta::hours(1)))
            .unwrap()
            .unwrap();
        tokio::pin!(revoke);

        tokio::time::advance(cool_off).await;
        assert!(futures::poll!(revoke.as_mut()).is_pending());
        token.revoke.cancel();
        assert!(futures::poll!(revoke.as_mut()).is_ready());

        // A response which is never revoked doesn't resolve at all.
        let (_token, _refresh_after, revoke) = source
            .extract(authorized(TimeDelta::hours(1)))
            .unwrap()
            .unwrap();
        tokio::pin!(revoke);

        tokio::time::advance(cool_off * 2).await;
        assert!(futures::poll!(revoke.as_mut()).is_pending());
    }
}
