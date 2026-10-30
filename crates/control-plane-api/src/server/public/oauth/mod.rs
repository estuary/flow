//! Connect the OAuth protocol routes to control-plane sessions and credentials.
use std::sync::Arc;

pub use oauth_server::{Config, router};

#[async_trait::async_trait]
impl oauth_server::Backend for crate::App {
    type Session = crate::Envelope;

    fn config(&self) -> Result<&Config, oauth_server::Error> {
        self.oauth.as_deref().ok_or(oauth_server::Error(
            "server_error",
            "unable to complete OAuth request",
        ))
    }

    async fn authorize_user(
        self: &Arc<Self>,
        env: Self::Session,
    ) -> Result<uuid::Uuid, oauth_server::Error> {
        let claims = env.claims().map_err(|_| {
            oauth_server::Error(
                "access_denied",
                "an unrestricted human user credential is required",
            )
        })?;
        if claims.role != "authenticated"
            || claims.capability_mask.is_some()
            || claims.prefix_scope.is_some()
            || crate::server::is_service_account(&self.pg_pool, claims.sub)
                .await
                .map_err(oauth_server::internal)?
        {
            return Err(oauth_server::Error(
                "access_denied",
                "an unrestricted human user credential is required",
            ));
        }
        Ok(claims.sub)
    }

    async fn create_refresh_token(
        &self,
        user_id: uuid::Uuid,
        client_id: &str,
    ) -> anyhow::Result<oauth_server::RefreshTokenResponse> {
        let credential = super::token_exchange::create_refresh_token(
            &self.pg_pool,
            user_id,
            false,
            "P90D",
            Some(&format!("OAuth: {client_id}")),
        )
        .await?
        .expect("90 days is within the refresh-token validity bounds");
        Ok(oauth_server::RefreshTokenResponse {
            id: credential.id,
            secret: credential.secret,
        })
    }

    async fn generate_access_token(
        &self,
        id: models::Id,
        secret: &str,
    ) -> tonic::Result<oauth_server::TokenResponse> {
        let response =
            super::token_exchange::generate_access_token(&self.pg_pool, id, secret).await?;
        Ok(oauth_server::TokenResponse {
            access_token: response.access_token,
            refresh_token: response.refresh_token.map(|credential| {
                oauth_server::RefreshTokenResponse {
                    id: credential.id,
                    secret: credential.secret,
                }
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn consent_requires_unrestricted_human(pool: sqlx::PgPool) {
        use axum::extract::FromRequestParts;
        use oauth_server::Backend;

        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        for (mask, scope, service_account, allowed) in [
            (None, None, false, true),
            (Some(vec![]), None, false, false),
            (Some(vec!["admin".into()]), None, false, false),
            (None, Some(vec!["acmeCo/".into()]), false, false),
            (None, Some(vec![]), false, false),
            (None, None, true, false),
        ] {
            if service_account {
                sqlx::query("INSERT INTO internal.service_accounts (user_id, catalog_name, created_by) VALUES ($1, 'acmeCo/bot', $1)")
                    .bind(alice).execute(&pool).await.unwrap();
            }
            let bearer = server.make_restricted_access_token(alice, None, mask, scope);
            let (mut parts, _) = axum::http::Request::builder()
                .uri("/oauth/authorize")
                .header("authorization", format!("Bearer {bearer}"))
                .body(())
                .unwrap()
                .into_parts();
            let env = crate::Envelope::from_request_parts(&mut parts, &server.app)
                .await
                .unwrap();
            let result = server.app.authorize_user(env).await;
            if allowed {
                assert_eq!(result.unwrap(), alice);
            } else {
                assert_eq!(result.unwrap_err().0, "access_denied");
            }
        }
        let (mut parts, _) = axum::http::Request::builder()
            .uri("/oauth/authorize")
            .body(())
            .unwrap()
            .into_parts();
        let env = crate::Envelope::from_request_parts(&mut parts, &server.app)
            .await
            .unwrap();
        assert_eq!(
            server.app.authorize_user(env).await.unwrap_err().0,
            "access_denied"
        );
    }
}
