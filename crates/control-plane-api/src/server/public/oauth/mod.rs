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
