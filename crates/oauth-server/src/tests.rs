use super::*;

fn config() -> Config {
    Config {
        issuer: "https://auth.example.test".parse().unwrap(),
        consent_url: "https://dashboard.example.test/consent".parse().unwrap(),
        encryption_key: B64.encode([0; 64]),
    }
}

const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";

fn request() -> AuthorizationRequest {
    AuthorizationRequest {
        response_type: "code".into(),
        client_id: "https://client.example.test/metadata.json".into(),
        redirect_uri: "https://client.example.test/callback".into(),
        code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM".into(),
        code_challenge_method: "S256".into(),
        state: Some("client-state".into()),
    }
}

fn exchange(code: String) -> TokenRequest {
    TokenRequest {
        grant_type: "authorization_code".into(),
        client_id: Some(request().client_id),
        redirect_uri: Some(request().redirect_uri),
        code: Some(code),
        code_verifier: Some(VERIFIER.into()),
        refresh_token: None,
    }
}

#[test]
fn code_redemption() {
    let config = config();
    let issued = IssuedCode {
        request: request(),
        id: models::Id::zero(),
        secret: "credential".into(),
    };
    let expires = tokens::now().timestamp() + 60;
    let code = seal(&config, "code", expires, &issued).unwrap();
    let credential = validate_exchange(&config, &exchange(code.clone())).unwrap();
    assert_eq!(credential.secret, issued.secret);

    for field in ["verifier", "missing verifier", "client", "redirect"] {
        let mut req = exchange(code.clone());
        match field {
            "verifier" => req.code_verifier = Some("x".repeat(43)),
            "missing verifier" => req.code_verifier = None,
            "client" => req.client_id = Some("https://other.example.test/metadata.json".into()),
            "redirect" => req.redirect_uri = Some("https://other.example.test/callback".into()),
            _ => unreachable!(),
        }
        assert!(validate_exchange(&config, &req).is_err(), "{field}");
    }
    let mut other_issuer = self::config();
    other_issuer.issuer = "https://other.example.test".parse().unwrap();
    let mut tampered = B64.decode(&code).unwrap();
    *tampered.last_mut().unwrap() ^= 1;
    for bad_code in [
        B64.encode(tampered),
        seal(&config, "code", tokens::now().timestamp() - 1, &issued).unwrap(),
        seal(&config, "consent", expires, &issued).unwrap(),
        seal(&other_issuer, "code", expires, &issued).unwrap(),
    ] {
        assert!(validate_exchange(&config, &exchange(bad_code)).is_err());
    }
}

struct FakeBackend {
    config: Config,
    created: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Backend for FakeBackend {
    type Session = axum::extract::State<Arc<Self>>;

    fn config(&self) -> Result<&Config, Error> {
        Ok(&self.config)
    }

    async fn authorize_user(self: &Arc<Self>, _: Self::Session) -> Result<uuid::Uuid, Error> {
        Ok(uuid::Uuid::nil())
    }

    async fn create_refresh_token(
        &self,
        user_id: uuid::Uuid,
        client_id: &str,
    ) -> anyhow::Result<RefreshTokenResponse> {
        assert_eq!(user_id, uuid::Uuid::nil());
        assert_eq!(client_id, request().client_id);
        self.created
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(RefreshTokenResponse {
            id: models::Id::zero(),
            secret: "initial".into(),
        })
    }

    async fn generate_access_token(
        &self,
        id: models::Id,
        secret: &str,
    ) -> tonic::Result<TokenResponse> {
        assert_eq!(id, models::Id::zero());
        assert_eq!(secret, "initial");
        Ok(TokenResponse {
            access_token: "access-token".into(),
            refresh_token: Some(RefreshTokenResponse {
                id,
                secret: "rotated".into(),
            }),
        })
    }
}

#[tokio::test]
async fn approval_and_denial() {
    let app = Arc::new(FakeBackend {
        config: config(),
        created: 0.into(),
    });
    let ticket = seal(
        &app.config,
        "consent",
        tokens::now().timestamp() + 600,
        Pending {
            request: request(),
            client_name: "Example client".into(),
        },
    )
    .unwrap();
    for approved in [false, true] {
        let response = consent(
            axum::extract::State(app.clone()),
            axum::extract::State(app.clone()),
            axum::Json(Decision {
                request: ticket.clone(),
                approved,
            }),
        )
        .await
        .unwrap()
        .0;
        let redirect: url::Url = response["redirect_uri"].as_str().unwrap().parse().unwrap();
        let params = redirect
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(params["state"], "client-state");
        assert_eq!(params["iss"], "https://auth.example.test");
        assert_eq!(
            app.created.load(std::sync::atomic::Ordering::Relaxed),
            usize::from(approved)
        );
        if !approved {
            assert_eq!(params["error"], "access_denied");
            assert!(!params.contains_key("code"));
            continue;
        }
        let response = token(
            axum::extract::State(app.clone()),
            Ok(axum::Form(exchange(params["code"].to_string()))),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(response["access_token"], "access-token");
        assert_eq!(response["token_type"], "Bearer");
        assert_eq!(response["expires_in"], 3600);
        let mut refresh = exchange(String::new());
        refresh.grant_type = "refresh_token".into();
        refresh.refresh_token = Some(response["refresh_token"].as_str().unwrap().into());
        assert_eq!(
            validate_exchange(&app.config, &refresh).unwrap().secret,
            "rotated"
        );
    }
}
