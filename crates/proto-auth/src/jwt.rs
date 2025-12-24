use base64::Engine;

/// Sign claims to produce a JWT token.
pub fn sign<Claims>(
    claims: Claims,
    key: &jsonwebtoken::EncodingKey,
) -> Result<String, tonic::Status>
where
    Claims: serde::Serialize,
{
    // Select an appropriate algorithm and header for the key.
    // NOTE: AlgorithmFamily is not exported, so we cannot match over it
    // and must use algorithms().first() which has hard-coded behavior:
    //  - Hmac => HS256
    //  - Rsa => RS256
    //  - Ec => ES256
    //  - Ed => EdDSA
    let algo_family = key.family();
    let algo = algo_family
        .algorithms()
        .first()
        .expect("hard-coded and never empty");

    let header = jsonwebtoken::Header::new(*algo);

    let token = jsonwebtoken::encode(&header, &claims, key)
        .map_err(|e| tonic::Status::internal(format!("failed to encode token: {e}")))?;

    Ok(token)
}

/// Verified is a type-safe wrapper around verified JWT claims.
/// It can only be constructed by the verify() routine,
/// which gives assurance that the claims have been verified.
pub struct Verified<Claims>(Claims, std::time::SystemTime);

impl<Claims> Verified<Claims> {
    /// Return the verified claims.
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    /// Return the expiry time of the token.
    pub fn expiry(&self) -> std::time::SystemTime {
        self.1
    }

    /// Return the remaining duration of the token, with respect to now().
    pub fn valid_for(&self) -> std::time::Duration {
        self.1
            .duration_since(std::time::SystemTime::now())
            .unwrap_or_default()
    }
}

/// Verify a JWT token and return its verified claims.
pub fn verify<Claims>(
    token: &[u8],
    require_capability: u32,
    keys: &[jsonwebtoken::DecodingKey],
) -> Result<Verified<Claims>, tonic::Status>
where
    Claims: serde::de::DeserializeOwned + Clone,
{
    // Note that the default Validation checks `exp` claim.
    let mut validation = jsonwebtoken::Validation::default();
    let mut last_error = None;

    // Try each verification key.
    for key in keys {
        validation.algorithms.clear();
        validation
            .algorithms
            .extend_from_slice(key.family().algorithms());

        let claims =
            match jsonwebtoken::decode::<Box<serde_json::value::RawValue>>(token, key, &validation)
            {
                Ok(token_data) => token_data.claims,
                Err(err) => {
                    last_error = Some(tonic::Status::unauthenticated(format!(
                        "failed to verify token: {err}"
                    )));
                    continue;
                }
            };

        #[derive(serde::Deserialize)]
        struct ClaimsSkim {
            exp: u64,
            #[serde(default)]
            cap: Option<u32>,
        }
        let skim: ClaimsSkim = serde_json::from_str(claims.get()).map_err(|err| {
            tonic::Status::unauthenticated(format!(
                "failed to skim `cap` claim from verified JWT: {err}"
            ))
        })?;

        if skim.cap.unwrap_or_default() & require_capability != require_capability {
            return Err(tonic::Status::permission_denied(format!(
                "missing capability: have {:#x}, require {:#x}",
                skim.cap.unwrap_or_default(),
                require_capability
            )));
        }

        let claims: Claims = serde_json::from_str(claims.get()).map_err(|err| {
            tonic::Status::unauthenticated(format!(
                "failed to deserialize verified JWT claims: {err}"
            ))
        })?;

        return Ok(Verified(
            claims,
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(skim.exp),
        ));
    }

    Err(last_error.expect("at least one key provided"))
}

/// Unverified is a type-safe wrapper around unverified JWT claims.
/// It can only be constructed by the parse_unverified() routine.
pub struct Unverified<Claims>(Claims, std::time::SystemTime);

impl<Claims> Unverified<Claims> {
    /// Return the unverified claims.
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    /// Return the expiry time of the token.
    pub fn expiry(&self) -> std::time::SystemTime {
        self.1
    }

    /// Return the remaining duration of the token, with respect to now().
    pub fn valid_for(&self) -> std::time::Duration {
        self.1
            .duration_since(std::time::SystemTime::now())
            .unwrap_or_default()
    }
}

/// Parse the claims of a JWT token without verifying them.
pub fn parse_unverified<Claims>(token: &[u8]) -> Result<Unverified<Claims>, tonic::Status>
where
    Claims: serde::de::DeserializeOwned + Clone,
{
    let mut parts = token.split(|b| *b == b'.');

    let _header = parts
        .next()
        .ok_or_else(|| tonic::Status::unauthenticated("JWT token missing header"))?;
    let payload = parts
        .next()
        .ok_or_else(|| tonic::Status::unauthenticated("JWT token missing payload"))?;

    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|err| {
            tonic::Status::unauthenticated(format!("JWT token has invalid base64: {err}"))
        })?;

    #[derive(serde::Deserialize)]
    struct ClaimsSkim {
        exp: u64,
    }
    let skim: ClaimsSkim = serde_json::from_slice(&claims)
        .map_err(|err| tonic::Status::unauthenticated(format!("failed to skim claims: {err}")))?;

    let claims: Claims = serde_json::from_slice(&claims).map_err(|err| {
        tonic::Status::unauthenticated(format!(
            "failed to deserialize unverified JWT claims: {err}"
        ))
    })?;

    return Ok(Unverified(
        claims,
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(skim.exp),
    ));
}

/// Parse base64-encoded HMAC keys used for signing and verification.
/// Keys are separated by whitespace or commas, the first key is used for signing,
/// and all keys are used for verification.
pub fn parse_base64_hmac_keys(
    base64_keys: &str,
) -> Result<(jsonwebtoken::EncodingKey, Vec<jsonwebtoken::DecodingKey>), tonic::Status> {
    let mut encoding_key: Option<jsonwebtoken::EncodingKey> = None;
    let mut decoding_keys = Vec::new();

    for (i, key_str) in base64_keys.replace(',', " ").split_whitespace().enumerate() {
        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(key_str)
            .map_err(|e| tonic::Status::invalid_argument(format!("key {i}: {e}")))?;

        if encoding_key.is_none() {
            encoding_key = Some(jsonwebtoken::EncodingKey::from_secret(&key_bytes));
        }
        decoding_keys.push(jsonwebtoken::DecodingKey::from_secret(&key_bytes));
    }

    let signing_key = encoding_key
        .ok_or_else(|| tonic::Status::invalid_argument("at least one key must be provided"))?;

    Ok((signing_key, decoding_keys))
}
