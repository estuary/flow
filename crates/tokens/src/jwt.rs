use crate::{DateTime, Source, TimeDelta};
use base64::Engine;

/// EncodingKey is a type alias for jsonwebtoken::EncodingKey.
/// Prefer it over jsonwebtoken::EncodingKey.
pub type EncodingKey = jsonwebtoken::EncodingKey;

/// DecodingKey is a type alias for jsonwebtoken::DecodingKey.
/// Prefer it over jsonwebtoken::DecodingKey.
pub type DecodingKey = jsonwebtoken::DecodingKey;

/// Sign claims to produce a JWT token.
pub fn sign<Claims>(claims: Claims, key: &EncodingKey) -> Result<String, tonic::Status>
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

    let token = jsonwebtoken::encode(&header, &claims, key).map_err(|e| {
        tonic::Status::internal(format!("failed to encode token via {algo:?}: {e}"))
    })?;

    Ok(token)
}

/// Verified is a type-safe wrapper around verified JWT claims.
/// It can only be constructed by the verify() routine,
/// which gives assurance that the claims have been verified.
#[derive(Debug)]
pub struct Verified<Claims>(Claims, DateTime);

impl<Claims> Verified<Claims> {
    /// Return the verified claims.
    #[inline]
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    /// Return the token's expiry.
    #[inline]
    pub fn expiry(&self) -> DateTime {
        self.1
    }

    /// Return the remaining TimeDelta of the token.
    #[inline]
    pub fn valid_for(&self) -> TimeDelta {
        self.1 - crate::now()
    }
}

/// Verify a JWT token and return its Claims.
/// `require_capability` is a bit-mask. If non-zero, then the `cap` claim
/// must be a present u32 integer and include all set bits of the mask.
pub fn verify<Claims>(
    token: &[u8],
    require_capability: u32,
    keys: &[DecodingKey],
) -> Result<Verified<Claims>, tonic::Status>
where
    Claims: serde::de::DeserializeOwned,
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
            exp: i64,
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
            DateTime::from_timestamp_secs(skim.exp).unwrap_or_default(),
        ));
    }

    if let Some(last_error) = last_error {
        Err(last_error)
    } else {
        Err(tonic::Status::internal("no verification keys provided"))
    }
}

/// Unverified is a type-safe wrapper around unverified JWT claims.
/// It can only be constructed by the parse_unverified() routine.
#[derive(Debug)]
pub struct Unverified<Claims>(Claims, DateTime);

impl<Claims> Unverified<Claims> {
    /// Return the unverified claims.
    #[inline]
    pub fn claims(&self) -> &Claims {
        &self.0
    }

    /// Return the token's expiry.
    #[inline]
    pub fn expiry(&self) -> DateTime {
        self.1
    }

    /// Return the remaining TimeDelta of the token.
    #[inline]
    pub fn valid_for(&self) -> TimeDelta {
        self.1 - crate::now()
    }
}

/// Parse the claims of a JWT token without verifying them.
pub fn parse_unverified<Claims>(token: &[u8]) -> tonic::Result<Unverified<Claims>>
where
    Claims: serde::de::DeserializeOwned,
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
        exp: i64,
    }
    let skim: ClaimsSkim = serde_json::from_slice(&claims)
        .map_err(|err| tonic::Status::unauthenticated(format!("failed to skim claims: {err}")))?;

    let claims: Claims = serde_json::from_slice(&claims).map_err(|err| {
        tonic::Status::unauthenticated(format!(
            "failed to deserialize unverified JWT claims: {err}"
        ))
    })?;

    Ok(Unverified(
        claims,
        DateTime::from_timestamp_secs(skim.exp).unwrap_or_default(),
    ))
}

/// SignedSource is a Source that produces self-signed JWT tokens.
/// It holds claims and updates their time-based fields before each signing.
pub struct SignedSource<Claims> {
    /// Claims to be signed into the JWT.
    pub claims: Claims,
    /// Function to set the `iat` (issued-at) and `exp` (expiration) Claims.
    /// Called with `(claims, iat, exp)` where both timestamps are Unix seconds.
    pub set_time_claims: Box<dyn Fn(&mut Claims, DateTime, DateTime) + Send + Sync>,
    /// Duration for which the signed token is valid.
    pub duration: TimeDelta,
    /// Key used to sign the JWT.
    pub key: EncodingKey,
}

impl<Claims> SignedSource<Claims>
where
    Claims: serde::Serialize + Send + Sync + 'static,
{
    pub fn sign(&mut self) -> tonic::Result<String> {
        let iat = crate::now();
        let exp = iat + self.duration;

        (self.set_time_claims)(&mut self.claims, iat, exp);

        sign(&self.claims, &self.key)
    }
}

impl<Claims> Source for SignedSource<Claims>
where
    Claims: serde::Serialize + Send + Sync + 'static,
{
    type Token = String;
    type Revoke = std::future::Pending<()>;

    async fn refresh(
        &mut self,
        _started: DateTime,
    ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
        Ok(Ok((self.sign()?, self.duration, std::future::pending())))
    }
}

/// Parse an Iterator of base64-encoded HMAC keys used for signing and verification.
/// The first key is used for signing, and all keys are used for verification.
pub fn parse_base64_hmac_keys<I, S>(it: I) -> tonic::Result<(EncodingKey, Vec<DecodingKey>)>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let it = it.into_iter();
    let mut encoding_key: Option<EncodingKey> = None;
    let mut decoding_keys = Vec::with_capacity(it.size_hint().0);

    for item in it {
        let key_bytes = parse_base64(item.as_ref())?;

        if encoding_key.is_none() {
            encoding_key = Some(EncodingKey::from_secret(&key_bytes));
        }
        decoding_keys.push(DecodingKey::from_secret(&key_bytes));
    }

    let signing_key = encoding_key
        .ok_or_else(|| tonic::Status::invalid_argument("at least one key must be provided"))?;

    Ok((signing_key, decoding_keys))
}

/// Parse base64-encoded HMAC keys used for signing and verification.
/// Keys are separated by whitespace or commas, the first key is used for signing,
/// and all keys are used for verification.
pub fn parse_base64_hmac_keys_str(
    base64_keys: &str,
) -> Result<(EncodingKey, Vec<DecodingKey>), tonic::Status> {
    parse_base64_hmac_keys(base64_keys.replace(',', " ").split_whitespace())
}

#[inline]
pub fn parse_base64(s: &str) -> tonic::Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid base64: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestClaims {
        sub: String,
        iat: i64,
        exp: i64,
        #[serde(default)]
        cap: u32,
    }

    fn make_claims(sub: &str, cap: u32, expired: bool) -> TestClaims {
        let now = crate::now().timestamp();
        TestClaims {
            sub: sub.to_string(),
            iat: now,
            exp: if expired { now - 3600 } else { now + 3600 },
            cap,
        }
    }

    #[test]
    fn test_sign_and_verify() {
        let k1 = base64::engine::general_purpose::STANDARD.encode(b"key1");
        let k2 = base64::engine::general_purpose::STANDARD.encode(b"key2");
        let (enc, dec) = parse_base64_hmac_keys([k1, k2]).unwrap();
        assert_eq!(dec.len(), 2);

        // Sign with first key, verify with capability check.
        let token = sign(&make_claims("test", 0x0F, false), &enc).unwrap();
        let verified: Verified<TestClaims> = verify(token.as_bytes(), 0x05, &dec).unwrap();
        assert_eq!(verified.claims().sub, "test");
        assert!(verified.valid_for() > TimeDelta::zero());

        // Verifies against second key in list (first key wrong).
        let wrong = DecodingKey::from_secret(b"wrong");
        verify::<TestClaims>(token.as_bytes(), 0, &[wrong, dec[0].clone()]).unwrap();
    }

    #[test]
    fn test_verify_failures() {
        let key = EncodingKey::from_secret(b"secret");
        let dec = DecodingKey::from_secret(b"secret");
        let wrong = DecodingKey::from_secret(b"wrong");

        // Wrong key.
        let token = sign(&make_claims("test", 0, false), &key).unwrap();
        let err = verify::<TestClaims>(token.as_bytes(), 0, &[wrong]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        // Expired token.
        let token = sign(&make_claims("test", 0, true), &key).unwrap();
        let err = verify::<TestClaims>(token.as_bytes(), 0, &[dec.clone()]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        // Missing capability.
        let token = sign(&make_claims("test", 0x01, false), &key).unwrap();
        let err = verify::<TestClaims>(token.as_bytes(), 0x02, &[dec]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        // No keys provided.
        let err = verify::<TestClaims>(token.as_bytes(), 0, &[]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);

        // Invalid base64 keys.
        assert!(parse_base64_hmac_keys_str("").is_err());
        assert!(parse_base64_hmac_keys_str("!!!").is_err());
    }

    #[test]
    fn test_parse_unverified() {
        let key = EncodingKey::from_secret(b"secret");
        let token = sign(&make_claims("test", 0x42, false), &key).unwrap();

        // Corrupt signature still parses.
        let corrupted = format!("{}.badsig", &token[..token.rfind('.').unwrap()]);
        let unverified: Unverified<TestClaims> = parse_unverified(corrupted.as_bytes()).unwrap();
        assert_eq!(unverified.claims().sub, "test");
        assert!(unverified.valid_for() > TimeDelta::zero());

        // Malformed tokens fail.
        assert!(parse_unverified::<TestClaims>(b"no-dots").is_err());
        assert!(parse_unverified::<TestClaims>(b"one.!!!").is_err());
    }

    #[tokio::test]
    async fn test_signed_source_refresh() {
        let mut source = SignedSource {
            claims: TestClaims {
                sub: "test".to_string(),
                iat: 0,
                exp: 0,
                cap: 0,
            },
            set_time_claims: Box::new(|c, iat, exp| {
                c.iat = iat.timestamp();
                c.exp = exp.timestamp();
            }),
            duration: TimeDelta::hours(1),
            key: EncodingKey::from_secret(b"secret"),
        };

        let Ok(Ok((token, valid_for, _revoke))) = source.refresh(DateTime::UNIX_EPOCH).await else {
            panic!("expected success");
        };
        assert_eq!(valid_for, TimeDelta::hours(1));

        let dec = DecodingKey::from_secret(b"secret");
        let verified: Verified<TestClaims> = verify(token.as_bytes(), 0, &[dec]).unwrap();
        assert_eq!(verified.claims().sub, "test");
        assert_eq!(verified.claims().exp, verified.claims().iat + 3600);
    }
}
