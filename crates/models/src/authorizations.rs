use std::cmp::max;
use validator::Validate;

/// ControlClaims are claims encoded within control-plane access tokens.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ControlClaims {
    // Note that many more fields, such as additional user metadata,
    // are available if we choose to parse them.

    // Audience for which the token is intended.
    pub aud: String,
    // Unix timestamp, in seconds, at which the token was issued.
    pub iat: u64,
    // Unix timestamp, in seconds, at which the token expires.
    pub exp: u64,
    // Authorized User ID.
    pub sub: uuid::Uuid,
    // PostgreSQL role to be used for the token.
    pub role: String,
    // Authorized user email, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    // Capability-bundle names to which this token's authority is masked.
    //
    // `None` is an unmasked token — today that's every token we mint, and a
    // full-authority credential stays unmasked once masking exists. `Some`
    // is a masked token whose authority is its user's live grants
    // intersected with the capability bits of the recognized bundle names
    // herein, and an empty list is valid: it mints an identity-only token.
    // Every individual capability is itself a same-named bundle, so the
    // vocabulary spans coarse bundles and single capability bits alike.
    //
    // Deliberately an opaque list of strings rather than
    // `authz::CapabilitySet`, so that this shared claim doesn't structurally
    // depend on the newest capability variant. A name an instance doesn't
    // recognize must parse and then be inert — it can never widen authority —
    // which is what makes mixed-version fleets and future capability names
    // safe by construction. See `authz::CapabilityMask::from_claim`.
    //
    // Leniency is over names only. The claim's shape stays strict — an array,
    // absent, or null — because the control plane is its sole minter, so a
    // differently-shaped claim is a corrupt or forged token and failing
    // verification is the fail-safe outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_mask: Option<Vec<String>>,
}

impl ControlClaims {
    pub fn time_remaining(&self) -> time::Duration {
        let now = time::OffsetDateTime::now_utc();
        let exp = time::OffsetDateTime::from_unix_timestamp(self.exp as i64).unwrap();

        max(exp - now, time::Duration::ZERO)
    }
}

// Data-plane claims are represented by proto_gazette::Claims,
// which is not re-exported by this crate.

/// TaskAuthorizationRequest is sent by data-plane reactors to request
/// an authorization to a collection which is sourced or produced.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct TaskAuthorizationRequest {
    /// # JWT token to be authorized and signed.
    /// JWT is signed by the requesting data-plane for authorization of a
    /// task to a collection.
    pub token: String,
}

/// TaskAuthorization is an authorization granted to a task for the purpose of
/// interacting with collection journals which it sources or produces.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskAuthorization {
    /// # JWT token which has been authorized for use.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
}

/// UserCollectionAuthorizationRequest requests an authorization to interact
/// with a collection within its data-plane on behalf of a user.
/// It must be accompanied by a control-plane Authorization token.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserCollectionAuthorizationRequest {
    /// # Collection name to be authorized.
    #[validate(nested)]
    pub collection: crate::Collection,
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// If this is non-zero, it lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    ///
    /// If zero, the request will block server-side until it can be
    /// definitively rejected.
    #[serde(default)]
    pub started_unix: u64,
}

/// UserCollectionAuthorization is an authorization granted to a user for the
/// purpose of interacting with collection journals within its data-plane.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserCollectionAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is authorized for journal operations of the
    /// requested collection and capability.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Prefix of collection Journal names.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub journal_name_prefix: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if other fields are not set.
    #[serde(default)]
    pub retry_millis: u64,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserPrefixAuthorizationRequest {
    /// # Prefix to be authorized.
    #[validate(nested)]
    pub prefix: crate::Prefix,
    /// # Name of the data-plane to be authorized.
    #[validate(nested)]
    pub data_plane: crate::Name,
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// This timestamp lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    pub started_unix: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserPrefixAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is authorized for journal operations over the
    /// requested prefix and capability.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Address of Reactors for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_address: String,
    /// # JWT token which has been authorized for use with reactors.
    /// The token is authorized for shard operations over the
    /// requested prefix and capability.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_token: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserTaskAuthorizationRequest {
    /// # Task name to be authorized.
    #[validate(nested)]
    pub task: crate::Name,
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// If this is non-zero, it lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    ///
    /// If zero, the request will block server-side until it can be
    /// definitively rejected.
    #[serde(default)]
    pub started_unix: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserTaskAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is capable of LIST and READ of task ops journals.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Name of the journal holding task logs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_logs_journal: String,
    /// # Name of the journal holding task stats.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_stats_journal: String,
    /// # Address of Reactors for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_address: String,
    /// # JWT token which has been authorized for use with reactors.
    /// The token is authorized for shard operations of the
    /// requested task and capability.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_token: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
    /// # Prefix of task Shard IDs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub shard_id_prefix: String,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DekafAuthResponse {
    /// # Control plane access token with the requested role
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    // Name of the journal that contains the logs for the specified task
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_logs_journal: String,
    // Name of the journal that contains the stats for the specified task
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_stats_journal: String,
    // The built spec of the materialization. This is actually proto_flow::flow::MaterializationSpec
    // but we can't depend on `proto_flow` here, so `RawValue` it is
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_spec: Option<crate::RawValue>,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
    /// # Target dataplane FQDN for redirect when task has been migrated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dataplane_fqdn: Option<String>,
    /// # Target Dekaf Kafka URI for redirect when task has been migrated.
    /// Used for Kafka protocol redirects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dekaf_address: Option<String>,
    /// # Target redirect Dekaf instance's schema registry URL.
    /// Used for serving schema registry HTTP redirects when a task has been migrated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dekaf_registry_address: Option<String>,
}

const fn capability_read() -> crate::Capability {
    crate::Capability::Read
}

#[cfg(test)]
mod test {
    use super::ControlClaims;
    use crate::authz::CapabilityMask;

    #[test]
    fn test_capability_mask_claim_forms() {
        // Every form of the claim parses. A token which fails to parse is a
        // token which fails to authenticate, so an unrecognized name must
        // never be a deserialization error: it's carried through and is then
        // inert when the mask is built.
        let base = serde_json::json!({
            "aud": "authenticated",
            "iat": 1000,
            "exp": 2000,
            "sub": "11111111-1111-1111-1111-111111111111",
            "role": "authenticated",
        });
        // `None` leaves the field absent; `Some` sets it, including to an
        // explicit JSON null. Absent and null both mean "no mask", while an
        // empty array is a mask enabling nothing — and the difference
        // is load-bearing: `[]` attenuates authority to nothing, absence
        // doesn't attenuate at all.
        let cases = [
            // Absent: every token minted before capability masks existed.
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!([
                "CatalogRead",
                "JournalRead",
                "Delegate"
            ])),
            Some(serde_json::json!(["Viewer"])),
            Some(serde_json::json!([])),
            Some(serde_json::json!(["SpecEdit", "FutureCapability"])),
            Some(serde_json::json!(["FutureCapability"])),
        ];

        let outcomes: Vec<(Option<Vec<String>>, CapabilityMask)> = cases
            .into_iter()
            .map(|mask| {
                let mut claims = base.clone();
                if let Some(mask) = mask {
                    claims["capability_mask"] = mask;
                }
                let claims: ControlClaims = serde_json::from_value(claims).unwrap();
                let mask = CapabilityMask::from_claim(claims.capability_mask.as_deref());
                (claims.capability_mask, mask)
            })
            .collect();

        // Claim-less forms map to the unmasked (full) set. Asserted rather
        // than snapshotted so this test doesn't churn when a capability
        // variant is added.
        for (claim, mask) in &outcomes {
            if claim.is_none() {
                assert_eq!(*mask, CapabilityMask::ALL_CAPABILITIES);
            }
        }
        let bounded: Vec<_> = outcomes
            .into_iter()
            .filter(|(claim, _)| claim.is_some())
            .collect();

        insta::assert_debug_snapshot!(bounded, @r#"
        [
            (
                Some(
                    [
                        "CatalogRead",
                        "JournalRead",
                        "Delegate",
                    ],
                ),
                CapabilityMask(
                    EnumSet(CatalogRead | JournalRead | Delegate),
                ),
            ),
            (
                Some(
                    [
                        "Viewer",
                    ],
                ),
                CapabilityMask(
                    EnumSet(CatalogRead | JournalRead | ViewDataPlanePrivateNetworking),
                ),
            ),
            (
                Some(
                    [],
                ),
                CapabilityMask(
                    EnumSet(),
                ),
            ),
            (
                Some(
                    [
                        "SpecEdit",
                        "FutureCapability",
                    ],
                ),
                CapabilityMask(
                    EnumSet(SpecEdit),
                ),
            ),
            (
                Some(
                    [
                        "FutureCapability",
                    ],
                ),
                CapabilityMask(
                    EnumSet(),
                ),
            ),
        ]
        "#);
    }

    #[test]
    fn test_capability_mask_claim_rejects_malformed_shapes() {
        // Leniency is over names only. The claim must be an array of
        // strings, absent, or null; any other shape fails deserialization,
        // and a token whose claims fail to parse fails to authenticate.
        // The control plane is the claim's sole minter, so a
        // differently-shaped claim is corrupt or forged, and refusing the
        // token is the fail-safe outcome.
        let base = serde_json::json!({
            "aud": "authenticated",
            "iat": 1000,
            "exp": 2000,
            "sub": "11111111-1111-1111-1111-111111111111",
            "role": "authenticated",
        });
        let errors: Vec<String> = [
            serde_json::json!("CatalogRead"),
            serde_json::json!(42),
            serde_json::json!(true),
            serde_json::json!({"names": ["CatalogRead"]}),
            serde_json::json!([["CatalogRead"]]),
            serde_json::json!(["CatalogRead", 42]),
            serde_json::json!([null]),
        ]
        .into_iter()
        .map(|mask| {
            let mut claims = base.clone();
            claims["capability_mask"] = mask;
            serde_json::from_value::<ControlClaims>(claims)
                .unwrap_err()
                .to_string()
        })
        .collect();

        insta::assert_debug_snapshot!(errors, @r#"
        [
            "invalid type: string \"CatalogRead\", expected a sequence",
            "invalid type: integer `42`, expected a sequence",
            "invalid type: boolean `true`, expected a sequence",
            "invalid type: map, expected a sequence",
            "invalid type: sequence, expected a string",
            "invalid type: integer `42`, expected a string",
            "invalid type: null, expected a string",
        ]
        "#);
    }

    #[test]
    fn test_capability_mask_claim_round_trip() {
        let claims = ControlClaims {
            aud: "authenticated".to_string(),
            iat: 1000,
            exp: 2000,
            sub: uuid::Uuid::nil(),
            role: "authenticated".to_string(),
            email: None,
            capability_mask: None,
        };

        // An unmasked token doesn't carry the claim at all, so tokens we
        // mint today are unchanged on the wire.
        insta::assert_json_snapshot!(claims, @r#"
        {
          "aud": "authenticated",
          "iat": 1000,
          "exp": 2000,
          "sub": "00000000-0000-0000-0000-000000000000",
          "role": "authenticated"
        }
        "#);

        // An empty mask is distinct from an absent one on the wire, and
        // survives a round trip as such.
        let masked = ControlClaims {
            capability_mask: Some(Vec::new()),
            ..claims
        };
        insta::assert_json_snapshot!(masked, @r#"
        {
          "aud": "authenticated",
          "iat": 1000,
          "exp": 2000,
          "sub": "00000000-0000-0000-0000-000000000000",
          "role": "authenticated",
          "capability_mask": []
        }
        "#);

        // A populated mask serializes its names verbatim — including names
        // this binary doesn't recognize — and they survive a round trip
        // intact. Carry-through is load-bearing in a mixed-version fleet:
        // names minted by a newer instance must pass through an older one
        // unchanged rather than being silently dropped.
        let masked = ControlClaims {
            capability_mask: Some(vec!["SpecEdit".to_string(), "FutureCapability".to_string()]),
            ..masked
        };
        let round_tripped: ControlClaims =
            serde_json::from_value(serde_json::to_value(&masked).unwrap()).unwrap();
        assert_eq!(round_tripped.capability_mask, masked.capability_mask);

        insta::assert_json_snapshot!(masked, @r#"
        {
          "aud": "authenticated",
          "iat": 1000,
          "exp": 2000,
          "sub": "00000000-0000-0000-0000-000000000000",
          "role": "authenticated",
          "capability_mask": [
            "SpecEdit",
            "FutureCapability"
          ]
        }
        "#);
    }
}
