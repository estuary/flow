pub mod task_collection_auth;
pub mod task_dekaf_auth;
pub mod task_secret_decrypt;
pub mod user_collection_auth;
pub mod user_prefix_auth;
pub mod user_secret_decrypt;
pub mod user_task_auth;

pub use task_collection_auth::TaskCollectionAuth;
pub use task_dekaf_auth::TaskDekafAuth;
pub use task_secret_decrypt::TaskSecretDecrypt;
pub use user_collection_auth::UserCollectionAuth;
pub use user_prefix_auth::UserPrefixAuth;
pub use user_secret_decrypt::UserSecretDecrypt;
pub use user_task_auth::UserTaskAuth;

/// Extract a decryption, as used by both [`TaskSecretDecrypt`] and
/// [`UserSecretDecrypt`].
///
/// A decryption is one-shot, so it has no meaningful validity period: a Token
/// is either the plaintext, or a server-directed retry of the operation.
pub fn extract_secret_decryption(
    model: models::authorizations::SecretDecryption,
) -> tonic::Result<
    Result<(models::authorizations::SecretDecryption, tokens::TimeDelta), tokens::TimeDelta>,
> {
    if model.retry_millis != 0 {
        return Ok(Err(tokens::TimeDelta::milliseconds(
            model.retry_millis as i64,
        )));
    }
    if model.value.is_none() || model.secret_id.is_none() {
        return Err(tonic::Status::internal(
            "decryption response carries neither a value nor a retry",
        ));
    }

    Ok(Ok((model, tokens::TimeDelta::zero())))
}

#[cfg(test)]
mod tests {
    use super::extract_secret_decryption;

    #[test]
    fn test_extract_secret_decryption() {
        let retry = extract_secret_decryption(models::authorizations::SecretDecryption {
            retry_millis: 5000,
            ..Default::default()
        });
        assert_eq!(retry.unwrap().unwrap_err(), tokens::TimeDelta::seconds(5));

        let ok = extract_secret_decryption(models::authorizations::SecretDecryption {
            value: Some(models::RawValue::from_str(r#""p4ssw0rd""#).unwrap()),
            secret_id: Some(models::Id::new([1, 2, 3, 4, 5, 6, 7, 8])),
            retry_millis: 0,
        });
        let (model, _valid_for) = ok.unwrap().unwrap();
        assert_eq!(model.value.as_ref().unwrap().get(), r#""p4ssw0rd""#);
        assert_eq!(model.secret_id.unwrap().to_string(), "0102030405060708");

        // Debug renders the identity of a decryption but never its plaintext.
        insta::assert_debug_snapshot!(model, @r###"
        SecretDecryption {
            value: Some(
                "<redacted>",
            ),
            secret_id: Some(
                0102030405060708,
            ),
            retry_millis: 0,
        }
        "###);

        // Neither a value nor a retry is a response we cannot act on.
        let neither = extract_secret_decryption(Default::default());
        assert_eq!(neither.unwrap_err().code(), tonic::Code::Internal);
    }
}
