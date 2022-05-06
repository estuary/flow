pub mod handler;
#[cfg(test)]
mod test;

use std::process::{Output, Stdio};
use std::sync::Arc;

use doc::ptr::Token;
use doc::{Annotation, Schema, SchemaIndexBuilder};
use doc::{FailedValidation, Pointer};
use json::schema::build::build_schema;
use json::validator::{FullContext, Outcome, Validator};

use crate::SopsArgs;

use handler::{EncryptReq, EncryptedConfig, Error, Format, Secret};

#[derive(Debug)]
struct ValidPlainConfig {
    plain_json: Secret<serde_json::Value>,
    encrypt_ptrs: Vec<String>,
}

fn validate(req: EncryptReq) -> Result<ValidPlainConfig, Error> {
    let EncryptReq { schema, config } = req;

    let schema_url = url::Url::parse("request://schema").unwrap();
    let built_schema: Schema = build_schema(schema_url.clone(), &schema)?;

    let mut builder = SchemaIndexBuilder::new();
    builder.add(&built_schema)?;
    builder.verify_references()?;
    tracing::trace!("finished building json schema");
    let index = builder.into_index();

    let mut validator = Validator::<'_, Annotation, FullContext>::new(&index);
    validator.prepare(&schema_url)?;
    // Deserialization cannot fail.
    ::json::de::walk(&config.0, &mut validator).unwrap();

    if validator.invalid() {
        // Just return the validation in the response, without logging the errors, since the errors
        // themselves may contain sensitive data.
        return Err(Error::FailedValidation(Secret(FailedValidation {
            document: config.0,
            basic_output: ::json::validator::build_basic_output(validator.outcomes()),
        })));
    }

    let encrypt_ptrs = validator
        .outcomes()
        .iter()
        .filter_map(|(outcome, span)| {
            if let Outcome::Annotation(Annotation::Secret(true)) = outcome {
                Some(span.instance_ptr.clone())
            } else {
                None
            }
        })
        .collect();
    tracing::debug!(?encrypt_ptrs, "config validated successfully");
    Ok(ValidPlainConfig {
        plain_json: config,
        encrypt_ptrs,
    })
}

struct PreparedPlainConfig {
    plain_json: Secret<serde_json::Value>,
}

fn add_encrypted_suffixes(
    conf: ValidPlainConfig,
    suffix: &str,
) -> Result<PreparedPlainConfig, Error> {
    let ValidPlainConfig {
        mut plain_json,
        encrypt_ptrs,
    } = conf;
    for ptr in encrypt_ptrs {
        add_suffix_to_location(&mut plain_json.0, &ptr, suffix)?;
    }
    Ok(PreparedPlainConfig { plain_json })
}

async fn encrypt(
    plain_config: PreparedPlainConfig,
    sops_config: Arc<SopsArgs>,
    output_format: Format,
) -> Result<EncryptedConfig, Error> {
    // Running a child process with tokio is a pain because we need to use async apis for writing
    // to the stdin of the process. There's no practical benefit to doing that, so we instead use
    // good old fashioned threads! The `spawn_blocking` call is required in order to offload this
    // to a thread where it's OK to block without screwing with the async executor.
    tokio::task::spawn_blocking(move || run_sops_blocking(plain_config, sops_config, output_format))
        .await?
}

fn run_sops_blocking(
    plain_config: PreparedPlainConfig,
    sops_config: Arc<SopsArgs>,
    output_format: Format,
) -> Result<EncryptedConfig, Error> {
    let mut child = std::process::Command::new("sops")
        .args(&[
            "--encrypt",
            "--gcp-kms",
            &sops_config.gcp_kms,
            "--encrypted-suffix",
            &sops_config.encrypted_suffix,
            "--input-type=json",
            "--output-type",
            output_format.sops_type(),
            "/dev/stdin",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    serde_json::to_writer(child.stdin.take().unwrap(), &plain_config.plain_json)?;
    drop_plain_config(plain_config.plain_json.0);

    let Output {
        status,
        stdout,
        stderr,
    } = child.wait_with_output()?;

    if status.success() {
        Ok(EncryptedConfig {
            document: stdout,
            format: output_format,
        })
    } else {
        let sops_stderr = String::from_utf8_lossy(&stderr).to_string();
        Err(Error::SopsFailed(status, sops_stderr))
    }
}

fn add_suffix_to_location(
    conf: &mut serde_json::Value,
    location_ptr: &str,
    suffix: &str,
) -> Result<(), Error> {
    let mut parent = conf;
    let parsed_ptr = Pointer::from_str(location_ptr);
    let mut iter = parsed_ptr.iter().peekable();
    let mut leaf_prop = None;
    while let Some(token) = iter.next() {
        if iter.peek().is_none() {
            leaf_prop = Some(token);
            break;
        }
        match token {
            Token::Property(p) => parent = parent.get_mut(p).expect("deref property"),
            Token::Index(i) => parent = parent.get_mut(i).expect("deref index"),
            Token::NextIndex => panic!("nextIndex is invalid"),
        }
    }

    // parent must be an object, since there's no way to add a suffix to array indices. If it's
    // not, then we return an error.
    let parent_obj = parent.as_object_mut().ok_or_else(|| {
        Error::InvalidSecretLocation(location_ptr.to_owned(), "it is not an object property")
    })?;

    // If ptr is an empty string, then parent will point to the root document, and leaf_prop will
    // be None. If ptr is non-empty, then leaf_prop must be Some.
    if let Some(token) = leaf_prop {
        if let Token::Property(prop) = token {
            let (mut key, value) = parent_obj.remove_entry(prop).unwrap();
            key.push_str(suffix);
            parent_obj.insert(key, value);
        } else {
            panic!(
                "expected leaf token to be an object property, but was {:?}",
                token
            );
        }
    } else {
        return Err(Error::InvalidSecretLocation(
            location_ptr.to_owned(),
            "it is the root document",
        ));
    }
    Ok(())
}

/// We'd like to avoid keeping sensitive values around in memory, but `serde_json::Value` doesn't
/// have an easy way to zero out the whole document, so this just traverses the object and
/// overwrites all the String values. There's no safe way to zero numbers, but they seem far less
/// likely to hold sensitive information anyway.
fn drop_plain_config(json: serde_json::Value) {
    use serde_json::Value::*;
    match json {
        String(mut s) => {
            let n = s.len();
            s.clear();
            s.extend(std::iter::repeat("f").take(n));
        }
        Object(o) => {
            for (_, value) in o {
                drop_plain_config(value);
            }
        }
        Array(a) => {
            for value in a {
                drop_plain_config(value);
            }
        }
        _ => { /* nothing much we can do about the rest */ }
    }
}
