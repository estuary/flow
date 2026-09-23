use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error(
        "environment variable name {name:?} must start with an ASCII letter or underscore and contain only ASCII letters, digits, and underscores"
    )]
    InvalidName { name: String },
    #[error("environment variable {name:?} is reserved by the {runtime} connector runtime")]
    Reserved { name: String, runtime: &'static str },
}

pub fn validate_python(environment: &BTreeMap<String, String>) -> Result<(), Error> {
    validate(environment, "Python", |name| {
        matches!(name, "PYTHONHOME" | "PYTHONPATH") || name.starts_with("UV_")
    })
}

pub fn validate_deno(environment: &BTreeMap<String, String>) -> Result<(), Error> {
    validate(environment, "Deno", |name| name.starts_with("DENO_"))
}

fn validate(
    environment: &BTreeMap<String, String>,
    runtime: &'static str,
    is_reserved: impl Fn(&str) -> bool,
) -> Result<(), Error> {
    for name in environment.keys() {
        if !is_portable_name(name) {
            return Err(Error::InvalidName { name: name.clone() });
        }
        if is_reserved(name) {
            return Err(Error::Reserved {
                name: name.clone(),
                runtime,
            });
        }
    }
    Ok(())
}

fn is_portable_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod test {
    use super::{Error, validate_deno, validate_python};
    use std::collections::BTreeMap;

    fn environment(names: &[&str]) -> BTreeMap<String, String> {
        names
            .iter()
            .map(|name| (name.to_string(), "not included in errors".to_string()))
            .collect()
    }

    #[test]
    fn validates_python_environment() {
        assert_eq!(
            validate_python(&environment(&["API_KEY", "_REGION_2"])),
            Ok(())
        );
        for name in ["", "2FAST", "HAS-DASH", "UNICODE_é"] {
            assert!(matches!(
                validate_python(&environment(&[name])),
                Err(Error::InvalidName { name: actual }) if actual == name
            ));
        }
        for name in ["PYTHONHOME", "PYTHONPATH", "UV_CACHE_DIR", "UV_"] {
            assert!(matches!(
                validate_python(&environment(&[name])),
                Err(Error::Reserved { name: actual, runtime: "Python" }) if actual == name
            ));
        }
        assert_eq!(validate_python(&environment(&["uv_CACHE_DIR"])), Ok(()));
    }

    #[test]
    fn validates_deno_environment() {
        assert_eq!(
            validate_deno(&environment(&["API_KEY", "NO_COLOR"])),
            Ok(())
        );
        for name in ["DENO_DIR", "DENO_AUTH_TOKENS", "DENO_"] {
            assert!(matches!(
                validate_deno(&environment(&[name])),
                Err(Error::Reserved { name: actual, runtime: "Deno" }) if actual == name
            ));
        }
        assert_eq!(validate_deno(&environment(&["deno_DIR"])), Ok(()));
    }

    #[test]
    fn errors_never_include_values() {
        let secret = "super-secret-value";
        let error = validate_python(&BTreeMap::from([(
            "UV_TOKEN".to_string(),
            secret.to_string(),
        )]))
        .unwrap_err()
        .to_string();

        assert!(!error.contains(secret));
    }
}
