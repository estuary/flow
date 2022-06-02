use std::array::TryFromSliceError;

use base64::DecodeError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SSH endpoint is invalid.")]
    InvalidSshEndpoint,

    #[error("SSH private key is invalid.")]
    InvalidSshCredential,

    #[error("Local port number of 0 is invalid")]
    ZeroLocalPort,

    #[error("SSH error.")]
    ThrusshError(#[from] thrussh::Error),

    #[error("io operation error.")]
    IoError(#[from] std::io::Error),

    #[error("openssl error.")]
    OpenSslError(#[from] openssl::error::ErrorStack),

    #[error("ssh_endpoint parse error. Expected format: ssh://<host_url_or_ip>[:port]")]
    UrlParseError(#[from] url::ParseError),

    #[error("IP parse error.")]
    IpAddrParseError(#[from] std::net::AddrParseError),

    #[error("Json serialization error.")]
    JsonError(#[from] serde_json::Error),

    #[error("Failed to parse OpenSSH key.")]
    KeyParsingError(#[from] thrussh_keys::Error),

    #[error("Failed to decode base64 content of OpenSSH key.")]
    DecodeError(#[from] DecodeError),
}
