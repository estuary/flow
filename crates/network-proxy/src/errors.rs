#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("SSH endpoint is invalid.")]
    InvalidSshEndpoint,

    #[error("SSH private key is invalid.")]
    InvalidSshCredential,

    #[error("Local port number of 0 is invalid")]
    ZeroLocalPort,

    #[error("SSH error: {source:?}.")]
    ThrusshError{#[from] source: thrussh::Error},

    #[error("io operation error: {source:?}.")]
    IoError{#[from] source: std::io::Error},

    #[error("openssl error: {source:?}.")]
    OpenSslError{#[from] source: openssl::error::ErrorStack},

    #[error("base64 decoding error: {source:?}.")]
    Base64DecodeError{#[from] source: base64::DecodeError},

    #[error("ssh_endpoint parse error: {source:?}. Expected format: ssh://<host_url_or_ip>[:port]")]
    UrlParseError{#[from] source: url::ParseError},

    #[error("IP parse error: {source:?}.")]
    IpAddrParseError{#[from] source: std::net::AddrParseError}
}
