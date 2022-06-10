use base64::DecodeError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io operation error.")]
    IoError(#[from] std::io::Error),

    #[error("Json serialization error.")]
    JsonError(#[from] serde_json::Error),

    #[error("Failed to decode base64 content of OpenSSH key.")]
    DecodeError(#[from] DecodeError),

    #[error("SSH forwarding network tunnel exit with non-zero exit code {0}")]
    TunnelExitNonZero(String)
}
