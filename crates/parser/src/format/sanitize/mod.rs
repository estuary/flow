use crate::{Output, ParseConfig};

pub mod datetime;

#[derive(Debug, thiserror::Error)]
pub enum SanitizeError {
    #[error("sanitizing datetimes: {0}")]
    DatetimeSanitizeError(#[from] datetime::DatetimeSanitizeError),
}

pub fn sanitize_output(config: &ParseConfig, output: Output) -> Result<Output, SanitizeError> {
    datetime::sanitize_datetime(config, output).map_err(SanitizeError::DatetimeSanitizeError)
}
