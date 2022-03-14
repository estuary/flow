use crate::apis::FlowRuntimeProtocol;
use crate::errors::{Error, Must};
use clap::ArgEnum;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

// The key of the docker image label that indicates the connector protocol.
const FLOW_RUNTIME_PROTOCOL_KEY: &str = "FLOW_RUNTIME_PROTOCOL";
const CONNECTOR_PROTOCOL_KEY: &str = "CONNECTOR_PROTOCOL";

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ImageInspect {
    pub config: ImageConfig,
    pub repo_tags: Option<Vec<String>>,
}

impl ImageInspect {
    pub fn parse_from_json_file(path: Option<String>) -> Result<Self, Error> {
        if path.is_none() {}
        match path {
            None => {
                return Err(Error::MissingImageInspectFile);
            }
            Some(p) => {
                let reader = BufReader::new(File::open(p)?);
                let image_inspects: Vec<ImageInspect> = serde_json::from_reader(reader)?;
                match image_inspects.len() {
                    1 => Ok(image_inspects[0].clone()),
                    _ => Err(Error::InvalidImageInspectFile),
                }
            }
        }
    }

    pub fn get_entrypoint(&self, default: Vec<String>) -> Vec<String> {
        match self.config.entrypoint.len() {
            0 => {
                tracing::warn!(
                    "No entry point is specified in the image, using default: {:?}",
                    default
                );
                default
            }
            _ => self.config.entrypoint.clone(),
        }
    }

    pub fn infer_runtime_protocol(&self) -> FlowRuntimeProtocol {
        if let Some(ref labels) = self.config.labels {
            if let Some(value) = labels.get(FLOW_RUNTIME_PROTOCOL_KEY) {
                return FlowRuntimeProtocol::from_str(&value, false).or_bail();
            }
        }

        if let Some(repo_tags) = &self.repo_tags {
            for tag in repo_tags {
                if tag.starts_with("ghcr.io/estuary/materialize-") {
                    return FlowRuntimeProtocol::Materialize;
                }
            }
        }

        return FlowRuntimeProtocol::Capture;
    }

    pub fn get_connector_protocol<T: ArgEnum + std::fmt::Debug>(&self, default: T) -> T {
        if let Some(ref labels) = self.config.labels {
            if let Some(value) = labels.get(CONNECTOR_PROTOCOL_KEY) {
                return T::from_str(&value, false).or_bail();
            }
        }
        tracing::warn!(
            "No connector protocol is specified in the image, using default: {:?}",
            default
        );
        default
    }
}
