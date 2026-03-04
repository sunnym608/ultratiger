use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillManifest {
    pub name: String,
    pub version: String,
    pub entrypoint: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostCall {
    FsRead,
    FsWrite,
    NetOutbound,
    BrowserControl,
}

#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    #[error("manifest parse error: {0}")]
    Parse(String),
    #[error("unknown capability: {0}")]
    UnknownCapability(String),
    #[error("missing required capability: {0}")]
    MissingCapability(String),
}

const ALLOWED_CAPABILITIES: &[&str] = &["fs.read", "fs.write", "net.outbound", "browser.control"];

pub fn parse_manifest(raw_json: &str) -> Result<SkillManifest, SkillError> {
    serde_json::from_str(raw_json).map_err(|err| SkillError::Parse(err.to_string()))
}

pub fn validate_capabilities(manifest: &SkillManifest) -> Result<(), SkillError> {
    for capability in &manifest.capabilities {
        let prefix = capability.split(':').next().unwrap_or_default();
        if !ALLOWED_CAPABILITIES.contains(&prefix) {
            return Err(SkillError::UnknownCapability(capability.clone()));
        }
    }
    Ok(())
}

pub fn check_host_call_allowed(
    manifest: &SkillManifest,
    host_call: HostCall,
) -> Result<(), SkillError> {
    let prefixes = manifest
        .capabilities
        .iter()
        .map(|cap| cap.split(':').next().unwrap_or_default().to_owned())
        .collect::<HashSet<_>>();

    let needed = match host_call {
        HostCall::FsRead => "fs.read",
        HostCall::FsWrite => "fs.write",
        HostCall::NetOutbound => "net.outbound",
        HostCall::BrowserControl => "browser.control",
    };

    if !prefixes.contains(needed) {
        return Err(SkillError::MissingCapability(needed.to_owned()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_and_validator_work() {
        let raw = r#"{"name":"demo","version":"0.1.0","entrypoint":"main","capabilities":["fs.read:/tmp"]}"#;
        let manifest = parse_manifest(raw).expect("manifest should parse");
        validate_capabilities(&manifest).expect("capability should be valid");
        check_host_call_allowed(&manifest, HostCall::FsRead).expect("fs.read should be allowed");
    }

    #[test]
    fn invalid_capability_rejected() {
        let raw = r#"{"name":"demo","version":"0.1.0","entrypoint":"main","capabilities":["shell.exec"]}"#;
        let manifest = parse_manifest(raw).expect("manifest should parse");
        let err = validate_capabilities(&manifest).expect_err("unknown capability should fail");
        assert!(err.to_string().contains("unknown capability"));
    }
}
