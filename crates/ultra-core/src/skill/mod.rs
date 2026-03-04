mod package;
mod runtime;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub use package::{load_signed_package, SkillPackage};
pub use runtime::{execute_signed_skill, SkillExecutionResult, SkillRuntimeConfig};

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
    #[error("package load error: {0}")]
    Package(String),
    #[error("runtime error: {0}")]
    Runtime(String),
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
    use std::fs;

    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn parser_and_validator_work() {
        let raw = r#"{"name":"demo","version":"0.1.0","entrypoint":"run","capabilities":["fs.read:/tmp"]}"#;
        let manifest = parse_manifest(raw).expect("manifest should parse");
        validate_capabilities(&manifest).expect("capability should be valid");
        check_host_call_allowed(&manifest, HostCall::FsRead).expect("fs.read should be allowed");
    }

    #[test]
    fn invalid_capability_rejected() {
        let raw =
            r#"{"name":"demo","version":"0.1.0","entrypoint":"run","capabilities":["shell.exec"]}"#;
        let manifest = parse_manifest(raw).expect("manifest should parse");
        let err = validate_capabilities(&manifest).expect_err("unknown capability should fail");
        assert!(err.to_string().contains("unknown capability"));
    }

    #[test]
    fn signed_package_loader_verifies_sha256() {
        let tmp = std::env::temp_dir().join("ultra_skill_test_pkg");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).expect("temp dir should be created");

        fs::write(
            tmp.join("Manifest.json"),
            r#"{"name":"demo","version":"0.1.0","entrypoint":"run","capabilities":["fs.read:/tmp"]}"#,
        )
        .expect("manifest written");
        let wasm_bytes = b"not-real-wasm";
        fs::write(tmp.join("skill.wasm"), wasm_bytes).expect("wasm written");

        let checksum = hex::encode(Sha256::digest(wasm_bytes));
        fs::write(tmp.join("signature.sha256"), checksum).expect("signature written");

        let package = load_signed_package(&tmp).expect("signed package should load");
        assert_eq!(package.manifest.name, "demo");

        let _ = fs::remove_dir_all(&tmp);
    }
}
