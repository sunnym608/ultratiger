use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::{parse_manifest, SkillError, SkillManifest};

#[derive(Debug, Clone)]
pub struct SkillPackage {
    pub manifest: SkillManifest,
    pub wasm_path: PathBuf,
    pub wasm_bytes: Vec<u8>,
    pub package_dir: PathBuf,
}

pub fn load_signed_package(package_dir: impl AsRef<Path>) -> Result<SkillPackage, SkillError> {
    let package_dir = package_dir.as_ref().to_path_buf();
    let manifest_path = package_dir.join("Manifest.json");
    let wasm_path = package_dir.join("skill.wasm");
    let signature_path = package_dir.join("signature.sha256");

    let manifest_raw = fs::read_to_string(&manifest_path)
        .map_err(|err| SkillError::Package(format!("manifest read failed: {err}")))?;
    let manifest = parse_manifest(&manifest_raw)?;

    let wasm_bytes = fs::read(&wasm_path)
        .map_err(|err| SkillError::Package(format!("wasm read failed: {err}")))?;

    let signature = fs::read_to_string(&signature_path)
        .map_err(|err| SkillError::Package(format!("signature read failed: {err}")))?;
    let expected = signature.trim().to_lowercase();
    let actual = hex::encode(Sha256::digest(&wasm_bytes));

    if expected != actual {
        return Err(SkillError::Package(
            "signature mismatch for skill.wasm".to_owned(),
        ));
    }

    Ok(SkillPackage {
        manifest,
        wasm_path,
        wasm_bytes,
        package_dir,
    })
}
