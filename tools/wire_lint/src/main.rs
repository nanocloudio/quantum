use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut expected = None;
    let mut candidate = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--expected" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--expected requires a path"))?;
                expected = Some(PathBuf::from(path));
            }
            "--candidate" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--candidate requires a path"))?;
                candidate = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!("usage: wire_lint [--expected <path>] [--candidate <path>]");
                return Ok(());
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    let expected_path =
        expected.unwrap_or_else(|| PathBuf::from("../clustor/artifacts/wire_catalog.json"));
    let candidate_path = candidate.unwrap_or_else(|| PathBuf::from("wire/catalog.json"));

    compare_catalogs(&expected_path, &candidate_path)?;
    println!("wire catalogs match");
    Ok(())
}

fn compare_catalogs(expected: &PathBuf, candidate: &PathBuf) -> Result<()> {
    ensure_file(expected)?;
    ensure_file(candidate)?;
    let expected_sha = sha256_file(expected)?;
    let candidate_sha = sha256_file(candidate)?;
    if expected_sha != candidate_sha {
        return Err(anyhow!(
            "wire catalogs differ:\n  expected ({}) sha256={}\n  candidate ({}) sha256={}",
            expected.display(),
            expected_sha,
            candidate.display(),
            candidate_sha
        ));
    }
    Ok(())
}

fn ensure_file(path: &PathBuf) -> Result<()> {
    if path.is_file() {
        Ok(())
    } else {
        Err(anyhow!("required file is missing: {}", path.display()))
    }
}

fn sha256_file(path: &PathBuf) -> Result<String> {
    let mut hasher = Sha256::new();
    let data = fs::read(path).with_context(|| format!("unable to read {}", path.display()))?;
    hasher.update(&data);
    Ok(format!("{:x}", hasher.finalize()))
}
