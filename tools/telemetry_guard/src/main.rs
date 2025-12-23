use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;

/// Minimal structure check: top-level object with optional metrics/traces/logs arrays.
#[derive(Deserialize)]
struct TelemetryCatalog {
    #[allow(dead_code)]
    metrics: Option<Vec<serde_json::Value>>,
    #[allow(dead_code)]
    traces: Option<Vec<serde_json::Value>>,
    #[allow(dead_code)]
    logs: Option<Vec<serde_json::Value>>,
}

fn main() -> Result<()> {
    let mut catalogs = Vec::new();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--catalog" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--catalog requires a path"))?;
                catalogs.push(PathBuf::from(path));
            }
            "--help" | "-h" => {
                eprintln!("usage: telemetry_guard --catalog <path> [--catalog <path> ...]");
                return Ok(());
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if catalogs.is_empty() {
        let repo_root = repo_root()?;
        catalogs.push(repo_root.join("telemetry/catalog.json"));
        catalogs.push(
            repo_root
                .parent()
                .ok_or_else(|| anyhow!("repository root has no parent; expected ../clustor"))?
                .join("clustor/telemetry/catalog.json"),
        );
    }

    for catalog in catalogs {
        validate_catalog(&catalog)
            .with_context(|| format!("failed to validate catalog {}", catalog.display()))?;
    }

    Ok(())
}

fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| anyhow!("unable to determine repo root from {manifest_dir:?}"))
        .and_then(|path| {
            path.canonicalize()
                .context("failed to canonicalize repository root")
        })
}

fn validate_catalog(path: &PathBuf) -> Result<()> {
    let data = fs::read_to_string(path)?;
    let catalog: TelemetryCatalog = serde_json::from_str(&data)?;
    if catalog.metrics.is_none() && catalog.traces.is_none() && catalog.logs.is_none() {
        return Err(anyhow!(
            "catalog {} must contain at least one of metrics/traces/logs",
            path.display()
        ));
    }
    Ok(())
}
