use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaDefinition {
    pub workload: String,
    pub kind: String,
    pub name: String,
    pub version: u16,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub migration_note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaChange {
    pub workload: String,
    pub kind: String,
    pub name: String,
    pub change: String,
    pub details: String,
    pub requires_migration_note: bool,
}

use serde::Serialize;

pub fn diff_schemas(old_path: &Path, new_path: &Path) -> Result<Vec<SchemaChange>> {
    let old = load_schema_file(old_path).context("load old schema registry")?;
    let new = load_schema_file(new_path).context("load new schema registry")?;
    let mut result = Vec::new();
    let mut new_map: HashMap<_, _> = new
        .into_iter()
        .map(|entry| (schema_key(&entry), entry))
        .collect();
    for entry in old {
        let key = schema_key(&entry);
        match new_map.remove(&key) {
            None => result.push(SchemaChange {
                workload: entry.workload.clone(),
                kind: entry.kind.clone(),
                name: entry.name.clone(),
                change: "removed".into(),
                details: format!("schema removed (version {})", entry.version),
                requires_migration_note: false,
            }),
            Some(updated) => {
                if updated.version > entry.version {
                    result.push(SchemaChange {
                        workload: entry.workload.clone(),
                        kind: entry.kind.clone(),
                        name: entry.name.clone(),
                        change: "version_bumped".into(),
                        details: format!("{} -> {}", entry.version, updated.version),
                        requires_migration_note: updated.migration_note.is_none(),
                    });
                } else if updated.version < entry.version {
                    result.push(SchemaChange {
                        workload: entry.workload.clone(),
                        kind: entry.kind.clone(),
                        name: entry.name.clone(),
                        change: "version_regressed".into(),
                        details: format!("{} -> {}", entry.version, updated.version),
                        requires_migration_note: true,
                    });
                } else if entry.metadata != updated.metadata {
                    result.push(SchemaChange {
                        workload: entry.workload.clone(),
                        kind: entry.kind.clone(),
                        name: entry.name.clone(),
                        change: "metadata_changed".into(),
                        details: "metadata diff".into(),
                        requires_migration_note: false,
                    });
                }
            }
        }
    }
    for entry in new_map.into_values() {
        result.push(SchemaChange {
            workload: entry.workload.clone(),
            kind: entry.kind.clone(),
            name: entry.name.clone(),
            change: "added".into(),
            details: format!("version {}", entry.version),
            requires_migration_note: entry.migration_note.is_none(),
        });
    }
    result.sort_by(|a, b| {
        (&a.workload, &a.kind, &a.name, &a.change, &a.details).cmp(&(
            &b.workload,
            &b.kind,
            &b.name,
            &b.change,
            &b.details,
        ))
    });
    Ok(result)
}

fn load_schema_file(path: &Path) -> Result<Vec<SchemaDefinition>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let entries: Vec<SchemaDefinition> =
        serde_json::from_slice(&bytes).context("decode schema registry")?;
    Ok(entries)
}

fn schema_key(entry: &SchemaDefinition) -> (String, String, String) {
    (
        entry.workload.clone(),
        entry.kind.clone(),
        entry.name.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn detects_version_and_additions() {
        let old = json!([
            {"workload":"mqtt","kind":"log","name":"publish","version":1},
            {"workload":"mqtt","kind":"snapshot","name":"v1","version":1}
        ]);
        let new = json!([
            {"workload":"mqtt","kind":"log","name":"publish","version":2},
            {"workload":"mqtt","kind":"snapshot","name":"v2","version":1,"migration_note":"doc"},
            {"workload":"noop","kind":"log","name":"noop","version":1}
        ]);
        let old_file = NamedTempFile::new().unwrap();
        let new_file = NamedTempFile::new().unwrap();
        fs::write(old_file.path(), serde_json::to_vec(&old).unwrap()).unwrap();
        fs::write(new_file.path(), serde_json::to_vec(&new).unwrap()).unwrap();
        let changes = diff_schemas(old_file.path(), new_file.path()).unwrap();
        assert!(changes.iter().any(|c| c.change == "version_bumped"));
        assert!(changes.iter().any(|c| c.change == "added"));
        assert!(changes.iter().any(|c| c.change == "removed"));
    }
}
