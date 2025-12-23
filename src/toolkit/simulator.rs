use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OverloadConfig {
    pub workloads: Vec<WorkloadProfile>,
    #[serde(default = "default_duration")]
    pub duration_seconds: u64,
    #[serde(default)]
    pub pattern: LoadPattern,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadProfile {
    pub workload: String,
    #[serde(default = "default_ratio")]
    pub ratio: f64,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub capability_epoch: Option<u64>,
    #[serde(default)]
    pub feature_flags: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadPattern {
    Spike,
    #[default]
    Sustained,
    Burst,
    Failover,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadLoadReport {
    pub workload: String,
    pub version: Option<String>,
    pub capability_epoch: Option<u64>,
    pub average_rps: u64,
    pub peak_rps: u64,
    pub feature_flags: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverloadReport {
    pub duration_seconds: u64,
    pub pattern: LoadPattern,
    pub total_peak_rps: u64,
    pub workloads: Vec<WorkloadLoadReport>,
}

pub fn simulate_overload(cfg: OverloadConfig, baseline_rps: u64) -> OverloadReport {
    let OverloadConfig {
        workloads: profiles,
        duration_seconds,
        pattern,
    } = cfg;
    let total_ratio: f64 = profiles.iter().map(|profile| profile.ratio).sum();
    let normalized = if total_ratio > 0.0 {
        total_ratio
    } else {
        profiles.len() as f64
    };
    let workloads_len = profiles.len() as f64;
    let (pattern_multiplier, peak_multiplier) = match pattern {
        LoadPattern::Spike => (1.5, 3.0),
        LoadPattern::Burst => (1.2, 2.0),
        LoadPattern::Failover => (1.0, 2.5),
        LoadPattern::Sustained => (1.0, 1.5),
    };
    let mut workloads = Vec::new();
    let mut total_peak = 0u64;
    for profile in profiles {
        let ratio = if normalized > 0.0 {
            profile.ratio / normalized
        } else if workloads_len > 0.0 {
            1.0 / workloads_len
        } else {
            0.0
        };
        let average_rps = (baseline_rps as f64 * ratio * pattern_multiplier).round() as u64;
        let peak_rps = (average_rps as f64 * peak_multiplier).round() as u64;
        total_peak = total_peak.saturating_add(peak_rps);
        workloads.push(WorkloadLoadReport {
            workload: profile.workload,
            version: profile.version,
            capability_epoch: profile.capability_epoch,
            average_rps,
            peak_rps,
            feature_flags: profile.feature_flags,
        });
    }
    OverloadReport {
        duration_seconds,
        pattern,
        total_peak_rps: total_peak,
        workloads,
    }
}

pub fn load_overload_config(path: &Path) -> Result<OverloadConfig> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let cfg = serde_json::from_slice(&bytes).context("decode overload config")?;
    Ok(cfg)
}

fn default_duration() -> u64 {
    300
}

fn default_ratio() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulator_generates_summary() {
        let cfg = OverloadConfig {
            workloads: vec![
                WorkloadProfile {
                    workload: "mqtt".into(),
                    ratio: 2.0,
                    version: Some("1.0".into()),
                    capability_epoch: Some(1),
                    feature_flags: HashMap::new(),
                },
                WorkloadProfile {
                    workload: "noop".into(),
                    ratio: 1.0,
                    version: None,
                    capability_epoch: None,
                    feature_flags: HashMap::new(),
                },
            ],
            duration_seconds: 60,
            pattern: LoadPattern::Spike,
        };
        let report = simulate_overload(cfg, 1_000);
        assert_eq!(report.workloads.len(), 2);
        assert!(report.total_peak_rps > 0);
    }
}
