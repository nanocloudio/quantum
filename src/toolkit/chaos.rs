use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ChaosScenario {
    pub name: String,
    pub protocol: String,
    #[serde(default)]
    pub capability_epoch: Option<u64>,
    pub faults: Vec<ChaosFault>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChaosFault {
    Disconnect {
        duration_ms: u64,
        targets: Vec<String>,
    },
    Latency {
        latency_ms: u64,
        targets: Vec<String>,
    },
    DropTraffic {
        percent: u8,
        targets: Vec<String>,
    },
    SchemaMismatch {
        expected_version: u16,
        actual_version: u16,
    },
}

#[derive(Debug, Serialize)]
pub struct ChaosPlan {
    pub name: String,
    pub protocol: String,
    pub capability_epoch: Option<u64>,
    pub total_faults: usize,
    pub steps: Vec<ChaosStep>,
}

#[derive(Debug, Serialize)]
pub struct ChaosStep {
    pub description: String,
    pub validation: String,
}

pub fn plan_chaos(scenario: ChaosScenario) -> ChaosPlan {
    let mut steps = Vec::with_capacity(scenario.faults.len());
    for fault in scenario.faults {
        match fault {
            ChaosFault::Disconnect {
                duration_ms,
                targets,
            } => steps.push(ChaosStep {
                description: format!("disconnect {:?} for {}ms", targets, duration_ms),
                validation: "verify reconnection metrics and synthetic probes".into(),
            }),
            ChaosFault::Latency {
                latency_ms,
                targets,
            } => steps.push(ChaosStep {
                description: format!("inject {}ms latency on {:?}", latency_ms, targets),
                validation: "check backpressure + telemetry endpoints".into(),
            }),
            ChaosFault::DropTraffic { percent, targets } => steps.push(ChaosStep {
                description: format!("drop {}% traffic for {:?}", percent, targets),
                validation: "ensure alerts fire and chaos tags recorded".into(),
            }),
            ChaosFault::SchemaMismatch {
                expected_version,
                actual_version,
            } => steps.push(ChaosStep {
                description: format!(
                    "inject schema drift (expected {}, actual {})",
                    expected_version, actual_version
                ),
                validation: "confirm schema diff tooling reports mismatch".into(),
            }),
        }
    }
    ChaosPlan {
        name: scenario.name,
        protocol: scenario.protocol,
        capability_epoch: scenario.capability_epoch,
        total_faults: steps.len(),
        steps,
    }
}

pub fn load_scenario(path: &Path) -> Result<ChaosScenario> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let scenario = serde_json::from_slice(&bytes).context("decode chaos scenario")?;
    Ok(scenario)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_contains_steps() {
        let scenario = ChaosScenario {
            name: "latency-test".into(),
            protocol: "mqtt".into(),
            capability_epoch: Some(7),
            faults: vec![ChaosFault::Latency {
                latency_ms: 500,
                targets: vec!["tenant-a".into()],
            }],
        };
        let plan = plan_chaos(scenario);
        assert_eq!(plan.total_faults, 1);
        assert!(plan.steps[0].description.contains("500"));
    }
}
