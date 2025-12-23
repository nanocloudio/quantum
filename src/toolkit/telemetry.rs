use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetrySample {
    pub timestamp_ms: u64,
    pub metric: String,
    pub value: f64,
    #[serde(default)]
    pub workload: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub capability_epoch: Option<u64>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pub workload: Option<String>,
    pub version: Option<String>,
    pub capability_epoch: Option<u64>,
    pub speed: f64,
    pub apply_delay: bool,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            workload: None,
            version: None,
            capability_epoch: None,
            speed: 1.0,
            apply_delay: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayedSample {
    pub metric: String,
    pub value: f64,
    pub timestamp_ms: u64,
    pub delay_ms: u64,
    pub workload: Option<String>,
    pub version: Option<String>,
    pub capability_epoch: Option<u64>,
    pub labels: HashMap<String, String>,
}

pub fn replay_samples(samples: Vec<TelemetrySample>, opts: &ReplayOptions) -> Vec<ReplayedSample> {
    let mut filtered: Vec<_> = samples
        .into_iter()
        .filter(|sample| match opts.workload.as_ref() {
            Some(w) => sample.workload.as_ref() == Some(w),
            None => true,
        })
        .filter(|sample| match opts.version.as_ref() {
            Some(v) => sample.version.as_ref() == Some(v),
            None => true,
        })
        .filter(|sample| match opts.capability_epoch {
            Some(epoch) => sample.capability_epoch == Some(epoch),
            None => true,
        })
        .collect();
    filtered.sort_by_key(|sample| sample.timestamp_ms);
    let mut replayed = Vec::with_capacity(filtered.len());
    let mut previous = None;
    let speed = if opts.speed <= 0.0 { 1.0 } else { opts.speed };
    for sample in filtered {
        let delay_ms = previous
            .map(|ts| (((sample.timestamp_ms - ts) as f64) / speed).round() as u64)
            .unwrap_or(0);
        if opts.apply_delay && delay_ms > 0 {
            thread::sleep(Duration::from_millis(delay_ms));
        }
        previous = Some(sample.timestamp_ms);
        replayed.push(ReplayedSample {
            metric: sample.metric.clone(),
            value: sample.value,
            timestamp_ms: sample.timestamp_ms,
            delay_ms,
            workload: sample.workload.clone(),
            version: sample.version.clone(),
            capability_epoch: sample.capability_epoch,
            labels: sample.labels.clone(),
        });
    }
    replayed
}

#[derive(Debug, Clone, Copy)]
pub enum TelemetryResource {
    Workloads,
    ProtocolMetrics,
    WorkloadHealth,
    WorkloadPlacements,
}

#[derive(Debug, Default)]
pub struct TelemetryFilters {
    pub tenant: Option<String>,
    pub workload: Option<String>,
    pub version: Option<String>,
}

pub fn filter_endpoint_data(
    content: &Value,
    resource: TelemetryResource,
    filters: &TelemetryFilters,
) -> Value {
    match content {
        Value::Array(items) => {
            let filtered: Vec<Value> = items
                .iter()
                .filter(|value| match value {
                    Value::Object(map) => {
                        compare_field(map.get("tenant"), filters.tenant.as_deref())
                            && compare_field(map.get("workload"), filters.workload.as_deref())
                            && compare_field(map.get("version"), filters.version.as_deref())
                    }
                    _ => true,
                })
                .cloned()
                .collect();
            Value::Array(filtered)
        }
        Value::Object(map) => {
            let mut clone = map.clone();
            clone.insert(
                "resource".into(),
                Value::String(
                    match resource {
                        TelemetryResource::Workloads => "workloads",
                        TelemetryResource::ProtocolMetrics => "protocol_metrics",
                        TelemetryResource::WorkloadHealth => "workload_health",
                        TelemetryResource::WorkloadPlacements => "workload_placements",
                    }
                    .into(),
                ),
            );
            Value::Object(clone)
        }
        other => other.clone(),
    }
}

fn compare_field(value: Option<&Value>, filter: Option<&str>) -> bool {
    match (value, filter) {
        (_, None) => true,
        (Some(Value::String(actual)), Some(expected)) => actual == expected,
        (Some(Value::Number(num)), Some(expected)) => num.to_string() == expected,
        _ => false,
    }
}

pub fn load_samples(path: &Path) -> Result<Vec<TelemetrySample>> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let samples = serde_json::from_slice(&bytes).context("decode telemetry samples")?;
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_filters_and_orders() {
        let samples = vec![
            TelemetrySample {
                timestamp_ms: 10,
                metric: "a".into(),
                value: 1.0,
                workload: Some("mqtt".into()),
                version: Some("1.0".into()),
                capability_epoch: Some(1),
                labels: HashMap::new(),
            },
            TelemetrySample {
                timestamp_ms: 20,
                metric: "b".into(),
                value: 2.0,
                workload: Some("mqtt".into()),
                version: Some("1.0".into()),
                capability_epoch: Some(1),
                labels: HashMap::new(),
            },
        ];
        let opts = ReplayOptions {
            workload: Some("mqtt".into()),
            ..ReplayOptions::default()
        };
        let replayed = replay_samples(samples, &opts);
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[1].delay_ms, 10);
    }
}
