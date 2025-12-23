use serde::Serialize;
use std::collections::HashMap;
use std::hash::Hasher;
use twox_hash::XxHash64;

#[derive(Debug)]
pub struct SyntheticOptions {
    pub protocol: String,
    pub workload_version: Option<String>,
    pub feature_flags: HashMap<String, String>,
    pub count: u32,
}

#[derive(Debug, Serialize)]
pub struct SyntheticResult {
    pub protocol: String,
    pub workload_version: Option<String>,
    pub probes: u32,
    pub success: u32,
    pub failed: u32,
    pub average_latency_ms: u64,
}

pub fn run_probes(opts: SyntheticOptions) -> SyntheticResult {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(opts.protocol.as_bytes());
    if let Some(version) = &opts.workload_version {
        hasher.write(version.as_bytes());
    }
    for (key, value) in &opts.feature_flags {
        hasher.write(key.as_bytes());
        hasher.write(value.as_bytes());
    }
    let seed = hasher.finish();
    let success_ratio = ((seed % 40) as f64 + 60.0) / 100.0;
    let success = (opts.count as f64 * success_ratio).round() as u32;
    let failed = opts.count.saturating_sub(success);
    let latency = 20 + (seed % 30);
    SyntheticResult {
        protocol: opts.protocol,
        workload_version: opts.workload_version,
        probes: opts.count,
        success,
        failed,
        average_latency_ms: latency,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_results() {
        let mut flags = HashMap::new();
        flags.insert("offline_queue".into(), "true".into());
        let opts = SyntheticOptions {
            protocol: "mqtt".into(),
            workload_version: Some("1.0".into()),
            feature_flags: flags,
            count: 100,
        };
        let result = run_probes(opts);
        assert_eq!(result.probes, 100);
        assert!(result.success > result.failed);
    }
}
