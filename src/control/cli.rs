//! CLI inspection types for protocol assignments.
//!
//! Provides structured output for CLI commands to inspect tenant capabilities,
//! protocol assignments, and capability registry state.

use super::capabilities::{CapabilityRegistry, TenantCapabilities};
use serde::Serialize;
use std::io::Write;

/// Output format for CLI inspection commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InspectFormat {
    /// Human-readable table format.
    #[default]
    Table,
    /// JSON format.
    Json,
    /// YAML format (if available).
    Yaml,
    /// Compact single-line format.
    Compact,
}

impl std::str::FromStr for InspectFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(InspectFormat::Table),
            "json" => Ok(InspectFormat::Json),
            "yaml" | "yml" => Ok(InspectFormat::Yaml),
            "compact" => Ok(InspectFormat::Compact),
            other => Err(format!("unknown format: {}", other)),
        }
    }
}

/// Tenant inspection output for CLI.
#[derive(Debug, Clone, Serialize)]
pub struct TenantInspectOutput {
    pub tenant_id: String,
    pub active: bool,
    pub epoch: u64,
    pub protocols: Vec<ProtocolInspectOutput>,
    pub global_features: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<&TenantCapabilities> for TenantInspectOutput {
    fn from(caps: &TenantCapabilities) -> Self {
        Self {
            tenant_id: caps.tenant_id.clone(),
            active: caps.active,
            epoch: caps.epoch,
            protocols: caps
                .protocols
                .values()
                .map(|a| ProtocolInspectOutput {
                    protocol: a.protocol.as_str().to_string(),
                    enabled: a.enabled,
                    features: a.features.iter().map(|f| f.as_str().to_string()).collect(),
                    prg_affinity: a.prg_affinity.clone(),
                    priority: a.priority,
                    limits: LimitsInspectOutput {
                        max_connections: a.limits.max_connections,
                        max_message_size: format_bytes(a.limits.max_message_size as u64),
                        max_messages_per_second: if a.limits.max_messages_per_second == 0 {
                            "unlimited".to_string()
                        } else {
                            a.limits.max_messages_per_second.to_string()
                        },
                        max_bytes_per_second: if a.limits.max_bytes_per_second == 0 {
                            "unlimited".to_string()
                        } else {
                            format_bytes(a.limits.max_bytes_per_second)
                        },
                        max_offline_queue_bytes: format_bytes(a.limits.max_offline_queue_bytes),
                        max_retained_messages: a.limits.max_retained_messages,
                        max_subscriptions: a.limits.max_subscriptions_per_connection,
                    },
                })
                .collect(),
            global_features: caps
                .global_features
                .iter()
                .map(|f| f.as_str().to_string())
                .collect(),
            created_at: format_timestamp(caps.created_at),
            updated_at: format_timestamp(caps.updated_at),
        }
    }
}

/// Protocol inspection output.
#[derive(Debug, Clone, Serialize)]
pub struct ProtocolInspectOutput {
    pub protocol: String,
    pub enabled: bool,
    pub features: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prg_affinity: Option<String>,
    pub priority: u8,
    pub limits: LimitsInspectOutput,
}

/// Limits inspection output (with formatted values).
#[derive(Debug, Clone, Serialize)]
pub struct LimitsInspectOutput {
    pub max_connections: u32,
    pub max_message_size: String,
    pub max_messages_per_second: String,
    pub max_bytes_per_second: String,
    pub max_offline_queue_bytes: String,
    pub max_retained_messages: u32,
    pub max_subscriptions: u32,
}

/// Registry inspection output.
#[derive(Debug, Clone, Serialize)]
pub struct RegistryInspectOutput {
    pub epoch: u64,
    pub tenant_count: usize,
    pub available_protocols: Vec<String>,
    pub tenants: Vec<TenantSummaryOutput>,
}

/// Tenant summary for registry listing.
#[derive(Debug, Clone, Serialize)]
pub struct TenantSummaryOutput {
    pub tenant_id: String,
    pub active: bool,
    pub protocols: Vec<String>,
    pub epoch: u64,
}

/// Capability inspector for CLI commands.
#[derive(Debug)]
pub struct CapabilityInspector {
    format: InspectFormat,
}

impl CapabilityInspector {
    pub fn new(format: InspectFormat) -> Self {
        Self { format }
    }

    /// Inspect a single tenant's capabilities.
    pub fn inspect_tenant<W: Write>(
        &self,
        writer: &mut W,
        caps: &TenantCapabilities,
    ) -> std::io::Result<()> {
        let output = TenantInspectOutput::from(caps);
        match self.format {
            InspectFormat::Json => {
                writeln!(writer, "{}", serde_json::to_string_pretty(&output).unwrap())?;
            }
            InspectFormat::Yaml => {
                // Fallback to JSON-like YAML representation
                writeln!(writer, "---")?;
                writeln!(writer, "tenant_id: {}", output.tenant_id)?;
                writeln!(writer, "active: {}", output.active)?;
                writeln!(writer, "epoch: {}", output.epoch)?;
                writeln!(writer, "protocols:")?;
                for proto in &output.protocols {
                    writeln!(writer, "  - protocol: {}", proto.protocol)?;
                    writeln!(writer, "    enabled: {}", proto.enabled)?;
                    writeln!(writer, "    priority: {}", proto.priority)?;
                    writeln!(writer, "    features: {:?}", proto.features)?;
                }
                writeln!(writer, "global_features: {:?}", output.global_features)?;
            }
            InspectFormat::Compact => {
                let protocols: Vec<_> = output.protocols.iter().map(|p| &p.protocol).collect();
                writeln!(
                    writer,
                    "{} active={} epoch={} protocols={:?}",
                    output.tenant_id, output.active, output.epoch, protocols
                )?;
            }
            InspectFormat::Table => {
                writeln!(writer, "Tenant: {}", output.tenant_id)?;
                writeln!(
                    writer,
                    "  Status: {}",
                    if output.active { "active" } else { "inactive" }
                )?;
                writeln!(writer, "  Epoch: {}", output.epoch)?;
                writeln!(writer, "  Created: {}", output.created_at)?;
                writeln!(writer, "  Updated: {}", output.updated_at)?;
                writeln!(writer)?;

                if !output.global_features.is_empty() {
                    writeln!(writer, "  Global Features:")?;
                    for feat in &output.global_features {
                        writeln!(writer, "    - {}", feat)?;
                    }
                    writeln!(writer)?;
                }

                writeln!(writer, "  Protocols:")?;
                for proto in &output.protocols {
                    writeln!(writer, "    {}:", proto.protocol.to_uppercase())?;
                    writeln!(writer, "      Enabled: {}", proto.enabled)?;
                    writeln!(writer, "      Priority: {}", proto.priority)?;
                    if let Some(affinity) = &proto.prg_affinity {
                        writeln!(writer, "      PRG Affinity: {}", affinity)?;
                    }
                    writeln!(writer, "      Features:")?;
                    for feat in &proto.features {
                        writeln!(writer, "        - {}", feat)?;
                    }
                    writeln!(writer, "      Limits:")?;
                    writeln!(
                        writer,
                        "        Max Connections: {}",
                        proto.limits.max_connections
                    )?;
                    writeln!(
                        writer,
                        "        Max Message Size: {}",
                        proto.limits.max_message_size
                    )?;
                    writeln!(
                        writer,
                        "        Max Msgs/sec: {}",
                        proto.limits.max_messages_per_second
                    )?;
                    writeln!(
                        writer,
                        "        Max Bytes/sec: {}",
                        proto.limits.max_bytes_per_second
                    )?;
                    writeln!(
                        writer,
                        "        Max Offline Queue: {}",
                        proto.limits.max_offline_queue_bytes
                    )?;
                    writeln!(
                        writer,
                        "        Max Retained: {}",
                        proto.limits.max_retained_messages
                    )?;
                    writeln!(
                        writer,
                        "        Max Subscriptions: {}",
                        proto.limits.max_subscriptions
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Inspect the capability registry.
    pub fn inspect_registry<W: Write>(
        &self,
        writer: &mut W,
        registry: &CapabilityRegistry,
    ) -> std::io::Result<()> {
        let output = RegistryInspectOutput {
            epoch: registry.epoch(),
            tenant_count: registry.tenant_count(),
            available_protocols: registry
                .available_protocols()
                .iter()
                .map(|p| p.as_str().to_string())
                .collect(),
            tenants: registry
                .active_tenants()
                .iter()
                .map(|t| TenantSummaryOutput {
                    tenant_id: t.tenant_id.clone(),
                    active: t.active,
                    protocols: t.protocols.keys().map(|p| p.as_str().to_string()).collect(),
                    epoch: t.epoch,
                })
                .collect(),
        };

        match self.format {
            InspectFormat::Json => {
                writeln!(writer, "{}", serde_json::to_string_pretty(&output).unwrap())?;
            }
            InspectFormat::Yaml => {
                writeln!(writer, "---")?;
                writeln!(writer, "epoch: {}", output.epoch)?;
                writeln!(writer, "tenant_count: {}", output.tenant_count)?;
                writeln!(
                    writer,
                    "available_protocols: {:?}",
                    output.available_protocols
                )?;
                writeln!(writer, "tenants:")?;
                for tenant in &output.tenants {
                    writeln!(writer, "  - tenant_id: {}", tenant.tenant_id)?;
                    writeln!(writer, "    active: {}", tenant.active)?;
                    writeln!(writer, "    protocols: {:?}", tenant.protocols)?;
                }
            }
            InspectFormat::Compact => {
                writeln!(
                    writer,
                    "epoch={} tenants={} protocols={:?}",
                    output.epoch, output.tenant_count, output.available_protocols
                )?;
            }
            InspectFormat::Table => {
                writeln!(writer, "Capability Registry")?;
                writeln!(writer, "  Epoch: {}", output.epoch)?;
                writeln!(writer, "  Tenant Count: {}", output.tenant_count)?;
                writeln!(
                    writer,
                    "  Available Protocols: {}",
                    output.available_protocols.join(", ")
                )?;
                writeln!(writer)?;
                writeln!(writer, "  Tenants:")?;
                writeln!(
                    writer,
                    "    {:<20} {:<10} {:<30} {:>8}",
                    "TENANT_ID", "STATUS", "PROTOCOLS", "EPOCH"
                )?;
                writeln!(writer, "    {:-<20} {:-<10} {:-<30} {:->8}", "", "", "", "")?;
                for tenant in &output.tenants {
                    let status = if tenant.active { "active" } else { "inactive" };
                    let protocols = tenant.protocols.join(", ");
                    writeln!(
                        writer,
                        "    {:<20} {:<10} {:<30} {:>8}",
                        tenant.tenant_id, status, protocols, tenant.epoch
                    )?;
                }
            }
        }
        Ok(())
    }

    /// List protocols for a tenant.
    pub fn list_protocols<W: Write>(
        &self,
        writer: &mut W,
        caps: &TenantCapabilities,
    ) -> std::io::Result<()> {
        match self.format {
            InspectFormat::Json => {
                let protocols: Vec<_> = caps
                    .protocols
                    .iter()
                    .map(|(k, v)| {
                        serde_json::json!({
                            "protocol": k.as_str(),
                            "enabled": v.enabled,
                            "priority": v.priority,
                            "features": v.features.iter().map(|f| f.as_str()).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                writeln!(
                    writer,
                    "{}",
                    serde_json::to_string_pretty(&protocols).unwrap()
                )?;
            }
            InspectFormat::Compact => {
                for (proto, assignment) in &caps.protocols {
                    write!(
                        writer,
                        "{}:{}",
                        proto.as_str(),
                        if assignment.enabled { "on" } else { "off" }
                    )?;
                    write!(writer, " ")?;
                }
                writeln!(writer)?;
            }
            InspectFormat::Table | InspectFormat::Yaml => {
                writeln!(
                    writer,
                    "{:<12} {:<10} {:>8} FEATURES",
                    "PROTOCOL", "STATUS", "PRIORITY"
                )?;
                writeln!(writer, "{:-<12} {:-<10} {:->8} {:-<40}", "", "", "", "")?;
                for (proto, assignment) in &caps.protocols {
                    let status = if assignment.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    };
                    let features: Vec<_> = assignment.features.iter().map(|f| f.as_str()).collect();
                    writeln!(
                        writer,
                        "{:<12} {:<10} {:>8} {}",
                        proto.as_str(),
                        status,
                        assignment.priority,
                        features.join(", ")
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_timestamp(unix_secs: u64) -> String {
    // Simple ISO-like format without chrono dependency
    if unix_secs == 0 {
        return "unknown".to_string();
    }
    format!("{}", unix_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::capabilities::{ProtocolAssignment, ProtocolFeatureFlag, ProtocolType};

    #[test]
    fn test_inspect_format_parse() {
        assert_eq!(
            "table".parse::<InspectFormat>().unwrap(),
            InspectFormat::Table
        );
        assert_eq!(
            "json".parse::<InspectFormat>().unwrap(),
            InspectFormat::Json
        );
        assert_eq!(
            "yaml".parse::<InspectFormat>().unwrap(),
            InspectFormat::Yaml
        );
        assert_eq!(
            "compact".parse::<InspectFormat>().unwrap(),
            InspectFormat::Compact
        );
        assert!("unknown".parse::<InspectFormat>().is_err());
    }

    #[test]
    fn test_tenant_inspect_output() {
        let mut caps = TenantCapabilities::new("test-tenant");
        caps.set_protocol(ProtocolAssignment::new(ProtocolType::Mqtt));
        caps.add_global_feature(ProtocolFeatureFlag::AuditLogging);

        let output = TenantInspectOutput::from(&caps);
        assert_eq!(output.tenant_id, "test-tenant");
        assert!(output.active);
        assert_eq!(output.protocols.len(), 1);
        assert!(output
            .global_features
            .contains(&"audit_logging".to_string()));
    }

    #[test]
    fn test_inspect_tenant_json() {
        let caps = TenantCapabilities::new("test-tenant");
        let inspector = CapabilityInspector::new(InspectFormat::Json);
        let mut output = Vec::new();
        inspector.inspect_tenant(&mut output, &caps).unwrap();
        let json_str = String::from_utf8(output).unwrap();
        assert!(json_str.contains("test-tenant"));
    }

    #[test]
    fn test_inspect_tenant_compact() {
        let mut caps = TenantCapabilities::new("test-tenant");
        caps.set_protocol(ProtocolAssignment::new(ProtocolType::Mqtt));
        let inspector = CapabilityInspector::new(InspectFormat::Compact);
        let mut output = Vec::new();
        inspector.inspect_tenant(&mut output, &caps).unwrap();
        let line = String::from_utf8(output).unwrap();
        assert!(line.contains("test-tenant"));
        assert!(line.contains("active=true"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(256 * 1024 * 1024), "256.0 MB");
    }

    #[test]
    fn test_inspect_registry() {
        let mut registry = CapabilityRegistry::new();
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let inspector = CapabilityInspector::new(InspectFormat::Json);
        let mut output = Vec::new();
        inspector.inspect_registry(&mut output, &registry).unwrap();
        let json_str = String::from_utf8(output).unwrap();
        assert!(json_str.contains("tenant-1"));
        assert!(json_str.contains("mqtt"));
    }
}
