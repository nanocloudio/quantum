#![cfg(feature = "interop")]

//! External interop harnesses. Ignored by default; run with `cargo test --test interop --features interop -- --ignored`.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

fn binary_available(bin: &str) -> bool {
    Path::new(bin).exists() || which::which(bin).is_ok()
}

#[test]
fn mosquitto_pub_sub_tcp_tls() {
    let mosquitto_pub =
        std::env::var("MOSQUITTO_PUB_BIN").unwrap_or_else(|_| "mosquitto_pub".into());
    let mosquitto_sub =
        std::env::var("MOSQUITTO_SUB_BIN").unwrap_or_else(|_| "mosquitto_sub".into());
    if !binary_available(&mosquitto_pub) || !binary_available(&mosquitto_sub) {
        eprintln!("skipping: mosquitto_pub/sub not found in PATH");
        return;
    }
    let target =
        std::env::var("MOSQUITTO_TARGET").unwrap_or_else(|_| "mqtts://127.0.0.1:8883".to_string());
    let ca = std::env::var("MOSQUITTO_CA").ok();
    if ca.is_none() {
        eprintln!("skipping: set MOSQUITTO_CA to the broker CA bundle");
        return;
    }
    let topic = std::env::var("MOSQUITTO_TOPIC").unwrap_or_else(|_| "interop/test".into());
    let payload = format!(
        "interop-{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );

    let mut sub = Command::new(&mosquitto_sub);
    sub.args(["-L", &target, "-C", "1", "-t", &topic]);
    if let Some(ca) = &ca {
        sub.args(["--cafile", ca]);
    }
    sub.stdout(Stdio::piped());
    let mut sub_child = sub.spawn().expect("spawn mosquitto_sub");
    sleep(Duration::from_millis(200));

    let mut pubcmd = Command::new(&mosquitto_pub);
    pubcmd.args(["-L", &target, "-m", &payload, "-t", &topic]);
    if let Some(ca) = &ca {
        pubcmd.args(["--cafile", ca]);
    }
    let pub_status = pubcmd.status().expect("publish status");
    assert!(pub_status.success());

    let mut output = String::new();
    if let Some(stdout) = sub_child.stdout.as_mut() {
        let _ = stdout.read_to_string(&mut output);
    }
    let status = sub_child.wait().expect("wait mosquitto_sub");
    assert!(status.success());
    assert!(
        output.contains(&payload),
        "subscriber output missing payload: {output}"
    );
}

#[cfg(feature = "quic")]
#[test]
fn paho_qos2_resume_over_quic() {
    let paho = std::env::var("PAHO_SAMPLE").unwrap_or_else(|_| "paho_c_pub".into());
    if !binary_available(&paho) {
        eprintln!("skipping: paho sample client not found");
        return;
    }
    let args = match std::env::var("PAHO_SAMPLE_ARGS") {
        Ok(val) if !val.trim().is_empty() => val,
        _ => {
            eprintln!(
                "skipping: set PAHO_SAMPLE_ARGS with QoS2 QUIC invocation for the sample client"
            );
            return;
        }
    };
    let payload = format!(
        "interop-{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let mut parts: Vec<String> = args
        .split_whitespace()
        .map(|s| s.replace("${PAYLOAD}", &payload))
        .collect();
    if !parts.iter().any(|p| p.contains("qos")) {
        parts.extend(["--qos".into(), "2".into()]);
    }
    let mut cmd = Command::new(&paho);
    cmd.args(parts);
    let status = cmd.status().expect("run paho sample");
    assert!(status.success());
}
