use std::process::Command;

use tempfile::tempdir;

#[test]
#[ignore = "requires data/fixtures/mqtt to exist"]
fn mqtt_fixture_dry_run_succeeds() {
    let bin = env!("CARGO_BIN_EXE_quantum");
    let temp = tempdir().expect("create temp dir");
    let status = Command::new(bin)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "init",
            "--fixtures",
            "data/fixtures/mqtt",
            "--data-root",
            temp.path().to_str().unwrap(),
            "--workload",
            "mqtt",
            "--dry-run",
        ])
        .status()
        .expect("run quantum init");
    assert!(status.success(), "quantum init exited with {status:?}");
}
