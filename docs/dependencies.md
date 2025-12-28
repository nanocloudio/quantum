# Dependency Management

## Automated Auditing

The CI workflow includes an optional `audit` job using `rustsec/audit-check`. This job:
- Runs on every push and PR
- Is non-blocking (`continue-on-error: true`) to avoid breaking builds for advisory-only issues
- Reports known vulnerabilities from the RustSec advisory database

## Manual Review Process

When the automated audit is not sufficient or for deeper review:

1. **Check for advisories**:
   ```bash
   cargo install cargo-audit
   cargo audit
   ```

2. **Review dependency tree**:
   ```bash
   cargo tree -e features
   ```

3. **Check for outdated dependencies**:
   ```bash
   cargo install cargo-outdated
   cargo outdated
   ```

## Dependency Policies

- **Minimize defaults**: Disable default features where not needed (see `Cargo.toml` comments)
- **Single TLS stack**: Use rustls exclusively; no native-tls or openssl
- **Single async runtime**: Tokio only; no async-std, smol, or actix-rt
- **Dev-only dependencies**: Keep test/dev tools in `[dev-dependencies]`

## Heavy Dependencies

The following dependencies are justified despite their size:

| Dependency | Justification |
|------------|---------------|
| `clustor` | Core Raft consensus and durability framework |
| `tokio` | Async runtime (required for all async I/O) |
| `rustls` | TLS implementation for listeners |
| `reqwest` | HTTP client for forward-plane and telemetry |
| `clap` | CLI argument parsing with derive macros |
| `tracing-subscriber` | Structured logging (optional, behind feature flag) |
