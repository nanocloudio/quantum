# Security

Quantum's security model: transport, identity, authorisation,
at-rest encryption, and signing. The model is intentionally narrow —
TLS / mTLS for transport, SPIFFE-derived identity, RBAC + per-tenant
ACLs for authorisation, AEAD for storage, Ed25519 for signed
artifacts. There are no optional auth methods; every surface enforces
the same stack.

The mechanics of the modules that implement these contracts (`tls`,
`rbac`, `key_manager`, `audit_logger`, `wal`, `snapshot_engine`) are
documented in the Clustor substrate; this document specifies what
they guarantee for Quantum.

## Transport

| Property | Setting |
|---|---|
| Protocol | TLS 1.3 (mandatory); QUIC v1 supported as a peer transport |
| Cipher suites | ChaCha20-Poly1305, AES-128-GCM, AES-256-GCM |
| Key exchange | P-256 ECDH (default); X25519 supported |
| Client authentication | mTLS — mandatory in production |
| Session resumption | TLS 1.3 PSK; QUIC 0-RTT disabled for non-CONNECT packets |
| SNI / ALPN | SNI selects per-tenant cert chain; ALPN routes to `mqtt`, `mqtt-quic`, `kafka`, `amqp` |
| OCSP stapling | Enabled by default |
| CRL max staleness | 300 seconds |

mTLS is not optional. A connection that fails certificate validation
is closed before any application bytes are read; the audit log
records the connection attempt, the SNI, the cert digest (if any),
and the failure reason.

## Identity

| Layer | Identity source |
|---|---|
| Transport | Client X.509 certificate validated against per-tenant trust bundle |
| Application | SPIFFE ID extracted from the cert's URI SAN (when present); falls back to certificate Subject when SPIFFE is not in use |
| Optional auxiliary | JWT in MQTT 5 `Authentication Data` / Kafka SASL OAUTHBEARER / AMQP `sasl-mechanism` extends RBAC roles but never replaces mTLS |

The trust domain derives from the SPIFFE ID when present, otherwise
from `RAFT_TRUST_DOMAIN`, otherwise `local`. Dev builds may
self-generate ephemeral certs under `storage_dir/cp/certs`;
production supplies explicit material.

JWTs are auxiliary, never primary. They can carry additional role
assertions that the RBAC module evaluates alongside the cert-derived
identity, but a connection without a valid client cert is rejected
regardless of JWT presence.

## Authorisation

`rbac` evaluates per-operation policy against:

1. Cert-derived identity (always present in mTLS).
2. SPIFFE ID (when present).
3. JWT-asserted roles (when present and validated).
4. Per-tenant ACL bundle from CP-Raft.

Roles:

| Role | Permissions |
|---|---|
| `Operator` | Full read-write across operator-scoped endpoints (`/admin`, placement plans, tenant CRUD). |
| `TenantAdmin` | Read-write within one tenant's namespace (ACLs, quotas, certificates). |
| `Observer` | Read-only against `/metrics`, `/why`, audit logs. |
| `BreakGlass` | Time-limited (`breakglass_max_ttl_ms`, default 300ms) elevated access for incident response; every action is signed and audited. |

Tenant ACLs gate publish, subscribe, and admin operations per
subject. Wildcards in ACLs follow the same semantics as the protocol
they describe (MQTT `+`/`#`, AMQP topic exchange wildcards).

Stale CP cache forces deny-by-default. An ACL evaluation against a
Stale or Expired cache fails closed with `MQTT-5 0x87`
(Not authorized) / Kafka `CLUSTER_AUTHORIZATION_FAILED` / AMQP
`connection.close{reply-code=access-refused}`.

## At-rest encryption

All persisted data is encrypted with AEAD:

| Layer | Cipher | Key source |
|---|---|---|
| WAL segments | AES-256-GCM | DEK from `key_manager` (epoch-rotated) |
| Snapshots | AES-256-GCM | DEK from `key_manager` |
| Retained payloads | AES-256-GCM | Same DEK as the topic's PRG snapshot |
| Offline queue payloads | AES-256-GCM | Same DEK as the session's PRG snapshot |
| CP-Raft state | AES-256-GCM | Separate DEK; same KEK |

### Key rotation

`key_manager` rotates DEKs weekly (`rotation_interval_h = 168` by
default), retains the previous epoch for 48h (`retention_h = 48`),
and reserves nonces in 65 536-window batches to avoid nonce reuse
under concurrent encryption.

KEK rotation is operator-initiated. KEKs unwrap stored DEKs; rotating
a KEK re-wraps all active DEKs but does not require re-encrypting
data.

## Signing

Ed25519 signing for artifacts that require tamper evidence:

| Artifact | Signed by |
|---|---|
| Snapshots | `snapshot_engine`; verified on import and replay |
| CP-Raft manifests | CP-Raft signing key; verified on every cache load |
| Audit log entries | `audit_logger`; verified during compliance review |
| Durability proofs | Per Clustor §9.8; verified before adapter emits ACK |
| Break-glass actions | RBAC + signed audit; verified during forensics |

Signing keys are stored separately from encryption keys and rotated
on a different schedule (manual, typically per compliance cycle).

## Audit

`audit_logger` emits a signed structured event for:

| Event class | Examples |
|---|---|
| Auth | CONNECT success / failure, mTLS cert digest, SPIFFE ID, SASL outcome |
| Authorisation | ACL allow / deny per (session, subject, operation) |
| Admin | Tenant create / delete, placement plans, throttle overrides, leader transfer, shrink plans |
| DR | Checkpoint export, WAL archive shipment, fenced promotion |
| Break-glass | Every elevated action with operator identity and reason |
| Quota | Sustained-overage disconnects with metric values |

Retention default: 400 days. Entries are Ed25519-signed; tampering
invalidates the chain. Events also emit OpenTelemetry spans for live
correlation. See [observability.md](observability.md) for the full
event flow.

## Threat model

Quantum assumes:

- **Trusted operators** with access to the CP-Raft admin surface.
  Operator actions are audited but not cryptographically constrained
  beyond signature.
- **Untrusted clients** authenticated by mTLS. Clients cannot bypass
  tenant boundaries, escalate privileges, or read data outside their
  ACL.
- **Hostile network** between clients and the broker. All
  client-facing traffic is TLS 1.3; downgrade attacks fail closed.
- **Trusted-but-bounded peer network** between cluster nodes.
  Inter-node traffic is mTLS; a compromised node can vote in Raft
  but cannot read data it has no PRG replica for.
- **Crash-honest disk.** WAL `fdatasync` is honoured; partial writes
  are detected by the AEAD tag. Storage that silently corrupts (e.g.,
  non-ECC RAM, lying disks) is out of scope.

Out of scope: DDoS mitigation at the edge (use an L4 load balancer or
VIP), per-tenant network isolation (use separate listener processes
or VLANs), and cryptographic protection against compromised CP-Raft
voters (a majority-compromised CP can rewrite manifests).
