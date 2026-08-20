# Security

Quantum's security model: transport, identity, authorisation, and
audit. The model is intentionally narrow — TLS for transport,
certificate-derived identity, role-based authorisation, tamper-evident
audit — with no optional authentication methods.

**Status: largely a design target.** What runs today:

- **Transport TLS.** The bare-metal graph terminates TLS 1.3 in the
  fluxor `tls` foundation module in front of `peer_router`. The Linux
  development graphs serve cleartext.
- **Role gating.** clustor's `operations` module gates admin
  operations by role (Operator / TenantAdmin / Observer /
  BreakGlass); the default role is Operator, which preserves
  allow-all behaviour until an identity source is wired.
- **Audit chain.** `governance`'s audit component emits
  sequence-numbered structured events chained with HMAC-SHA256. The
  key is currently derived at boot, so the chain is tamper-evident
  within a run but not against an attacker who can re-derive the
  key; durable key material is part of the design below.

Not implemented: client authentication of any kind (MQTT CONNECT,
Kafka SASL, and AMQP `Connection.StartOk` are accepted without
credential validation — AMQP advertises `PLAIN` but does not verify
it), per-tenant ACLs, at-rest encryption (the WAL's AEAD parameter is
reserved and unread), and signature schemes for snapshots or
manifests.

The remainder of this document is the target model.
**Status: design target, not wired.**

## Transport

| Property | Target |
|---|---|
| Protocol | TLS 1.3, mandatory for production listeners; QUIC v1 for the MQTT-over-QUIC path |
| Client authentication | mTLS — mandatory in production |
| SNI | Selects the per-tenant certificate chain |

A connection that fails certificate validation is closed before any
application bytes are read; the audit log records the attempt, the
SNI, the certificate digest, and the failure reason.

## Identity

| Layer | Identity source |
|---|---|
| Transport | Client X.509 certificate validated against a per-tenant trust bundle |
| Application | SPIFFE ID from the certificate's URI SAN when present; certificate Subject otherwise |

Token-based credentials would only ever extend role assertions; a
connection without a valid client certificate is rejected regardless.

## Authorisation

Role policy evaluates the certificate-derived identity against the
per-tenant ACL bundle from the control plane:

| Role | Permissions |
|---|---|
| `Operator` | Full read-write across operator-scoped admin endpoints. |
| `TenantAdmin` | Read-write within one tenant's namespace (ACLs, quotas, certificates). |
| `Observer` | Read-only against metrics and diagnostic surfaces. |
| `BreakGlass` | Time-limited elevated access for incident response; every action audited. |

Tenant ACLs gate publish, subscribe, and admin operations per
subject, with wildcards following the semantics of the protocol they
describe. A Stale or Expired control-plane cache forces
deny-by-default.

## At-rest encryption

The target is AEAD over all persisted data — WAL segments, snapshots,
retained and offline payloads — with data-encryption keys rotated on
an epoch schedule and unwrapped by a separately-rotated key-encryption
key. The `durability` module carries the key-epoch scaffolding
(weekly rotation interval); the encryption itself is not wired.

## Audit

`governance`'s audit component emits a chained structured event for
authentication outcomes, authorisation decisions, admin operations,
quota-driven disconnects, and DR actions. Events carry a sequence
number and an HMAC over `[seq][timestamp][event]`, so truncation and
tampering are detectable given the key. Audit events do not flow
through the metrics pipeline: metrics are decimated and aggregated,
audit must be preserved verbatim.

## Threat model

Quantum assumes:

- **Trusted operators** with access to the admin surface; operator
  actions are audited.
- **Untrusted clients**, to be authenticated by mTLS; clients must
  not be able to bypass tenant boundaries or read data outside their
  ACL.
- **Hostile network** between clients and the broker; all
  client-facing traffic is TLS in production.
- **Trusted-but-bounded peer network** between cluster nodes.
- **Crash-honest disk.** WAL fsync is honoured; storage that
  silently corrupts is out of scope.

Out of scope: DDoS mitigation at the edge (use an L4 load balancer),
per-tenant network isolation (use separate listeners or VLANs), and
protection against a majority-compromised control plane.
