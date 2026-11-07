# One tenant's broker

`quantum-tenant-broker` is a service bundle
(`packaging/service/quantum-tenant-broker/`): one tenant's single-node MQTT
broker, reached only over mutual TLS by that tenant's devices. A platform that
hosts tenants deploys one per tenant. Nanocloud deploys it as a graph pod in a
tenant's namespace.

```text
linux_net ⇄ tls ⇄ broker (the quantum_broker stack: peer_router ⇄ protocol ⇄ …)
```

## Who gets in

Fluxor's `tls` is the only door. It runs as a server with `verify_peer: 1`
and `peer_auth: ca_uri`, and admits a client only when all of these hold:

- its chain verifies to `client_ca`;
- its leaf is within its validity on the platform's trusted clock;
- its leaf carries a URI SAN equal to `device_uri`, or strictly under it
  when `device_uri` ends in `/`.

With `device_uri` set to `spiffe://<trust-domain>/device/<namespace>/`, every
device of one namespace is admitted and nothing else is. A client with no
certificate, one from another CA, an expired one, or one under another
namespace's prefix fails the handshake. That includes a prefix that only
shares leading characters, such as `…/device/a-evil/` against `…/device/a/`.
The client never reaches the broker, and `tls` logs
`[tls] peer auth FAIL conn=<n> profile=ca_uri reason=<why>`.

**What this isolates.** Tenants are isolated by deployment: each tenant has
its own broker, process, WAL and port, and its own trust prefix. Inside one
broker every admitted device reaches every topic. The broker does not yet read
the peer identity `tls` reports on `peer_identity`, so a CONNECT's client id
and topics are the device's to choose. Topic ACLs keyed by the certificate's
SPIFFE ID close that gap ([security.md](security.md)). The tenant inside the
broker is still the single tenant 0 ([multi_tenancy.md](multi_tenancy.md)),
which here is accurate, since the broker serves one tenant.

## Params

| Param | Type | Default | |
|---|---|---|---|
| `listen_port` | integer | 8883 | TCP port for MQTT over TLS. Graph pods share their node's network, so two brokers on one node need different ports |
| `server_cert` | string, required | | Path of the broker's certificate chain, leaf first, PEM or DER |
| `server_key` | string, required | | Path of its private key |
| `client_ca` | string, required | | Path of the CA certificate, or a bundle of up to `tls`'s anchor limit, that device certificates must chain to. A bundle is how an authority rotation is expressed |
| `device_uri` | string, required | | The URI SAN a device certificate must carry: this URI, or any URI under it when it ends in `/`. At most 128 bytes |
| `client_id` | string | `quantum-tenant` | The broker front's client id. Unused, because the fronts are left unwired |

The credentials are paths that `tls` reads at runtime where the graph runs,
typically a volume lent to the workload. So an image carries no credential,
and on aarch64 hosts rotated files are picked up within 10 s without a
restart. Certificate lifetimes are checked against the platform's trusted
clock (`clock_policy: require`), which a Linux host provides. The broker's
WAL lives in the working directory.

The stack's in-graph publish and subscription fronts are not wired, so MQTT
over TLS is this graph's only surface. An application that also wants the
fronts includes the stack itself ([broker_front.md](broker_front.md)).

## Running it

```sh
fluxor run quantum-tenant-broker \
  --param listen_port=18883 \
  --param server_cert=/srv/t/a/server.crt --param server_key=/srv/t/a/server.key \
  --param client_ca=/srv/t/a/devices-ca.crt \
  --param device_uri=spiffe://cell.example/device/a/
```

`tests/integration/tenant_broker.sh` builds the bundle's graph with its params
against a PKI it generates with `openssl`, then drives it end to end. Two
devices of namespace `a` connect, publish at QoS 1 and receive each other's
exact bytes. A device of namespace `b`, one under `a-evil/`, one signed by
another CA, an expired one, and a client with no certificate each get no
CONNACK, and `tls` logs each refusal with its reason (`name-mismatch`,
`unknown-ca`, `expired`). An admitted device still connects afterwards.
