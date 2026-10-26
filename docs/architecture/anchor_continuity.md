# Anchor continuity: replacing the ingress host under a session

Source: `modules/common/cores/anchor_continuity_core.rs` (the anchor's
record, the production gate over the output horizon, the worker's
retained productions, the re-anchor exchange),
`modules/common/cores/production_seat_core.rs` (the worker's seats),
`modules/common/cores/session_site_core.rs` (a session exported to a
surviving site), `modules/common/wire.rs` (`MSG_ANCHOR_REANCHOR`,
`_REANCHORED`, `_HORIZON`, `_PRODUCTION`, `_QAC`, `_QAC_RETIRE`,
`_CONSUMED`, `MSG_CLIENT_DELIVERED`, `MSG_CLIENT_RESUMED`,
`MSG_SITE_EXPORT`, `MSG_SITE_RECORD`), the modules that run them
(`quantum_mqtt_anchor`, `session_processor`), and the gates below.

[session_continuity.md](session_continuity.md) replaces the WORKER
behind an anchor that stays. This document is the other direction: the
ingress host under the anchor is replaced — a paired-anchor takeover,
planned or after the primary host loses power — while the worker stays,
and, for a whole site, a session resumed at a surviving site. It is
Quantum's half of Wormhole's transport continuity: the transport
(TCP/TLS, mirrored to a standby host by Fluxor's providers and Wormhole's
paired anchors) moves the client's connection; Quantum makes sure the
MQTT session on it neither repeats nor loses work.

The modules run it. `quantum_mqtt_anchor` with `continuity: 1` attaches
every connection as `CC_TRANSPORT_MIGRATABLE`, passes the worker's
productions through a production gate per connection, acknowledges to
the transport only the client bytes its worker has consumed, exports
each connection's `QAC1` record on `state_out` and holds the paired
anchor's from `state_in` for a takeover; `session_processor` seats every
session attached that way
(`cores/production_seat_core.rs`), numbers and retains its responses,
answers the re-anchor exchange and offers again what the replacement's
horizon lacks, and exports a session for a surviving site on request
(`QSS1`, below). [In the modules](#in-the-modules) says how they are wired
and what each claims.

## Production ids and the mirrored output horizon

A worker's response is a PRODUCTION. The worker numbers them per
session, from 1, and hands each to the anchor in the production envelope
(`MSG_ANCHOR_PRODUCTION`, the delivery envelope's layout outbound):

    [conn_id:u16 LE][session_id:16][session_epoch:u32 LE]
    [production_id:u64 LE][inner_msg_type:u8][inner payload after conn_id]

A production counts as GIVEN — the PUBACK, PUBREC or SUBACK the client is
owed has been answered — only once it is inside the transport's
**mirrored output horizon**: the standby host holds every byte of it, so
the primary's loss cannot take it. That is Wormhole's RPO-zero rule, and
it is the anchor's to enforce, because the anchor is what hands
responses to the transport.

The horizon is Fluxor's `TRANSPORT_CONTINUITY`: `tls` (`write_horizon:
1`) counts each connection's clear bytes as its consumer wrote them and
reports, as `net_proto::MSG_HORIZON [conn_id][bytes_through:u64]`, how
many of them the records the standby holds carry (Clustor's
`peer_router` relays it to the anchor as `MSG_CLIENT_HORIZON`). A
takeover continues the numbering, and the new host reports at once how
far the old one's output reached. The hook is `OutputHorizon`:

| Operation | Meaning |
|---|---|
| `submit(id, len)` | production `id` went onto the transport as `len` bytes of the connection's output, strictly in order: `Pending` (waiting for the horizon), `Duplicate` (already inside it), `NotReady` (more productions beyond the horizon than it tracks: offer again under the same id) |
| `accepted_through()` | the highest production id whose last byte the standby holds |

`TransportHorizon` implements it over the byte reports: it keeps where the
connection's output stands (`written`) and the end byte of each
production beyond the horizon (32 at most), and accepts each one the
horizon reaches. Positions are exact because, on a connection under
continuity, **every byte the anchor writes after the attachment is a
production**: the worker answers PINGREQ too (the anchor forwards the
mark and its PINGRESP comes back as a production), and a production's
encoding depends only on the response and the negotiated version — the
codec uses no outbound topic alias on such a connection.

`ProductionGate` is the anchor's side: it submits the next id, refuses a
gap (`Gap`), answers a re-offer of an id already inside the horizon
`Accepted` without submitting it again, never believes a horizon past
what was written, and reports each advance once to the worker as
`MSG_ANCHOR_HORIZON [session_id:16][epoch:u32 LE][accepted_through:u64 LE]
[bytes_through:u64 LE]` — `bytes_through` the end of that production —
taken only at the worker's bound epoch, so a stale emitter's report is
ignored.

A horizon report never moves back, and never runs past what the anchor
wrote — or, on a connection taken over, past what the standby held: a
report beyond both would place the next production as already held and
it would never be written.

`ProductionRing` is the worker's side: every production stays retained,
with its bytes, until a horizon report covers it. The ring is bounded; a
full ring refuses the next production, which the worker then does not
emit. That is backpressure, never eviction — an evicted response is an
acknowledgement lost. A production the channel to the anchor refuses
gives its id back (`unproduce`), so the anchor never sees a gap; a
horizon report whose bytes do not move on with its ids is ignored.

## What the anchor exports: `QAC1`

The transport carries the client's bytes, the delivery cursor and the
records with the output bytes they carry. What the MQTT anchor adds is its
own record, fixed 88 bytes, little-endian, CRC32-checked:

    ["QAC1":4][session_id:16][epoch:u32][anchor_id:8][worker:u8]
    [mqtt_version:u8][flags:u8][reserved=0:u8][tenant:u32][listener:u16]
    [keepalive_s:u16][client_hash:u64][next_delivery:u64]
    [next_production:u64][production_accepted:u64]
    [checkpoint_delivery:u64][crc32:u32]

It is `AnchorConn`'s identity, binding and negotiated parameters plus the
counters; the parser's partial frame and the hold are not in it, because
the standby transport re-assembles deliveries from the bytes it holds.
`decode` refuses a wrong length, a flipped bit, a non-zero reserved byte,
a zero epoch or session, and counters that cannot be one anchor's
(delivery or production ids not from 1, a horizon at or past the next
production). The primary anchor exports it, with the client the
connection was accepted from, at attachment and again whenever its
binding changes (a re-anchor, a handoff to the other worker, an
adoption); it travels to the standby beside the transport's checkpoint,
and the paired anchor holds the newest record of each session until the
connection closes on the primary (`MSG_ANCHOR_QAC_RETIRE`) or a takeover
claims it.

**Import.** `anchor_import(record, transport)` builds the replacement
over the cursors the standby transport holds (`TransportCursors`: the id
the next delivery it hands takes, the last delivery the old anchor
acknowledged, and `out_bytes` — the output the standby holds, its
horizon at the takeover). The delivery numbering continues from the
transport's cursors, not the record's: the record was exported at a
binding change, and the last acknowledgement may be on either side of
it. Whether the cursors leave a delivery out is the worker's to judge
(below); cursors no connection could have — none mirrored, or an
acknowledgement at or past the next delivery — are refused
`STATUS_CURSOR_MISMATCH`. A session at the last epoch has none left to
claim and is refused `STATUS_STALE_EPOCH`. The imported connection is
held (`WaitAttached`) until the worker answers.

## The client's bytes: acknowledged once consumed

The transport hands a takeover the client's bytes the old anchor never
acknowledged. Fluxor's `tls` under `delivery_ack: 1` counts a received
record as consumed only once its consumer says so —
`net_proto::CMD_DELIVERED [conn_id][consumer_cursor:u64][in_bytes:u64]`,
"the connection's first `in_bytes` clear bytes are consumed, and the next
delivery after them takes `consumer_cursor`" — mirrors the cursor with
the bytes, and keeps every record past them for the standby. At a
takeover the standby hands them again starting exactly at `in_bytes`,
after telling its consumer where it stands
(`net_proto::MSG_RESUMED [conn_id][consumer_cursor][in_bytes][out_bytes]`).
Clustor's `peer_router` relays the two as `MSG_CLIENT_DELIVERED` and
`MSG_CLIENT_RESUMED`.

The anchor acknowledges a **packet boundary** — a byte where the codec
holds no partial packet, with the delivery id the next packet takes —
and only once the bound worker has consumed every delivery before it
(`MSG_ANCHOR_CONSUMED [session_id][epoch][delivery_hw]`, which the worker
sends as its high-water moves). A delivery written toward the worker and
lost with the anchor's host is then still the transport's to hand again.
`DeliveryCursor` keeps this per connection: boundaries move only
forward, one is latched at a time so a steady stream is acknowledged
every worker round trip rather than chasing the newest, a connection
resumed here starts from its predecessor's acknowledgement, and a
session moved to another worker waits for that worker's report.

The replacement must number the bytes handed again exactly as the old
anchor did, so every id is a function of the byte stream: each packet
takes the next delivery id in decode order, whatever happens to it next
— forwarded, held behind earlier frames, or (a PINGREQ's liveness mark)
held until a worker is authoritative. Deliveries the worker already
consumed then arrive under their own ids and are refused as duplicates.

## The re-anchor exchange

What no record can say is what the worker did with frames the old anchor
handed it after the last mirrored delivery cursor, and where its
productions stand against the output the standby holds. So the
replacement asks, on the SessionCtrlV1 sideband (session-scoped, the
session header first):

    MSG_ANCHOR_REANCHOR   [session_id:16][new_epoch:u32][anchor_id:8]
                          [next_delivery:u64][bytes_held:u64]
    MSG_ANCHOR_REANCHORED [session_id:16][new_epoch:u32][status:u8]
                          [delivery_hw:u64][next_production:u64]
                          [released:u64][released_bytes:u64]

The worker decides with `reanchor_verdict`:

| Condition | Status |
|---|---|
| not this worker's session | `STATUS_UNKNOWN_SESSION` |
| `new_epoch` at or below the bound epoch | `STATUS_STALE_EPOCH` |
| `next_delivery` past the consumed high-water + 1: a delivery was acknowledged to the transport and never reached the worker, and the transport will not hand it again | `STATUS_CURSOR_MISMATCH` |
| `bytes_held` below the end of the last production the worker released: the client would never see it | `STATUS_CURSOR_MISMATCH` |
| a production above the release that the worker numbered without its bytes: it can be neither placed nor re-offered | `STATUS_CURSOR_MISMATCH` |
| otherwise | `STATUS_OK`, rebound at `new_epoch` |

The answer always carries the worker's cursors, and the anchor applies
only its own answer (session and epoch must match; the worker's
high-water must cover every delivery the transport was told was
consumed, and its release lie inside what the standby holds). On
`STATUS_OK`:

- a delivery at or below the worker's high-water was consumed: the
  worker refuses it as a duplicate (`admit_delivery`; `owes` tells a
  replacement which ids it need not send) — **no duplicate delivery**;
- every production the worker retains is re-offered under its original
  id, oldest first, and its new productions wait until the last re-offer
  has gone; the replacement places each one from `released_bytes` by
  its encoded length: held whole by the standby (given, nothing
  written), held in part (only the rest written), or not at all
  (written) — **no response emitted twice, none lost, every one
  whole**.

Any other answer closes the connection: the client reconnects and
resumes from committed state, which is the honest outcome when the
session cannot continue exactly. The directory learns the new anchor
with `ATTACH` at the new epoch naming it, as after a swap.

A planned cut quiesces first, so the horizon covers every production and
a worker with nothing retained re-anchors exactly. A crash takeover
needs the ring: a worker that released a production the standby never
held (only possible over a transport whose horizon is not the
standby's) is refused, by design.

## A session at a surviving site: `QSS1`

A planned handoff moves a session between two workers that read the
same node's `topic_engine` and the same committed stream, so QSR2 leaves
out what the importer already has. A surviving SITE has neither the lost
site's `topic_engine` nor a worker that held the session, so the site
record carries what the session's delivery state is made of, in the same
vocabulary:

    ["QSS1":4][session_id:16][epoch:u32][source_site:u32][export_seq:u64]
    [tenant:u32][stream_hash:u64][generation:u32][slot:u32][shard:u32]
    [protocol:u8][keep_alive_ms:u32][next_msg_id:u32]
    [session_expiry_s:u32][receive_maximum:u16]
    [flags:u8][will_present:u8][will_qos:u8][will_retain:u8]
    [will_delay_ms:u32][will_topic_len:u16][topic]
    [will_payload_len:u16][payload]
    [sub_count:u8]      × [qos:u8][options:u8][filter_len:u16][filter]
    [inflight_count:u8] × QSR2's 28-byte in-flight entry
    [owed_count:u8]     × [packet_id:u16 BE][qos:u8][released:u8]
                          [generation:u32][body_len:u16][PUBLISH body]
    [handoff tail: delivery_hw (the dedupe cursor), apply cursors]
    [crc32:u32]

Bounds: 32 subscriptions of up to 256 bytes, 16 in-flight flows, 16 owed
deliveries with bodies up to 1920 bytes (the delivery ledger's bound),
64 apply cursors. A session past any bound is refused at export, never
truncated; a record whose counts or lengths do not account for its bytes
exactly, or whose CRC does not match, is refused whole. The options byte
is 0: the broker applies no MQTT 5 subscription options. The record
carries the clean-start flag and the Will (present, QoS, retain, delay,
topic up to 256 bytes, payload up to 512), as QSR2 does; not
flow-control credit, which the session's in-flight window and owed
deliveries re-establish at the resume, nor its activity clock, which
restarts there.

The owed deliveries keep their packet ids and a released QoS 2 delivery
is owed its PUBREL, never a second PUBLISH; a recorded inbound QoS 2 flow
makes the client's retried PUBLISH a retry (`publish_is_retry`) whether
or not its PUBREC went out, never a second message.

**Import.** `site_admit(record, resume_epoch, site)` decides at the
surviving site, against the epoch the session directory committed for
the resume and the site's replica of the committed stream:

| Condition | Status |
|---|---|
| `resume_epoch` at or below the record's | `STATUS_STALE_EPOCH` |
| the site's replica holds the session at a later generation (a CONNECT committed after the export) | `STATUS_STALE_EPOCH` |
| the site applied an entry of the session past the record's cursor that the record lacks | `STATUS_CURSOR_MISMATCH` (`cursor_verdict`) |
| the site is behind the record somewhere (including a replica that has not yet applied the CONNECT behind the record's generation) | admitted with a replay floor, at the record's generation |
| otherwise | admitted |

A site keeps only the newest record of a session (`site_keeps`: a newer
epoch, or a later export at the same epoch). A refused record is a
RECONNECT: the client starts the session again from what the surviving
site's log holds.

## In the modules

**The anchor** (`quantum_mqtt_anchor`, `continuity: 1`):

| Port / param | What |
|---|---|
| `continuity` (param 14) | 1: attach as `CC_TRANSPORT_MIGRATABLE`, gate productions, acknowledge consumed input, export and import `QAC1`. |
| `state_out` (out[7]) | `MSG_ANCHOR_QAC [QAC1][peer_family][peer_addr:16][peer_port]` — at attachment, at every binding change, and on `cmd_in` `MSG_ANCHOR_QAC [session_id]` (a planned cut asks after its quiesce); `MSG_ANCHOR_QAC_RETIRE [session_id]` when the connection closes. |
| `state_in` (in[7]) | the paired anchor's `state_out`, carried by the transport's pairing (Wormhole's `transport_mirror`, `state_out` → `state_in`): its records, staged for a takeover. |
| `raw_in` `MSG_CLIENT_PEER` / `MSG_CLIENT_RESUMED` / `MSG_CLIENT_HORIZON` | a connection's client, a connection a takeover moved here with the transport's cursors, and its output horizon (Clustor's `peer_router` with `forward_peer: 1` relays Fluxor's `MSG_PEER`, `MSG_RESUMED` and `MSG_HORIZON`). |
| `out_frames` `MSG_CLIENT_DELIVERED` | `[conn_id][consumer_cursor:u64][in_bytes:u64]`, in order with the connection's output: what its worker has consumed (relayed as `CMD_DELIVERED`; the transport runs `delivery_ack: 1`). |

Records are read at the start of each step, before the transport's
frames. The stage holds the newest record of each session — a record at
a lower epoch than the one staged is ignored, and a full stage evicts
the record staged longest ago — until it is retired, replaced or
claimed. A connection the transport presents with a staged record's
client (`MSG_CLIENT_PEER`) claims it; Fluxor sends a resumed
connection's `MSG_RESUMED` right after its peer and before any data, so
`MSG_CLIENT_RESUMED` imports the claimed record, and anything else first
means a new connection from that client — the record's connection is
gone and the record is dropped. A resumed connection no record claimed
is closed: its bytes start mid-session. The record is imported over the
transport's cursors (`anchor_import`; cursors no connection could have
are refused and the connection closed), the codec resumes it at the
transport's `in_bytes` under the MQTT version the record carries, and
`MSG_ANCHOR_REANCHOR` goes to its worker. The bytes handed again are
decoded and held under ids from `consumer_cursor` until
`MSG_ANCHOR_REANCHORED`; OK makes the connection live at the new epoch
(the directory told, the record exported again, what the connection did
while it waited kept, the held deliveries the worker already consumed
refused by it as duplicates, and the seam's byte cursors counted afresh
on both sides, so the next handoff compares them); anything else closes
it and the client reconnects. The request is sent again every 250 ms
until answered (`REANCHOR_RETRY_MS`) — the worker answers a repeat from
the anchor it already re-anchored to as it answered the first — and a
connection unanswered after 10 s (`REANCHOR_GIVE_UP_MS`) is closed. A
connection whose transport id is not the one its session id names is
reached through an alias, and everything the anchor sends the worker for
it carries the id the session names, which is the one the worker binds;
a full alias table, or a codec with no slot for the connection, closes
it, and so does a new connection the transport presents under the id an
aliased session names, which would replace that session's binding
(`gatefull=`).

The client a record names is the one the transport reported for the
connection before its attachment; a connection whose client was never
reported (or was pushed out of the `PEERS_SEEN` most recent) exports no
record (`unpeered=`), and a takeover of it closes it.

Each worker response arrives as `MSG_ANCHOR_PRODUCTION`. The gate admits
only the bound worker's, for the connection's session and epoch; the
next id goes to the codec, which writes whatever of its bytes the standby
does not already hold, and it is given once the transport's horizon
reaches its last byte — reported to the worker (`MSG_ANCHOR_HORIZON`)
once per advance. A re-offer of an id already given is answered from the
horizon without a second send; a gap is refused. A response the frame
edge refuses is held whole, production id and all, and offered again
first. A re-offer that arrives before the anchor has read the worker's
`MSG_ANCHOR_REANCHORED` (the worker sends both at once; the anchor reads
responses before control) is held the same way until it has. A plain
response for a gated connection has no place in its numbering and is
never written: a DISCONNECT closes the connection instead, anything else
is dropped as a stale emitter's. Each new attachment gets a fresh gate.

A planned handoff or an adoption moves a session under continuity to
the other worker like any other, and the numbering moves with it: the
anchor's RESUME and ADOPT carry where its gate stands
(`[next_production:u64][bytes_written:u64]` after the session header),
and the worker the session moves to seats it there
(`open_resumed`), so its first response is the gate's next production,
placed after every byte the old worker's took. A planned handoff drains
only once the old worker's seat is settled — every production inside the
horizon, nothing re-offered or owed — so nothing a later takeover could
need is left in a ring that no longer serves the session. An adoption
cannot wait for a worker that is gone: what the lost worker wrote past
the horizon is not the new worker's to re-offer, so a takeover before the
horizon passes it is refused, never answered wrong. The productions of
the worker a session moves to, sent before the anchor has read its
RESUMED, are held until it has, as re-offers are.

**The worker** (`session_processor`) behind a pair of ingress hosts has
an anchor seam per host — `codec_in`/`codec_out` and `ctrl_in`/`ctrl_out`
for one, `codec2_in`/`codec2_out` and `ctrl2_in`/`ctrl2_out` for the
other — and serves each session on the seam its ATTACH, RESUME, ADOPT or
accepted `MSG_ANCHOR_REANCHOR` came from: its responses, productions and
control frames go there, and an answer goes back to the seam that asked.
A fan of one output to both hosts would stall on the host that died (a
fan moves a frame only when every output has room), so nothing is
fanned. The seams share the worker's connection-id space: a connection
id is served by one anchor at a time, and an attachment on an id whose
binding is another anchor's replaces it, as a recycled id does.

An ATTACH at
`CC_TRANSPORT_MIGRATABLE` takes a seat (16 per worker, 16 retained
productions each; a full table refuses the attachment, a full ring holds
the response back — backpressure, never eviction). An attachment starts
at production 1, and replaces whatever seat its connection id or session
held. A seated session's responses go out as productions; a horizon
report from the session's anchor at its epoch releases what it covers;
`MSG_ANCHOR_REANCHOR` is decided by `reanchor_verdict` against the
binding's consumed high-water and the seat's ring, and on OK the binding
moves to the new epoch and every retained production is offered again
under its id, continued each step until the channel has taken them all.
A response larger than a ring slot (a big PUBLISH to a subscriber, whose
QoS 1/2 delivery the delivery ledger owes again anyway) is numbered
without its bytes; until a horizon covers it a replacement whose horizon
is below it is refused, so it is never silently skipped. A PINGRESP a
full ring holds back is owed and answered on a later step.

**The horizon.** The gate reads the transport's own: `tls` holds every
record it seals under a crash-continuous pairing until the standby has
acknowledged it, and reports the bytes those records carry. So a planned
move and a crash with productions in flight — written, partly mirrored,
or not at all — both re-anchor exactly. A PINGREQ on a connection under
continuity is answered by the worker (its mark held while none is
authoritative), so the anchor writes nothing outside the production
numbering.

**Site export.** `MSG_SITE_EXPORT [session_id][source_site][export_seq]`
on the worker's control channel answers `MSG_SITE_RECORD` on `site_out`
(out[13]): the session's `QSS1` record — identity and placement, in-flight
ledger, owed deliveries from the delivery ledger, the dedupe and apply
cursors, the Will, and its subscriptions from the worker's own book
(`SiteSubBook`, filled on the apply side from the committed subscribe and
unsubscribe entries, cleared when the session's subscriptions end, and
carried in the session's handoff record when it moves between workers)
— or `[session_id][status]`: the worker serves no live attachment of it,
it is past a bound, or its state is not whole here (a subscription the
book could not hold, or an owed delivery the ledger had no room for).
Every request gets one answer or the other. A parked session has no
attachment to export: its state is in the committed log, which the
surviving site resumes it from. The carrier across
sites is the deployment's (Wormhole's site-loss flow); the import is
`site_admit`.

**Heartbeats.** Under continuity the anchor adds `[anchor] cont given=
dup= gap= hzrep= qac= staged= reanch= reref= gatefull= acked= stdrop=
runknown= unpeered=` to its heartbeat, and the worker, once any of it
has run, `[sess] cont seatref= full= unret= rel= hzstale= reanch= reref=
pongheld= siteout= siteref= consumed=`.

## Bounds

| Bound | Value | Exhausted |
|---|---:|---|
| `GATE_SEATS` (anchor) — connections under a production gate | 16 | an attachment or import without a gate is closed (`gatefull=`) |
| `HORIZON_PENDING` — productions written past the horizon per connection | 32 | the next production is held until the horizon moves |
| `STAGED_IMPORTS` — the paired anchor's records held | 16 (`GATE_SEATS`) | the record staged longest ago is dropped (`stdrop=`) |
| `PEERS_SEEN` — clients remembered until their connection attaches | 64 | the oldest is forgotten; its connection exports no record (`unpeered=`) |
| `REANCHOR_RETRY_MS` / `REANCHOR_GIVE_UP_MS` | 250 ms / 10 s | an unanswered re-anchor is closed |
| `SEAT_COUNT` (worker) — seated sessions | 16 | the attachment is refused, a move is refused (`seatref=`) |
| `SEAT_RING` — productions a seat retains | 16 | the response is held back (`full=`) |
| `RETAIN_BYTES_MAX` — bytes of one retained production | 1024 | numbered without its bytes (`unret=`), never re-offered |
| `SITE_SUBS_MAX` × `SITE_FILTER_MAX` — subscriptions in a site record | 32 × 256 B | the export is refused |
| `SITE_INFLIGHT_MAX` / `SITE_OWED_MAX` | 16 / 16 | the export is refused |
| `SITE_WILL_TOPIC_MAX` / `SITE_WILL_PAYLOAD_MAX` | 256 / 512 B | the export is refused |
| `SITE_SUB_BOOK` — subscriptions one worker's book holds | 256 | the session is marked lost; its export is refused |

## Gates

- `tools/core_tests/src/anchor_continuity_tests.rs` — the production
  envelope round-trips and is never truncated; a production is accepted
  only once the byte horizon reaches its last byte, reported once, never
  past what was written, and an over-report places nothing as held; a
  re-offer inside the horizon is not resubmitted; a gap is refused and a
  full horizon keeps the id; a replacement writes only what the standby
  does not hold; the ring retains until the horizon, refuses rather than
  evicts, and ignores backwards or foreign reports; `QAC1` round-trips,
  every flipped bit and impossible record is refused; an import takes the
  standby's counters, continues from a transport behind the record, and
  refuses cursors no transport could hold, none mirrored, and a session
  at the last epoch; the worker admits a replacement only when the
  transport hands again everything it has not consumed, and the
  replacement applies only its own answer; an acknowledgement goes only
  for a boundary the worker has consumed up to, one latched at a time,
  forward only, from a resumed connection's predecessor's, and waiting
  for a new worker's report after a move; a crash with deliveries consumed past the mirrored
  cursor and the standby's horizon at every byte from the last release
  through the last production applies every delivery once, and the
  client reads every response once, whole, in order; a worker that
  released past the standby's horizon is refused.
- `tools/core_tests/src/production_seat_tests.rs` — the worker's seats
  against the anchor's real gate: numbering, release by the horizon, a
  full ring and a full table, an oversize response, a crash takeover's
  re-offers with new productions held behind them, an id given back for
  a refused production, a recycled connection id starting from 1, a
  horizon report whose bytes go back or past what was produced ignored,
  a session moved to another worker numbering on from the anchor's gate,
  and a repeated re-anchor answered again.
- `tools/core_tests/src/session_site_tests.rs` — a session round-trips
  whole; every flipped bit and truncation is refused; bounds refuse at
  export; a recorded QoS 2 flow makes the retry a duplicate; the epoch,
  generation and cursor rules decide the import, a lagging site taking a
  floor; a site keeps only the newest record; the Will travels whole
  and out-of-place flags or Will bytes are refused; the subscription book
  follows the committed entries, tracks lost sessions one by one, and
  carries a session's subscriptions across a move between workers.
- Wormhole's `transport_continuity` and `continuity_cores` suites run the
  re-anchor exchange and the horizon gate through planned and crash
  takeovers over these cores, and its `site_loss` suite resumes sessions
  at the surviving region with `site_admit`.

`examples/paired_anchor/quantum_host.yaml` (each ingress host: Fluxor's
`ip` and `tls` with `write_horizon` and `delivery_ack` under Wormhole's
paired anchor and mirror, Clustor's `peer_router` with `forward_peer`,
the anchor with `continuity: 1`, its records on the mirror and its seam
on a `remote_channel` to the worker host) and
`examples/paired_anchor/quantum_worker.yaml` (one `session_processor`
over a single-node substrate, an anchor seam per ingress host) compose
the modules end to end; `tests/host/mqtt_peer.py` is the MQTT-over-TLS
client that counts what the broker owes it — every acknowledgement and
delivery once, in order — for Wormhole's paired-anchor rig to drive
through planned and crash takeovers.
