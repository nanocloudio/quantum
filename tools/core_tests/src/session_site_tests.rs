//! A session exported to a surviving site and imported there (`QSS1`):
//! the record carries subscriptions, the `QoS` 1/2 in-flight ledger, the
//! owed outbound deliveries, the next packet id, and the dedupe and apply
//! cursors; the import is decided by the resume epoch, the generation the
//! site's replica holds and the replay cursor rule.
//!
//! The failures prevented are a site-loss RESUME that quietly comes back
//! without its subscriptions or with a `QoS` 2 flow forgotten (a second
//! copy of a message the client already handed over), a record replayed
//! from behind a later CONNECT, and a record that misses an entry the
//! surviving site already applied.

use super::session_site::*;

const SID: [u8; 16] = [0x5D; 16];

fn session() -> SiteSession {
    let mut s = SiteSession::zero();
    s.session_id = SID;
    s.epoch = 1;
    s.source_site = 1;
    s.export_seq = 3;
    s.tenant = 7;
    s.stream_hash = 0x0123_4567_89AB_CDEF;
    s.generation = 4;
    s.slot = 12;
    s.shard = 9;
    s.protocol = 5;
    s.keep_alive_ms = 30_000;
    s.next_msg_id = 41;
    s.session_expiry_s = 3600;
    s.receive_maximum = 16;
    s.flags = SITE_FLAG_CLEAN_START;
    s.will_present = 1;
    s.will_qos = 1;
    s.will_retain = 1;
    s.will_delay_ms = 5_000;
    s.will_topic[..9].copy_from_slice(b"dev/7/off");
    s.will_topic_len = 9;
    s.will_payload[..4].copy_from_slice(b"gone");
    s.will_payload_len = 4;
    assert!(s.subscribe(b"sensors/+/temp", 1, 0));
    assert!(s.subscribe(b"alerts/#", 2, 0x04));
    // A QoS 2 PUBLISH from the client, PUBREC already sent.
    assert!(s.add_inflight(SiteInflight {
        packet_id: 77,
        qos: 2,
        phase: SITE_QOS2_PUBREC,
        direction: SITE_DIR_PUBLISH,
        generation: 4,
        wal_index: 900,
        correlation_id: 5,
        partition_id: 2,
        rec_sent: 1,
    }));
    // A QoS 1 delivery to the client, unanswered, and a QoS 2 one whose
    // PUBREL already went out.
    assert!(s.add_owed(40, 1, 0, 4, b"\x30\x05abcde"));
    assert!(s.add_owed(39, 2, 1, 4, b"\x34\x03xyz"));
    s.tail = HandoffTail::from_apply(18, &[120, 0, 64]);
    s
}

fn encode(s: &SiteSession) -> Vec<u8> {
    let mut b = vec![0u8; SITE_RECORD_MAX];
    let n = s.encode(&mut b);
    assert_eq!(n, s.encoded_len());
    b.truncate(n);
    b
}

fn same(want: &SiteSession, got: &SiteSession) {
    assert_eq!(want.session_id, got.session_id);
    assert_eq!(
        (
            want.epoch,
            want.source_site,
            want.export_seq,
            want.tenant,
            want.stream_hash
        ),
        (
            got.epoch,
            got.source_site,
            got.export_seq,
            got.tenant,
            got.stream_hash
        )
    );
    assert_eq!(
        (
            want.generation,
            want.slot,
            want.shard,
            want.protocol,
            want.keep_alive_ms
        ),
        (
            got.generation,
            got.slot,
            got.shard,
            got.protocol,
            got.keep_alive_ms
        )
    );
    assert_eq!(
        (
            want.next_msg_id,
            want.session_expiry_s,
            want.receive_maximum
        ),
        (got.next_msg_id, got.session_expiry_s, got.receive_maximum)
    );
    assert_eq!(
        (
            want.flags,
            want.will_present,
            want.will_qos,
            want.will_retain,
            want.will_delay_ms
        ),
        (
            got.flags,
            got.will_present,
            got.will_qos,
            got.will_retain,
            got.will_delay_ms
        )
    );
    assert_eq!(
        want.will_topic[..usize::from(want.will_topic_len)],
        got.will_topic[..usize::from(got.will_topic_len)]
    );
    assert_eq!(
        want.will_payload[..usize::from(want.will_payload_len)],
        got.will_payload[..usize::from(got.will_payload_len)]
    );
    assert_eq!(want.sub_count, got.sub_count);
    for at in 0..usize::from(want.sub_count) {
        assert_eq!(want.subs[at], got.subs[at]);
    }
    assert_eq!(want.inflight_count, got.inflight_count);
    assert_eq!(want.inflight, got.inflight);
    assert_eq!(want.owed_count, got.owed_count);
    for at in 0..usize::from(want.owed_count) {
        assert_eq!(want.owed[at], got.owed[at]);
    }
    assert_eq!(want.tail, got.tail);
}

// ── The record ──────────────────────────────────────────────────────────────

#[test]
fn a_session_round_trips_whole() {
    let s = session();
    let b = encode(&s);
    let back = SiteSession::decode(&b).expect("decodes");
    same(&s, &back);
    assert_eq!(back.subs[1].filter(), b"alerts/#");
    assert_eq!(back.owed[0].body(), b"\x30\x05abcde");
    assert_eq!(back.tail.delivery_hw, 18, "the dedupe cursor");
    assert_eq!(back.tail.cursor(2), 64, "the apply cursor");
}

#[test]
fn every_flipped_bit_and_every_truncation_is_refused() {
    let b = encode(&session());
    for at in 0..b.len() {
        let mut flipped = b.clone();
        flipped[at] ^= 0x10;
        assert!(SiteSession::decode(&flipped).is_err(), "byte {at}");
    }
    for len in 0..b.len() {
        assert!(SiteSession::decode(&b[..len]).is_err(), "length {len}");
    }
}

#[test]
fn a_record_past_its_bounds_is_refused_at_export_never_truncated() {
    let mut s = session();
    let long = [b'a'; SITE_FILTER_MAX + 1];
    assert!(!s.subscribe(&long, 1, 0));
    assert!(!s.subscribe(b"", 1, 0));
    while s.subscribe(b"x/y", 0, 0) {}
    assert_eq!(usize::from(s.sub_count), SITE_SUBS_MAX);
    let big = [0u8; DELIVER_BODY_MAX + 1];
    assert!(!s.add_owed(1, 1, 0, 4, &big));
    while s.add_inflight(SiteInflight {
        packet_id: 1,
        qos: 1,
        ..SiteInflight::default()
    }) {}
    assert_eq!(usize::from(s.inflight_count), SITE_INFLIGHT_MAX);
    // The fullest record still fits its bound.
    while s.add_owed(2, 1, 0, 4, &[0u8; DELIVER_BODY_MAX]) {}
    let b = encode(&s);
    assert!(b.len() <= SITE_RECORD_MAX);
    assert!(SiteSession::decode(&b).is_ok());
    let mut short = vec![0u8; b.len() - 1];
    assert_eq!(s.encode(&mut short), 0);
}

/// After a resume the client sends its `QoS` 2 PUBLISH again: the record
/// holds the flow, so it is a retry, not a second message — whether or
/// not the PUBREC went out before the site was lost.
#[test]
fn a_recorded_qos2_flow_makes_the_clients_retry_a_duplicate() {
    let mut s = SiteSession::decode(&encode(&session())).expect("decodes");
    assert!(s.publish_is_retry(77));
    assert!(!s.publish_is_retry(78), "a new packet id is new work");
    let i = (0..usize::from(s.inflight_count))
        .find(|&i| s.inflight[i].packet_id == 77)
        .expect("recorded");
    s.inflight[i].phase = 0;
    s.inflight[i].rec_sent = 0;
    assert!(s.publish_is_retry(77), "proposed, PUBREC not yet out");
}

// ── The import rule ─────────────────────────────────────────────────────────

fn importer(apply: &[u64]) -> SiteImporter<'_> {
    SiteImporter {
        apply,
        last_skip: None,
        local_generation: None,
    }
}

#[test]
fn a_site_level_with_the_record_resumes_it_at_the_higher_epoch() {
    let s = session();
    let v = site_admit(&s, 2, &importer(&[120, 0, 64]));
    assert_eq!(v.status, SS_OK);
    assert_eq!(v.generation, 4);
    assert!(!v.floor);
}

#[test]
fn a_resume_at_or_below_the_records_epoch_is_stale() {
    let s = session();
    assert_eq!(site_admit(&s, 1, &importer(&[120])).status, SS_STALE_EPOCH);
    assert_eq!(site_admit(&s, 0, &importer(&[120])).status, SS_STALE_EPOCH);
}

/// The surviving site's replica committed a later CONNECT of the session
/// than the record: the record is behind the log.
#[test]
fn a_record_behind_the_sites_generation_is_stale() {
    let s = session();
    let mut imp = importer(&[120, 0, 64]);
    imp.local_generation = Some(5);
    assert_eq!(site_admit(&s, 2, &imp).status, SS_STALE_EPOCH);
    imp.local_generation = Some(4);
    assert_eq!(site_admit(&s, 2, &imp).status, SS_OK);
}

/// The surviving site's replica has not yet applied the CONNECT behind
/// the record's generation — the usual cross-site lag: it is behind the
/// record, and resumes it with a floor at the record's generation.
#[test]
fn a_site_behind_the_records_generation_resumes_it_with_a_floor() {
    let s = session();
    let mut imp = importer(&[100, 0, 64]);
    imp.local_generation = Some(3);
    let v = site_admit(&s, 2, &imp);
    assert_eq!(v.status, SS_OK);
    assert_eq!(v.generation, 4);
    assert!(v.floor);
}

#[test]
fn a_site_past_an_entry_the_record_lacks_refuses_it() {
    let s = session();
    let mut imp = importer(&[130, 0, 64]);
    imp.last_skip = Some((0, 125));
    assert_eq!(site_admit(&s, 2, &imp).status, SS_CURSOR_MISMATCH);
}

#[test]
fn a_site_behind_the_record_imports_it_with_a_floor() {
    let s = session();
    let v = site_admit(&s, 2, &importer(&[100, 0, 64]));
    assert_eq!(v.status, SS_OK);
    assert!(
        v.floor,
        "entries at or below the record's cursor are skipped"
    );
}

#[test]
fn a_site_keeps_only_the_newest_record_of_a_session() {
    let mut s = session();
    assert!(site_keeps(None, &s));
    assert!(!site_keeps(Some((1, 3)), &s), "the same record again");
    assert!(!site_keeps(Some((1, 4)), &s), "an older export");
    s.export_seq = 4;
    assert!(site_keeps(Some((1, 3)), &s));
    s.epoch = 2;
    s.export_seq = 1;
    assert!(site_keeps(Some((1, 9)), &s), "a newer epoch");
}

/// The worker's subscription book: filled and emptied by the committed
/// subscribe / unsubscribe entries, a re-subscription replacing the `QoS`
/// rather than adding an entry, cleared with the session; a subscription
/// it cannot hold marks the session lost (its export is refused) until
/// the session is cleared.
#[test]
fn the_subscription_book_follows_the_committed_entries() {
    let mut book = Box::new(SiteSubBook::new());
    book.subscribe(3, 1, b"a/b");
    book.subscribe(3, 2, b"c/#");
    book.subscribe(4, 0, b"a/b");
    book.subscribe(3, 0, b"a/b");
    let mine: Vec<(u8, Vec<u8>)> = book
        .of(3)
        .map(|e| (e.qos, e.filter[..usize::from(e.len)].to_vec()))
        .collect();
    assert_eq!(mine, vec![(0, b"a/b".to_vec()), (2, b"c/#".to_vec())]);
    book.unsubscribe(3, b"a/b");
    assert_eq!(book.of(3).count(), 1);
    assert_eq!(book.of(4).count(), 1);
    // A filter past a record's bound: the session is lost, not truncated.
    book.subscribe(3, 1, &[b'x'; SITE_FILTER_MAX + 1]);
    assert!(book.is_lost(3));
    assert!(!book.is_lost(4));
    book.clear(3);
    assert!(!book.is_lost(3));
    assert_eq!(book.of(3).count(), 0);
    // A full book loses the session whose subscription did not fit: slot
    // 4 holds one entry, so slot 9's last of SITE_SUB_BOOK does not fit.
    for k in 0..SITE_SUB_BOOK {
        book.subscribe(9, 0, format!("t/{k}").as_bytes());
    }
    assert_eq!(book.of(9).count(), SITE_SUB_BOOK - 1);
    assert!(book.is_lost(9));
    book.subscribe(7, 0, b"late");
    assert!(book.is_lost(7));
    assert!(!book.is_lost(4));
}

/// Losing many sessions marks exactly those: clearing one makes it
/// exportable again and leaves the others lost.
#[test]
fn lost_sessions_are_tracked_one_by_one() {
    let mut book = Box::new(SiteSubBook::new());
    for slot in 0..64u32 {
        book.lose(slot * 7);
    }
    assert!((0..64u32).all(|slot| book.is_lost(slot * 7)));
    assert!(!book.is_lost(1));
    book.clear(7);
    assert!(!book.is_lost(7));
    assert!(book.is_lost(14));
}

/// A record whose Will is past its bound is refused at export, and one
/// whose flags or Will bytes are out of place is refused at decode.
#[test]
fn a_will_out_of_bounds_or_out_of_place_is_refused() {
    let mut s = session();
    s.will_payload_len = u16::try_from(SITE_WILL_PAYLOAD_MAX + 1).expect("fits");
    let mut b = vec![0u8; SITE_RECORD_MAX];
    assert_eq!(s.encode(&mut b), 0);
    let s = session();
    let good = encode(&s);
    // Flags sit right after the receive maximum; a Will QoS of 3 two
    // bytes later. Both are re-signed so only the field is wrong.
    let flags_at = 4 + 16 + 4 + 4 + 8 + 4 + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 4 + 2;
    for (at, v) in [(flags_at, 0x80u8), (flags_at + 2, 3)] {
        let mut bad = good.clone();
        bad[at] = v;
        let body = bad.len() - 4;
        let crc = ss_crc32(&bad[..body]);
        bad[body..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            SiteSession::decode(&bad).err(),
            Some(SS_CORRUPT),
            "byte {at}"
        );
    }
}

/// A session's book entries travel in its handoff record: the importing
/// worker's book holds them as the exporter's did, and a session the
/// exporter had lost arrives lost.
#[test]
fn the_book_section_carries_a_sessions_subscriptions_across_a_move() {
    let mut a = Box::new(SiteSubBook::new());
    a.subscribe_with(12, 1, 0x04, b"sensors/+/temp");
    a.subscribe(12, 2, b"alerts/#");
    a.subscribe(13, 0, b"other");
    let mut sec = vec![0u8; SITE_BOOK_SECTION_MAX];
    let n = a.export_section(12, &mut sec);
    let mut b = Box::new(SiteSubBook::new());
    b.subscribe(12, 0, b"stale/entry");
    assert!(b.import_section(12, &sec[..n]));
    let got: Vec<(u8, u8, Vec<u8>)> = b
        .of(12)
        .map(|e| (e.qos, e.options, e.filter[..usize::from(e.len)].to_vec()))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, 0x04, b"sensors/+/temp".to_vec()),
            (2, 0, b"alerts/#".to_vec())
        ]
    );
    assert!(!b.is_lost(12));
    // Lost at the exporter, lost at the importer.
    a.lose(13);
    let n = a.export_section(13, &mut sec);
    assert!(b.import_section(13, &sec[..n]));
    assert!(b.is_lost(13));
    // A section that does not parse leaves the session lost.
    assert!(!b.import_section(14, &[0, 1, 9]));
    assert!(b.is_lost(14));
}
