//! `messaging`'s offline queue (`offline_queue_core.rs`).
//!
//! The failures prevented: a delivery larger than a fixed entry refused
//! (any delivery a worker can make must queue); a session's envelopes
//! replayed out of order, or another session's interleaved; chunks leaked
//! or shared so the arena fills or an envelope comes back corrupted; a
//! released or expired entry left linked in its session's list.

use super::offline_queue::*;

fn queue() -> Box<OfflineQueue> {
    let mut q = Box::new(OfflineQueue::new());
    q.init();
    q
}

/// Run `f` on a thread whose stack holds a queue being built.
fn big_stack(f: fn()) {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(f)
        .expect("spawn")
        .join()
        .expect("test thread");
}

fn env(tag: u32, len: usize) -> Vec<u8> {
    let mut v: Vec<u8> = (0..len)
        .map(|i| u8::try_from((i * 7 + tag as usize) % 256).expect("a byte"))
        .collect();
    v[..4].copy_from_slice(&tag.to_le_bytes());
    v
}

fn drain(q: &mut OfflineQueue, session: u32) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; OFFLINE_ENVELOPE_MAX];
    while let Some(n) = q.front(session, &mut buf) {
        out.push(buf[..n].to_vec());
        assert!(q.pop_front(session));
    }
    out
}

#[test]
fn every_size_a_delivery_can_have_queues_and_comes_back_whole() {
    big_stack(every_size_a_delivery_can_have_queues_and_comes_back_whole_body);
}

fn every_size_a_delivery_can_have_queues_and_comes_back_whole_body() {
    let mut q = queue();
    let sizes = [
        1,
        OFFLINE_CHUNK - 1,
        OFFLINE_CHUNK,
        OFFLINE_CHUNK + 1,
        1050,
        OFFLINE_ENVELOPE_MAX,
    ];
    for (k, &len) in sizes.iter().enumerate() {
        let e = env(u32::try_from(k).expect("small"), len.max(4));
        assert_eq!(q.push(3, &e, u64::MAX), Ok(()), "{len} bytes");
    }
    let got = drain(&mut q, 3);
    let want: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(k, &len)| env(u32::try_from(k).expect("small"), len.max(4)))
        .collect();
    assert_eq!(got, want);
    assert_eq!(q.live(), 0);
    assert_eq!(
        q.push(3, &vec![0; OFFLINE_ENVELOPE_MAX + 1], 0),
        Err(Refused::Size)
    );
    assert_eq!(q.push(3, &[], 0), Err(Refused::Size));
    assert_eq!(
        q.push(u32::try_from(OFFLINE_SESSIONS).expect("fits"), &[1], 0),
        Err(Refused::Slot)
    );
}

#[test]
fn each_session_gets_its_own_in_order_with_others_interleaved() {
    big_stack(each_session_gets_its_own_in_order_with_others_interleaved_body);
}

fn each_session_gets_its_own_in_order_with_others_interleaved_body() {
    let mut q = queue();
    for n in 0..30u32 {
        let session = n % 3;
        assert!(q.push(session, &env(n, 300), u64::MAX).is_ok());
    }
    assert_eq!((q.queued(0), q.queued(1), q.queued(2)), (10, 10, 10));
    let one: Vec<u32> = drain(&mut q, 1)
        .iter()
        .map(|e| u32::from_le_bytes([e[0], e[1], e[2], e[3]]))
        .collect();
    assert_eq!(one, (0..30).filter(|n| n % 3 == 1).collect::<Vec<_>>());
    assert_eq!(q.queued(1), 0);
    assert_eq!(q.release(0), 10);
    assert_eq!(q.queued(0), 0);
    assert_eq!(drain(&mut q, 2).len(), 10);
    assert_eq!(q.live(), 0);
}

#[test]
fn the_arena_is_reused_without_leaking_or_sharing_chunks() {
    big_stack(the_arena_is_reused_without_leaking_or_sharing_chunks_body);
}

fn the_arena_is_reused_without_leaking_or_sharing_chunks_body() {
    let mut q = queue();
    // Fill the arena with large envelopes, many times over.
    let per = OFFLINE_ENVELOPE_MAX.div_ceil(OFFLINE_CHUNK);
    let fit = OFFLINE_CHUNKS / per;
    for round in 0..3u32 {
        for n in 0..u32::try_from(fit).expect("fits") {
            let e = env(round * 10_000 + n, OFFLINE_ENVELOPE_MAX);
            assert!(
                q.push(n % 7, &e, u64::MAX).is_ok(),
                "round {round} item {n}"
            );
        }
        assert_eq!(
            q.push(9, &env(1, OFFLINE_ENVELOPE_MAX), u64::MAX),
            Err(Refused::Full),
            "no chunks left"
        );
        assert!(
            q.push(9, &env(1, 4), u64::MAX).is_ok(),
            "a small one still fits"
        );
        for s in 0..7 {
            for e in drain(&mut q, s) {
                let tag = u32::from_le_bytes([e[0], e[1], e[2], e[3]]);
                assert_eq!(e, env(tag, OFFLINE_ENVELOPE_MAX), "uncorrupted");
            }
        }
        q.release(9);
        assert_eq!(q.live(), 0);
    }
}

#[test]
fn entries_run_out_before_chunks_with_small_envelopes() {
    big_stack(entries_run_out_before_chunks_with_small_envelopes_body);
}

fn entries_run_out_before_chunks_with_small_envelopes_body() {
    let mut q = queue();
    for n in 0..u32::try_from(OFFLINE_ENTRIES).expect("fits") {
        assert!(q.push(n % 100, &env(n, 4), u64::MAX).is_ok());
    }
    assert_eq!(q.push(1, &env(0, 4), u64::MAX), Err(Refused::Full));
    assert!(q.pop_front(5));
    assert!(
        q.push(1, &env(0, 4), u64::MAX).is_ok(),
        "a freed entry is reused"
    );
}

#[test]
fn expiry_unlinks_from_anywhere_in_a_sessions_list() {
    big_stack(expiry_unlinks_from_anywhere_in_a_sessions_list_body);
}

fn expiry_unlinks_from_anywhere_in_a_sessions_list_body() {
    let mut q = queue();
    for n in 0..6u32 {
        // Odd ones expire first.
        let expiry = if n % 2 == 1 { 100 } else { 1_000 };
        assert!(q.push(4, &env(n, 600), expiry).is_ok());
    }
    assert_eq!(q.earliest(), Some(1));
    assert_eq!(q.expire(100), 3);
    assert_eq!(q.queued(4), 3);
    let left: Vec<u32> = drain(&mut q, 4)
        .iter()
        .map(|e| u32::from_le_bytes([e[0], e[1], e[2], e[3]]))
        .collect();
    assert_eq!(left, vec![0, 2, 4]);
    assert_eq!(q.earliest(), None);
}

#[test]
fn a_session_continues_in_order_after_its_head_is_taken_and_more_arrive() {
    big_stack(a_session_continues_in_order_after_its_head_is_taken_and_more_arrive_body);
}

fn a_session_continues_in_order_after_its_head_is_taken_and_more_arrive_body() {
    let mut q = queue();
    for n in 0..3u32 {
        assert!(q.push(8, &env(n, 10), u64::MAX).is_ok());
    }
    assert!(q.pop_front(8));
    for n in 3..5u32 {
        assert!(q.push(8, &env(n, 10), u64::MAX).is_ok());
    }
    let order: Vec<u32> = drain(&mut q, 8)
        .iter()
        .map(|e| u32::from_le_bytes([e[0], e[1], e[2], e[3]]))
        .collect();
    assert_eq!(order, vec![1, 2, 3, 4]);
    let mut small = [0u8; 4];
    assert!(q.push(8, &env(9, 10), u64::MAX).is_ok());
    assert_eq!(
        q.front(8, &mut small),
        None,
        "an out buffer too small reads nothing"
    );
    assert_eq!(q.queued(8), 1);
}
