//! A session exported for a surviving site (`QSS1`,
//! `cores/session_site_core.rs`, docs/architecture/anchor_continuity.md,
//! "A session at a surviving site").
//!
//! `MSG_SITE_EXPORT [session_id:16][source_site:u32][export_seq:u64]` on
//! the worker's control channel asks for a session this worker serves:
//! identity and placement, its in-flight ledger, the deliveries the
//! delivery ledger says it is owed, the dedupe and apply cursors, and
//! its subscriptions —
//! which live in `topic_engine`, so the worker keeps its own book of them
//! (`SiteSubBook`, `cores/session_site_core.rs`), filled on the apply side
//! from the same committed
//! `QOP_SUBSCRIBE` / `QOP_UNSUBSCRIBE` entries every replica applies. The
//! record goes out on `site_out` as `MSG_SITE_RECORD`, for the cross-site
//! carrier; a session with no live attachment here, or one past any
//! bound of the record (or whose state is not whole here, `is_lost`), is
//! answered `[session_id][status]` instead — refused whole,
//! never truncated. Every request gets one answer or the other.

use super::abi::SyscallTable;
use super::site_core::{SiteSession, SiteSub, SITE_OWED_MAX, SITE_SUBS_MAX, SS_CORRUPT, SS_OK};
use super::{sessions, wire, ModuleState};

/// Answer a `MSG_SITE_EXPORT` (see the module doc).
///
/// # Safety
/// `sys` is the kernel's live syscall table.
pub unsafe fn export(s: &mut ModuleState, sys: &SyscallTable, p: &[u8]) {
    if p.len() < 16 + 4 + 8 || s.out_site < 0 {
        return;
    }
    let mut sid = [0u8; 16];
    sid.copy_from_slice(&p[..16]);
    let source_site = u32::from_le_bytes([p[16], p[17], p[18], p[19]]);
    let mut q = [0u8; 8];
    q.copy_from_slice(&p[20..28]);
    let export_seq = u64::from_le_bytes(q);
    let mut status = build(s, &sid, source_site, export_seq);
    if status == SS_OK {
        let n = s.site_rec.encode(&mut s.site_buf);
        if n != 0
            && wire::channel_write_msg(sys, s.out_site, wire::MSG_SITE_RECORD, &s.site_buf[..n]) > 0
        {
            s.site_exported = s.site_exported.wrapping_add(1);
            return;
        }
        // A record the encoder refuses, or one the channel has no room
        // for, is answered with a status the requester can act on.
        status = SS_CORRUPT;
    }
    let mut out = [0u8; 17];
    out[..16].copy_from_slice(&sid);
    out[16] = status;
    wire::channel_write_msg(sys, s.out_site, wire::MSG_SITE_RECORD, &out);
    s.site_refused = s.site_refused.wrapping_add(1);
}

/// Assemble the session `sid` into `s.site_rec`.
fn build(s: &mut ModuleState, sid: &[u8; 16], source_site: u32, export_seq: u64) -> u8 {
    let conn = super::worker::sid_conn_of(sid);
    let Some((epoch, hw)) = super::worker::bound(&s.worker, conn, sid) else {
        return super::site_core::SS_UNKNOWN_SESSION;
    };
    let Some(si) = sessions::find_by_conn(&s.sessions, conn) else {
        return super::site_core::SS_UNKNOWN_SESSION;
    };
    s.site_rec = SiteSession::zero();
    let r = &mut s.site_rec;
    r.session_id = *sid;
    r.epoch = epoch;
    r.source_site = source_site;
    r.export_seq = export_seq;
    if !sessions::fill_site(&s.sessions, si, r) {
        return SS_CORRUPT;
    }
    // Subscriptions, from the book.
    if s.sub_book.is_lost(si as u32) {
        return SS_CORRUPT;
    }
    let mut n = 0usize;
    for e in s.sub_book.of(si as u32) {
        if n == SITE_SUBS_MAX {
            return SS_CORRUPT;
        }
        let mut sub = SiteSub::zero();
        sub.qos = e.qos;
        sub.options = e.options;
        sub.len = e.len;
        sub.filter = e.filter;
        r.subs[n] = sub;
        n += 1;
    }
    r.sub_count = n as u8;
    // Owed deliveries, from the ledger, oldest first.
    let mut idx = [0usize; SITE_OWED_MAX + 1];
    let k = s.ledger.owed(r.tenant, r.stream_hash, &mut idx);
    if k > SITE_OWED_MAX {
        return SS_CORRUPT;
    }
    for (j, &i) in idx[..k].iter().enumerate() {
        let e = &s.ledger.entries[i];
        let o = &mut r.owed[j];
        o.packet_id = e.packet_id;
        o.qos = e.qos;
        o.released = e.released;
        o.generation = e.generation;
        let b = e.body();
        o.len = b.len() as u16;
        o.body[..b.len()].copy_from_slice(b);
    }
    r.owed_count = k as u8;
    r.tail = super::site_core::HandoffTail::from_apply(hw, &s.apply_index);
    SS_OK
}
