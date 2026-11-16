//! MULTI / EXEC / DISCARD / WATCH / UNWATCH.

use crate::exec::{CTX_MULTI, Ctx};
use crate::resp::Frame;
use crate::server::{Store, cflags};
use bytes::Bytes;

pub fn multi(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if c.has(cflags::MULTI) {
        return Frame::err("ERR MULTI calls can not be nested");
    }
    c.flags |= cflags::MULTI;
    Frame::ok()
}

pub fn discard_transaction(s: &mut Store, cid: u64) {
    if let Some(c) = s.clients.get_mut(&cid) {
        c.mstate.clear();
        c.multi_mem = 0;
        c.flags &= !(cflags::MULTI | cflags::DIRTY_CAS | cflags::DIRTY_EXEC);
    }
    unwatch_all(s, cid);
}

pub fn discard(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    if !ctx.client().has(cflags::MULTI) {
        return Frame::err("ERR DISCARD without MULTI");
    }
    discard_transaction(ctx.s, ctx.cid);
    Frame::ok()
}

pub fn exec(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let cid = ctx.cid;
    let c = ctx.client();
    if !c.has(cflags::MULTI) {
        return Frame::err("ERR EXEC without MULTI");
    }
    if c.has(cflags::DIRTY_EXEC) {
        discard_transaction(ctx.s, cid);
        return Frame::err("EXECABORT Transaction discarded because of previous errors.");
    }
    if c.has(cflags::DIRTY_CAS) {
        discard_transaction(ctx.s, cid);
        return Frame::NullArray;
    }
    let queue = std::mem::take(&mut c.mstate);
    unwatch_all(ctx.s, cid);
    // Replica-read-only may have changed while queuing (e.g. became replica).
    let is_replica = ctx.s.repl.is_replica() && ctx.s.cfg.replica_read_only;
    let from_master = ctx.client_ref().map(|c| c.has(cflags::MASTER)).unwrap_or(false);
    if is_replica && !from_master {
        let has_write = queue.iter().any(|a| crate::exec::resolve(a).map(|c| c.is_write_for(a)).unwrap_or(false));
        if has_write {
            discard_transaction(ctx.s, cid);
            return Frame::err("EXECABORT Transaction discarded because of: READONLY You can't write against a read only replica.");
        }
    }
    let mut replies = Vec::with_capacity(queue.len());
    for args in queue {
        let db = ctx.s.clients.get(&cid).map(|c| c.db).unwrap_or(0);
        let r = match crate::exec::resolve(&args) {
            Ok(cmd) => {
                if let Err(e) = crate::acl::check_command(ctx.s, cid, cmd, &args) {
                    e
                } else {
                    if let Some(c) = ctx.s.clients.get_mut(&cid) {
                        c.last_cmd = cmd.full_name();
                    }
                    crate::exec::call(ctx.s, cid, db, cmd, &args, CTX_MULTI)
                }
            }
            Err(e) => e,
        };
        let r = match r {
            Frame::Block(spec) => spec.timeout_reply,
            r => r,
        };
        replies.push(r);
    }
    if let Some(c) = ctx.s.clients.get_mut(&cid) {
        c.flags &= !(cflags::MULTI | cflags::DIRTY_CAS | cflags::DIRTY_EXEC);
        c.multi_mem = 0;
        ctx.db = c.db;
    }
    // EXEC itself is not propagated; inner commands are (wrapped by flush).
    ctx.no_prop = true;
    Frame::Array(replies)
}

pub fn watch(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.client().has(cflags::MULTI) {
        return Frame::err("ERR WATCH inside MULTI is not allowed");
    }
    let db = ctx.db;
    let cid = ctx.cid;
    for k in &args[1..] {
        let entry = (db, k.clone());
        let c = ctx.s.clients.get_mut(&cid).unwrap();
        if c.watched.contains(&entry) {
            continue;
        }
        c.watched.push(entry.clone());
        ctx.s.watched.entry(entry).or_default().push(cid);
        // A key that is already logically expired counts as modified when it gets deleted.
    }
    Frame::ok()
}

pub fn unwatch_all(s: &mut Store, cid: u64) {
    let Some(c) = s.clients.get_mut(&cid) else { return };
    let keys = std::mem::take(&mut c.watched);
    c.flags &= !cflags::DIRTY_CAS;
    for k in keys {
        if let Some(v) = s.watched.get_mut(&k) {
            v.retain(|&id| id != cid);
            if v.is_empty() {
                s.watched.remove(&k);
            }
        }
    }
}

pub fn unwatch(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    unwatch_all(ctx.s, ctx.cid);
    Frame::ok()
}
