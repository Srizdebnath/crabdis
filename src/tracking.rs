//! Client-side caching (CLIENT TRACKING): default mode key table, BCAST prefixes,
//! OPTIN/OPTOUT, NOLOOP, REDIRECT, RESP2 `__redis__:invalidate` and RESP3 pushes.

use crate::cmd::Cmd;
use crate::resp::Frame;
use crate::server::{Store, cflags};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Default)]
pub struct Tracking {
    pub table: HashMap<Bytes, HashSet<u64>>,
    pub prefixes: BTreeMap<Bytes, HashSet<u64>>,
    pub clients: usize,
    pub total_items: usize,
}

impl Tracking {
    pub fn enabled(&self) -> bool {
        self.clients > 0
    }
}

pub fn enable(s: &mut Store, cid: u64, redirect: u64, bcast: bool, optin: bool, optout: bool, noloop: bool, prefixes: Vec<Bytes>) {
    let Some(c) = s.clients.get_mut(&cid) else { return };
    if !c.has(cflags::TRACKING) {
        s.tracking.clients += 1;
    }
    c.flags |= cflags::TRACKING;
    c.flags &= !(cflags::TRACKING_BCAST | cflags::TRACKING_OPTIN | cflags::TRACKING_OPTOUT | cflags::TRACKING_NOLOOP | cflags::TRACKING_BROKEN_REDIR);
    if bcast {
        c.flags |= cflags::TRACKING_BCAST;
    }
    if optin {
        c.flags |= cflags::TRACKING_OPTIN;
    }
    if optout {
        c.flags |= cflags::TRACKING_OPTOUT;
    }
    if noloop {
        c.flags |= cflags::TRACKING_NOLOOP;
    }
    c.tracking_redirect = redirect;
    if bcast {
        let prefixes = if prefixes.is_empty() { vec![Bytes::new()] } else { prefixes };
        for p in prefixes {
            if !c.tracking_prefixes.contains(&p) {
                c.tracking_prefixes.push(p.clone());
            }
            s.tracking.prefixes.entry(p).or_default().insert(cid);
        }
    }
}

pub fn disable(s: &mut Store, cid: u64) {
    let Some(c) = s.clients.get_mut(&cid) else { return };
    if !c.has(cflags::TRACKING) {
        return;
    }
    c.flags &= !(cflags::TRACKING
        | cflags::TRACKING_BCAST
        | cflags::TRACKING_OPTIN
        | cflags::TRACKING_OPTOUT
        | cflags::TRACKING_NOLOOP
        | cflags::TRACKING_CACHING
        | cflags::TRACKING_BROKEN_REDIR);
    let prefixes = std::mem::take(&mut c.tracking_prefixes);
    c.tracking_redirect = 0;
    for p in prefixes {
        if let Some(set) = s.tracking.prefixes.get_mut(&p) {
            set.remove(&cid);
            if set.is_empty() {
                s.tracking.prefixes.remove(&p);
            }
        }
    }
    s.tracking.clients -= 1;
    if s.tracking.clients == 0 {
        s.tracking.table.clear();
        s.tracking.total_items = 0;
    }
}

pub fn remember_keys(s: &mut Store, cid: u64, cmd: &'static Cmd, args: &[Bytes]) {
    let Some(c) = s.clients.get_mut(&cid) else { return };
    if !c.has(cflags::TRACKING) || c.has(cflags::TRACKING_BCAST) {
        return;
    }
    let caching = c.has(cflags::TRACKING_CACHING);
    if c.has(cflags::TRACKING_OPTIN) && !caching {
        return;
    }
    if c.has(cflags::TRACKING_OPTOUT) && caching {
        return;
    }
    // CACHING applies only to the next command (outside MULTI it's reset each command).
    if !c.has(cflags::MULTI) {
        c.flags &= !cflags::TRACKING_CACHING;
    }
    let idx = cmd.key_indexes(args);
    for i in idx {
        let k = args[i].clone();
        let set = s.tracking.table.entry(k).or_default();
        if set.insert(cid) {
            s.tracking.total_items += 1;
        }
    }
    // Enforce tracking-table-max-keys by evicting random keys (sending invalidations).
    let max = s.cfg.tracking_table_max_keys;
    if max > 0 {
        while s.tracking.table.len() > max {
            let k = s.tracking.table.keys().next().cloned().unwrap();
            send_invalidation_for(s, &k, true);
        }
    }
}

fn invalidation_frame(proto: u8, key: Option<&Bytes>) -> Frame {
    let payload = match key {
        Some(k) => Frame::Array(vec![Frame::Bulk(k.clone())]),
        None => Frame::NullArray,
    };
    if proto >= 3 {
        Frame::Push(vec![Frame::bulk_str("invalidate"), payload])
    } else {
        Frame::Push(vec![Frame::bulk_str("message"), Frame::bulk_str("__redis__:invalidate"), payload])
    }
}

fn send_to(s: &mut Store, cid: u64, key: Option<&Bytes>) {
    let Some(c) = s.clients.get(&cid) else { return };
    let target = if c.tracking_redirect != 0 { c.tracking_redirect } else { cid };
    if target != cid {
        match s.clients.get(&target) {
            Some(t) => {
                if t.proto < 3 && !t.channels.contains(b"__redis__:invalidate".as_slice()) {
                    return;
                }
                t.push(invalidation_frame(t.proto, key));
            }
            None => {
                // Redirect client gone: notify with tracking-redir-broken once.
                let c = s.clients.get_mut(&cid).unwrap();
                if !c.has(cflags::TRACKING_BROKEN_REDIR) {
                    c.flags |= cflags::TRACKING_BROKEN_REDIR;
                    if c.proto >= 3 {
                        c.push(Frame::Push(vec![Frame::bulk_str("tracking-redir-broken"), Frame::Integer(target as i64)]));
                    }
                }
            }
        }
        return;
    }
    if c.proto < 3 {
        // RESP2 without redirect can't receive invalidations.
        return;
    }
    c.push(invalidation_frame(c.proto, key));
}

fn send_invalidation_for(s: &mut Store, key: &Bytes, ignore_noloop: bool) {
    if let Some(ids) = s.tracking.table.remove(key) {
        s.tracking.total_items = s.tracking.total_items.saturating_sub(ids.len());
        let cur = s.current_client;
        for id in ids {
            let noloop = s.clients.get(&id).map(|c| c.has(cflags::TRACKING_NOLOOP)).unwrap_or(false);
            if !ignore_noloop && noloop && id == cur {
                continue;
            }
            // Only clients still in non-BCAST tracking mode.
            if s.clients.get(&id).map(|c| c.has(cflags::TRACKING) && !c.has(cflags::TRACKING_BCAST)).unwrap_or(false) {
                send_to(s, id, Some(key));
            }
        }
    }
}

pub fn invalidate_key(s: &mut Store, key: &Bytes) {
    send_invalidation_for(s, key, false);
    if !s.tracking.prefixes.is_empty() {
        let cur = s.current_client;
        let mut targets = Vec::new();
        for (p, ids) in s.tracking.prefixes.iter() {
            if key.starts_with(p) {
                for &id in ids {
                    targets.push(id);
                }
            }
        }
        targets.sort_unstable();
        targets.dedup();
        for id in targets {
            let noloop = s.clients.get(&id).map(|c| c.has(cflags::TRACKING_NOLOOP)).unwrap_or(false);
            if noloop && id == cur {
                continue;
            }
            send_to(s, id, Some(key));
        }
    }
}

pub fn invalidate_all(s: &mut Store) {
    let ids: Vec<u64> = s.clients.values().filter(|c| c.has(cflags::TRACKING)).map(|c| c.id).collect();
    for id in ids {
        send_to(s, id, None);
    }
    s.tracking.table.clear();
    s.tracking.total_items = 0;
}
