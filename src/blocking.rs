//! Blocking operations (BLPOP & co, XREAD BLOCK, WAIT, CLIENT PAUSE): clients are
//! parked in FIFO queues per key and served by re-executing their command when the
//! key is signalled as ready, exactly like Redis' handleClientsBlockedOnKeys.

use crate::resp::Frame;
use crate::server::{Blocked, Store, cflags};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub enum BlockKind {
    Keys,
    /// WAIT numreplicas: wait until replicas acked `offset`.
    Wait { offset: i64, numreplicas: usize },
    /// WAITAOF numlocal numreplicas
    WaitAof { offset: i64, numlocal: usize, numreplicas: usize },
    /// Postponed by CLIENT PAUSE.
    Pause,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockSpec {
    pub keys: Vec<Bytes>,
    /// Absolute unix ms deadline, None = forever.
    pub deadline: Option<i64>,
    pub timeout_reply: Frame,
    pub kind: BlockKind,
    /// Arguments to store for re-execution instead of the original ones
    /// (e.g. XREAD with `$` resolved to a concrete ID).
    pub rewrite: Option<Vec<Bytes>>,
}

impl BlockSpec {
    pub fn keys(keys: Vec<Bytes>, deadline: Option<i64>, timeout_reply: Frame) -> Frame {
        Frame::Block(Box::new(BlockSpec { keys, deadline, timeout_reply, kind: BlockKind::Keys, rewrite: None }))
    }
}

#[derive(Default)]
pub struct Blocking {
    pub waiting: HashMap<(usize, Bytes), VecDeque<u64>>,
    pub ready: Vec<(usize, Bytes)>,
    pub ready_set: HashSet<(usize, Bytes)>,
    pub paused: VecDeque<u64>,
    pub wait_clients: Vec<u64>,
    pub blocked_clients: usize,
    pub blocked_on_keys: usize,
    pub blocked_on_nokey: usize,
}

/// Parse a blocking timeout (seconds, may be fractional) into an absolute deadline.
pub fn parse_timeout(arg: &[u8], now: i64) -> Result<Option<i64>, Frame> {
    let v = crate::util::parse_f64(arg)
        .filter(|v| v.is_finite())
        .ok_or_else(|| Frame::err("ERR timeout is not a float or out of range"))?;
    if v < 0.0 {
        return Err(Frame::err("ERR timeout is negative"));
    }
    let ms = (v * 1000.0).round();
    if ms > (i64::MAX / 2) as f64 {
        return Err(Frame::err("ERR timeout is out of range"));
    }
    let ms = ms as i64;
    if ms == 0 { Ok(None) } else { Ok(Some(now + ms)) }
}

pub fn parse_timeout_ms(arg: &[u8], now: i64) -> Result<Option<i64>, Frame> {
    let v = crate::util::parse_i64(arg).ok_or_else(|| Frame::err("ERR timeout is not an integer or out of range"))?;
    if v < 0 {
        return Err(Frame::err("ERR timeout is negative"));
    }
    if v > i64::MAX / 2 {
        return Err(Frame::err("ERR timeout is out of range"));
    }
    if v == 0 { Ok(None) } else { Ok(Some(now + v)) }
}

impl Store {
    pub fn signal_ready(&mut self, db: usize, key: &[u8]) {
        if self.blocking.waiting.is_empty() {
            return;
        }
        let k = (db, Bytes::copy_from_slice(key));
        if !self.blocking.waiting.contains_key(&k) {
            return;
        }
        if self.blocking.ready_set.insert(k.clone()) {
            self.blocking.ready.push(k);
        }
    }

    pub fn signal_deleted_stream(&mut self, db: usize, key: &[u8]) {
        self.signal_ready(db, key);
    }

    /// Register the client as blocked. Returns the receiver for the eventual reply.
    pub fn block_client(
        &mut self,
        cid: u64,
        args: Vec<Bytes>,
        db: usize,
        spec: BlockSpec,
    ) -> tokio::sync::oneshot::Receiver<Frame> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        for k in &spec.keys {
            self.blocking.waiting.entry((db, k.clone())).or_default().push_back(cid);
        }
        match spec.kind {
            BlockKind::Keys => self.blocking.blocked_on_keys += 1,
            BlockKind::Pause => self.blocking.paused.push_back(cid),
            BlockKind::Wait { .. } | BlockKind::WaitAof { .. } => {
                self.blocking.wait_clients.push(cid);
                self.blocking.blocked_on_nokey += 1;
            }
        }
        self.blocking.blocked_clients += 1;
        let now = crate::util::now_ms();
        if let Some(c) = self.clients.get_mut(&cid) {
            c.flags |= cflags::BLOCKED;
            c.blocked = Some(Blocked {
                args,
                db,
                keys: spec.keys,
                tx: Some(tx),
                timeout_reply: spec.timeout_reply,
                deadline: spec.deadline,
                kind: spec.kind,
                since: now,
            });
        }
        rx
    }

    /// Remove the client from blocking structures and deliver `reply`.
    pub fn unblock_client(&mut self, cid: u64, reply: Option<Frame>) -> bool {
        let Some(c) = self.clients.get_mut(&cid) else { return false };
        let Some(mut b) = c.blocked.take() else { return false };
        c.flags &= !cflags::BLOCKED;
        for k in &b.keys {
            let kk = (b.db, k.clone());
            if let Some(q) = self.blocking.waiting.get_mut(&kk) {
                q.retain(|&id| id != cid);
                if q.is_empty() {
                    self.blocking.waiting.remove(&kk);
                }
            }
        }
        match b.kind {
            BlockKind::Keys => self.blocking.blocked_on_keys -= 1,
            BlockKind::Pause => self.blocking.paused.retain(|&id| id != cid),
            BlockKind::Wait { .. } | BlockKind::WaitAof { .. } => {
                self.blocking.wait_clients.retain(|&id| id != cid);
                self.blocking.blocked_on_nokey -= 1;
            }
        }
        self.blocking.blocked_clients -= 1;
        let reply = reply.unwrap_or_else(|| b.timeout_reply.clone());
        if let Some(tx) = b.tx.take() {
            let _ = tx.send(reply);
        }
        true
    }

    /// Serve clients blocked on keys that became ready.
    pub fn handle_ready_keys(&mut self) {
        let mut rounds = 0;
        while !self.blocking.ready.is_empty() {
            rounds += 1;
            if rounds > 10_000 {
                break;
            }
            let ready = std::mem::take(&mut self.blocking.ready);
            self.blocking.ready_set.clear();
            for (db, key) in ready {
                let ids: Vec<u64> = match self.blocking.waiting.get(&(db, key.clone())) {
                    Some(q) => q.iter().copied().collect(),
                    None => continue,
                };
                for id in ids {
                    let Some(c) = self.clients.get(&id) else { continue };
                    let Some(b) = c.blocked.as_ref() else { continue };
                    if b.kind != BlockKind::Keys {
                        continue;
                    }
                    let args = b.args.clone();
                    let bdb = b.db;
                    let reply = crate::exec::reexecute_blocked(self, id, bdb, &args);
                    match reply {
                        Frame::Block(_) => continue,
                        r => {
                            self.unblock_client(id, Some(r));
                        }
                    }
                }
            }
        }
    }
}

/// Unblock clients whose deadline passed (safety net; connections also time out locally).
pub fn expire_blocked(s: &mut Store, now: i64) {
    if s.blocking.blocked_clients == 0 {
        return;
    }
    let expired: Vec<u64> = s
        .clients
        .values()
        .filter(|c| c.blocked.as_ref().and_then(|b| b.deadline).is_some_and(|d| d <= now))
        .map(|c| c.id)
        .collect();
    for id in expired {
        let wait_reply = crate::repl::wait_timeout_reply(s, id);
        s.unblock_client(id, wait_reply);
    }
}

/// Re-run commands postponed by CLIENT PAUSE.
pub fn unpause(s: &mut Store) {
    let ids: Vec<u64> = s.blocking.paused.drain(..).collect();
    for id in ids {
        let Some(c) = s.clients.get_mut(&id) else { continue };
        let Some(mut b) = c.blocked.take() else { continue };
        c.flags &= !cflags::BLOCKED;
        s.blocking.blocked_clients -= 1;
        let Some(tx) = b.tx.take() else { continue };
        match crate::exec::process(s, id, b.args) {
            crate::exec::Outcome::Block(rx) => {
                tokio::spawn(async move {
                    if let Ok(f) = rx.await {
                        let _ = tx.send(f);
                    }
                });
            }
            crate::exec::Outcome::Reply(f) | crate::exec::Outcome::Close(f) => {
                let _ = tx.send(f);
            }
        }
    }
    s.handle_ready_keys();
}
