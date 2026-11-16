//! Databases and keyspace access helpers (lazy expiry, notifications, WATCH signals).

use crate::dict::Dict;
use crate::server::Store;
use crate::value::{RObj, Value};
use bytes::Bytes;

pub struct Db {
    pub id: usize,
    pub dict: Dict<RObj>,
    pub expires: Dict<i64>,
    pub avg_ttl: i64,
    pub expires_cursor: u64,
    /// Keys containing hashes with field-level TTLs.
    pub hexpires: Dict<()>,
}

impl Db {
    pub fn new(id: usize) -> Self {
        Db { id, dict: Dict::new(), expires: Dict::new(), avg_ttl: 0, expires_cursor: 0, hexpires: Dict::new() }
    }
    pub fn len(&self) -> usize {
        self.dict.len()
    }
    pub fn expire_of(&self, k: &[u8]) -> Option<i64> {
        if self.expires.is_empty() {
            return None;
        }
        self.expires.get(k).copied()
    }
    pub fn clear(&mut self) {
        self.dict.clear();
        self.expires.clear();
        self.hexpires.clear();
        self.avg_ttl = 0;
    }
}

/// Lookup flags.
pub const LOOKUP_NONE: u32 = 0;
pub const LOOKUP_NOTOUCH: u32 = 1;
pub const LOOKUP_NONOTIFY: u32 = 2;
pub const LOOKUP_NOSTATS: u32 = 4;
pub const LOOKUP_WRITE: u32 = 8;
pub const LOOKUP_NOEXPIRE: u32 = 16;

pub type TypeResult<T> = Result<T, crate::resp::Frame>;

pub fn wrongtype() -> crate::resp::Frame {
    crate::resp::Frame::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

impl Store {
    pub fn now_ms(&self) -> i64 {
        // Within a command / script, Redis uses a cached time for expiry decisions.
        if self.cmd_time_snapshot > 0 { self.cmd_time_snapshot } else { crate::util::now_ms() }
    }

    /// Returns true if the key is (logically) expired. Deletes it if allowed.
    pub fn expire_if_needed(&mut self, db: usize, key: &[u8], force_delete: bool) -> bool {
        let Some(when) = self.dbs[db].expire_of(key) else { return false };
        if self.persist.loading {
            return false;
        }
        let now = self.now_ms();
        if when > now {
            return false;
        }
        // Replicas don't actively delete keys; they wait for the master's DEL,
        // but report the key as missing.
        if self.repl.is_replica() && !force_delete {
            return true;
        }
        if self.pause_blocks_expire() {
            return true;
        }
        self.delete_expired_key(db, key);
        true
    }

    pub fn pause_blocks_expire(&self) -> bool {
        self.pause.is_some()
    }

    pub fn delete_expired_key(&mut self, db: usize, key: &[u8]) {
        let k = Bytes::copy_from_slice(key);
        self.db_delete_raw(db, key);
        self.stats.expired_keys += 1;
        self.notify(crate::config::NOTIFY_EXPIRED, "expired", &k, db);
        self.signal_modified(db, &k);
        self.propagate_deletion(db, k);
    }

    pub fn propagate_deletion(&mut self, db: usize, key: Bytes) {
        let lazy = self.cfg.get("lazyfree-lazy-expire") == Some("yes");
        let cmd = if lazy { "UNLINK" } else { "DEL" };
        self.propagate_now(db, vec![Bytes::from_static(cmd.as_bytes()), key]);
    }

    /// Read lookup. Returns None if missing/expired.
    pub fn lookup(&mut self, db: usize, key: &[u8], flags: u32) -> Option<&mut RObj> {
        let expired = flags & LOOKUP_NOEXPIRE == 0 && self.expire_if_needed(db, key, false);
        let present = !expired && self.dbs[db].dict.contains_key(key);
        if flags & LOOKUP_NOSTATS == 0 && flags & LOOKUP_WRITE == 0 {
            if present {
                self.stats.keyspace_hits += 1;
            } else {
                self.stats.keyspace_misses += 1;
                if flags & LOOKUP_NONOTIFY == 0 {
                    let k = Bytes::copy_from_slice(key);
                    self.notify(crate::config::NOTIFY_KEY_MISS, "keymiss", &k, db);
                }
            }
        }
        if !present {
            return None;
        }
        let no_touch = flags & LOOKUP_NOTOUCH != 0 || self.current_client_no_touch();
        let lfu = self.cfg.maxmemory_policy.contains("lfu");
        let (lf, dt) = (self.cfg.lfu_log_factor, self.cfg.lfu_decay_time);
        let o = self.dbs[db].dict.get_mut(key)?;
        if !no_touch {
            if lfu {
                o.lru = crate::evict::lfu_touch(o.lru, lf, dt);
            } else {
                o.lru = crate::server::lru_clock();
            }
        }
        Some(o)
    }

    fn current_client_no_touch(&self) -> bool {
        self.clients
            .get(&self.current_client)
            .map(|c| c.has(crate::server::cflags::NO_TOUCH))
            .unwrap_or(false)
    }

    pub fn lookup_read(&mut self, db: usize, key: &[u8]) -> Option<&mut RObj> {
        self.lookup(db, key, LOOKUP_NONE)
    }

    pub fn lookup_write(&mut self, db: usize, key: &[u8]) -> Option<&mut RObj> {
        self.lookup(db, key, LOOKUP_WRITE)
    }

    pub fn exists(&mut self, db: usize, key: &[u8]) -> bool {
        self.lookup(db, key, LOOKUP_NOTOUCH | LOOKUP_NOSTATS | LOOKUP_WRITE).is_some()
    }

    /// Add a new key (must not exist).
    pub fn db_add(&mut self, db: usize, key: Bytes, v: Value) {
        let is_list_like = matches!(v, Value::List(_) | Value::ZSet(_) | Value::Stream(_));
        self.dbs[db].dict.insert(key.clone(), RObj::new(v));
        if is_list_like {
            self.signal_ready(db, &key);
        }
        self.notify(crate::config::NOTIFY_NEW, "new", &key, db);
    }

    /// High level set: overwrite value, optionally keeping TTL. Signals modification.
    pub fn set_key(&mut self, db: usize, key: Bytes, v: Value, keep_ttl: bool, signal: bool) {
        let existed = self.dbs[db].dict.contains_key(&key) && !self.expire_if_needed(db, &key, true);
        if existed {
            let was_hash_ttl = self.dbs[db].hexpires.contains_key(&key);
            let is_list_like = matches!(v, Value::List(_) | Value::ZSet(_) | Value::Stream(_));
            let o = self.dbs[db].dict.get_mut(&key).unwrap();
            o.v = v;
            if was_hash_ttl {
                self.dbs[db].hexpires.remove(&key);
            }
            if is_list_like {
                self.signal_ready(db, &key);
            }
        } else {
            self.db_add(db, key.clone(), v);
        }
        if !keep_ttl {
            self.dbs[db].expires.remove(&key);
        }
        if signal {
            self.signal_modified(db, &key);
        }
    }

    /// Delete key without notifications. Returns true if existed.
    pub fn db_delete_raw(&mut self, db: usize, key: &[u8]) -> bool {
        let d = &mut self.dbs[db];
        if !d.expires.is_empty() {
            d.expires.remove(key);
        }
        if !d.hexpires.is_empty() {
            d.hexpires.remove(key);
        }
        if let Some((k, o)) = d.dict.remove(key) {
            if matches!(o.v, Value::Stream(_)) || matches!(o.v, Value::List(_) | Value::ZSet(_)) {
                // Wake clients blocked on this key so they can re-evaluate (e.g. XREADGROUP errors).
                if matches!(o.v, Value::Stream(_)) {
                    self.signal_deleted_stream(db, &k);
                }
            }
            crate::evict::lazyfree(o);
            true
        } else {
            false
        }
    }

    /// Delete a key with WATCH signal (no notification). Returns true if deleted.
    pub fn db_delete(&mut self, db: usize, key: &[u8]) -> bool {
        if self.expire_if_needed(db, key, true) {
            return false;
        }
        if self.db_delete_raw(db, key) {
            self.signal_modified(db, &Bytes::copy_from_slice(key));
            true
        } else {
            false
        }
    }

    pub fn set_expire(&mut self, db: usize, key: &[u8], when: i64) {
        let k = self.dbs[db].dict.get_key_value(key).map(|(k, _)| k.clone());
        if let Some(k) = k {
            self.dbs[db].expires.insert(k, when);
        }
    }

    pub fn remove_expire(&mut self, db: usize, key: &[u8]) -> bool {
        self.dbs[db].expires.remove(key).is_some()
    }

    pub fn get_expire(&self, db: usize, key: &[u8]) -> Option<i64> {
        self.dbs[db].expire_of(key)
    }

    /// Delete key if its collection became empty (after a pop/remove).
    pub fn delete_if_empty(&mut self, db: usize, key: &[u8]) -> bool {
        let empty = self.dbs[db].dict.get(key).map(|o| o.v.is_empty_collection()).unwrap_or(false);
        if empty {
            self.db_delete_raw(db, key);
            let k = Bytes::copy_from_slice(key);
            self.notify(crate::config::NOTIFY_GENERIC, "del", &k, db);
            true
        } else {
            false
        }
    }

    /// Mark key as modified: invalidates WATCH and client-side caching.
    pub fn signal_modified(&mut self, db: usize, key: &Bytes) {
        if !self.watched.is_empty() {
            if let Some(ids) = self.watched.get(&(db, key.clone())) {
                for id in ids.clone() {
                    if let Some(c) = self.clients.get_mut(&id) {
                        c.flags |= crate::server::cflags::DIRTY_CAS;
                    }
                }
            }
        }
        if self.tracking.enabled() {
            crate::tracking::invalidate_key(self, key);
        }
    }

    pub fn signal_flushed_db(&mut self, db: Option<usize>) {
        if !self.watched.is_empty() {
            let keys: Vec<(usize, Bytes)> = self.watched.keys().cloned().collect();
            for (d, k) in keys {
                if db.is_none() || db == Some(d) {
                    let exists = self.dbs[d].dict.contains_key(&k);
                    if exists || db.is_none() || db == Some(d) {
                        if let Some(ids) = self.watched.get(&(d, k)) {
                            for id in ids.clone() {
                                if let Some(c) = self.clients.get_mut(&id) {
                                    c.flags |= crate::server::cflags::DIRTY_CAS;
                                }
                            }
                        }
                    }
                }
            }
        }
        if self.tracking.enabled() {
            crate::tracking::invalidate_all(self);
        }
    }

    /// Get a typed value for reading. Ok(None) when missing, Err(WRONGTYPE) on mismatch.
    pub fn get_type<'a>(
        &'a mut self,
        db: usize,
        key: &[u8],
        f: fn(&Value) -> bool,
    ) -> TypeResult<Option<&'a mut RObj>> {
        match self.lookup_read(db, key) {
            None => Ok(None),
            Some(o) => {
                if f(&o.v) {
                    Ok(Some(o))
                } else {
                    Err(wrongtype())
                }
            }
        }
    }

    pub fn get_type_w<'a>(
        &'a mut self,
        db: usize,
        key: &[u8],
        f: fn(&Value) -> bool,
    ) -> TypeResult<Option<&'a mut RObj>> {
        match self.lookup_write(db, key) {
            None => Ok(None),
            Some(o) => {
                if f(&o.v) {
                    Ok(Some(o))
                } else {
                    Err(wrongtype())
                }
            }
        }
    }

    pub fn notify(&mut self, class: u32, event: &str, key: &Bytes, db: usize) {
        crate::pubsub::notify_keyspace_event(self, class, event, key, db);
    }

    pub fn incr_dirty(&mut self, n: u64) {
        self.dirty += n;
    }

    /// Pick a random non-expired key.
    pub fn random_key(&mut self, db: usize) -> Option<Bytes> {
        let mut tries = 0;
        loop {
            let k = self.dbs[db].dict.random_entry().map(|(k, _)| k.clone())?;
            if self.dbs[db].expire_of(&k).is_some() {
                tries += 1;
                let all_volatile = self.dbs[db].expires.len() == self.dbs[db].dict.len();
                if all_volatile && tries > 100 && self.repl.is_replica() {
                    return Some(k);
                }
                if self.expire_if_needed(db, &k, false) {
                    if self.repl.is_replica() && tries > 100 {
                        return Some(k);
                    }
                    continue;
                }
            }
            return Some(k);
        }
    }

    pub fn total_keys(&self) -> usize {
        self.dbs.iter().map(|d| d.len()).sum()
    }
}
