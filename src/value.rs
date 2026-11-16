//! Value types stored in the keyspace.

use crate::dict::Dict;
use crate::stream::Stream;
use crate::zset::ZSet;
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Clone)]
pub enum Value {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(Box<HashObj>),
    Set(Box<Dict<()>>),
    ZSet(Box<ZSet>),
    Stream(Box<Stream>),
}

#[derive(Clone, Default)]
pub struct HashTtl {
    pub by_field: Dict<i64>,
    pub by_time: std::collections::BTreeSet<(i64, Bytes)>,
}

#[derive(Clone, Default)]
pub struct HashObj {
    pub map: Dict<Bytes>,
    /// Per-field expiration (unix ms), Redis 7.4 hash field expiry.
    pub ttl: Option<Box<HashTtl>>,
}

impl HashObj {
    pub fn new() -> Self {
        HashObj { map: Dict::new(), ttl: None }
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn field_ttl(&self, f: &[u8]) -> Option<i64> {
        self.ttl.as_ref().and_then(|t| t.by_field.get(f).copied())
    }
    pub fn remove(&mut self, f: &[u8]) -> bool {
        self.persist(f);
        self.map.remove(f).is_some()
    }
    pub fn set_ttl(&mut self, f: &[u8], when: i64) {
        let key = self.map.get_key_value(f).map(|(k, _)| k.clone());
        if let Some(k) = key {
            let t = self.ttl.get_or_insert_with(Default::default);
            if let Some(old) = t.by_field.insert(k.clone(), when) {
                t.by_time.remove(&(old, k.clone()));
            }
            t.by_time.insert((when, k));
        }
    }
    pub fn persist(&mut self, f: &[u8]) -> bool {
        let Some(t) = self.ttl.as_mut() else { return false };
        let r = match t.by_field.remove(f) {
            Some((k, w)) => {
                t.by_time.remove(&(w, k));
                true
            }
            None => false,
        };
        if t.by_field.is_empty() {
            self.ttl = None;
        }
        r
    }
    /// Remove expired fields. Returns the removed field names.
    pub fn expire_fields(&mut self, now: i64) -> Vec<Bytes> {
        let mut dead = Vec::new();
        while let Some(t) = self.ttl.as_ref() {
            match t.by_time.first() {
                Some((w, f)) if *w <= now => {
                    let f = f.clone();
                    self.remove(&f);
                    dead.push(f);
                }
                _ => break,
            }
        }
        dead
    }
    pub fn min_ttl(&self) -> Option<i64> {
        self.ttl.as_ref().and_then(|t| t.by_time.first().map(|(w, _)| *w))
    }
}

/// A keyspace entry: value plus LRU clock / LFU counter.
#[derive(Clone)]
pub struct RObj {
    pub v: Value,
    /// LRU: seconds clock (24 bits as in Redis). LFU: (minutes << 8) | counter.
    pub lru: u32,
}

impl RObj {
    pub fn new(v: Value) -> Self {
        RObj { v, lru: crate::server::lru_clock() }
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
        }
    }

    /// OBJECT ENCODING approximation based on Redis thresholds.
    pub fn encoding(&self, cfg: &crate::config::Config) -> &'static str {
        match self {
            Value::Str(b) => {
                if b.len() <= 20 && crate::util::parse_i64(b).is_some() {
                    "int"
                } else if b.len() <= 44 {
                    "embstr"
                } else {
                    "raw"
                }
            }
            Value::List(l) => {
                if l.len() <= cfg.list_max_listpack_size_entries()
                    && l.iter().map(|e| e.len()).sum::<usize>() <= 8192
                {
                    "listpack"
                } else {
                    "quicklist"
                }
            }
            Value::Hash(h) => {
                if h.ttl.is_none()
                    && h.len() <= cfg.hash_max_listpack_entries
                    && h.map.iter().all(|(k, v)| {
                        k.len() <= cfg.hash_max_listpack_value && v.len() <= cfg.hash_max_listpack_value
                    })
                {
                    "listpack"
                } else if h.ttl.is_some() && h.len() <= cfg.hash_max_listpack_entries {
                    "listpackex"
                } else {
                    "hashtable"
                }
            }
            Value::Set(s) => {
                if s.len() <= cfg.set_max_intset_entries
                    && s.keys().all(|k| crate::util::parse_i64(k).is_some())
                {
                    "intset"
                } else if s.len() <= cfg.set_max_listpack_entries
                    && s.keys().all(|k| k.len() <= cfg.set_max_listpack_value)
                {
                    "listpack"
                } else {
                    "hashtable"
                }
            }
            Value::ZSet(z) => {
                if z.len() <= cfg.zset_max_listpack_entries
                    && z.dict.keys().all(|k| k.len() <= cfg.zset_max_listpack_value)
                {
                    "listpack"
                } else {
                    "skiplist"
                }
            }
            Value::Stream(_) => "stream",
        }
    }

    /// Rough memory usage in bytes (MEMORY USAGE / maxmemory accounting).
    pub fn mem_usage(&self) -> usize {
        match self {
            Value::Str(b) => 16 + b.len(),
            Value::List(l) => 32 + l.iter().map(|e| e.len() + 16).sum::<usize>(),
            Value::Hash(h) => 48 + h.map.iter().map(|(k, v)| k.len() + v.len() + 64).sum::<usize>(),
            Value::Set(s) => 48 + s.keys().map(|k| k.len() + 48).sum::<usize>(),
            Value::ZSet(z) => 64 + z.dict.keys().map(|k| k.len() + 96).sum::<usize>(),
            Value::Stream(s) => s.mem_usage(),
        }
    }

    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::Str(_) => false,
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.map.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
            Value::Stream(_) => false,
        }
    }
}

/// Commands that recreate a key (AOF rewrite without RDB preamble).
pub fn rewrite_commands(key: &Bytes, v: &Value) -> Vec<Vec<Bytes>> {
    const BATCH: usize = 64;
    let b = |s: &'static [u8]| Bytes::from_static(s);
    let mut out: Vec<Vec<Bytes>> = Vec::new();
    match v {
        Value::Str(s) => out.push(vec![b(b"SET"), key.clone(), s.clone()]),
        Value::List(l) => {
            for chunk in l.iter().collect::<Vec<_>>().chunks(BATCH) {
                let mut c = vec![b(b"RPUSH"), key.clone()];
                c.extend(chunk.iter().map(|x| (*x).clone()));
                out.push(c);
            }
        }
        Value::Set(s) => {
            for chunk in s.keys().collect::<Vec<_>>().chunks(BATCH) {
                let mut c = vec![b(b"SADD"), key.clone()];
                c.extend(chunk.iter().map(|x| (*x).clone()));
                out.push(c);
            }
        }
        Value::ZSet(z) => {
            for chunk in z.iter().collect::<Vec<_>>().chunks(BATCH) {
                let mut c = vec![b(b"ZADD"), key.clone()];
                for (m, sc) in chunk {
                    c.push(Bytes::from(crate::util::fmt_g17(*sc)));
                    c.push(m.clone());
                }
                out.push(c);
            }
        }
        Value::Hash(h) => {
            let pairs: Vec<(&Bytes, &Bytes)> = h.map.iter().collect();
            for chunk in pairs.chunks(BATCH) {
                let mut c = vec![b(b"HSET"), key.clone()];
                for (f, val) in chunk {
                    c.push((*f).clone());
                    c.push((*val).clone());
                }
                out.push(c);
            }
            for (f, _) in &pairs {
                if let Some(t) = h.field_ttl(f) {
                    out.push(vec![b(b"HPEXPIREAT"), key.clone(), Bytes::from(t.to_string()), b(b"FIELDS"), b(b"1"), (*f).clone()]);
                }
            }
        }
        Value::Stream(s) => {
            let mut any = false;
            for node in s.nodes.values() {
                for e in node.entries.iter().filter(|e| !e.deleted) {
                    any = true;
                    let mut c = vec![b(b"XADD"), key.clone(), e.id.to_bytes()];
                    c.extend(e.fv.iter().cloned());
                    out.push(c);
                }
            }
            if !any {
                // Empty stream: create it with a dummy entry and delete it.
                out.push(vec![b(b"XADD"), key.clone(), b(b"MAXLEN"), b(b"0"), s.last_id.to_bytes(), b(b"x"), b(b"y")]);
            }
            out.push(vec![
                b(b"XSETID"),
                key.clone(),
                s.last_id.to_bytes(),
                b(b"ENTRIESADDED"),
                Bytes::from(s.entries_added.to_string()),
                b(b"MAXDELETEDID"),
                s.max_deleted_id.to_bytes(),
            ]);
            for (gname, g) in &s.groups {
                out.push(vec![b(b"XGROUP"), b(b"CREATE"), key.clone(), gname.clone(), g.last_id.to_bytes(), b(b"ENTRIESREAD"), Bytes::from(g.entries_read.to_string())]);
                for (id, n) in &g.pel {
                    let Some(owner) = &n.consumer else { continue };
                    out.push(vec![
                        b(b"XCLAIM"),
                        key.clone(),
                        gname.clone(),
                        owner.clone(),
                        b(b"0"),
                        id.to_bytes(),
                        b(b"TIME"),
                        Bytes::from(n.delivery_time.to_string()),
                        b(b"RETRYCOUNT"),
                        Bytes::from(n.delivery_count.to_string()),
                        b(b"JUSTID"),
                        b(b"FORCE"),
                    ]);
                }
                for (cname, c) in &g.consumers {
                    if c.pel.is_empty() {
                        out.push(vec![b(b"XGROUP"), b(b"CREATECONSUMER"), key.clone(), gname.clone(), cname.clone()]);
                    }
                }
            }
        }
    }
    out
}
