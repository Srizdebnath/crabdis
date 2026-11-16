//! maxmemory eviction (approximated LRU / LFU / random / TTL, Redis-style sampling).

use crate::server::Store;
use crate::value::RObj;
use bytes::Bytes;

pub fn lazyfree(o: RObj) {
    // Large values are freed on a background thread to avoid latency spikes.
    let big = match &o.v {
        crate::value::Value::List(l) => l.len() > 64,
        crate::value::Value::Hash(h) => h.len() > 64,
        crate::value::Value::Set(s) => s.len() > 64,
        crate::value::Value::ZSet(z) => z.len() > 64,
        crate::value::Value::Stream(_) => true,
        _ => false,
    };
    if big {
        std::thread::spawn(move || drop(o));
    }
}

fn lfu_minutes() -> u32 {
    ((crate::util::now_ms() / 60000) as u32) & 0xFFFF
}

pub fn lfu_init() -> u32 {
    (lfu_minutes() << 8) | 5
}

fn lfu_decr(lru: u32, decay: u32) -> u32 {
    let ldt = lru >> 8;
    let counter = lru & 255;
    let now = lfu_minutes();
    let elapsed = if now >= ldt { now - ldt } else { 65535 - ldt + now };
    let periods = if decay > 0 { elapsed / decay } else { 0 };
    if periods > 0 { counter.saturating_sub(periods) } else { counter }
}

pub fn lfu_counter(lru: u32, decay: u32) -> u32 {
    lfu_decr(lru, decay)
}

pub fn lfu_touch(lru: u32, factor: u32, decay: u32) -> u32 {
    let mut counter = lfu_decr(lru, decay);
    if counter < 255 {
        let r = crate::util::rand_f64();
        let base = counter.saturating_sub(5) as f64;
        let p = 1.0 / (base * factor as f64 + 1.0);
        if r < p {
            counter += 1;
        }
    }
    (lfu_minutes() << 8) | counter
}

pub fn idle_ms(lru: u32) -> u64 {
    let now = crate::server::lru_clock();
    let secs = if now >= lru { now - lru } else { (1 << 24) - lru + now };
    secs as u64 * 1000
}

pub fn maybe_evict(s: &mut Store) {
    if s.cfg.maxmemory > 0 {
        perform_evictions(s);
    }
}

/// Evict keys until memory is under maxmemory. Returns false if not possible.
pub fn perform_evictions(s: &mut Store) -> bool {
    let max = s.cfg.maxmemory as usize;
    if max == 0 || s.repl.is_replica() {
        return true;
    }
    let policy = s.cfg.maxmemory_policy.clone();
    if policy == "noeviction" {
        return s.used_memory() <= max;
    }
    let samples = s.cfg.maxmemory_samples.max(1);
    let volatile = policy.starts_with("volatile");
    let mut guard = 0;
    while s.used_memory() > max {
        guard += 1;
        if guard > 100_000 {
            return false;
        }
        let mut best: Option<(usize, Bytes, u64)> = None;
        for db in 0..s.dbs.len() {
            let d = &s.dbs[db];
            let pool: Vec<Bytes> = if volatile {
                d.expires.sample(samples).into_iter().map(|(k, _)| k.clone()).collect()
            } else {
                d.dict.sample(samples).into_iter().map(|(k, _)| k.clone()).collect()
            };
            for k in pool {
                let score: u64 = if policy.ends_with("random") {
                    crate::util::rand_u64() >> 1
                } else if policy.ends_with("ttl") {
                    match d.expires.get(&k) {
                        Some(&w) => u64::MAX - w as u64,
                        None => continue,
                    }
                } else if policy.ends_with("lfu") {
                    match d.dict.get(&k) {
                        Some(o) => 255 - lfu_counter(o.lru, s.cfg.lfu_decay_time) as u64,
                        None => continue,
                    }
                } else {
                    match d.dict.get(&k) {
                        Some(o) => idle_ms(o.lru),
                        None => continue,
                    }
                };
                if best.as_ref().map(|b| score > b.2).unwrap_or(true) {
                    best = Some((db, k, score));
                }
            }
        }
        let Some((db, key, _)) = best else { return false };
        s.db_delete_raw(db, &key);
        s.stats.evicted_keys += 1;
        s.signal_modified(db, &key);
        s.notify(crate::config::NOTIFY_EVICTED, "evicted", &key, db);
        s.propagate_deletion(db, key);
        crate::exec::flush_propagation(s);
    }
    true
}

pub fn over_maxmemory(s: &Store) -> bool {
    s.cfg.maxmemory > 0 && s.used_memory() as u64 > s.cfg.maxmemory
}

/// LFU field for an object loaded with a saved frequency counter.
pub fn lfu_with_counter(counter: u8) -> u32 {
    (lfu_minutes() << 8) | counter as u32
}

/// LRU clock value for an object idle for `ms` milliseconds.
pub fn lru_with_idle(ms: u64) -> u32 {
    let now = crate::server::lru_clock();
    let secs = ((ms / 1000) as u32) & ((1 << 24) - 1);
    if now >= secs { now - secs } else { (1 << 24) - (secs - now) }
}
