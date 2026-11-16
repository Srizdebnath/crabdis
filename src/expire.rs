//! Active expiration cycle (sampling of volatile keys, like activeExpireCycle).

use crate::server::Store;
use bytes::Bytes;

pub fn active_expire_cycle(s: &mut Store, per_db: usize) {
    if s.pause.is_some() {
        return;
    }
    let now = crate::util::now_ms();
    let start = std::time::Instant::now();
    let budget = std::time::Duration::from_millis(25);
    for db in 0..s.dbs.len() {
        // Hash field expiry.
        if !s.dbs[db].hexpires.is_empty() {
            expire_hash_fields(s, db, now, 64);
        }
        if s.dbs[db].expires.is_empty() {
            continue;
        }
        let mut rounds = 0;
        loop {
            rounds += 1;
            let sample: Vec<(Bytes, i64)> = {
                let d = &s.dbs[db];
                let mut out = Vec::with_capacity(per_db);
                let mut cursor = d.expires_cursor;
                let mut visited = 0;
                loop {
                    cursor = d.expires.scan(cursor, |k, v| out.push((k.clone(), *v)));
                    visited += 1;
                    if cursor == 0 || out.len() >= per_db || visited > per_db * 4 {
                        break;
                    }
                }
                s.dbs[db].expires_cursor = cursor;
                out
            };
            if sample.is_empty() {
                break;
            }
            let mut expired = 0;
            let n = sample.len();
            for (k, when) in sample {
                if when <= now {
                    if s.repl.is_replica() {
                        continue;
                    }
                    s.delete_expired_key(db, &k);
                    crate::exec::flush_propagation(s);
                    expired += 1;
                }
            }
            s.stats.expired_stale_perc = expired as f64 / n.max(1) as f64;
            if expired * 10 < n || rounds > 16 {
                break;
            }
            if start.elapsed() > budget {
                s.stats.expired_time_cap_reached += 1;
                return;
            }
        }
    }
}

pub fn expire_hash_fields(s: &mut Store, db: usize, now: i64, max_keys: usize) {
    let keys: Vec<Bytes> = s.dbs[db].hexpires.keys().take(max_keys).cloned().collect();
    for k in keys {
        crate::cmd::hash::expire_fields_of(s, db, &k, now);
    }
}
