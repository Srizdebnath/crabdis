//! Hash commands, including Redis 7.4 hash-field expiration.

use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::server::Store;
use crate::tri;
use crate::value::{HashObj, Value};
use bytes::Bytes;

fn is_hash(v: &Value) -> bool {
    matches!(v, Value::Hash(_))
}

/// Lazily expire due fields of a hash. Returns true if the key was deleted.
pub fn expire_fields_of(s: &mut Store, db: usize, key: &[u8], now: i64) -> bool {
    if s.repl.is_replica() || s.persist.loading {
        return false;
    }
    let Some(o) = s.dbs[db].dict.get_mut(key) else {
        s.dbs[db].hexpires.remove(key);
        return false;
    };
    let Value::Hash(h) = &mut o.v else { return false };
    match h.min_ttl() {
        Some(w) if w <= now => {}
        Some(_) => return false,
        None => {
            s.dbs[db].hexpires.remove(key);
            return false;
        }
    }
    let dead = h.expire_fields(now);
    let empty = h.map.is_empty();
    let still_ttl = h.ttl.is_some();
    let k = Bytes::copy_from_slice(key);
    if !still_ttl {
        s.dbs[db].hexpires.remove(key);
    }
    if dead.is_empty() {
        return false;
    }
    s.stats.expired_subkeys += dead.len() as u64;
    let mut argv = vec![Bytes::from_static(b"HDEL"), k.clone()];
    argv.extend(dead.iter().cloned());
    s.propagate_now(db, argv);
    s.notify(NOTIFY_HASH, "hexpired", &k, db);
    if empty {
        s.db_delete_raw(db, key);
        s.notify(NOTIFY_GENERIC, "del", &k, db);
    }
    s.signal_modified(db, &k);
    empty
}

fn prep(ctx: &mut Ctx, key: &[u8]) {
    let db = ctx.db;
    if !ctx.s.dbs[db].hexpires.is_empty() && ctx.s.dbs[db].hexpires.contains_key(key) {
        let now = ctx.s.now_ms();
        expire_fields_of(ctx.s, db, key, now);
    }
}

fn hash_ref<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut HashObj>, Frame> {
    prep(ctx, key);
    match ctx.s.get_type(ctx.db, key, is_hash)? {
        Some(o) => match &mut o.v {
            Value::Hash(h) => Ok(Some(h)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn hash_mut<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut HashObj>, Frame> {
    prep(ctx, key);
    match ctx.s.get_type_w(ctx.db, key, is_hash)? {
        Some(o) => match &mut o.v {
            Value::Hash(h) => Ok(Some(h)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn hash_or_create<'a>(ctx: &'a mut Ctx, key: &Bytes) -> Result<&'a mut HashObj, Frame> {
    let db = ctx.db;
    if hash_mut(ctx, key)?.is_none() {
        ctx.s.db_add(db, key.clone(), Value::Hash(Box::new(HashObj::new())));
    }
    match &mut ctx.s.dbs[db].dict.get_mut(key).unwrap().v {
        Value::Hash(h) => Ok(h),
        _ => unreachable!(),
    }
}

fn after_hash_ttl_change(ctx: &mut Ctx, key: &Bytes) {
    let db = ctx.db;
    let has = match ctx.s.dbs[db].dict.get(key).map(|o| &o.v) {
        Some(Value::Hash(h)) => h.ttl.is_some(),
        _ => false,
    };
    if has {
        ctx.s.dbs[db].hexpires.insert(key.clone(), ());
    } else {
        ctx.s.dbs[db].hexpires.remove(key);
    }
}

pub fn hset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 != 0 {
        return Frame::err(format!("ERR wrong number of arguments for '{}' command", ctx.cmd.name));
    }
    let key = &args[1];
    let h = tri!(hash_or_create(ctx, key));
    let mut created = 0;
    for kv in args[2..].chunks(2) {
        h.persist(&kv[0]);
        if h.map.insert(kv[0].clone(), kv[1].clone()).is_none() {
            created += 1;
        }
    }
    after_hash_ttl_change(ctx, key);
    ctx.modified(key);
    ctx.notify(NOTIFY_HASH, "hset", key);
    ctx.dirty(((args.len() - 2) / 2) as u64);
    if ctx.cmd.name == "hmset" { Frame::ok() } else { Frame::Integer(created) }
}

pub fn hmset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hset(ctx, args)
}

pub fn hsetnx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let h = tri!(hash_or_create(ctx, key));
    if h.map.contains_key(&args[2]) {
        return Frame::Integer(0);
    }
    h.map.insert(args[2].clone(), args[3].clone());
    ctx.modified(key);
    ctx.notify(NOTIFY_HASH, "hset", key);
    ctx.dirty(1);
    Frame::Integer(1)
}

pub fn hget(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Null,
        Ok(Some(h)) => opt_bulk(h.map.get(&args[2]).cloned()),
    }
}

pub fn hmget(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Array(args[2..].iter().map(|_| Frame::Null).collect()),
        Ok(Some(h)) => Frame::Array(args[2..].iter().map(|f| opt_bulk(h.map.get(f).cloned())).collect()),
    }
}

pub fn hdel(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let n = match hash_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(h)) => args[2..].iter().filter(|f| h.remove(f)).count(),
    };
    if n > 0 {
        after_hash_ttl_change(ctx, key);
        ctx.notify(NOTIFY_HASH, "hdel", key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(n as u64);
    }
    Frame::Integer(n as i64)
}

pub fn hlen(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(h)) => Frame::Integer(h.len() as i64),
    }
}

pub fn hstrlen(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(h)) => Frame::Integer(h.map.get(&args[2]).map(|v| v.len()).unwrap_or(0) as i64),
    }
}

pub fn hexists(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(h)) => Frame::Integer(h.map.contains_key(&args[2]) as i64),
    }
}

fn hgetall_generic(ctx: &mut Ctx, args: &[Bytes], keys: bool, vals: bool) -> Frame {
    match hash_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => {
            if keys && vals {
                Frame::Map(vec![])
            } else {
                Frame::Array(vec![])
            }
        }
        Ok(Some(h)) => {
            if keys && vals {
                Frame::Map(h.map.iter().map(|(k, v)| (Frame::Bulk(k.clone()), Frame::Bulk(v.clone()))).collect())
            } else if keys {
                Frame::Array(h.map.keys().map(|k| Frame::Bulk(k.clone())).collect())
            } else {
                Frame::Array(h.map.values().map(|v| Frame::Bulk(v.clone())).collect())
            }
        }
    }
}

pub fn hgetall(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hgetall_generic(ctx, args, true, true)
}
pub fn hkeys(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hgetall_generic(ctx, args, true, false)
}
pub fn hvals(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hgetall_generic(ctx, args, false, true)
}

pub fn hincrby(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let by = tri!(int(&args[3]));
    let h = tri!(hash_or_create(ctx, key));
    let cur = match h.map.get(&args[2]) {
        Some(v) => match crate::util::parse_i64(v) {
            Some(n) => n,
            None => return Frame::err("ERR hash value is not an integer"),
        },
        None => 0,
    };
    let Some(n) = cur.checked_add(by) else {
        return Frame::err("ERR increment or decrement would overflow");
    };
    h.map.insert(args[2].clone(), Bytes::from(n.to_string()));
    ctx.modified(key);
    ctx.notify(NOTIFY_HASH, "hincrby", key);
    ctx.dirty(1);
    Frame::Integer(n)
}

pub fn hincrbyfloat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let by = tri!(float(&args[3]));
    if by.is_nan() || by.is_infinite() {
        return Frame::err("ERR value is NaN or Infinity");
    }
    let h = tri!(hash_or_create(ctx, key));
    let cur = match h.map.get(&args[2]) {
        Some(v) => match crate::util::parse_f64(v) {
            Some(n) => n,
            None => return Frame::err("ERR hash value is not a float"),
        },
        None => 0.0,
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return Frame::err("ERR increment would produce NaN or Infinity");
    }
    let nb = Bytes::from(crate::util::fmt_human(n));
    h.map.insert(args[2].clone(), nb.clone());
    ctx.modified(key);
    ctx.notify(NOTIFY_HASH, "hincrbyfloat", key);
    ctx.dirty(1);
    ctx.propagate_as(vec![Bytes::from_static(b"HSET"), key.clone(), args[2].clone(), nb.clone()]);
    Frame::Bulk(nb)
}

pub fn hrandfield(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut withvalues = false;
    let count = if args.len() >= 3 {
        let Some(c) = crate::util::parse_i64(&args[2]) else { return err_int() };
        if args.len() == 4 {
            if !eq(&args[3], "withvalues") {
                return err_syntax();
            }
            withvalues = true;
        } else if args.len() > 4 {
            return err_syntax();
        }
        Some(c)
    } else {
        None
    };
    let proto = ctx.proto();
    let h = match hash_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return if count.is_some() { Frame::Array(vec![]) } else { Frame::Null },
        Ok(Some(h)) => h,
    };
    let Some(count) = count else {
        return h.map.random_entry().map(|(k, _)| Frame::Bulk(k.clone())).unwrap_or(Frame::Null);
    };
    if count < -(i64::MAX / 2) || count > i64::MAX / 2 {
        return Frame::err("ERR value is out of range");
    }
    let pairs: Vec<(Bytes, Bytes)> = if count < 0 {
        (0..(-count) as usize)
            .map(|_| {
                let (k, v) = h.map.random_entry().unwrap();
                (k.clone(), v.clone())
            })
            .collect()
    } else {
        let n = (count as usize).min(h.len());
        let mut all: Vec<(Bytes, Bytes)> = h.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for i in 0..n {
            let j = i + crate::util::rand_range(all.len() - i);
            all.swap(i, j);
        }
        all.truncate(n);
        all
    };
    if withvalues {
        if proto >= 3 {
            Frame::Array(pairs.into_iter().map(|(k, v)| Frame::Array(vec![Frame::Bulk(k), Frame::Bulk(v)])).collect())
        } else {
            Frame::Array(pairs.into_iter().flat_map(|(k, v)| [Frame::Bulk(k), Frame::Bulk(v)]).collect())
        }
    } else {
        Frame::Array(pairs.into_iter().map(|(k, _)| Frame::Bulk(k)).collect())
    }
}

pub fn hscan(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = tri!(parse_scan(args, 2, false, true));
    let h = match hash_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(vec![Frame::bulk_str("0"), Frame::Array(vec![])]),
        Ok(Some(h)) => h,
    };
    let mut items: Vec<(Bytes, Bytes)> = Vec::new();
    let mut cursor = o.cursor;
    // Small hashes are returned in one go (like listpack encoding in Redis).
    if h.len() <= 128 {
        items = h.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        cursor = 0;
    } else {
        let mut iter = 0;
        loop {
            cursor = h.map.scan(cursor, |k, v| items.push((k.clone(), v.clone())));
            iter += 1;
            if cursor == 0 || items.len() >= o.count || iter >= o.count * 10 {
                break;
            }
        }
    }
    let mut out = Vec::new();
    for (k, v) in items {
        if let Some(p) = &o.pattern {
            if !crate::util::glob_match(p, &k, false) {
                continue;
            }
        }
        out.push(Frame::Bulk(k));
        if !o.novalues {
            out.push(Frame::Bulk(v));
        }
    }
    Frame::Array(vec![Frame::Bulk(Bytes::from(cursor.to_string())), Frame::Array(out)])
}

// ---- Hash field expiration ----

fn parse_fields(args: &[Bytes], at: usize, numerr: &str) -> Result<Vec<Bytes>, Frame> {
    if at >= args.len() || !eq(&args[at - 1], "fields") {
        return Err(Frame::err("ERR Mandatory argument FIELDS is missing or not at the right position"));
    }
    let n = match crate::util::parse_i64(&args[at]) {
        Some(v) if v >= 1 => v as usize,
        _ => return Err(Frame::err(format!("ERR {}", numerr))),
    };
    if n != args.len() - at - 1 {
        return Err(Frame::err("ERR The `numfields` parameter must match the number of arguments"));
    }
    Ok(args[at + 1..].to_vec())
}

const HFE_MAX_ABS_MS: i64 = 0x0000FFFFFFFFFFFF >> 2;

fn hexpire_generic(ctx: &mut Ctx, args: &[Bytes], basetime: i64, ms: bool) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let cmdname = ctx.cmd.name;
    // Type check first.
    if let Err(e) = hash_mut(ctx, key) {
        return e;
    }
    let Some(mut when) = crate::util::parse_i64(&args[2]) else { return err_int() };
    if when < 0 {
        return Frame::err("ERR invalid expire time, must be >= 0");
    }
    if !ms {
        if when > HFE_MAX_ABS_MS / 1000 {
            return Frame::err(format!("ERR invalid expire time in '{}' command", cmdname));
        }
        when *= 1000;
    }
    if when > HFE_MAX_ABS_MS - basetime {
        return Frame::err(format!("ERR invalid expire time in '{}' command", cmdname));
    }
    when += basetime;
    let mut at = 4;
    let mut cond = "";
    for c in ["nx", "xx", "gt", "lt"] {
        if eq(&args[3], c) {
            cond = c;
            at = 5;
        }
    }
    let fields = tri!(parse_fields(args, at, "Parameter `numFields` should be greater than 0"));
    let now = ctx.s.now_ms();
    let h = match hash_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(fields.iter().map(|_| Frame::Integer(-2)).collect()),
        Ok(Some(h)) => h,
    };
    let mut res = Vec::new();
    let mut updated = Vec::new();
    let mut deleted = Vec::new();
    for f in &fields {
        if !h.map.contains_key(f) {
            res.push(Frame::Integer(-2));
            continue;
        }
        let prev = h.field_ttl(f);
        let ok = match cond {
            "nx" => prev.is_none(),
            "xx" => prev.is_some(),
            "gt" => prev.is_some_and(|p| when > p),
            "lt" => prev.is_none_or(|p| when < p),
            _ => true,
        };
        if !ok {
            res.push(Frame::Integer(0));
            continue;
        }
        if when <= now {
            h.remove(f);
            deleted.push(f.clone());
            res.push(Frame::Integer(2));
        } else {
            h.set_ttl(f, when);
            updated.push(f.clone());
            res.push(Frame::Integer(1));
        }
    }
    let empty = h.map.is_empty();
    after_hash_ttl_change(ctx, key);
    if !deleted.is_empty() {
        let mut argv = vec![Bytes::from_static(b"HDEL"), key.clone()];
        argv.extend(deleted.iter().cloned());
        ctx.also_propagate(argv);
        ctx.notify(NOTIFY_HASH, "hdel", key);
        ctx.dirty(deleted.len() as u64);
    }
    if !updated.is_empty() {
        let mut argv = vec![
            Bytes::from_static(b"HPEXPIREAT"),
            key.clone(),
            Bytes::from(when.to_string()),
        ];
        if !cond.is_empty() {
            argv.push(Bytes::from(cond.to_ascii_uppercase()));
        }
        argv.push(Bytes::from_static(b"FIELDS"));
        argv.push(Bytes::from(updated.len().to_string()));
        argv.extend(updated.iter().cloned());
        ctx.also_propagate(argv);
        ctx.notify(NOTIFY_HASH, "hexpire", key);
        ctx.dirty(updated.len() as u64);
    }
    if !updated.is_empty() || !deleted.is_empty() {
        ctx.modified(key);
    }
    ctx.no_prop = true;
    if empty {
        ctx.s.db_delete_raw(db, key);
        ctx.notify(NOTIFY_GENERIC, "del", key);
    }
    Frame::Array(res)
}

pub fn hexpire(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    hexpire_generic(ctx, args, now, false)
}
pub fn hpexpire(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    hexpire_generic(ctx, args, now, true)
}
pub fn hexpireat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hexpire_generic(ctx, args, 0, false)
}
pub fn hpexpireat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    hexpire_generic(ctx, args, 0, true)
}

fn httl_generic(ctx: &mut Ctx, args: &[Bytes], ms: bool, abs: bool) -> Frame {
    let fields = match hash_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(_) => tri!(parse_fields(args, 3, "Number of fields must be a positive integer")),
    };
    let now = ctx.s.now_ms();
    let h = match hash_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(fields.iter().map(|_| Frame::Integer(-2)).collect()),
        Ok(Some(h)) => h,
    };
    Frame::Array(
        fields
            .iter()
            .map(|f| {
                if !h.map.contains_key(f) {
                    return Frame::Integer(-2);
                }
                match h.field_ttl(f) {
                    None => Frame::Integer(-1),
                    Some(w) => {
                        let v = if abs { w } else { (w - now).max(0) };
                        Frame::Integer(if ms { v } else if abs { v / 1000 } else { (v + 500) / 1000 })
                    }
                }
            })
            .collect(),
    )
}

pub fn httl(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    httl_generic(ctx, args, false, false)
}
pub fn hpttl(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    httl_generic(ctx, args, true, false)
}
pub fn hexpiretime(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    httl_generic(ctx, args, false, true)
}
pub fn hpexpiretime(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    httl_generic(ctx, args, true, true)
}

pub fn hpersist(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let fields = match hash_mut(ctx, key) {
        Err(e) => return e,
        Ok(_) => tri!(parse_fields(args, 3, "Number of fields must be a positive integer")),
    };
    let h = match hash_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(fields.iter().map(|_| Frame::Integer(-2)).collect()),
        Ok(Some(h)) => h,
    };
    let mut changed = 0;
    let res = fields
        .iter()
        .map(|f| {
            if !h.map.contains_key(f) {
                Frame::Integer(-2)
            } else if h.persist(f) {
                changed += 1;
                Frame::Integer(1)
            } else {
                Frame::Integer(-1)
            }
        })
        .collect();
    after_hash_ttl_change(ctx, key);
    if changed > 0 {
        ctx.modified(key);
        ctx.notify(NOTIFY_HASH, "hpersist", key);
        ctx.dirty(changed);
    }
    Frame::Array(res)
}
