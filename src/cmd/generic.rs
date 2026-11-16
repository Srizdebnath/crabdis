//! Generic keyspace commands.

use super::util::*;
use crate::config::*;
use crate::db::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use bytes::Bytes;

pub fn del(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    del_generic(ctx, args, false)
}

pub fn unlink(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    del_generic(ctx, args, true)
}

fn del_generic(ctx: &mut Ctx, args: &[Bytes], _lazy: bool) -> Frame {
    let mut n = 0;
    for k in &args[1..] {
        if ctx.s.db_delete(ctx.db, k) {
            ctx.notify(NOTIFY_GENERIC, "del", k);
            ctx.dirty(1);
            n += 1;
        }
    }
    Frame::Integer(n)
}

pub fn exists(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut n = 0;
    for k in &args[1..] {
        if ctx.s.lookup(ctx.db, k, LOOKUP_NOTOUCH).is_some() {
            n += 1;
        }
    }
    Frame::Integer(n)
}

pub fn touch(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut n = 0;
    for k in &args[1..] {
        if ctx.s.lookup(ctx.db, k, LOOKUP_NONE).is_some() {
            n += 1;
        }
    }
    Frame::Integer(n)
}

pub fn type_(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match ctx.s.lookup(ctx.db, &args[1], LOOKUP_NOTOUCH) {
        Some(o) => Frame::Simple(o.v.type_name().into()),
        None => Frame::Simple("none".into()),
    }
}

#[derive(PartialEq, Clone, Copy)]
enum Unit {
    Sec,
    Ms,
}

fn expire_generic(ctx: &mut Ctx, args: &[Bytes], basetime: i64, unit: Unit) -> Frame {
    let key = &args[1];
    let Some(mut when) = crate::util::parse_i64(&args[2]) else { return err_int() };
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for a in &args[3..] {
        if eq(a, "nx") {
            nx = true;
        } else if eq(a, "xx") {
            xx = true;
        } else if eq(a, "gt") {
            gt = true;
        } else if eq(a, "lt") {
            lt = true;
        } else {
            return Frame::err(format!("ERR Unsupported option {}", String::from_utf8_lossy(a)));
        }
    }
    if nx && (xx || gt || lt) {
        return Frame::err("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return Frame::err("ERR GT and LT options at the same time are not compatible");
    }
    let cmdname = ctx.cmd.name;
    let invalid = || Frame::err(format!("ERR invalid expire time in '{}' command", cmdname));
    if unit == Unit::Sec {
        if when > i64::MAX / 1000 || when < i64::MIN / 1000 {
            return invalid();
        }
        when *= 1000;
    }
    if when > i64::MAX - basetime {
        return invalid();
    }
    when += basetime;
    if ctx.s.lookup_write(ctx.db, key).is_none() {
        return Frame::Integer(0);
    }
    let cur = ctx.s.get_expire(ctx.db, key);
    if nx && cur.is_some() {
        return Frame::Integer(0);
    }
    if xx && cur.is_none() {
        return Frame::Integer(0);
    }
    if gt {
        match cur {
            None => return Frame::Integer(0),
            Some(c) if when <= c => return Frame::Integer(0),
            _ => {}
        }
    }
    if lt {
        if let Some(c) = cur {
            if when >= c {
                return Frame::Integer(0);
            }
        }
    }
    let k = key.clone();
    if when <= ctx.s.now_ms() && !ctx.s.persist.loading && !ctx.s.repl.is_replica() {
        ctx.s.db_delete(ctx.db, key);
        ctx.dirty(1);
        ctx.notify(NOTIFY_GENERIC, "del", &k);
        let lazy = ctx.s.cfg.get("lazyfree-lazy-expire") == Some("yes");
        ctx.propagate_as(vec![Bytes::from_static(if lazy { b"UNLINK" } else { b"DEL" }), k]);
        return Frame::Integer(1);
    }
    ctx.s.set_expire(ctx.db, key, when);
    ctx.modified(&k);
    ctx.notify(NOTIFY_GENERIC, "expire", &k);
    ctx.dirty(1);
    ctx.propagate_as(vec![Bytes::from_static(b"PEXPIREAT"), k, Bytes::from(when.to_string())]);
    Frame::Integer(1)
}

pub fn expire(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    expire_generic(ctx, args, now, Unit::Sec)
}
pub fn pexpire(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    expire_generic(ctx, args, now, Unit::Ms)
}
pub fn expireat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    expire_generic(ctx, args, 0, Unit::Sec)
}
pub fn pexpireat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    expire_generic(ctx, args, 0, Unit::Ms)
}

fn ttl_generic(ctx: &mut Ctx, args: &[Bytes], ms: bool, abs: bool) -> Frame {
    if ctx.s.lookup(ctx.db, &args[1], LOOKUP_NOTOUCH).is_none() {
        return Frame::Integer(-2);
    }
    match ctx.s.get_expire(ctx.db, &args[1]) {
        None => Frame::Integer(-1),
        Some(when) => {
            let v = if abs { when } else { (when - ctx.s.now_ms()).max(0) };
            Frame::Integer(if ms { v } else if abs { v / 1000 } else { (v + 500) / 1000 })
        }
    }
}

pub fn ttl(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, args, false, false)
}
pub fn pttl(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, args, true, false)
}
pub fn expiretime(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, args, false, true)
}
pub fn pexpiretime(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, args, true, true)
}

pub fn persist(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.lookup_write(ctx.db, &args[1]).is_none() {
        return Frame::Integer(0);
    }
    if ctx.s.remove_expire(ctx.db, &args[1]) {
        let k = args[1].clone();
        ctx.modified(&k);
        ctx.notify(NOTIFY_GENERIC, "persist", &k);
        ctx.dirty(1);
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

pub fn keys(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let pat = &args[1];
    let all = pat.as_ref() == b"*";
    let now = ctx.s.now_ms();
    let db = &ctx.s.dbs[ctx.db];
    let replica = ctx.s.repl.is_replica();
    let _ = replica;
    let mut out = Vec::new();
    for (k, _) in db.dict.iter() {
        if !all && !crate::util::glob_match(pat, k, false) {
            continue;
        }
        if let Some(w) = db.expire_of(k) {
            if w <= now {
                continue;
            }
        }
        out.push(Frame::Bulk(k.clone()));
    }
    Frame::Array(out)
}

pub fn scan(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = tri!(parse_scan(args, 1, true, false));
    let db = ctx.db;
    let mut keys: Vec<Bytes> = Vec::new();
    let mut cursor = o.cursor;
    let maxiter = o.count * 10;
    let mut iter = 0;
    loop {
        cursor = ctx.s.dbs[db].dict.scan(cursor, |k, _| keys.push(k.clone()));
        iter += 1;
        if cursor == 0 || iter >= maxiter || keys.len() >= o.count {
            break;
        }
    }
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        if let Some(p) = &o.pattern {
            if !crate::util::glob_match(p, &k, false) {
                continue;
            }
        }
        if ctx.s.expire_if_needed(db, &k, false) {
            continue;
        }
        if let Some(t) = &o.type_ {
            let tn = ctx.s.dbs[db].dict.get(&k).map(|o| o.v.type_name()).unwrap_or("");
            if tn != t {
                continue;
            }
        }
        out.push(Frame::Bulk(k));
    }
    Frame::Array(vec![Frame::Bulk(Bytes::from(cursor.to_string())), Frame::Array(out)])
}

pub fn randomkey(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    match ctx.s.random_key(ctx.db) {
        Some(k) => Frame::Bulk(k),
        None => Frame::Null,
    }
}

fn rename_generic(ctx: &mut Ctx, args: &[Bytes], nx: bool) -> Frame {
    let (src, dst) = (&args[1], &args[2]);
    let db = ctx.db;
    if ctx.s.lookup_write(db, src).is_none() {
        return err_nokey();
    }
    if src == dst {
        return if nx { Frame::Integer(0) } else { Frame::ok() };
    }
    let expire = ctx.s.get_expire(db, src);
    let dst_exists = ctx.s.lookup_write(db, dst).is_some();
    if dst_exists {
        if nx {
            return Frame::Integer(0);
        }
        ctx.s.db_delete_raw(db, dst);
    }
    let hexp = ctx.s.dbs[db].hexpires.contains_key(src);
    let (_, o) = ctx.s.dbs[db].dict.remove(src).unwrap();
    ctx.s.dbs[db].expires.remove(src);
    ctx.s.dbs[db].hexpires.remove(src);
    let is_ready = matches!(o.v, crate::value::Value::List(_) | crate::value::Value::ZSet(_) | crate::value::Value::Stream(_));
    ctx.s.dbs[db].dict.insert(dst.clone(), o);
    if let Some(e) = expire {
        ctx.s.set_expire(db, dst, e);
    }
    if hexp {
        ctx.s.dbs[db].hexpires.insert(dst.clone(), ());
    }
    if is_ready {
        ctx.s.signal_ready(db, dst);
    }
    ctx.modified(src);
    ctx.modified(dst);
    ctx.notify(NOTIFY_GENERIC, "rename_from", src);
    ctx.notify(NOTIFY_GENERIC, "rename_to", dst);
    ctx.dirty(1);
    if nx { Frame::Integer(1) } else { Frame::ok() }
}

pub fn rename(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    rename_generic(ctx, args, false)
}
pub fn renamenx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    rename_generic(ctx, args, true)
}

pub fn move_(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_some() {
        return Frame::err("ERR MOVE is not allowed in cluster mode");
    }
    let Some(dst) = crate::util::parse_i64(&args[2]) else { return err_int() };
    if dst < 0 || dst as usize >= ctx.s.dbs.len() {
        return Frame::err("ERR DB index is out of range");
    }
    let dst = dst as usize;
    let src = ctx.db;
    if dst == src {
        return Frame::err("ERR source and destination objects are the same");
    }
    let key = &args[1];
    if ctx.s.lookup_write(src, key).is_none() {
        return Frame::Integer(0);
    }
    if ctx.s.lookup_write(dst, key).is_some() {
        return Frame::Integer(0);
    }
    let expire = ctx.s.get_expire(src, key);
    let hexp = ctx.s.dbs[src].hexpires.contains_key(key);
    let (k, o) = ctx.s.dbs[src].dict.remove(key).unwrap();
    ctx.s.dbs[src].expires.remove(key);
    ctx.s.dbs[src].hexpires.remove(key);
    let ready = matches!(o.v, crate::value::Value::List(_) | crate::value::Value::ZSet(_) | crate::value::Value::Stream(_));
    ctx.s.dbs[dst].dict.insert(k.clone(), o);
    if let Some(e) = expire {
        ctx.s.set_expire(dst, key, e);
    }
    if hexp {
        ctx.s.dbs[dst].hexpires.insert(k.clone(), ());
    }
    if ready {
        ctx.s.signal_ready(dst, key);
    }
    ctx.s.signal_modified(src, &k);
    ctx.s.signal_modified(dst, &k);
    ctx.s.notify(NOTIFY_GENERIC, "move_from", &k, src);
    ctx.s.notify(NOTIFY_GENERIC, "move_to", &k, dst);
    ctx.dirty(1);
    Frame::Integer(1)
}

pub fn copy(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (src, dst) = (&args[1], &args[2]);
    let mut dstdb = ctx.db;
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if eq(&args[i], "replace") {
            replace = true;
            i += 1;
        } else if eq(&args[i], "db") && i + 1 < args.len() {
            let Some(d) = crate::util::parse_i64(&args[i + 1]) else { return err_int() };
            if d < 0 || d as usize >= ctx.s.dbs.len() {
                return Frame::err("ERR DB index is out of range");
            }
            if ctx.s.cluster.is_some() && d != 0 {
                return Frame::err("ERR Copying to another database is not allowed in cluster mode");
            }
            dstdb = d as usize;
            i += 2;
        } else {
            return err_syntax();
        }
    }
    let srcdb = ctx.db;
    if srcdb == dstdb && src == dst {
        return Frame::err("ERR source and destination objects are the same");
    }
    let Some(o) = ctx.s.lookup_write(srcdb, src) else { return Frame::Integer(0) };
    let v = o.v.clone();
    let expire = ctx.s.get_expire(srcdb, src);
    let hexp = ctx.s.dbs[srcdb].hexpires.contains_key(src);
    if ctx.s.lookup_write(dstdb, dst).is_some() {
        if !replace {
            return Frame::Integer(0);
        }
        ctx.s.db_delete_raw(dstdb, dst);
    }
    ctx.s.db_add(dstdb, dst.clone(), v);
    if let Some(e) = expire {
        ctx.s.set_expire(dstdb, dst, e);
    }
    if hexp {
        ctx.s.dbs[dstdb].hexpires.insert(dst.clone(), ());
    }
    ctx.s.signal_modified(dstdb, dst);
    ctx.s.notify(NOTIFY_GENERIC, "copy_to", dst, dstdb);
    ctx.dirty(1);
    Frame::Integer(1)
}

pub fn object_encoding(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let cfg = ctx.s.cfg.clone();
    match ctx.s.lookup(ctx.db, &args[2], LOOKUP_NOTOUCH | LOOKUP_NONOTIFY) {
        Some(o) => Frame::bulk_str(o.v.encoding(&cfg)),
        None => Frame::Null,
    }
}

pub fn object_freq(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let decay = ctx.s.cfg.lfu_decay_time;
    let lfu = ctx.s.cfg.maxmemory_policy.contains("lfu");
    match ctx.s.lookup(ctx.db, &args[2], LOOKUP_NOTOUCH | LOOKUP_NONOTIFY) {
        Some(o) => {
            if !lfu {
                return Frame::err("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            }
            Frame::Integer(crate::evict::lfu_counter(o.lru, decay) as i64)
        }
        None => Frame::Null,
    }
}

pub fn object_idletime(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let lfu = ctx.s.cfg.maxmemory_policy.contains("lfu");
    match ctx.s.lookup(ctx.db, &args[2], LOOKUP_NOTOUCH | LOOKUP_NONOTIFY) {
        Some(o) => {
            if lfu {
                return Frame::err("ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            }
            Frame::Integer((crate::evict::idle_ms(o.lru) / 1000) as i64)
        }
        None => Frame::Null,
    }
}

pub fn object_refcount(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match ctx.s.lookup(ctx.db, &args[2], LOOKUP_NOTOUCH | LOOKUP_NONOTIFY) {
        Some(o) => {
            // Shared integers in Redis report INT_MAX refcount.
            if let crate::value::Value::Str(b) = &o.v {
                if let Some(v) = crate::util::parse_i64(b) {
                    if (0..10000).contains(&v) && ctx.s.cfg.maxmemory == 0 {
                        return Frame::Integer(2147483647);
                    }
                }
            }
            Frame::Integer(1)
        }
        None => Frame::Null,
    }
}

pub fn object_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "OBJECT",
        &[
            "ENCODING <key>",
            "    Return the kind of internal representation used in order to store the value",
            "    associated with a <key>.",
            "FREQ <key>",
            "    Return the access frequency index of the <key>. The returned integer is",
            "    proportional to the logarithm of the recent access frequency of the key.",
            "IDLETIME <key>",
            "    Return the idle time of the <key>, that is the approximated number of",
            "    seconds elapsed since the last access to the key.",
            "REFCOUNT <key>",
            "    Return the number of references of the value associated with the specified",
            "    <key>.",
        ],
    )
}

pub fn dump(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let Some(o) = ctx.s.lookup_read(ctx.db, &args[1]) else { return Frame::Null };
    let v = o.v.clone();
    Frame::Bulk(Bytes::from(crate::rdb::dump_payload(&v)))
}

pub fn restore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let Some(mut ttl) = crate::util::parse_i64(&args[2]) else { return err_int() };
    let mut replace = false;
    let mut absttl = false;
    let mut idle: Option<i64> = None;
    let mut freq: Option<i64> = None;
    let mut i = 4;
    while i < args.len() {
        if eq(&args[i], "replace") {
            replace = true;
            i += 1;
        } else if eq(&args[i], "absttl") {
            absttl = true;
            i += 1;
        } else if eq(&args[i], "idletime") && i + 1 < args.len() && freq.is_none() {
            let Some(v) = crate::util::parse_i64(&args[i + 1]) else { return err_int() };
            if v < 0 {
                return Frame::err("ERR Invalid IDLETIME value, must be >= 0");
            }
            idle = Some(v);
            i += 2;
        } else if eq(&args[i], "freq") && i + 1 < args.len() && idle.is_none() {
            let Some(v) = crate::util::parse_i64(&args[i + 1]) else { return err_int() };
            if !(0..=255).contains(&v) {
                return Frame::err("ERR Invalid FREQ value, must be >= 0 and <= 255");
            }
            freq = Some(v);
            i += 2;
        } else {
            return err_syntax();
        }
    }
    if ttl < 0 {
        return Frame::err("ERR Invalid TTL value, must be >= 0");
    }
    let db = ctx.db;
    if !replace && ctx.s.lookup_write(db, key).is_some() {
        return Frame::err("BUSYKEY Target key name already exists.");
    }
    let v = match crate::rdb::restore_payload(&args[3]) {
        Ok(v) => v,
        Err(e) => return Frame::err(e),
    };
    if ttl > 0 && !absttl {
        ttl += ctx.s.now_ms();
    }
    let deleted = replace && ctx.s.db_delete_raw(db, key);
    if ttl > 0 && ttl <= ctx.s.now_ms() {
        // Already expired: behave like the key was set and immediately expired.
        if deleted {
            ctx.modified(key);
            ctx.notify(NOTIFY_GENERIC, "del", key);
            ctx.dirty(1);
            ctx.propagate_as(vec![Bytes::from_static(b"DEL"), key.clone()]);
        }
        return Frame::ok();
    }
    let has_hfe = matches!(&v, crate::value::Value::Hash(h) if h.ttl.is_some());
    ctx.s.db_add(db, key.clone(), v);
    if ttl > 0 {
        ctx.s.set_expire(db, key, ttl);
    }
    if has_hfe {
        ctx.s.dbs[db].hexpires.insert(key.clone(), ());
    }
    if let Some(o) = ctx.s.dbs[db].dict.get_mut(key) {
        if let Some(idle) = idle {
            o.lru = crate::server::lru_clock().wrapping_sub(idle as u32) & ((1 << 24) - 1);
        }
        if let Some(f) = freq {
            o.lru = (o.lru & !255) | f as u32;
        }
    }
    ctx.modified(key);
    ctx.notify(NOTIFY_GENERIC, "restore", key);
    ctx.dirty(1);
    Frame::ok()
}

pub fn migrate_keys(args: &[Bytes]) -> Vec<usize> {
    // MIGRATE host port key|"" db timeout [COPY] [REPLACE] [AUTH pw] [AUTH2 u p] [KEYS k...]
    if args.len() > 3 && !args[3].is_empty() {
        return vec![3];
    }
    let mut i = 6;
    while i < args.len() {
        if eq(&args[i], "keys") {
            return (i + 1..args.len()).collect();
        }
        if eq(&args[i], "auth") {
            i += 1;
        } else if eq(&args[i], "auth2") {
            i += 2;
        }
        i += 1;
    }
    vec![]
}

pub fn migrate(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    use std::io::{Read, Write};
    let host = String::from_utf8_lossy(&args[1]).into_owned();
    let Some(port) = crate::util::parse_i64(&args[2]) else { return err_int() };
    let Some(dbid) = crate::util::parse_i64(&args[4]) else { return err_int() };
    let Some(timeout) = crate::util::parse_i64(&args[5]) else { return err_int() };
    let timeout = if timeout <= 0 { 1000 } else { timeout };
    let (mut copy, mut replace) = (false, false);
    let mut auth: Option<(Option<Bytes>, Bytes)> = None;
    let mut keys: Vec<Bytes> = Vec::new();
    let mut i = 6;
    while i < args.len() {
        if eq(&args[i], "copy") {
            copy = true;
        } else if eq(&args[i], "replace") {
            replace = true;
        } else if eq(&args[i], "auth") && i + 1 < args.len() {
            auth = Some((None, args[i + 1].clone()));
            i += 1;
        } else if eq(&args[i], "auth2") && i + 2 < args.len() {
            auth = Some((Some(args[i + 1].clone()), args[i + 2].clone()));
            i += 2;
        } else if eq(&args[i], "keys") {
            if !args[3].is_empty() {
                return Frame::err("ERR When using MIGRATE KEYS option, the key argument must be set to the empty string");
            }
            keys.extend(args[i + 1..].iter().cloned());
            break;
        } else {
            return err_syntax();
        }
        i += 1;
    }
    if keys.is_empty() {
        keys.push(args[3].clone());
    }
    let db = ctx.db;
    let mut payloads = Vec::new();
    for k in &keys {
        if let Some(o) = ctx.s.lookup_read(db, k) {
            let v = o.v.clone();
            let ttl = ctx.s.get_expire(db, k).map(|w| (w - ctx.s.now_ms()).max(1)).unwrap_or(0);
            payloads.push((k.clone(), ttl, crate::rdb::dump_payload(&v)));
        }
    }
    if payloads.is_empty() {
        return Frame::Simple("NOKEY".into());
    }
    let addr = format!("{}:{}", host, port);
    let to = std::time::Duration::from_millis(timeout as u64);
    let sockaddr = match std::net::ToSocketAddrs::to_socket_addrs(&addr).ok().and_then(|mut a| a.next()) {
        Some(a) => a,
        None => return Frame::err(format!("IOERR error or timeout connecting to the client")),
    };
    let mut sock = match std::net::TcpStream::connect_timeout(&sockaddr, to) {
        Ok(s) => s,
        Err(_) => return Frame::err("IOERR error or timeout connecting to the client"),
    };
    let _ = sock.set_read_timeout(Some(to));
    let _ = sock.set_write_timeout(Some(to));
    let mut buf = Vec::new();
    let enc = |parts: Vec<Bytes>, out: &mut Vec<u8>| {
        Frame::Array(parts.into_iter().map(Frame::Bulk).collect()).encode(2, out);
    };
    let mut expected = 0;
    if let Some((u, p)) = &auth {
        let mut v = vec![Bytes::from_static(b"AUTH")];
        if let Some(u) = u {
            v.push(u.clone());
        }
        v.push(p.clone());
        enc(v, &mut buf);
        expected += 1;
    }
    enc(vec![Bytes::from_static(b"SELECT"), Bytes::from(dbid.to_string())], &mut buf);
    expected += 1;
    for (k, ttl, p) in &payloads {
        let mut v = vec![
            Bytes::from_static(if ctx.s.cluster.is_some() { b"RESTORE-ASKING" } else { b"RESTORE" }),
            k.clone(),
            Bytes::from(ttl.to_string()),
            Bytes::from(p.clone()),
        ];
        if replace {
            v.push(Bytes::from_static(b"REPLACE"));
        }
        enc(v, &mut buf);
        expected += 1;
    }
    if sock.write_all(&buf).is_err() {
        return Frame::err("IOERR error or timeout writing to target instance");
    }
    // Read replies line-by-line (all are simple strings or errors).
    let mut replies = Vec::new();
    let mut acc = Vec::new();
    let mut tmp = [0u8; 4096];
    while replies.len() < expected {
        while let Some(pos) = acc.windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&acc[..pos]).into_owned();
            acc.drain(..pos + 2);
            replies.push(line);
            if replies.len() >= expected {
                break;
            }
        }
        if replies.len() >= expected {
            break;
        }
        match sock.read(&mut tmp) {
            Ok(0) | Err(_) => return Frame::err("IOERR error or timeout reading to target instance"),
            Ok(n) => acc.extend_from_slice(&tmp[..n]),
        }
    }
    let skip = expected - payloads.len();
    for r in &replies[..skip] {
        if let Some(e) = r.strip_prefix('-') {
            return Frame::err(format!("ERR Target instance replied with error: {}", e));
        }
    }
    let mut first_err = None;
    let mut del = vec![Bytes::from_static(b"DEL")];
    for (r, (k, _, _)) in replies[skip..].iter().zip(&payloads) {
        if let Some(e) = r.strip_prefix('-') {
            if first_err.is_none() {
                first_err = Some(e.to_string());
            }
        } else if !copy {
            ctx.s.db_delete(db, k);
            ctx.notify(NOTIFY_GENERIC, "del", k);
            ctx.dirty(1);
            del.push(k.clone());
        }
    }
    if del.len() > 1 {
        ctx.propagate_as(del);
    } else {
        ctx.no_prop = true;
    }
    if let Some(e) = first_err {
        return Frame::err(format!("ERR Target instance replied with error: {}", e));
    }
    Frame::ok()
}
