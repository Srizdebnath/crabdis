//! String commands.

use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use crate::value::Value;
use bytes::{Bytes, BytesMut};

fn is_str(v: &Value) -> bool {
    matches!(v, Value::Str(_))
}

pub fn get_str(ctx: &mut Ctx, key: &[u8]) -> Result<Option<Bytes>, Frame> {
    match ctx.s.get_type(ctx.db, key, is_str)? {
        Some(o) => match &o.v {
            Value::Str(b) => Ok(Some(b.clone())),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

pub fn check_len(ctx: &Ctx, len: usize) -> Result<(), Frame> {
    if len > ctx.s.cfg.proto_max_bulk_len {
        Err(Frame::err("ERR string exceeds maximum allowed size (proto-max-bulk-len)"))
    } else {
        Ok(())
    }
}

pub fn get(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match get_str(ctx, &args[1]) {
        Ok(Some(b)) => Frame::Bulk(b),
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

#[derive(Default)]
struct SetOpts {
    nx: bool,
    xx: bool,
    get: bool,
    keepttl: bool,
    expire: Option<(i64, bool, bool)>, // (value, is_ms, is_abs)
}

fn parse_set_opts(args: &[Bytes], start: usize, cmd: &str) -> Result<SetOpts, Frame> {
    let mut o = SetOpts::default();
    let mut i = start;
    while i < args.len() {
        let a = &args[i];
        let has_next = i + 1 < args.len();
        if eq(a, "nx") && !o.xx {
            o.nx = true;
        } else if eq(a, "xx") && !o.nx {
            o.xx = true;
        } else if eq(a, "get") {
            o.get = true;
        } else if eq(a, "keepttl") && o.expire.is_none() {
            o.keepttl = true;
        } else if (eq(a, "ex") || eq(a, "px") || eq(a, "exat") || eq(a, "pxat"))
            && !o.keepttl
            && o.expire.is_none()
            && has_next
        {
            let v = crate::util::parse_i64(&args[i + 1]).ok_or_else(err_int)?;
            let ms = eq(a, "px") || eq(a, "pxat");
            let abs = eq(a, "exat") || eq(a, "pxat");
            o.expire = Some((v, ms, abs));
            i += 1;
        } else {
            return Err(err_syntax());
        }
        i += 1;
    }
    if let Some((v, ms, _)) = o.expire {
        if v <= 0 || (!ms && v > i64::MAX / 1000) {
            return Err(Frame::err(format!("ERR invalid expire time in '{}' command", cmd)));
        }
    }
    Ok(o)
}

fn abs_expire(now: i64, v: i64, ms: bool, abs: bool) -> Option<i64> {
    let mut when = if ms { v } else { v.checked_mul(1000)? };
    if !abs {
        when = when.checked_add(now)?;
    }
    Some(when)
}

/// Core SET implementation used by SET/SETEX/PSETEX/SETNX.
fn set_generic(ctx: &mut Ctx, key: &Bytes, val: &Bytes, o: SetOpts, cmd: &str) -> Frame {
    let db = ctx.db;
    let mut old = None;
    if o.get {
        match get_str(ctx, key) {
            Ok(v) => old = v,
            Err(e) => return e,
        }
    }
    let exists = ctx.s.lookup_write(db, key).is_some();
    if (o.nx && exists) || (o.xx && !exists) {
        return if o.get { opt_bulk(old) } else { Frame::Null };
    }
    let when = match o.expire {
        Some((v, ms, abs)) => match abs_expire(ctx.s.now_ms(), v, ms, abs) {
            Some(w) => Some(w),
            None => return Frame::err(format!("ERR invalid expire time in '{}' command", cmd)),
        },
        None => None,
    };
    ctx.s.set_key(db, key.clone(), Value::Str(val.clone()), o.keepttl, true);
    ctx.dirty(1);
    ctx.notify(NOTIFY_STRING, "set", key);
    if let Some(w) = when {
        ctx.s.set_expire(db, key, w);
        ctx.notify(NOTIFY_GENERIC, "expire", key);
        // Propagate with an absolute time so replicas/AOF agree.
        let mut argv = vec![Bytes::from_static(b"SET"), key.clone(), val.clone(), Bytes::from_static(b"PXAT"), Bytes::from(w.to_string())];
        if o.get {
            argv.push(Bytes::from_static(b"GET"));
        }
        ctx.propagate_as(argv);
    } else if cmd != "set" {
        ctx.propagate_as(vec![Bytes::from_static(b"SET"), key.clone(), val.clone()]);
    }
    if o.get { opt_bulk(old) } else { Frame::ok() }
}

pub fn set(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = tri!(parse_set_opts(args, 3, "set"));
    set_generic(ctx, &args[1], &args[2], o, "set")
}

pub fn setnx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = SetOpts { nx: true, ..Default::default() };
    match set_generic(ctx, &args[1], &args[2], o, "setnx") {
        Frame::Null => Frame::Integer(0),
        Frame::Error(e) => Frame::Error(e),
        _ => Frame::Integer(1),
    }
}

fn setex_generic(ctx: &mut Ctx, args: &[Bytes], ms: bool) -> Frame {
    let cmd = if ms { "psetex" } else { "setex" };
    let Some(v) = crate::util::parse_i64(&args[2]) else { return err_int() };
    if v <= 0 || (!ms && v > i64::MAX / 1000) {
        return Frame::err(format!("ERR invalid expire time in '{}' command", cmd));
    }
    let o = SetOpts { expire: Some((v, ms, false)), ..Default::default() };
    set_generic(ctx, &args[1], &args[3], o, cmd)
}

pub fn setex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setex_generic(ctx, args, false)
}
pub fn psetex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setex_generic(ctx, args, true)
}

pub fn getset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let old = match get_str(ctx, &args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let db = ctx.db;
    ctx.s.set_key(db, args[1].clone(), Value::Str(args[2].clone()), false, true);
    ctx.notify(NOTIFY_STRING, "set", &args[1]);
    ctx.dirty(1);
    ctx.propagate_as(vec![Bytes::from_static(b"SET"), args[1].clone(), args[2].clone()]);
    opt_bulk(old)
}

pub fn getdel(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let old = match get_str(ctx, &args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if old.is_some() {
        let db = ctx.db;
        ctx.s.db_delete(db, &args[1]);
        ctx.notify(NOTIFY_GENERIC, "del", &args[1]);
        ctx.dirty(1);
        ctx.propagate_as(vec![Bytes::from_static(b"DEL"), args[1].clone()]);
    }
    opt_bulk(old)
}

pub fn getex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let mut expire: Option<(i64, bool, bool)> = None;
    let mut persist = false;
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        if (eq(a, "ex") || eq(a, "px") || eq(a, "exat") || eq(a, "pxat")) && expire.is_none() && !persist && i + 1 < args.len() {
            let Some(v) = crate::util::parse_i64(&args[i + 1]) else { return err_int() };
            let ms = eq(a, "px") || eq(a, "pxat");
            let abs = eq(a, "exat") || eq(a, "pxat");
            expire = Some((v, ms, abs));
            i += 2;
        } else if eq(a, "persist") && expire.is_none() && !persist {
            persist = true;
            i += 1;
        } else {
            return err_syntax();
        }
    }
    let val = match get_str(ctx, key) {
        Ok(Some(v)) => v,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };
    let db = ctx.db;
    if let Some((v, ms, abs)) = expire {
        if v <= 0 || (!ms && v > i64::MAX / 1000) {
            return Frame::err("ERR invalid expire time in 'getex' command");
        }
        let Some(when) = abs_expire(ctx.s.now_ms(), v, ms, abs) else {
            return Frame::err("ERR invalid expire time in 'getex' command");
        };
        if when <= ctx.s.now_ms() && !ctx.s.repl.is_replica() {
            ctx.s.db_delete(db, key);
            ctx.notify(NOTIFY_GENERIC, "del", key);
            ctx.dirty(1);
            ctx.propagate_as(vec![Bytes::from_static(b"DEL"), key.clone()]);
        } else {
            ctx.s.set_expire(db, key, when);
            ctx.modified(key);
            ctx.notify(NOTIFY_GENERIC, "expire", key);
            ctx.dirty(1);
            ctx.propagate_as(vec![Bytes::from_static(b"PEXPIREAT"), key.clone(), Bytes::from(when.to_string())]);
        }
    } else if persist && ctx.s.remove_expire(db, key) {
        ctx.modified(key);
        ctx.notify(NOTIFY_GENERIC, "persist", key);
        ctx.dirty(1);
        ctx.propagate_as(vec![Bytes::from_static(b"PERSIST"), key.clone()]);
    }
    Frame::Bulk(val)
}

pub fn mget(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut out = Vec::with_capacity(args.len() - 1);
    for k in &args[1..] {
        match ctx.s.lookup_read(ctx.db, k) {
            Some(o) => match &o.v {
                Value::Str(b) => out.push(Frame::Bulk(b.clone())),
                _ => out.push(Frame::Null),
            },
            None => out.push(Frame::Null),
        }
    }
    Frame::Array(out)
}

pub fn mset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 == 0 {
        return Frame::err(format!("ERR wrong number of arguments for '{}' command", ctx.cmd.name));
    }
    let db = ctx.db;
    for kv in args[1..].chunks(2) {
        ctx.s.set_key(db, kv[0].clone(), Value::Str(kv[1].clone()), false, true);
        ctx.notify(NOTIFY_STRING, "set", &kv[0]);
    }
    ctx.dirty(((args.len() - 1) / 2) as u64);
    Frame::ok()
}

pub fn msetnx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 == 0 {
        return Frame::err(format!("ERR wrong number of arguments for '{}' command", ctx.cmd.name));
    }
    let db = ctx.db;
    for kv in args[1..].chunks(2) {
        if ctx.s.lookup_write(db, &kv[0]).is_some() {
            return Frame::Integer(0);
        }
    }
    mset(ctx, args);
    Frame::Integer(1)
}

pub fn incr_generic(ctx: &mut Ctx, key: &Bytes, by: i64) -> Frame {
    let db = ctx.db;
    let cur = match get_str(ctx, key) {
        Ok(Some(b)) => match crate::util::parse_i64(&b) {
            Some(v) => v,
            None => return err_int(),
        },
        Ok(None) => 0,
        Err(e) => return e,
    };
    let Some(n) = cur.checked_add(by) else {
        return Frame::err("ERR increment or decrement would overflow");
    };
    let nb = Bytes::from(n.to_string());
    if let Some(o) = ctx.s.dbs[db].dict.get_mut(key) {
        o.v = Value::Str(nb);
        ctx.modified(key);
    } else {
        ctx.s.set_key(db, key.clone(), Value::Str(nb), false, true);
    }
    ctx.notify(NOTIFY_STRING, "incrby", key);
    ctx.dirty(1);
    Frame::Integer(n)
}

pub fn incr(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    incr_generic(ctx, &args[1], 1)
}
pub fn decr(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    incr_generic(ctx, &args[1], -1)
}
pub fn incrby(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let by = tri!(int(&args[2]));
    incr_generic(ctx, &args[1], by)
}
pub fn decrby(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let by = tri!(int(&args[2]));
    if by == i64::MIN {
        return Frame::err("ERR decrement would overflow");
    }
    incr_generic(ctx, &args[1], -by)
}

pub fn incrbyfloat(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let by = tri!(float(&args[2]));
    let cur = match get_str(ctx, key) {
        Ok(Some(b)) => match crate::util::parse_f64(&b) {
            Some(v) => v,
            None => return err_float(),
        },
        Ok(None) => 0.0,
        Err(e) => return e,
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return Frame::err("ERR increment would produce NaN or Infinity");
    }
    let nb = Bytes::from(crate::util::fmt_human(n));
    if let Some(o) = ctx.s.dbs[db].dict.get_mut(key) {
        o.v = Value::Str(nb.clone());
        ctx.modified(key);
    } else {
        ctx.s.set_key(db, key.clone(), Value::Str(nb.clone()), false, true);
    }
    ctx.notify(NOTIFY_STRING, "incrbyfloat", key);
    ctx.dirty(1);
    ctx.propagate_as(vec![Bytes::from_static(b"SET"), key.clone(), nb.clone(), Bytes::from_static(b"KEEPTTL")]);
    Frame::Bulk(nb)
}

/// Obtain a mutable byte buffer for in-place string modification.
pub fn str_mut(v: &mut Value) -> BytesMut {
    let Value::Str(b) = v else { unreachable!() };
    let b = std::mem::take(b);
    match b.try_into_mut() {
        Ok(m) => m,
        Err(b) => BytesMut::from(&b[..]),
    }
}

pub fn append(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (key, add) = (&args[1], &args[2]);
    let db = ctx.db;
    let len = match ctx.s.get_type_w(db, key, is_str) {
        Err(e) => return e,
        Ok(None) => {
            ctx.s.set_key(db, key.clone(), Value::Str(add.clone()), false, true);
            add.len()
        }
        Ok(Some(o)) => {
            let Value::Str(b) = &o.v else { unreachable!() };
            let newlen = b.len() + add.len();
            if newlen > ctx.s.cfg.proto_max_bulk_len {
                return Frame::err("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
            }
            let o = ctx.s.dbs[db].dict.get_mut(key).unwrap();
            let mut m = str_mut(&mut o.v);
            m.extend_from_slice(add);
            o.v = Value::Str(m.freeze());
            ctx.modified(key);
            newlen
        }
    };
    ctx.notify(NOTIFY_STRING, "append", key);
    ctx.dirty(1);
    Frame::Integer(len as i64)
}

pub fn strlen(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match get_str(ctx, &args[1]) {
        Ok(Some(b)) => Frame::Integer(b.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

pub fn getrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let start = tri!(int(&args[2]));
    let end = tri!(int(&args[3]));
    let b = match get_str(ctx, &args[1]) {
        Ok(Some(b)) => b,
        Ok(None) => return Frame::Bulk(Bytes::new()),
        Err(e) => return e,
    };
    if start < 0 && end < 0 && start > end {
        return Frame::Bulk(Bytes::new());
    }
    match norm_range(start, end, b.len()) {
        Some((s, e)) => Frame::Bulk(b.slice(s..=e)),
        None => Frame::Bulk(Bytes::new()),
    }
}

pub fn setrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let Some(off) = crate::util::parse_i64(&args[2]) else { return err_int() };
    if off < 0 {
        return Frame::err("ERR offset is out of range");
    }
    let val = &args[3];
    let db = ctx.db;
    let off = off as usize;
    match ctx.s.get_type_w(db, key, is_str) {
        Err(e) => e,
        Ok(None) => {
            if val.is_empty() {
                return Frame::Integer(0);
            }
            tri!(check_len(ctx, off + val.len()));
            let mut v = vec![0u8; off + val.len()];
            v[off..].copy_from_slice(val);
            ctx.s.set_key(db, key.clone(), Value::Str(Bytes::from(v)), false, true);
            ctx.notify(NOTIFY_STRING, "setrange", key);
            ctx.dirty(1);
            Frame::Integer((off + val.len()) as i64)
        }
        Ok(Some(o)) => {
            let Value::Str(b) = &o.v else { unreachable!() };
            let curlen = b.len();
            if val.is_empty() {
                return Frame::Integer(curlen as i64);
            }
            tri!(check_len(ctx, off + val.len()));
            let o = ctx.s.dbs[db].dict.get_mut(key).unwrap();
            let mut m = str_mut(&mut o.v);
            if m.len() < off + val.len() {
                m.resize(off + val.len(), 0);
            }
            m[off..off + val.len()].copy_from_slice(val);
            let n = m.len();
            o.v = Value::Str(m.freeze());
            ctx.modified(key);
            ctx.notify(NOTIFY_STRING, "setrange", key);
            ctx.dirty(1);
            Frame::Integer(n as i64)
        }
    }
}

pub fn lcs(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (mut getlen, mut getidx, mut withmatchlen) = (false, false, false);
    let mut minmatchlen: i64 = 0;
    let mut i = 3;
    while i < args.len() {
        let a = &args[i];
        if eq(a, "idx") {
            getidx = true;
        } else if eq(a, "len") {
            getlen = true;
        } else if eq(a, "withmatchlen") {
            withmatchlen = true;
        } else if eq(a, "minmatchlen") && i + 1 < args.len() {
            minmatchlen = tri!(int(&args[i + 1]));
            if minmatchlen < 0 {
                minmatchlen = 0;
            }
            i += 1;
        } else {
            return err_syntax();
        }
        i += 1;
    }
    let a = match ctx.s.get_type(ctx.db, &args[1], is_str) {
        Ok(Some(o)) => match &o.v {
            Value::Str(b) => b.clone(),
            _ => unreachable!(),
        },
        Ok(None) => Bytes::new(),
        Err(_) => return Frame::err("ERR The specified keys must contain string values"),
    };
    let b = match ctx.s.get_type(ctx.db, &args[2], is_str) {
        Ok(Some(o)) => match &o.v {
            Value::Str(b) => b.clone(),
            _ => unreachable!(),
        },
        Ok(None) => Bytes::new(),
        Err(_) => return Frame::err("ERR The specified keys must contain string values"),
    };
    if getidx && getlen {
        return Frame::err("ERR If you want both the length and indexes, please just use IDX.");
    }
    let (alen, blen) = (a.len(), b.len());
    if (alen + 1).checked_mul(blen + 1).is_none_or(|n| n > 1 << 32) {
        return Frame::err("ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
    }
    let w = blen + 1;
    let mut dp = vec![0u32; (alen + 1) * w];
    for i in 1..=alen {
        for j in 1..=blen {
            dp[i * w + j] = if a[i - 1] == b[j - 1] {
                dp[(i - 1) * w + j - 1] + 1
            } else {
                dp[(i - 1) * w + j].max(dp[i * w + j - 1])
            };
        }
    }
    let idx = dp[alen * w + blen] as usize;
    let mut result = vec![0u8; idx];
    let mut matches = Vec::new();
    let (mut ai, mut bi) = (alen, blen);
    let mut k = idx;
    let (mut arange_start, mut arange_end, mut brange_start, mut brange_end) = (alen, 0usize, 0usize, 0usize);
    let computelcs = getidx || !getlen;
    while computelcs && ai > 0 && bi > 0 {
        let mut emit_range = false;
        if a[ai - 1] == b[bi - 1] {
            result[k - 1] = a[ai - 1];
            if arange_start == alen {
                arange_start = ai - 1;
                arange_end = ai - 1;
                brange_start = bi - 1;
                brange_end = bi - 1;
            } else if arange_start == ai && brange_start == bi {
                arange_start -= 1;
                brange_start -= 1;
            } else {
                emit_range = true;
            }
            if arange_start == 0 || brange_start == 0 {
                emit_range = true;
            }
            k -= 1;
            ai -= 1;
            bi -= 1;
        } else {
            let lcs1 = dp[(ai - 1) * w + bi];
            let lcs2 = dp[ai * w + bi - 1];
            if lcs1 > lcs2 {
                ai -= 1;
            } else {
                bi -= 1;
            }
            if arange_start != alen {
                emit_range = true;
            }
        }
        if emit_range {
            let mlen = arange_end - arange_start + 1;
            if minmatchlen == 0 || mlen as i64 >= minmatchlen {
                let mut m = vec![
                    Frame::Array(vec![Frame::Integer(arange_start as i64), Frame::Integer(arange_end as i64)]),
                    Frame::Array(vec![Frame::Integer(brange_start as i64), Frame::Integer(brange_end as i64)]),
                ];
                if withmatchlen {
                    m.push(Frame::Integer(mlen as i64));
                }
                matches.push(Frame::Array(m));
            }
            arange_start = alen;
            // Re-check current position if it was a match that started a new range.
            if ai > 0 && bi > 0 && a[ai] == b[bi] && false {
                unreachable!();
            }
        }
    }
    if getidx {
        Frame::Map(vec![
            (Frame::bulk_str("matches"), Frame::Array(matches)),
            (Frame::bulk_str("len"), Frame::Integer(idx as i64)),
        ])
    } else if getlen {
        Frame::Integer(idx as i64)
    } else {
        Frame::Bulk(Bytes::from(result))
    }
}
