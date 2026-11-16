//! List commands, including blocking variants.

use super::util::*;
use crate::blocking::{BlockSpec, parse_timeout};
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use crate::value::Value;
use bytes::Bytes;
use std::collections::VecDeque;

fn is_list(v: &Value) -> bool {
    matches!(v, Value::List(_))
}

fn list_mut<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut VecDeque<Bytes>>, Frame> {
    match ctx.s.get_type_w(ctx.db, key, is_list)? {
        Some(o) => match &mut o.v {
            Value::List(l) => Ok(Some(l)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn list_ref<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut VecDeque<Bytes>>, Frame> {
    match ctx.s.get_type(ctx.db, key, is_list)? {
        Some(o) => match &mut o.v {
            Value::List(l) => Ok(Some(l)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn push_generic(ctx: &mut Ctx, args: &[Bytes], left: bool, xx: bool) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let len = match list_mut(ctx, key) {
        Err(e) => return e,
        Ok(Some(l)) => {
            for v in &args[2..] {
                if left {
                    l.push_front(v.clone());
                } else {
                    l.push_back(v.clone());
                }
            }
            let n = l.len();
            ctx.s.signal_ready(db, key);
            n
        }
        Ok(None) => {
            if xx {
                return Frame::Integer(0);
            }
            let mut l = VecDeque::with_capacity(args.len() - 2);
            for v in &args[2..] {
                if left {
                    l.push_front(v.clone());
                } else {
                    l.push_back(v.clone());
                }
            }
            let n = l.len();
            ctx.s.db_add(db, key.clone(), Value::List(l));
            n
        }
    };
    ctx.modified(key);
    ctx.notify(NOTIFY_LIST, if left { "lpush" } else { "rpush" }, key);
    ctx.dirty((args.len() - 2) as u64);
    Frame::Integer(len as i64)
}

pub fn lpush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    push_generic(ctx, args, true, false)
}
pub fn rpush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    push_generic(ctx, args, false, false)
}
pub fn lpushx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    push_generic(ctx, args, true, true)
}
pub fn rpushx(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    push_generic(ctx, args, false, true)
}

/// Pop up to `count` elements; handles notifications and empty-key deletion.
fn pop_n(ctx: &mut Ctx, key: &Bytes, left: bool, count: usize) -> Result<Option<Vec<Bytes>>, Frame> {
    let db = ctx.db;
    let out = match list_mut(ctx, key)? {
        None => return Ok(None),
        Some(l) => {
            let n = count.min(l.len());
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(if left { l.pop_front().unwrap() } else { l.pop_back().unwrap() });
            }
            out
        }
    };
    ctx.notify(NOTIFY_LIST, if left { "lpop" } else { "rpop" }, key);
    ctx.s.delete_if_empty(db, key);
    ctx.modified(key);
    ctx.dirty(out.len() as u64);
    Ok(Some(out))
}

fn pop_generic(ctx: &mut Ctx, args: &[Bytes], left: bool) -> Frame {
    if args.len() > 3 {
        return Frame::err(format!("ERR wrong number of arguments for '{}' command", ctx.cmd.name));
    }
    let count = if args.len() == 3 {
        match crate::util::parse_i64(&args[2]) {
            Some(v) if v >= 0 => Some(v as usize),
            _ => return Frame::err("ERR value is out of range, must be positive"),
        }
    } else {
        None
    };
    let key = &args[1];
    match count {
        None => match pop_n(ctx, key, left, 1) {
            Err(e) => e,
            Ok(None) => Frame::Null,
            Ok(Some(mut v)) => Frame::Bulk(v.pop().unwrap()),
        },
        Some(0) => match list_ref(ctx, key) {
            Err(e) => e,
            Ok(None) => Frame::NullArray,
            Ok(Some(_)) => Frame::Array(vec![]),
        },
        Some(c) => match pop_n(ctx, key, left, c) {
            Err(e) => e,
            Ok(None) => Frame::NullArray,
            Ok(Some(v)) => Frame::array_of_bulks(v),
        },
    }
}

pub fn lpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    pop_generic(ctx, args, true)
}
pub fn rpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    pop_generic(ctx, args, false)
}

pub fn llen(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match list_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(l)) => Frame::Integer(l.len() as i64),
    }
}

pub fn lindex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let idx = tri!(int(&args[2]));
    match list_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Null,
        Ok(Some(l)) => {
            let i = if idx < 0 { l.len() as i64 + idx } else { idx };
            if i < 0 || i as usize >= l.len() { Frame::Null } else { Frame::Bulk(l[i as usize].clone()) }
        }
    }
}

pub fn lset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let idx = tri!(int(&args[2]));
    let key = &args[1];
    match list_mut(ctx, key) {
        Err(e) => e,
        Ok(None) => err_nokey(),
        Ok(Some(l)) => {
            let i = if idx < 0 { l.len() as i64 + idx } else { idx };
            if i < 0 || i as usize >= l.len() {
                return Frame::err("ERR index out of range");
            }
            l[i as usize] = args[3].clone();
            ctx.modified(key);
            ctx.notify(NOTIFY_LIST, "lset", key);
            ctx.dirty(1);
            Frame::ok()
        }
    }
}

pub fn lrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let s = tri!(int(&args[2]));
    let e = tri!(int(&args[3]));
    match list_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Array(vec![]),
        Ok(Some(l)) => match norm_range(s, e, l.len()) {
            None => Frame::Array(vec![]),
            Some((s, e)) => Frame::Array(l.range(s..=e).map(|v| Frame::Bulk(v.clone())).collect()),
        },
    }
}

pub fn ltrim(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let s = tri!(int(&args[2]));
    let e = tri!(int(&args[3]));
    let key = &args[1];
    let db = ctx.db;
    match list_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::ok(),
        Ok(Some(l)) => match norm_range(s, e, l.len()) {
            None => l.clear(),
            Some((s, e)) => {
                l.truncate(e + 1);
                l.drain(..s);
            }
        },
    }
    ctx.notify(NOTIFY_LIST, "ltrim", key);
    ctx.s.delete_if_empty(db, key);
    ctx.modified(key);
    ctx.dirty(1);
    Frame::ok()
}

pub fn lrem(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let count = tri!(int(&args[2]));
    let key = &args[1];
    let val = &args[3];
    let db = ctx.db;
    let removed = match list_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(l)) => {
            let mut removed = 0i64;
            let limit = if count == 0 { i64::MAX } else { count.abs() };
            if count >= 0 {
                let mut i = 0;
                while i < l.len() && removed < limit {
                    if l[i] == *val {
                        l.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                let mut i = l.len();
                while i > 0 && removed < limit {
                    i -= 1;
                    if l[i] == *val {
                        l.remove(i);
                        removed += 1;
                    }
                }
            }
            removed
        }
    };
    if removed > 0 {
        ctx.notify(NOTIFY_LIST, "lrem", key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(removed as u64);
    }
    Frame::Integer(removed)
}

pub fn linsert(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let after = if eq(&args[2], "after") {
        true
    } else if eq(&args[2], "before") {
        false
    } else {
        return err_syntax();
    };
    let key = &args[1];
    match list_mut(ctx, key) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(l)) => {
            let Some(pos) = l.iter().position(|v| *v == args[3]) else { return Frame::Integer(-1) };
            l.insert(if after { pos + 1 } else { pos }, args[4].clone());
            let n = l.len();
            ctx.modified(key);
            ctx.notify(NOTIFY_LIST, "linsert", key);
            ctx.dirty(1);
            Frame::Integer(n as i64)
        }
    }
}

pub fn lpos(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: i64 = 0;
    let mut i = 3;
    while i < args.len() {
        if i + 1 >= args.len() {
            return err_syntax();
        }
        let v = tri!(int(&args[i + 1]));
        if eq(&args[i], "rank") {
            if v == 0 {
                return Frame::err("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
            }
            if v == i64::MIN {
                return Frame::err("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807");
            }
            rank = v;
        } else if eq(&args[i], "count") {
            if v < 0 {
                return Frame::err("ERR COUNT can't be negative");
            }
            count = Some(v);
        } else if eq(&args[i], "maxlen") {
            if v < 0 {
                return Frame::err("ERR MAXLEN can't be negative");
            }
            maxlen = v;
        } else {
            return err_syntax();
        }
        i += 2;
    }
    let l = match list_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return if count.is_some() { Frame::Array(vec![]) } else { Frame::Null },
        Ok(Some(l)) => l,
    };
    let want = count.map(|c| if c == 0 { usize::MAX } else { c as usize }).unwrap_or(1);
    let mut skip = rank.unsigned_abs() - 1;
    let mut found = Vec::new();
    let n = l.len();
    let limit = if maxlen == 0 { n } else { (maxlen as usize).min(n) };
    for step in 0..limit {
        let idx = if rank > 0 { step } else { n - 1 - step };
        if l[idx] == args[2] {
            if skip > 0 {
                skip -= 1;
                continue;
            }
            found.push(idx as i64);
            if found.len() >= want {
                break;
            }
        }
    }
    if count.is_some() {
        Frame::Array(found.into_iter().map(Frame::Integer).collect())
    } else {
        found.first().map(|&v| Frame::Integer(v)).unwrap_or(Frame::Null)
    }
}

fn parse_where(a: &[u8]) -> Option<bool> {
    if eq(a, "left") {
        Some(true)
    } else if eq(a, "right") {
        Some(false)
    } else {
        None
    }
}

/// Core of LMOVE: returns Ok(None) if source missing.
fn lmove_core(ctx: &mut Ctx, src: &Bytes, dst: &Bytes, from_left: bool, to_left: bool) -> Result<Option<Bytes>, Frame> {
    let db = ctx.db;
    // Type-check destination first (Redis checks it before popping).
    if list_ref(ctx, src)?.map(|l| l.is_empty()).unwrap_or(true) {
        return Ok(None);
    }
    match ctx.s.get_type_w(db, dst, is_list) {
        Err(e) => return Err(e),
        Ok(_) => {}
    }
    let v = {
        let l = list_mut(ctx, src)?.unwrap();
        if from_left { l.pop_front().unwrap() } else { l.pop_back().unwrap() }
    };
    ctx.notify(NOTIFY_LIST, if from_left { "lpop" } else { "rpop" }, src);
    // If src == dst and became empty, re-add instead of delete.
    let src_empty = ctx.s.dbs[db].dict.get(src).map(|o| o.v.is_empty_collection()).unwrap_or(false);
    if src_empty && src != dst {
        ctx.s.delete_if_empty(db, src);
    }
    ctx.modified(src);
    match list_mut(ctx, dst)? {
        Some(l) => {
            if to_left {
                l.push_front(v.clone());
            } else {
                l.push_back(v.clone());
            }
            ctx.s.signal_ready(db, dst);
        }
        None => {
            let mut l = VecDeque::new();
            l.push_back(v.clone());
            ctx.s.db_add(db, dst.clone(), Value::List(l));
        }
    }
    ctx.modified(dst);
    ctx.notify(NOTIFY_LIST, if to_left { "lpush" } else { "rpush" }, dst);
    ctx.dirty(1);
    Ok(Some(v))
}

pub fn lmove(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (Some(fl), Some(tl)) = (parse_where(&args[3]), parse_where(&args[4])) else { return err_syntax() };
    match lmove_core(ctx, &args[1], &args[2], fl, tl) {
        Err(e) => e,
        Ok(v) => opt_bulk(v),
    }
}

pub fn rpoplpush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match lmove_core(ctx, &args[1], &args[2], false, true) {
        Err(e) => e,
        Ok(v) => opt_bulk(v),
    }
}

fn mpop_generic(ctx: &mut Ctx, args: &[Bytes], numkeys_idx: usize, block_deadline: Option<Option<i64>>) -> Frame {
    let nk = match crate::util::parse_i64(&args[numkeys_idx]) {
        Some(v) if v > 0 => v as usize,
        _ => return Frame::err("ERR numkeys should be greater than 0"),
    };
    if args.len() < numkeys_idx + 1 + nk + 1 {
        return err_syntax();
    }
    let keys = &args[numkeys_idx + 1..numkeys_idx + 1 + nk];
    let Some(left) = parse_where(&args[numkeys_idx + 1 + nk]) else { return err_syntax() };
    let mut count = 1usize;
    let mut i = numkeys_idx + 2 + nk;
    let mut count_set = false;
    while i < args.len() {
        if eq(&args[i], "count") && i + 1 < args.len() && !count_set {
            match crate::util::parse_i64(&args[i + 1]) {
                Some(v) if v > 0 => count = v as usize,
                _ => return Frame::err("ERR count should be greater than 0"),
            }
            count_set = true;
            i += 2;
        } else {
            return err_syntax();
        }
    }
    for k in keys {
        match list_ref(ctx, k) {
            Err(e) => return e,
            Ok(Some(l)) if !l.is_empty() => {}
            _ => continue,
        }
        let v = match pop_n(ctx, k, left, count) {
            Err(e) => return e,
            Ok(v) => v.unwrap(),
        };
        let mut argv = vec![Bytes::from_static(if left { b"LPOP" } else { b"RPOP" }), k.clone(), Bytes::from(v.len().to_string())];
        if block_deadline.is_none() {
            // LMPOP propagates as itself is fine, but rewrite for determinism.
            argv = std::mem::take(&mut argv);
        }
        ctx.propagate_as(argv);
        return Frame::Array(vec![Frame::Bulk(k.clone()), Frame::array_of_bulks(v)]);
    }
    match block_deadline {
        Some(deadline) if ctx.can_block() => BlockSpec::keys(keys.to_vec(), deadline, Frame::NullArray),
        _ => Frame::NullArray,
    }
}

pub fn lmpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    mpop_generic(ctx, args, 1, None)
}

pub fn blmpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    let deadline = tri!(parse_timeout(&args[1], now));
    if ctx.flags & crate::exec::CTX_REPROCESS != 0 {
        // Keep original deadline when re-executed.
        return mpop_generic(ctx, args, 2, Some(None));
    }
    mpop_generic(ctx, args, 2, Some(deadline))
}

fn bpop_generic(ctx: &mut Ctx, args: &[Bytes], left: bool) -> Frame {
    let now = ctx.s.now_ms();
    let deadline = tri!(parse_timeout(&args[args.len() - 1], now));
    let keys = &args[1..args.len() - 1];
    for k in keys {
        match list_ref(ctx, k) {
            Err(e) => return e,
            Ok(Some(l)) if !l.is_empty() => {
                let v = pop_n(ctx, k, left, 1).unwrap().unwrap();
                ctx.propagate_as(vec![Bytes::from_static(if left { b"LPOP" } else { b"RPOP" }), k.clone()]);
                return Frame::Array(vec![Frame::Bulk(k.clone()), Frame::Bulk(v[0].clone())]);
            }
            _ => {}
        }
    }
    if !ctx.can_block() {
        return Frame::NullArray;
    }
    BlockSpec::keys(keys.to_vec(), deadline, Frame::NullArray)
}

pub fn blpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bpop_generic(ctx, args, true)
}
pub fn brpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bpop_generic(ctx, args, false)
}

fn blmove_generic(ctx: &mut Ctx, src: &Bytes, dst: &Bytes, fl: bool, tl: bool, timeout: &Bytes) -> Frame {
    let now = ctx.s.now_ms();
    let deadline = tri!(parse_timeout(timeout, now));
    match lmove_core(ctx, src, dst, fl, tl) {
        Err(e) => e,
        Ok(Some(v)) => {
            ctx.propagate_as(vec![
                Bytes::from_static(b"LMOVE"),
                src.clone(),
                dst.clone(),
                Bytes::from_static(if fl { b"LEFT" } else { b"RIGHT" }),
                Bytes::from_static(if tl { b"LEFT" } else { b"RIGHT" }),
            ]);
            Frame::Bulk(v)
        }
        Ok(None) => {
            if !ctx.can_block() {
                return Frame::Null;
            }
            BlockSpec::keys(vec![src.clone()], deadline, Frame::Null)
        }
    }
}

pub fn blmove(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (Some(fl), Some(tl)) = (parse_where(&args[3]), parse_where(&args[4])) else { return err_syntax() };
    blmove_generic(ctx, &args[1], &args[2], fl, tl, &args[5])
}

pub fn brpoplpush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    blmove_generic(ctx, &args[1], &args[2], false, true, &args[3])
}
