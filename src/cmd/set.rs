//! Set commands.

use super::util::*;
use crate::config::*;
use crate::dict::Dict;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use crate::value::Value;
use bytes::Bytes;

fn is_set(v: &Value) -> bool {
    matches!(v, Value::Set(_))
}

fn set_ref<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut Dict<()>>, Frame> {
    match ctx.s.get_type(ctx.db, key, is_set)? {
        Some(o) => match &mut o.v {
            Value::Set(s) => Ok(Some(s)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn set_mut<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut Dict<()>>, Frame> {
    match ctx.s.get_type_w(ctx.db, key, is_set)? {
        Some(o) => match &mut o.v {
            Value::Set(s) => Ok(Some(s)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

pub fn sadd(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    if tri!(set_mut(ctx, key)).is_none() {
        ctx.s.db_add(db, key.clone(), Value::Set(Box::new(Dict::new())));
    }
    let s = set_mut(ctx, key).unwrap().unwrap();
    let mut added = 0;
    for m in &args[2..] {
        if s.insert(m.clone(), ()).is_none() {
            added += 1;
        }
    }
    if added > 0 {
        ctx.modified(key);
        ctx.notify(NOTIFY_SET, "sadd", key);
        ctx.dirty(added);
    }
    Frame::Integer(added as i64)
}

pub fn srem(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let n = match set_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(s)) => args[2..].iter().filter(|m| s.remove(m).is_some()).count(),
    };
    if n > 0 {
        ctx.notify(NOTIFY_SET, "srem", key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(n as u64);
    }
    Frame::Integer(n as i64)
}

pub fn scard(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match set_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(s)) => Frame::Integer(s.len() as i64),
    }
}

pub fn sismember(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match set_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(s)) => Frame::Integer(s.contains_key(&args[2]) as i64),
    }
}

pub fn smismember(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match set_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Array(args[2..].iter().map(|_| Frame::Integer(0)).collect()),
        Ok(Some(s)) => Frame::Array(args[2..].iter().map(|m| Frame::Integer(s.contains_key(m) as i64)).collect()),
    }
}

pub fn smembers(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match set_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Set(vec![]),
        Ok(Some(s)) => Frame::Set(s.keys().map(|k| Frame::Bulk(k.clone())).collect()),
    }
}

pub fn smove(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (src, dst, m) = (&args[1], &args[2], &args[3]);
    let db = ctx.db;
    let src_exists = match set_mut(ctx, src) {
        Err(e) => return e,
        Ok(s) => s.is_some(),
    };
    if let Err(e) = set_mut(ctx, dst) {
        return e;
    }
    if !src_exists {
        return Frame::Integer(0);
    }
    if src == dst {
        let has = set_ref(ctx, src).unwrap().unwrap().contains_key(m);
        return Frame::Integer(has as i64);
    }
    let removed = set_mut(ctx, src).unwrap().unwrap().remove(m).is_some();
    if !removed {
        return Frame::Integer(0);
    }
    ctx.notify(NOTIFY_SET, "srem", src);
    ctx.s.delete_if_empty(db, src);
    ctx.modified(src);
    if set_mut(ctx, dst).unwrap().is_none() {
        ctx.s.db_add(db, dst.clone(), Value::Set(Box::new(Dict::new())));
    }
    let added = set_mut(ctx, dst).unwrap().unwrap().insert(m.clone(), ()).is_none();
    ctx.modified(dst);
    if added {
        ctx.notify(NOTIFY_SET, "sadd", dst);
    }
    ctx.dirty(1);
    Frame::Integer(1)
}

pub fn spop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let count = if args.len() == 3 {
        match crate::util::parse_i64(&args[2]) {
            Some(v) if v >= 0 => Some(v as usize),
            _ => return Frame::err("ERR value is out of range, must be positive"),
        }
    } else if args.len() > 3 {
        return err_syntax();
    } else {
        None
    };
    let s = match set_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return if count.is_some() { Frame::Set(vec![]) } else { Frame::Null },
        Ok(Some(s)) => s,
    };
    let n = count.unwrap_or(1).min(s.len());
    let mut popped = Vec::with_capacity(n);
    if n == s.len() {
        popped = s.keys().cloned().collect();
        s.clear();
    } else {
        for _ in 0..n {
            let k = s.random_entry().unwrap().0.clone();
            s.remove(&k);
            popped.push(k);
        }
    }
    if popped.is_empty() {
        return if count.is_some() { Frame::Set(vec![]) } else { Frame::Null };
    }
    ctx.notify(NOTIFY_SET, "spop", key);
    let deleted = ctx.s.delete_if_empty(db, key);
    ctx.modified(key);
    ctx.dirty(popped.len() as u64);
    // Propagate deterministically: as SREM (or DEL when the whole set went away).
    if deleted && count.is_some() {
        ctx.propagate_as(vec![Bytes::from_static(b"DEL"), key.clone()]);
    } else {
        let mut argv = vec![Bytes::from_static(b"SREM"), key.clone()];
        argv.extend(popped.iter().cloned());
        ctx.propagate_as(argv);
    }
    match count {
        None => Frame::Bulk(popped.pop().unwrap()),
        Some(_) => Frame::Set(popped.into_iter().map(Frame::Bulk).collect()),
    }
}

pub fn srandmember(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let count = if args.len() == 3 {
        let Some(c) = crate::util::parse_i64(&args[2]) else { return err_int() };
        if c < -(i64::MAX / 2) || c > i64::MAX / 2 {
            return Frame::err("ERR value is out of range");
        }
        Some(c)
    } else if args.len() > 3 {
        return err_syntax();
    } else {
        None
    };
    let s = match set_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return if count.is_some() { Frame::Array(vec![]) } else { Frame::Null },
        Ok(Some(s)) => s,
    };
    match count {
        None => s.random_entry().map(|(k, _)| Frame::Bulk(k.clone())).unwrap_or(Frame::Null),
        Some(c) if c < 0 => Frame::Array((0..(-c) as usize).map(|_| Frame::Bulk(s.random_entry().unwrap().0.clone())).collect()),
        Some(c) => {
            let n = (c as usize).min(s.len());
            let mut all: Vec<Bytes> = s.keys().cloned().collect();
            for i in 0..n {
                let j = i + crate::util::rand_range(all.len() - i);
                all.swap(i, j);
            }
            all.truncate(n);
            Frame::Array(all.into_iter().map(Frame::Bulk).collect())
        }
    }
}

/// Collect sets for the given keys. Missing keys yield None.
fn gather(ctx: &mut Ctx, keys: &[Bytes], write: bool) -> Result<Vec<Option<Dict<()>>>, Frame> {
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        let r = if write { set_mut(ctx, k)? } else { set_ref(ctx, k)? };
        out.push(r.map(|s| s.clone()));
    }
    Ok(out)
}

#[derive(Clone, Copy)]
enum Op {
    Inter,
    Union,
    Diff,
}

fn compute(sets: Vec<Option<Dict<()>>>, op: Op) -> Vec<Bytes> {
    match op {
        Op::Union => {
            let mut acc: Dict<()> = Dict::new();
            for s in sets.into_iter().flatten() {
                for k in s.keys() {
                    acc.insert(k.clone(), ());
                }
            }
            acc.keys().cloned().collect()
        }
        Op::Inter => {
            if sets.iter().any(|s| s.is_none()) {
                return vec![];
            }
            let mut sets: Vec<Dict<()>> = sets.into_iter().flatten().collect();
            sets.sort_by_key(|s| s.len());
            let (first, rest) = sets.split_first().unwrap();
            first.keys().filter(|k| rest.iter().all(|s| s.contains_key(k))).cloned().collect()
        }
        Op::Diff => {
            let mut it = sets.into_iter();
            let Some(Some(first)) = it.next() else { return vec![] };
            let rest: Vec<Dict<()>> = it.flatten().collect();
            first.keys().filter(|k| !rest.iter().any(|s| s.contains_key(k))).cloned().collect()
        }
    }
}

fn setop(ctx: &mut Ctx, keys: &[Bytes], op: Op) -> Frame {
    let sets = tri!(gather(ctx, keys, false));
    Frame::Set(compute(sets, op).into_iter().map(Frame::Bulk).collect())
}

fn setop_store(ctx: &mut Ctx, dst: &Bytes, keys: &[Bytes], op: Op) -> Frame {
    let sets = tri!(gather(ctx, keys, true));
    let res = compute(sets, op);
    let db = ctx.db;
    let n = res.len();
    if n == 0 {
        if ctx.s.db_delete(db, dst) {
            ctx.notify(NOTIFY_GENERIC, "del", dst);
            ctx.dirty(1);
        }
        return Frame::Integer(0);
    }
    let mut d = Dict::with_capacity(n);
    for k in res {
        d.insert(k, ());
    }
    ctx.s.set_key(db, dst.clone(), Value::Set(Box::new(d)), false, true);
    let ev = match op {
        Op::Inter => "sinterstore",
        Op::Union => "sunionstore",
        Op::Diff => "sdiffstore",
    };
    ctx.notify(NOTIFY_SET, ev, dst);
    ctx.dirty(1);
    Frame::Integer(n as i64)
}

pub fn sinter(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop(ctx, &args[1..], Op::Inter)
}
pub fn sunion(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop(ctx, &args[1..], Op::Union)
}
pub fn sdiff(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop(ctx, &args[1..], Op::Diff)
}
pub fn sinterstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop_store(ctx, &args[1], &args[2..], Op::Inter)
}
pub fn sunionstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop_store(ctx, &args[1], &args[2..], Op::Union)
}
pub fn sdiffstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    setop_store(ctx, &args[1], &args[2..], Op::Diff)
}

pub fn sintercard(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let nk = match crate::util::parse_i64(&args[1]) {
        Some(v) if v > 0 => v as usize,
        _ => return Frame::err("ERR numkeys should be greater than 0"),
    };
    if args.len() < 2 + nk {
        return Frame::err("ERR Number of keys can't be greater than number of args");
    }
    let mut limit = 0usize;
    let mut i = 2 + nk;
    while i < args.len() {
        if eq(&args[i], "limit") && i + 1 < args.len() {
            match crate::util::parse_i64(&args[i + 1]) {
                Some(v) if v >= 0 => limit = v as usize,
                _ => return Frame::err("ERR LIMIT can't be negative"),
            }
            i += 2;
        } else {
            return err_syntax();
        }
    }
    let sets = tri!(gather(ctx, &args[2..2 + nk], false));
    let mut n = compute(sets, Op::Inter).len();
    if limit > 0 && n > limit {
        n = limit;
    }
    Frame::Integer(n as i64)
}

pub fn sscan(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = tri!(parse_scan(args, 2, false, false));
    let s = match set_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(vec![Frame::bulk_str("0"), Frame::Array(vec![])]),
        Ok(Some(s)) => s,
    };
    let mut items = Vec::new();
    let mut cursor = o.cursor;
    if s.len() <= 128 {
        items = s.keys().cloned().collect();
        cursor = 0;
    } else {
        let mut iter = 0;
        loop {
            cursor = s.scan(cursor, |k, _| items.push(k.clone()));
            iter += 1;
            if cursor == 0 || items.len() >= o.count || iter >= o.count * 10 {
                break;
            }
        }
    }
    let out = items
        .into_iter()
        .filter(|k| o.pattern.as_ref().is_none_or(|p| crate::util::glob_match(p, k, false)))
        .map(Frame::Bulk)
        .collect();
    Frame::Array(vec![Frame::Bulk(Bytes::from(cursor.to_string())), Frame::Array(out)])
}
