//! Sorted set commands.

use super::util::*;
use crate::blocking::{BlockSpec, parse_timeout};
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use crate::value::Value;
use crate::zset::{LexBound, LexRange, ScoreRange, ZSet};
use bytes::Bytes;

fn is_zset(v: &Value) -> bool {
    matches!(v, Value::ZSet(_))
}

fn z_ref<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut ZSet>, Frame> {
    match ctx.s.get_type(ctx.db, key, is_zset)? {
        Some(o) => match &mut o.v {
            Value::ZSet(z) => Ok(Some(z)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn z_mut<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut ZSet>, Frame> {
    match ctx.s.get_type_w(ctx.db, key, is_zset)? {
        Some(o) => match &mut o.v {
            Value::ZSet(z) => Ok(Some(z)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

pub fn store_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![1];
    if let Some(n) = args.get(2).and_then(|a| crate::util::parse_i64(a)) {
        if n > 0 {
            v.extend(3..(3 + n as usize).min(args.len()));
        }
    }
    v
}

/// Reply with members, optionally with scores (pairs in RESP3).
fn scored(items: Vec<(Bytes, f64)>, withscores: bool, proto: u8) -> Frame {
    if !withscores {
        return Frame::Array(items.into_iter().map(|(m, _)| Frame::Bulk(m)).collect());
    }
    if proto >= 3 {
        Frame::Array(items.into_iter().map(|(m, s)| Frame::Array(vec![Frame::Bulk(m), Frame::Double(s)])).collect())
    } else {
        Frame::Array(items.into_iter().flat_map(|(m, s)| [Frame::Bulk(m), Frame::Double(s)]).collect())
    }
}

pub fn parse_score_bound(a: &[u8]) -> Option<(f64, bool)> {
    if let Some(rest) = a.strip_prefix(b"(") {
        let v = crate::util::parse_f64(rest)?;
        Some((v, true))
    } else {
        let v = crate::util::parse_f64(a)?;
        Some((v, false))
    }
}

pub fn parse_score_range(min: &[u8], max: &[u8]) -> Result<ScoreRange, Frame> {
    let err = || Frame::err("ERR min or max is not a float");
    let (mn, mnex) = parse_score_bound(min).ok_or_else(err)?;
    let (mx, mxex) = parse_score_bound(max).ok_or_else(err)?;
    Ok(ScoreRange { min: mn, max: mx, minex: mnex, maxex: mxex })
}

fn parse_lex_bound(a: &Bytes) -> Option<LexBound> {
    match a.first() {
        Some(b'+') if a.len() == 1 => Some(LexBound::PosInf),
        Some(b'-') if a.len() == 1 => Some(LexBound::NegInf),
        Some(b'(') => Some(LexBound::Excl(a.slice(1..))),
        Some(b'[') => Some(LexBound::Incl(a.slice(1..))),
        _ => None,
    }
}

pub fn parse_lex_range(min: &Bytes, max: &Bytes) -> Result<LexRange, Frame> {
    let err = || Frame::err("ERR min or max not valid string range item");
    Ok(LexRange { min: parse_lex_bound(min).ok_or_else(err)?, max: parse_lex_bound(max).ok_or_else(err)? })
}

pub fn zadd(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) = (false, false, false, false, false, false);
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        if eq(a, "nx") {
            nx = true;
        } else if eq(a, "xx") {
            xx = true;
        } else if eq(a, "gt") {
            gt = true;
        } else if eq(a, "lt") {
            lt = true;
        } else if eq(a, "ch") {
            ch = true;
        } else if eq(a, "incr") {
            incr = true;
        } else {
            break;
        }
        i += 1;
    }
    let rest = &args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return err_syntax();
    }
    if nx && xx {
        return Frame::err("ERR XX and NX options at the same time are not compatible");
    }
    if (gt && nx) || (lt && nx) || (gt && lt) {
        return Frame::err("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    if incr && rest.len() > 2 {
        return Frame::err("ERR INCR option supports a single increment-element pair");
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for p in rest.chunks(2) {
        let Some(s) = crate::util::parse_f64(&p[0]) else { return err_float() };
        pairs.push((s, p[1].clone()));
    }
    let db = ctx.db;
    let exists = tri!(z_mut(ctx, key)).is_some();
    if !exists {
        if xx {
            return if incr { Frame::Null } else { Frame::Integer(0) };
        }
        ctx.s.db_add(db, key.clone(), Value::ZSet(Box::new(ZSet::new())));
    }
    let z = z_mut(ctx, key).unwrap().unwrap();
    let (mut added, mut updated, mut processed) = (0i64, 0i64, 0i64);
    let mut incr_result = None;
    for (score, m) in pairs {
        let cur = z.score(&m);
        match cur {
            Some(old) => {
                if nx {
                    continue;
                }
                let new = if incr {
                    let n = old + score;
                    if n.is_nan() {
                        return Frame::err("ERR resulting score is not a number (NaN)");
                    }
                    n
                } else {
                    score
                };
                if (gt && new <= old) || (lt && new >= old) {
                    continue;
                }
                incr_result = Some(new);
                if new != old {
                    z.insert(m, new);
                    updated += 1;
                }
                processed += 1;
            }
            None => {
                if xx {
                    continue;
                }
                z.insert(m, score);
                incr_result = Some(score);
                added += 1;
                processed += 1;
            }
        }
    }
    let empty = z.is_empty();
    if empty {
        ctx.s.db_delete_raw(db, key);
    }
    if added + updated > 0 {
        ctx.modified(key);
        ctx.notify(NOTIFY_ZSET, if incr { "zincr" } else { "zadd" }, key);
        ctx.dirty((added + updated) as u64);
        ctx.s.signal_ready(db, key);
    }
    let _ = processed;
    if incr {
        return match incr_result {
            Some(v) => Frame::Double(v),
            None => Frame::Null,
        };
    }
    Frame::Integer(if ch { added + updated } else { added })
}

pub fn zincrby(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let argv = vec![args[0].clone(), args[1].clone(), Bytes::from_static(b"INCR"), args[2].clone(), args[3].clone()];
    zadd(ctx, &argv)
}

pub fn zcard(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(z)) => Frame::Integer(z.len() as i64),
    }
}

pub fn zscore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Null,
        Ok(Some(z)) => z.score(&args[2]).map(Frame::Double).unwrap_or(Frame::Null),
    }
}

pub fn zmscore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Array(args[2..].iter().map(|_| Frame::Null).collect()),
        Ok(Some(z)) => Frame::Array(args[2..].iter().map(|m| z.score(m).map(Frame::Double).unwrap_or(Frame::Null)).collect()),
    }
}

pub fn zrem(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    let n = match z_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(z)) => args[2..].iter().filter(|m| z.remove(m)).count(),
    };
    if n > 0 {
        ctx.notify(NOTIFY_ZSET, "zrem", key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(n as u64);
    }
    Frame::Integer(n as i64)
}

pub fn zcount(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let r = tri!(parse_score_range(&args[2], &args[3]));
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(z)) => Frame::Integer(z.count_in_score(&r) as i64),
    }
}

pub fn zlexcount(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let r = tri!(parse_lex_range(&args[2], &args[3]));
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(z)) => Frame::Integer(z.count_in_lex(&r) as i64),
    }
}

fn rank_generic(ctx: &mut Ctx, args: &[Bytes], rev: bool) -> Frame {
    let withscore = match args.len() {
        3 => false,
        4 if eq(&args[3], "withscore") => true,
        _ => return err_syntax(),
    };
    match z_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => if withscore { Frame::NullArray } else { Frame::Null },
        Ok(Some(z)) => match z.rank(&args[2], rev) {
            Some(r) => {
                if withscore {
                    Frame::Array(vec![Frame::Integer(r as i64), Frame::Double(z.score(&args[2]).unwrap())])
                } else {
                    Frame::Integer(r as i64)
                }
            }
            None => if withscore { Frame::NullArray } else { Frame::Null },
        },
    }
}

pub fn zrank(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    rank_generic(ctx, args, false)
}
pub fn zrevrank(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    rank_generic(ctx, args, true)
}

#[derive(Clone, Copy, PartialEq)]
enum RangeType {
    Rank,
    Score,
    Lex,
}

struct RangeSpec {
    kind: RangeType,
    rev: bool,
    withscores: bool,
    offset: i64,
    limit: i64,
}

/// Execute a range query. `min`/`max` are in user order (already swapped for REV forms).
fn do_range(z: &ZSet, spec: &RangeSpec, a: &Bytes, b: &Bytes) -> Result<Vec<(Bytes, f64)>, Frame> {
    match spec.kind {
        RangeType::Rank => {
            let s = int(a)?;
            let e = int(b)?;
            Ok(match norm_range(s, e, z.len()) {
                None => vec![],
                Some((s, e)) => z.range_by_rank(s, e, spec.rev),
            })
        }
        RangeType::Score => {
            let r = if spec.rev { parse_score_range(b, a)? } else { parse_score_range(a, b)? };
            if spec.offset < 0 {
                return Ok(vec![]);
            }
            let lim = if spec.limit < 0 { None } else { Some(spec.limit as usize) };
            Ok(z.range_by_score(&r, spec.rev, spec.offset as usize, lim))
        }
        RangeType::Lex => {
            let r = if spec.rev { parse_lex_range(b, a)? } else { parse_lex_range(a, b)? };
            if spec.offset < 0 {
                return Ok(vec![]);
            }
            let lim = if spec.limit < 0 { None } else { Some(spec.limit as usize) };
            Ok(z.range_by_lex(&r, spec.rev, spec.offset as usize, lim))
        }
    }
}

/// Parse ZRANGE-style options from args[from..].
fn parse_range_opts(args: &[Bytes], from: usize, mut spec: RangeSpec, zrange_form: bool, allow_withscores: bool) -> Result<RangeSpec, Frame> {
    let mut i = from;
    let mut has_limit = false;
    while i < args.len() {
        let a = &args[i];
        if eq(a, "withscores") && allow_withscores {
            spec.withscores = true;
            i += 1;
        } else if eq(a, "limit") && i + 2 < args.len() {
            spec.offset = int(&args[i + 1])?;
            spec.limit = int(&args[i + 2])?;
            has_limit = true;
            i += 3;
        } else if zrange_form && eq(a, "rev") {
            spec.rev = true;
            i += 1;
        } else if zrange_form && eq(a, "byscore") && spec.kind == RangeType::Rank {
            spec.kind = RangeType::Score;
            i += 1;
        } else if zrange_form && eq(a, "bylex") && spec.kind == RangeType::Rank {
            spec.kind = RangeType::Lex;
            i += 1;
        } else {
            return Err(err_syntax());
        }
    }
    if has_limit && spec.kind == RangeType::Rank {
        return Err(Frame::err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"));
    }
    if spec.withscores && spec.kind == RangeType::Lex {
        return Err(Frame::err("ERR syntax error, WITHSCORES not supported in combination with BYLEX"));
    }
    Ok(spec)
}

fn range_cmd(ctx: &mut Ctx, args: &[Bytes], kind: RangeType, rev: bool, zrange_form: bool) -> Frame {
    let spec = RangeSpec { kind, rev, withscores: false, offset: 0, limit: -1 };
    let spec = tri!(parse_range_opts(args, 4, spec, zrange_form, kind != RangeType::Lex || zrange_form));
    let proto = ctx.proto();
    // In ZRANGE REV with BYSCORE/BYLEX, the user passes max then min.
    let (a, b) = (&args[2], &args[3]);
    let z = match z_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => {
            // Validate ranges even when key doesn't exist.
            let empty = ZSet::new();
            if let Err(e) = do_range(&empty, &spec, a, b) {
                return e;
            }
            return Frame::Array(vec![]);
        }
        Ok(Some(z)) => z,
    };
    match do_range(z, &spec, a, b) {
        Ok(items) => scored(items, spec.withscores, proto),
        Err(e) => e,
    }
}

pub fn zrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Rank, false, true)
}
pub fn zrevrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Rank, true, false)
}
pub fn zrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Score, false, false)
}
pub fn zrevrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Score, true, false)
}
pub fn zrangebylex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Lex, false, false)
}
pub fn zrevrangebylex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    range_cmd(ctx, args, RangeType::Lex, true, false)
}

pub fn zrangestore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let spec = RangeSpec { kind: RangeType::Rank, rev: false, withscores: false, offset: 0, limit: -1 };
    let spec = tri!(parse_range_opts(args, 5, spec, true, false));
    let (dst, src) = (&args[1], &args[2]);
    let items = match z_ref(ctx, src) {
        Err(e) => return e,
        Ok(None) => {
            let empty = ZSet::new();
            if let Err(e) = do_range(&empty, &spec, &args[3], &args[4]) {
                return e;
            }
            vec![]
        }
        Ok(Some(z)) => tri!(do_range(z, &spec, &args[3], &args[4])),
    };
    store_result(ctx, dst, items, "zrangestore")
}

fn store_result(ctx: &mut Ctx, dst: &Bytes, items: Vec<(Bytes, f64)>, ev: &str) -> Frame {
    let db = ctx.db;
    let n = items.len();
    if n == 0 {
        if ctx.s.db_delete(db, dst) {
            ctx.notify(NOTIFY_GENERIC, "del", dst);
            ctx.dirty(1);
        }
        return Frame::Integer(0);
    }
    let mut z = ZSet::new();
    for (m, s) in items {
        z.insert(m, s);
    }
    ctx.s.set_key(db, dst.clone(), Value::ZSet(Box::new(z)), false, true);
    ctx.notify(NOTIFY_ZSET, ev, dst);
    ctx.dirty(1);
    Frame::Integer(n as i64)
}

fn remrange(ctx: &mut Ctx, args: &[Bytes], kind: RangeType) -> Frame {
    let key = &args[1];
    let db = ctx.db;
    // Validate range before lookup.
    let spec = RangeSpec { kind, rev: false, withscores: false, offset: 0, limit: -1 };
    let empty = ZSet::new();
    if let Err(e) = do_range(&empty, &spec, &args[2], &args[3]) {
        return e;
    }
    let z = match z_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(z)) => z,
    };
    let items = tri!(do_range(z, &spec, &args[2], &args[3]));
    for (m, _) in &items {
        z.remove(m);
    }
    let n = items.len();
    if n > 0 {
        let ev = match kind {
            RangeType::Rank => "zremrangebyrank",
            RangeType::Score => "zremrangebyscore",
            RangeType::Lex => "zremrangebylex",
        };
        ctx.notify(NOTIFY_ZSET, ev, key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(n as u64);
    }
    Frame::Integer(n as i64)
}

pub fn zremrangebyrank(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    remrange(ctx, args, RangeType::Rank)
}
pub fn zremrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    remrange(ctx, args, RangeType::Score)
}
pub fn zremrangebylex(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    remrange(ctx, args, RangeType::Lex)
}

#[derive(Clone, Copy)]
enum Agg {
    Sum,
    Min,
    Max,
}

#[derive(Clone, Copy, PartialEq)]
enum SetOp {
    Union,
    Inter,
    Diff,
}

/// Generic ZUNION/ZINTER/ZDIFF[STORE]. `numkeys_at` = index of numkeys.
fn zsetop(ctx: &mut Ctx, args: &[Bytes], op: SetOp, dst: Option<&Bytes>, numkeys_at: usize) -> Frame {
    let fullname = ctx.cmd.full_name();
    let Some(nk) = crate::util::parse_i64(&args[numkeys_at]) else { return err_int() };
    if nk < 1 {
        return Frame::err(format!("ERR at least 1 input key is needed for '{}' command", fullname));
    }
    let nk = nk as usize;
    if numkeys_at + 1 + nk > args.len() {
        return err_syntax();
    }
    let keys = &args[numkeys_at + 1..numkeys_at + 1 + nk];
    let mut weights = vec![1.0f64; nk];
    let mut agg = Agg::Sum;
    let mut withscores = false;
    let mut i = numkeys_at + 1 + nk;
    while i < args.len() {
        let a = &args[i];
        if op != SetOp::Diff && eq(a, "weights") && i + nk < args.len() {
            for j in 0..nk {
                match crate::util::parse_f64(&args[i + 1 + j]) {
                    Some(w) => weights[j] = w,
                    None => return Frame::err("ERR weight value is not a float"),
                }
            }
            i += nk + 1;
        } else if op != SetOp::Diff && eq(a, "aggregate") && i + 1 < args.len() {
            let v = &args[i + 1];
            agg = if eq(v, "sum") {
                Agg::Sum
            } else if eq(v, "min") {
                Agg::Min
            } else if eq(v, "max") {
                Agg::Max
            } else {
                return err_syntax();
            };
            i += 2;
        } else if dst.is_none() && eq(a, "withscores") {
            withscores = true;
            i += 1;
        } else {
            return err_syntax();
        }
    }
    // Gather inputs (sets count as score 1).
    let mut inputs: Vec<Option<Vec<(Bytes, f64)>>> = Vec::with_capacity(nk);
    for k in keys {
        let r = if dst.is_some() { ctx.s.lookup_write(ctx.db, k) } else { ctx.s.lookup_read(ctx.db, k) };
        match r {
            None => inputs.push(None),
            Some(o) => match &o.v {
                Value::ZSet(z) => inputs.push(Some(z.iter().collect())),
                Value::Set(s) => inputs.push(Some(s.keys().map(|k| (k.clone(), 1.0)).collect())),
                _ => return crate::db::wrongtype(),
            },
        }
    }
    let wmul = |s: f64, w: f64| {
        let r = s * w;
        if r.is_nan() { 0.0 } else { r }
    };
    let aggf = |a: f64, b: f64| -> f64 {
        match agg {
            Agg::Sum => {
                let r = a + b;
                if r.is_nan() { 0.0 } else { r }
            }
            Agg::Min => a.min(b),
            Agg::Max => a.max(b),
        }
    };
    let result: Vec<(Bytes, f64)> = match op {
        SetOp::Union => {
            let mut acc: crate::dict::Dict<f64> = crate::dict::Dict::new();
            let mut order: Vec<Bytes> = Vec::new();
            for (idx, inp) in inputs.iter().enumerate() {
                if let Some(items) = inp {
                    for (m, s) in items {
                        let v = wmul(*s, weights[idx]);
                        match acc.get_mut(m) {
                            Some(cur) => *cur = aggf(*cur, v),
                            None => {
                                acc.insert(m.clone(), v);
                                order.push(m.clone());
                            }
                        }
                    }
                }
            }
            order.into_iter().map(|m| {
                let s = *acc.get(&m).unwrap();
                (m, s)
            }).collect()
        }
        SetOp::Inter => {
            if inputs.iter().any(|i| i.is_none()) {
                vec![]
            } else {
                let maps: Vec<crate::dict::Dict<f64>> = inputs
                    .iter()
                    .map(|i| {
                        let mut d = crate::dict::Dict::new();
                        for (m, s) in i.as_ref().unwrap() {
                            d.insert(m.clone(), *s);
                        }
                        d
                    })
                    .collect();
                let mut smallest = 0;
                for (i, m) in maps.iter().enumerate() {
                    if m.len() < maps[smallest].len() {
                        smallest = i;
                    }
                }
                let mut out = Vec::new();
                for (m, _) in inputs[smallest].as_ref().unwrap() {
                    let mut acc: Option<f64> = None;
                    let mut ok = true;
                    for (idx, map) in maps.iter().enumerate() {
                        match map.get(m) {
                            Some(s) => {
                                let v = wmul(*s, weights[idx]);
                                acc = Some(match acc {
                                    None => v,
                                    Some(a) => aggf(a, v),
                                });
                            }
                            None => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        out.push((m.clone(), acc.unwrap()));
                    }
                }
                out
            }
        }
        SetOp::Diff => {
            let Some(first) = &inputs[0] else {
                return finish_setop(ctx, dst, vec![], withscores);
            };
            let others: Vec<crate::dict::Dict<()>> = inputs[1..]
                .iter()
                .flatten()
                .map(|i| {
                    let mut d = crate::dict::Dict::new();
                    for (m, _) in i {
                        d.insert(m.clone(), ());
                    }
                    d
                })
                .collect();
            first.iter().filter(|(m, _)| !others.iter().any(|o| o.contains_key(m))).cloned().collect()
        }
    };
    finish_setop(ctx, dst, result, withscores)
}

fn finish_setop(ctx: &mut Ctx, dst: Option<&Bytes>, result: Vec<(Bytes, f64)>, withscores: bool) -> Frame {
    let mut z = ZSet::new();
    for (m, s) in result {
        z.insert(m, s);
    }
    match dst {
        Some(d) => {
            let ev = match ctx.cmd.name {
                "zunionstore" => "zunionstore",
                "zinterstore" => "zinterstore",
                _ => "zdiffstore",
            };
            store_result(ctx, d, z.iter().collect(), ev)
        }
        None => {
            let proto = ctx.proto();
            scored(z.iter().collect(), withscores, proto)
        }
    }
}

pub fn zunion(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    zsetop(ctx, args, SetOp::Union, None, 1)
}
pub fn zinter(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    zsetop(ctx, args, SetOp::Inter, None, 1)
}
pub fn zdiff(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    zsetop(ctx, args, SetOp::Diff, None, 1)
}
pub fn zunionstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let d = args[1].clone();
    zsetop(ctx, args, SetOp::Union, Some(&d), 2)
}
pub fn zinterstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let d = args[1].clone();
    zsetop(ctx, args, SetOp::Inter, Some(&d), 2)
}
pub fn zdiffstore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let d = args[1].clone();
    zsetop(ctx, args, SetOp::Diff, Some(&d), 2)
}

pub fn zintercard(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
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
    let mut argv = vec![Bytes::from_static(b"zinter"), args[1].clone()];
    argv.extend(args[2..2 + nk].iter().cloned());
    let r = zsetop(ctx, &argv, SetOp::Inter, None, 1);
    match r {
        Frame::Array(v) => {
            let n = v.len();
            Frame::Integer(if limit > 0 { n.min(limit) } else { n } as i64)
        }
        e => e,
    }
}

/// Pop up to `count` from min/max side.
fn pop_core(ctx: &mut Ctx, key: &Bytes, max: bool, count: usize) -> Result<Option<Vec<(Bytes, f64)>>, Frame> {
    let db = ctx.db;
    let z = match z_mut(ctx, key)? {
        None => return Ok(None),
        Some(z) => z,
    };
    let mut out = Vec::new();
    for _ in 0..count {
        let e = if max { z.last() } else { z.first() };
        match e {
            Some((m, s)) => {
                z.remove(&m);
                out.push((m, s));
            }
            None => break,
        }
    }
    if !out.is_empty() {
        ctx.notify(NOTIFY_ZSET, if max { "zpopmax" } else { "zpopmin" }, key);
        ctx.s.delete_if_empty(db, key);
        ctx.modified(key);
        ctx.dirty(out.len() as u64);
    }
    Ok(Some(out))
}

fn pop_generic(ctx: &mut Ctx, args: &[Bytes], max: bool) -> Frame {
    if args.len() > 3 {
        return err_syntax();
    }
    let count = if args.len() == 3 {
        match crate::util::parse_i64(&args[2]) {
            Some(v) if v >= 0 => Some(v as usize),
            _ => return Frame::err("ERR value is out of range, must be positive"),
        }
    } else {
        None
    };
    let proto = ctx.proto();
    match pop_core(ctx, &args[1], max, count.unwrap_or(1)) {
        Err(e) => e,
        Ok(None) => Frame::Array(vec![]),
        Ok(Some(items)) => {
            if count.is_some() && proto >= 3 {
                scored(items, true, proto)
            } else {
                Frame::Array(items.into_iter().flat_map(|(m, s)| [Frame::Bulk(m), Frame::Double(s)]).collect())
            }
        }
    }
}

pub fn zpopmin(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    pop_generic(ctx, args, false)
}
pub fn zpopmax(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    pop_generic(ctx, args, true)
}

fn bzpop_generic(ctx: &mut Ctx, args: &[Bytes], max: bool) -> Frame {
    let now = ctx.s.now_ms();
    let deadline = tri!(parse_timeout(&args[args.len() - 1], now));
    let keys = &args[1..args.len() - 1];
    for k in keys {
        match z_ref(ctx, k) {
            Err(e) => return e,
            Ok(Some(z)) if !z.is_empty() => {
                let items = pop_core(ctx, k, max, 1).unwrap().unwrap();
                let (m, s) = items.into_iter().next().unwrap();
                ctx.propagate_as(vec![Bytes::from_static(if max { b"ZPOPMAX" } else { b"ZPOPMIN" }), k.clone()]);
                return Frame::Array(vec![Frame::Bulk(k.clone()), Frame::Bulk(m), Frame::Double(s)]);
            }
            _ => {}
        }
    }
    if !ctx.can_block() {
        return Frame::NullArray;
    }
    BlockSpec::keys(keys.to_vec(), deadline, Frame::NullArray)
}

pub fn bzpopmin(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bzpop_generic(ctx, args, false)
}
pub fn bzpopmax(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bzpop_generic(ctx, args, true)
}

fn zmpop_generic(ctx: &mut Ctx, args: &[Bytes], at: usize, deadline: Option<Option<i64>>) -> Frame {
    let nk = match crate::util::parse_i64(&args[at]) {
        Some(v) if v > 0 => v as usize,
        _ => return Frame::err("ERR numkeys should be greater than 0"),
    };
    if args.len() < at + 1 + nk + 1 {
        return err_syntax();
    }
    let keys = &args[at + 1..at + 1 + nk];
    let wh = &args[at + 1 + nk];
    let max = if eq(wh, "max") {
        true
    } else if eq(wh, "min") {
        false
    } else {
        return err_syntax();
    };
    let mut count = 1usize;
    let mut i = at + 2 + nk;
    let mut seen = false;
    while i < args.len() {
        if eq(&args[i], "count") && i + 1 < args.len() && !seen {
            match crate::util::parse_i64(&args[i + 1]) {
                Some(v) if v > 0 => count = v as usize,
                _ => return Frame::err("ERR count should be greater than 0"),
            }
            seen = true;
            i += 2;
        } else {
            return err_syntax();
        }
    }
    for k in keys {
        match z_ref(ctx, k) {
            Err(e) => return e,
            Ok(Some(z)) if !z.is_empty() => {
                let items = pop_core(ctx, k, max, count).unwrap().unwrap();
                ctx.propagate_as(vec![
                    Bytes::from_static(if max { b"ZPOPMAX" } else { b"ZPOPMIN" }),
                    k.clone(),
                    Bytes::from(items.len().to_string()),
                ]);
                let arr = Frame::Array(items.into_iter().map(|(m, s)| Frame::Array(vec![Frame::Bulk(m), Frame::Double(s)])).collect());
                return Frame::Array(vec![Frame::Bulk(k.clone()), arr]);
            }
            _ => {}
        }
    }
    match deadline {
        Some(d) if ctx.can_block() => BlockSpec::keys(keys.to_vec(), d, Frame::NullArray),
        _ => Frame::NullArray,
    }
}

pub fn zmpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    zmpop_generic(ctx, args, 1, None)
}
pub fn bzmpop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    let d = tri!(parse_timeout(&args[1], now));
    zmpop_generic(ctx, args, 2, Some(d))
}

pub fn zrandmember(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut withscores = false;
    let count = if args.len() >= 3 {
        let Some(c) = crate::util::parse_i64(&args[2]) else { return err_int() };
        if args.len() == 4 {
            if !eq(&args[3], "withscores") {
                return err_syntax();
            }
            withscores = true;
        } else if args.len() > 4 {
            return err_syntax();
        }
        if c < -(i64::MAX / 2) || c > i64::MAX / 2 {
            return Frame::err("ERR value is out of range");
        }
        Some(c)
    } else {
        None
    };
    let proto = ctx.proto();
    let z = match z_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return if count.is_some() { Frame::Array(vec![]) } else { Frame::Null },
        Ok(Some(z)) => z,
    };
    let Some(c) = count else {
        return z.dict.random_entry().map(|(m, _)| Frame::Bulk(m.clone())).unwrap_or(Frame::Null);
    };
    let items: Vec<(Bytes, f64)> = if c < 0 {
        (0..(-c) as usize).map(|_| {
            let (m, s) = z.dict.random_entry().unwrap();
            (m.clone(), *s)
        }).collect()
    } else {
        let n = (c as usize).min(z.len());
        let mut all: Vec<(Bytes, f64)> = z.iter().collect();
        for i in 0..n {
            let j = i + crate::util::rand_range(all.len() - i);
            all.swap(i, j);
        }
        all.truncate(n);
        all
    };
    scored(items, withscores, proto)
}

pub fn zscan(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let o = tri!(parse_scan(args, 2, false, true));
    let z = match z_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(vec![Frame::bulk_str("0"), Frame::Array(vec![])]),
        Ok(Some(z)) => z,
    };
    let mut items: Vec<(Bytes, f64)> = Vec::new();
    let mut cursor = o.cursor;
    if z.len() <= 128 {
        items = z.iter().collect();
        cursor = 0;
    } else {
        let mut iter = 0;
        loop {
            cursor = z.dict.scan(cursor, |k, v| items.push((k.clone(), *v)));
            iter += 1;
            if cursor == 0 || items.len() >= o.count || iter >= o.count * 10 {
                break;
            }
        }
    }
    let mut out = Vec::new();
    for (m, s) in items {
        if let Some(p) = &o.pattern {
            if !crate::util::glob_match(p, &m, false) {
                continue;
            }
        }
        out.push(Frame::Bulk(m));
        if !o.noscores {
            out.push(Frame::Bulk(Bytes::from(crate::util::fmt_g17(s))));
        }
    }
    Frame::Array(vec![Frame::Bulk(Bytes::from(cursor.to_string())), Frame::Array(out)])
}
