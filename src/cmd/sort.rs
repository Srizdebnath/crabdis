//! SORT / SORT_RO, following Redis' sort.c semantics.

use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::value::Value;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::VecDeque;

/// Key extraction for SORT: the source key plus the STORE destination.
pub fn sort_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![1];
    let mut i = 2;
    while i < args.len() {
        if eq(&args[i], "limit") {
            i += 3;
            continue;
        }
        if (eq(&args[i], "get") || eq(&args[i], "by")) && i + 1 < args.len() {
            i += 2;
            continue;
        }
        if eq(&args[i], "store") && i + 1 < args.len() {
            v.push(i + 1);
            i += 2;
            continue;
        }
        i += 1;
    }
    v
}

/// Resolve a SORT BY/GET pattern for element `subst`. Returns the looked-up value.
fn lookup_by_pattern(ctx: &mut Ctx, pattern: &[u8], subst: &[u8]) -> Option<Bytes> {
    if pattern == b"#" {
        return Some(Bytes::copy_from_slice(subst));
    }
    let star = pattern.iter().position(|&c| c == b'*')?;
    // "->" after the star selects a hash field.
    let (keypat, field) = match pattern[star..].windows(2).position(|w| w == b"->") {
        Some(p) if star + p + 2 < pattern.len() => (&pattern[..star + p], Some(&pattern[star + p + 2..])),
        _ => (pattern, None),
    };
    let mut key = Vec::with_capacity(keypat.len() + subst.len());
    key.extend_from_slice(&keypat[..star]);
    key.extend_from_slice(subst);
    key.extend_from_slice(&keypat[star + 1..]);
    let db = ctx.db;
    let now = ctx.s.now_ms();
    if field.is_some() {
        crate::cmd::hash::expire_fields_of(ctx.s, db, &Bytes::from(key.clone()), now);
    }
    let o = ctx.s.lookup_read(db, &key)?;
    match (field, &o.v) {
        (None, Value::Str(b)) => Some(b.clone()),
        (Some(f), Value::Hash(h)) => h.map.get(f).cloned(),
        _ => None,
    }
}

struct Item {
    val: Bytes,
    score: f64,
    cmp: Option<Bytes>,
}

pub fn sort_ro(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    sort_generic(ctx, args, true)
}

pub fn sort(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    sort_generic(ctx, args, false)
}

fn sort_generic(ctx: &mut Ctx, args: &[Bytes], readonly: bool) -> Frame {
    let mut desc = false;
    let mut alpha = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut sortby: Option<Bytes> = None;
    let mut dontsort = false;
    let mut gets: Vec<Bytes> = Vec::new();
    let mut store: Option<Bytes> = None;
    let cluster = ctx.s.cluster.is_some();
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        let left = args.len() - i - 1;
        if eq(a, "asc") {
            desc = false;
        } else if eq(a, "desc") {
            desc = true;
        } else if eq(a, "alpha") {
            alpha = true;
        } else if eq(a, "limit") && left >= 2 {
            let off = tri!(int(&args[i + 1]));
            let cnt = tri!(int(&args[i + 2]));
            limit = Some((off, cnt));
            i += 2;
        } else if eq(a, "store") && left >= 1 && !readonly {
            store = Some(args[i + 1].clone());
            i += 1;
        } else if eq(a, "by") && left >= 1 {
            let p = args[i + 1].clone();
            if !p.contains(&b'*') {
                dontsort = true;
            } else if cluster {
                return Frame::err("ERR BY option of SORT denied in Cluster mode.");
            }
            sortby = Some(p);
            i += 1;
        } else if eq(a, "get") && left >= 1 {
            if cluster {
                return Frame::err("ERR GET option of SORT denied in Cluster mode.");
            }
            gets.push(args[i + 1].clone());
            i += 1;
        } else {
            return err_syntax();
        }
        i += 1;
    }
    let key = &args[1];
    let db = ctx.db;
    let is_zset;
    // Collect elements.
    let mut elems: Vec<Bytes> = match ctx.s.lookup_read(db, key) {
        None => {
            is_zset = false;
            Vec::new()
        }
        Some(o) => match &o.v {
            Value::List(l) => {
                is_zset = false;
                if dontsort && desc { l.iter().rev().cloned().collect() } else { l.iter().cloned().collect() }
            }
            Value::Set(s) => {
                is_zset = false;
                s.keys().cloned().collect()
            }
            Value::ZSet(z) => {
                is_zset = true;
                if dontsort {
                    // Fetch only the requested range directly in zset order.
                    let n = z.len();
                    let (start, end) = limit_range(limit, n);
                    if start >= n || start > end {
                        Vec::new()
                    } else {
                        z.range_by_rank(start, end, desc).into_iter().map(|(m, _)| m).collect()
                    }
                } else {
                    z.iter().map(|(m, _)| m).collect()
                }
            }
            _ => return crate::db::wrongtype(),
        },
    };
    // Sets have no meaningful order: when BY nosort is used on a set the output must
    // still be deterministic for replication/scripts, so sort lexicographically.
    let mut lex_nosort = false;
    if dontsort && !is_zset {
        if let Some(Value::Set(_)) = ctx.s.dbs[db].dict.get(key).map(|o| &o.v) {
            if store.is_some() || ctx.flags & crate::exec::CTX_LUA != 0 {
                dontsort = false;
                alpha = true;
                sortby = None;
                lex_nosort = true;
            }
        }
    }
    let zset_limited = is_zset && dontsort;
    let mut items: Vec<Item> = elems.drain(..).map(|val| Item { val, score: 0.0, cmp: None }).collect();
    if !dontsort {
        for it in items.iter_mut() {
            let byval = match &sortby {
                Some(p) => lookup_by_pattern(ctx, p, &it.val),
                None => Some(it.val.clone()),
            };
            if alpha {
                it.cmp = byval;
            } else if let Some(b) = byval {
                match crate::util::parse_f64(&b) {
                    Some(f) => it.score = f,
                    None => {
                        // Redis accepts strtod prefixes like " 1" but rejects garbage.
                        match std::str::from_utf8(&b).ok().and_then(|s| s.trim().parse::<f64>().ok()) {
                            Some(f) if !f.is_nan() => it.score = f,
                            _ => return Frame::err("ERR One or more scores can't be converted into double"),
                        }
                    }
                }
            }
        }
        let cmp = |a: &Item, b: &Item| -> Ordering {
            let o = if alpha {
                match (&a.cmp, &b.cmp) {
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Less,
                    (_, None) => Ordering::Greater,
                    (Some(x), Some(y)) => {
                        if sortby.is_some() || lex_nosort {
                            x.as_ref().cmp(y.as_ref())
                        } else {
                            // strcoll in the C locale == byte compare
                            x.as_ref().cmp(y.as_ref())
                        }
                    }
                }
            } else {
                a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal)
            };
            // Ties are broken by comparing the elements themselves, as Redis does.
            let o = if o == Ordering::Equal { a.val.as_ref().cmp(b.val.as_ref()) } else { o };
            if desc { o.reverse() } else { o }
        };
        items.sort_by(cmp);
    }
    // LIMIT
    let items: Vec<Item> = if zset_limited {
        items
    } else {
        let n = items.len();
        let (start, end) = limit_range(limit, n);
        if n == 0 || start >= n || start > end {
            Vec::new()
        } else {
            items.into_iter().skip(start).take(end - start + 1).collect()
        }
    };
    // Build output.
    let mut out: Vec<Option<Bytes>> = Vec::new();
    for it in &items {
        if gets.is_empty() {
            out.push(Some(it.val.clone()));
        } else {
            for g in &gets {
                out.push(lookup_by_pattern(ctx, g, &it.val));
            }
        }
    }
    match store {
        None => Frame::Array(out.into_iter().map(opt_bulk).collect()),
        Some(dst) => {
            let n = out.len();
            if n == 0 {
                if ctx.s.db_delete(db, &dst) {
                    ctx.modified(&dst);
                    ctx.notify(NOTIFY_GENERIC, "del", &dst);
                    ctx.dirty(1);
                }
            } else {
                let l: VecDeque<Bytes> = out.into_iter().map(|o| o.unwrap_or_default()).collect();
                ctx.s.set_key(db, dst.clone(), Value::List(l), false, true);
                ctx.notify(NOTIFY_LIST, "sortstore", &dst);
                ctx.dirty(n as u64);
            }
            Frame::Integer(n as i64)
        }
    }
}

fn limit_range(limit: Option<(i64, i64)>, n: usize) -> (usize, usize) {
    let n = n as i64;
    let (mut start, count) = limit.unwrap_or((0, -1));
    if start < 0 {
        start = 0;
    }
    let mut end = if count < 0 { n - 1 } else { start + count - 1 };
    if start >= n {
        start = n.max(0);
        end = n - 1;
    }
    if end >= n {
        end = n - 1;
    }
    if end < start {
        return (1, 0);
    }
    (start as usize, end as usize)
}
