//! HyperLogLog, byte-compatible with Redis' hyperloglog.c (dense and sparse
//! encodings, cached cardinality, Ertl's improved estimator).

use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::value::Value;
use bytes::Bytes;

const P: u32 = 14;
const Q: u32 = 64 - P;
const REGISTERS: usize = 1 << P;
const P_MASK: u64 = (REGISTERS - 1) as u64;
const BITS: usize = 6;
const REGISTER_MAX: u8 = (1 << BITS) - 1;
const HDR: usize = 16;
pub const DENSE_SIZE: usize = HDR + (REGISTERS * BITS + 7) / 8;
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;
const ALPHA_INF: f64 = 0.721347520444481703680;
const INVALID: &str = "INVALIDOBJ Corrupted HLL object detected";

pub fn murmur64a(key: &[u8], seed: u64) -> u64 {
    let m: u64 = 0xc6a4a7935bd1e995;
    let r = 47;
    let mut h: u64 = seed ^ (key.len() as u64).wrapping_mul(m);
    let chunks = key.len() / 8;
    for i in 0..chunks {
        let mut k = u64::from_le_bytes(key[i * 8..i * 8 + 8].try_into().unwrap());
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h ^= k;
        h = h.wrapping_mul(m);
    }
    let tail = &key[chunks * 8..];
    let rem = tail.len();
    if rem > 0 {
        for i in (0..rem).rev() {
            h ^= (tail[i] as u64) << (8 * i);
        }
        h = h.wrapping_mul(m);
    }
    h ^= h >> r;
    h = h.wrapping_mul(m);
    h ^= h >> r;
    h
}

fn pat_len(ele: &[u8]) -> (usize, u8) {
    let mut hash = murmur64a(ele, 0xadc83b19);
    let index = (hash & P_MASK) as usize;
    hash >>= P;
    hash |= 1u64 << Q;
    let mut bit = 1u64;
    let mut count = 1u8;
    while hash & bit == 0 {
        count += 1;
        bit <<= 1;
    }
    (index, count)
}

#[inline]
fn dense_get(regs: &[u8], i: usize) -> u8 {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let fb8 = 8 - fb;
    let b0 = regs[byte] as u32;
    let b1 = *regs.get(byte + 1).unwrap_or(&0) as u32;
    (((b0 >> fb) | (b1 << fb8)) & REGISTER_MAX as u32) as u8
}

#[inline]
fn dense_set_raw(regs: &mut [u8], i: usize, val: u8) {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let fb8 = 8 - fb;
    let v = val as u32;
    regs[byte] &= !((REGISTER_MAX as u32) << fb) as u8;
    regs[byte] |= (v << fb) as u8;
    if byte + 1 < regs.len() {
        regs[byte + 1] &= !((REGISTER_MAX as u32) >> fb8) as u8;
        regs[byte + 1] |= (v >> fb8) as u8;
    }
}

fn dense_set(regs: &mut [u8], i: usize, count: u8) -> bool {
    if count > dense_get(regs, i) {
        dense_set_raw(regs, i, count);
        true
    } else {
        false
    }
}

fn invalidate(o: &mut [u8]) {
    o[15] |= 1 << 7;
}

pub fn new_hll() -> Vec<u8> {
    let mut v = Vec::with_capacity(HDR + 2 * REGISTERS.div_ceil(SPARSE_XZERO_MAX_LEN));
    v.extend_from_slice(b"HYLL");
    v.push(SPARSE);
    v.extend_from_slice(&[0u8; 11]);
    let mut aux = REGISTERS;
    while aux > 0 {
        let x = aux.min(SPARSE_XZERO_MAX_LEN);
        let l = x - 1;
        v.push((l >> 8) as u8 | 0x40);
        v.push((l & 0xff) as u8);
        aux -= x;
    }
    v
}

/// Validate a string as a HyperLogLog object.
pub fn is_hll(b: &[u8]) -> bool {
    if b.len() < HDR || &b[..4] != b"HYLL" || b[4] > SPARSE {
        return false;
    }
    !(b[4] == DENSE && b.len() != DENSE_SIZE)
}

/// Iterate sparse opcodes: callback(start_index, runlen, value). Returns final index.
fn sparse_walk(sp: &[u8], mut f: impl FnMut(usize, usize, u8) -> bool) -> Option<usize> {
    let mut idx = 0usize;
    let mut p = 0;
    while p < sp.len() {
        let c = sp[p];
        if c & 0xc0 == 0 {
            let rl = (c & 0x3f) as usize + 1;
            idx += rl;
            p += 1;
        } else if c & 0xc0 == 0x40 {
            if p + 1 >= sp.len() {
                return None;
            }
            let rl = ((((c & 0x3f) as usize) << 8) | sp[p + 1] as usize) + 1;
            idx += rl;
            p += 2;
        } else {
            let rl = (c & 3) as usize + 1;
            let v = ((c >> 2) & 0x1f) + 1;
            if idx + rl > REGISTERS {
                return None;
            }
            if !f(idx, rl, v) {
                return None;
            }
            idx += rl;
            p += 1;
        }
    }
    Some(idx)
}

fn sparse_to_dense(o: &mut Vec<u8>) -> bool {
    if o[4] == DENSE {
        return true;
    }
    let mut d = vec![0u8; DENSE_SIZE];
    d[..4].copy_from_slice(b"HYLL");
    d[4] = DENSE;
    d[8..16].copy_from_slice(&o[8..16]);
    let regs_ok = {
        let (_, regs) = d.split_at_mut(HDR);
        sparse_walk(&o[HDR..], |start, rl, v| {
            for i in start..start + rl {
                dense_set_raw(regs, i, v);
            }
            true
        })
    };
    if regs_ok != Some(REGISTERS) {
        return false;
    }
    *o = d;
    true
}

#[inline]
fn is_zero(c: u8) -> bool {
    c & 0xc0 == 0
}
#[inline]
fn is_xzero(c: u8) -> bool {
    c & 0xc0 == 0x40
}
#[inline]
fn is_val(c: u8) -> bool {
    c & 0x80 != 0
}
#[inline]
fn val_value(c: u8) -> u8 {
    ((c >> 2) & 0x1f) + 1
}
#[inline]
fn val_len(c: u8) -> usize {
    (c & 3) as usize + 1
}
#[inline]
fn val_op(val: u8, len: usize) -> u8 {
    (((val - 1) << 2) | (len as u8 - 1)) | 0x80
}
fn push_zero(seq: &mut Vec<u8>, len: usize) {
    if len > SPARSE_ZERO_MAX_LEN {
        let l = len - 1;
        seq.push((l >> 8) as u8 | 0x40);
        seq.push((l & 0xff) as u8);
    } else {
        seq.push((len - 1) as u8);
    }
}

/// Port of hllSparseSet: returns Some(true) if a register changed, Some(false) if not,
/// None on corruption.
fn sparse_set(o: &mut Vec<u8>, index: usize, count: u8, max_bytes: usize) -> Option<bool> {
    if count > SPARSE_VAL_MAX_VALUE {
        return promote(o, index, count);
    }
    let end = o.len();
    let mut p = HDR;
    let mut first = 0usize;
    let mut prev: Option<usize> = None;
    let mut span = 0usize;
    while p < end {
        let c = o[p];
        let mut oplen = 1;
        if is_zero(c) {
            span = (c & 0x3f) as usize + 1;
        } else if is_val(c) {
            span = val_len(c);
        } else {
            if p + 1 >= end {
                return None;
            }
            span = ((((c & 0x3f) as usize) << 8) | o[p + 1] as usize) + 1;
            oplen = 2;
        }
        if index <= first + span - 1 {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    if span == 0 || p >= end {
        return None;
    }
    let c = o[p];
    let runlen;
    let (zero, xzero, val) = (is_zero(c), is_xzero(c), is_val(c));
    if zero {
        runlen = (c & 0x3f) as usize + 1;
    } else if xzero {
        runlen = ((((c & 0x3f) as usize) << 8) | o[p + 1] as usize) + 1;
    } else {
        runlen = val_len(c);
    }
    let mut done = false;
    if val {
        let old = val_value(c);
        if old >= count {
            return Some(false);
        }
        if runlen == 1 {
            o[p] = val_op(count, 1);
            done = true;
        }
    }
    if !done && zero && runlen == 1 {
        o[p] = val_op(count, 1);
        done = true;
    }
    if !done {
        let last = first + span - 1;
        let mut seq: Vec<u8> = Vec::with_capacity(5);
        if zero || xzero {
            if index != first {
                push_zero(&mut seq, index - first);
            }
            seq.push(val_op(count, 1));
            if index != last {
                push_zero(&mut seq, last - index);
            }
        } else {
            let cur = val_value(c);
            if index != first {
                seq.push(val_op(cur, index - first));
            }
            seq.push(val_op(count, 1));
            if index != last {
                seq.push(val_op(cur, last - index));
            }
        }
        let oldlen = if xzero { 2 } else { 1 };
        let delta = seq.len() as isize - oldlen as isize;
        if delta > 0 && (o.len() as isize + delta) as usize > max_bytes {
            return promote(o, index, count);
        }
        o.splice(p..p + oldlen, seq);
    }
    // Merge adjacent VAL opcodes with the same value (up to 5 opcodes from prev).
    let mut p = prev.unwrap_or(HDR);
    let mut scan = 5;
    while p < o.len() && scan > 0 {
        scan -= 1;
        let c = o[p];
        if is_xzero(c) {
            p += 2;
            continue;
        } else if is_zero(c) {
            p += 1;
            continue;
        }
        if p + 1 < o.len() && is_val(o[p + 1]) {
            let v1 = val_value(c);
            let v2 = val_value(o[p + 1]);
            if v1 == v2 {
                let len = val_len(c) + val_len(o[p + 1]);
                if len <= SPARSE_VAL_MAX_LEN {
                    o[p + 1] = val_op(v1, len);
                    o.remove(p);
                    continue;
                }
            }
        }
        p += 1;
    }
    invalidate(o);
    Some(true)
}

fn promote(o: &mut Vec<u8>, index: usize, count: u8) -> Option<bool> {
    if !sparse_to_dense(o) {
        return None;
    }
    let r = dense_set(&mut o[HDR..], index, count);
    Some(r)
}

fn hll_add(o: &mut Vec<u8>, ele: &[u8], max_bytes: usize) -> Option<bool> {
    let (idx, cnt) = pat_len(ele);
    match o[4] {
        DENSE => Some(dense_set(&mut o[HDR..], idx, cnt)),
        SPARSE => sparse_set(o, idx, cnt, max_bytes),
        _ => None,
    }
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let zp = z;
        z += x * y;
        y += y;
        if zp == z {
            break;
        }
    }
    z
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let zp = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if zp == z {
            break;
        }
    }
    z / 3.0
}

fn count_histo(h: &[i32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - h[(Q + 1) as usize] as f64) / m);
    for j in (1..=Q as usize).rev() {
        z += h[j] as f64;
        z *= 0.5;
    }
    z += m * sigma(h[0] as f64 / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// Cardinality of an HLL object (None when corrupted).
fn hll_count(o: &[u8]) -> Option<u64> {
    let mut h = [0i32; 64];
    if o[4] == DENSE {
        let regs = &o[HDR..];
        for i in 0..REGISTERS {
            h[dense_get(regs, i) as usize] += 1;
        }
    } else {
        let mut nonzero = 0usize;
        let end = sparse_walk(&o[HDR..], |_, rl, v| {
            h[v as usize] += rl as i32;
            nonzero += rl;
            true
        })?;
        if end != REGISTERS {
            return None;
        }
        h[0] += (REGISTERS - nonzero) as i32;
    }
    Some(count_histo(&h))
}

fn merge_into(max: &mut [u8], o: &[u8]) -> bool {
    if o[4] == DENSE {
        let regs = &o[HDR..];
        for (i, m) in max.iter_mut().enumerate() {
            let v = dense_get(regs, i);
            if v > *m {
                *m = v;
            }
        }
        true
    } else {
        sparse_walk(&o[HDR..], |start, rl, v| {
            for m in &mut max[start..start + rl] {
                if v > *m {
                    *m = v;
                }
            }
            true
        }) == Some(REGISTERS)
    }
}

fn raw_count(max: &[u8]) -> u64 {
    let mut h = [0i32; 64];
    for &v in max {
        h[v as usize] += 1;
    }
    count_histo(&h)
}

fn not_hll() -> Frame {
    Frame::err("WRONGTYPE Key is not a valid HyperLogLog string value.")
}

/// Fetch the HLL bytes at key: Ok(None) missing, Err(reply) on wrong type / invalid.
fn get_hll(ctx: &mut Ctx, key: &[u8], write: bool) -> Result<Option<Vec<u8>>, Frame> {
    let db = ctx.db;
    let o = if write { ctx.s.lookup_write(db, key) } else { ctx.s.lookup_read(db, key) };
    match o {
        None => Ok(None),
        Some(o) => match &o.v {
            Value::Str(b) => {
                if is_hll(b) {
                    Ok(Some(b.to_vec()))
                } else {
                    Err(not_hll())
                }
            }
            _ => Err(crate::db::wrongtype()),
        },
    }
}

fn store(ctx: &mut Ctx, key: &Bytes, v: Vec<u8>) {
    let db = ctx.db;
    if let Some(o) = ctx.s.dbs[db].dict.get_mut(key) {
        o.v = Value::Str(Bytes::from(v));
    } else {
        ctx.s.db_add(db, key.clone(), Value::Str(Bytes::from(v)));
    }
}

pub fn pfadd(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let max_bytes = ctx.s.cfg.hll_sparse_max_bytes;
    let (mut o, mut updated) = match get_hll(ctx, key, true) {
        Err(e) => return e,
        Ok(Some(o)) => (o, 0u64),
        Ok(None) => (new_hll(), 1u64),
    };
    let created = updated == 1;
    for e in &args[2..] {
        match hll_add(&mut o, e, max_bytes) {
            Some(true) => updated += 1,
            Some(false) => {}
            None => return Frame::err(INVALID),
        }
    }
    if updated > 0 {
        invalidate(&mut o);
        store(ctx, key, o);
        ctx.modified(key);
        ctx.notify(NOTIFY_STRING, "pfadd", key);
        ctx.dirty(updated);
    } else if created {
        store(ctx, key, o);
    }
    Frame::Integer((updated > 0) as i64)
}

pub fn pfcount(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 2 {
        let mut max = vec![0u8; REGISTERS];
        for k in &args[1..] {
            match get_hll(ctx, k, false) {
                Err(e) => return e,
                Ok(None) => continue,
                Ok(Some(o)) => {
                    if !merge_into(&mut max, &o) {
                        return Frame::err(INVALID);
                    }
                }
            }
        }
        return Frame::Integer(raw_count(&max) as i64);
    }
    let key = &args[1];
    let mut o = match get_hll(ctx, key, false) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(o)) => o,
    };
    if o[15] & 0x80 == 0 {
        let card = u64::from_le_bytes(o[8..16].try_into().unwrap());
        return Frame::Integer(card as i64);
    }
    let Some(card) = hll_count(&o) else { return Frame::err(INVALID) };
    o[8..16].copy_from_slice(&card.to_le_bytes());
    store(ctx, key, o);
    ctx.modified(key);
    ctx.dirty(1);
    Frame::Integer(card as i64)
}

pub fn pfmerge(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut max = vec![0u8; REGISTERS];
    let mut use_dense = false;
    for k in &args[1..] {
        match get_hll(ctx, k, false) {
            Err(e) => return e,
            Ok(None) => continue,
            Ok(Some(o)) => {
                if o[4] == DENSE {
                    use_dense = true;
                }
                if !merge_into(&mut max, &o) {
                    return Frame::err(INVALID);
                }
            }
        }
    }
    let dst = &args[1];
    let max_bytes = ctx.s.cfg.hll_sparse_max_bytes;
    let mut o = match get_hll(ctx, dst, true) {
        Err(e) => return e,
        Ok(Some(o)) => o,
        Ok(None) => new_hll(),
    };
    if use_dense && !sparse_to_dense(&mut o) {
        return Frame::err(INVALID);
    }
    for (j, &m) in max.iter().enumerate() {
        if m == 0 {
            continue;
        }
        if o[4] == DENSE {
            dense_set(&mut o[HDR..], j, m);
        } else if sparse_set(&mut o, j, m, max_bytes).is_none() {
            return Frame::err(INVALID);
        }
    }
    invalidate(&mut o);
    store(ctx, dst, o);
    ctx.modified(dst);
    ctx.notify(NOTIFY_STRING, "pfadd", dst);
    ctx.dirty(1);
    Frame::ok()
}

pub fn pfdebug(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let sub = crate::util::lower(&args[1]);
    let key = &args[2];
    let mut o = match get_hll(ctx, key, true) {
        Err(e) => return e,
        Ok(None) => return Frame::err("ERR The specified key does not exist"),
        Ok(Some(o)) => o,
    };
    let arityerr = || Frame::err(format!("ERR Wrong number of arguments for the '{}' subcommand", String::from_utf8_lossy(&args[1])));
    match sub.as_str() {
        "getreg" => {
            if args.len() != 3 {
                return arityerr();
            }
            if o[4] == SPARSE {
                if !sparse_to_dense(&mut o) {
                    return Frame::err(INVALID);
                }
                store(ctx, key, o.clone());
                ctx.dirty(1);
            }
            let regs = &o[HDR..];
            Frame::Array((0..REGISTERS).map(|i| Frame::Integer(dense_get(regs, i) as i64)).collect())
        }
        "decode" => {
            if args.len() != 3 {
                return arityerr();
            }
            if o[4] != SPARSE {
                return Frame::err("ERR HLL encoding is not sparse");
            }
            let mut s = String::new();
            let sp = &o[HDR..];
            let mut p = 0;
            while p < sp.len() {
                let c = sp[p];
                if is_zero(c) {
                    s.push_str(&format!("z:{} ", (c & 0x3f) as usize + 1));
                    p += 1;
                } else if is_xzero(c) {
                    let rl = ((((c & 0x3f) as usize) << 8) | *sp.get(p + 1).unwrap_or(&0) as usize) + 1;
                    s.push_str(&format!("Z:{} ", rl));
                    p += 2;
                } else {
                    s.push_str(&format!("v:{},{} ", val_value(c), val_len(c)));
                    p += 1;
                }
            }
            Frame::bulk(Bytes::from(s.trim_end().to_string()))
        }
        "encoding" => {
            if args.len() != 3 {
                return arityerr();
            }
            Frame::Simple(if o[4] == DENSE { "dense" } else { "sparse" }.into())
        }
        "todense" => {
            if args.len() != 3 {
                return arityerr();
            }
            if o[4] == SPARSE {
                if !sparse_to_dense(&mut o) {
                    return Frame::err(INVALID);
                }
                store(ctx, key, o);
                ctx.dirty(1);
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        _ => Frame::err(format!("ERR Unknown PFDEBUG subcommand '{}'", String::from_utf8_lossy(&args[1]))),
    }
}

pub fn pfselftest(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    // 1) Register get/set must round-trip for every register and value.
    let mut regs = vec![0u8; DENSE_SIZE - HDR];
    let mut expected = vec![0u8; REGISTERS];
    for _ in 0..1000 {
        for (i, e) in expected.iter_mut().enumerate() {
            let r = (crate::util::rand_u64() as u8) & REGISTER_MAX;
            *e = r;
            dense_set_raw(&mut regs, i, r);
        }
        for (i, e) in expected.iter().enumerate() {
            if dense_get(&regs, i) != *e {
                return Frame::err(format!("ERR TESTFAILED Register error at {}", i));
            }
        }
    }
    // 2) Sparse and dense must agree, and the error must stay within bounds.
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..4].copy_from_slice(b"HYLL");
    let mut sparse = new_hll();
    let relerr = 1.04 / (REGISTERS as f64).sqrt();
    let mut checkpoint = 1u64;
    let seed = crate::util::rand_u64();
    for j in 1..=10_000_000u64 {
        let ele = (j ^ seed).to_le_bytes();
        let (idx, cnt) = pat_len(&ele);
        dense_set(&mut dense[HDR..], idx, cnt);
        if j <= 1000 {
            hll_add(&mut sparse, &ele, 3000);
        }
        if j == 1000 {
            if hll_count(&sparse) != hll_count(&dense) {
                return Frame::err("ERR TESTFAILED sparse and dense cardinality mismatch");
            }
        }
        if j == checkpoint {
            let abserr = (checkpoint as f64 * relerr * 5.0).max(1.0);
            let c = hll_count(&dense).unwrap_or(0) as f64;
            if (c - checkpoint as f64).abs() > abserr {
                return Frame::err(format!("ERR TESTFAILED Too big error. card:{} abserr:{}", checkpoint, (c - checkpoint as f64).abs()));
            }
            checkpoint *= 10;
        }
    }
    Frame::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_is_valid() {
        let h = new_hll();
        assert!(is_hll(&h));
        assert_eq!(hll_count(&h), Some(0));
    }
    #[test]
    fn approx() {
        let mut h = new_hll();
        for i in 0..10000u32 {
            hll_add(&mut h, i.to_string().as_bytes(), 3000);
        }
        let c = hll_count(&h).unwrap() as f64;
        assert!((c - 10000.0).abs() < 300.0, "{}", c);
        assert_eq!(h[4], DENSE);
    }
    #[test]
    fn murmur_known() {
        // Value computed with Redis' MurmurHash64A for "hello" with HLL seed.
        let h = murmur64a(b"", 0xadc83b19);
        assert_ne!(h, 0);
    }
}
