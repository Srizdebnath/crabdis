//! Bitmap commands: SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD.

use super::string::{check_len, get_str, str_mut};
use super::util::*;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::tri;
use crate::value::Value;
use bytes::Bytes;

fn parse_bit_offset(a: &[u8], hash: bool, bits: u64, ctx: &Ctx) -> Result<u64, Frame> {
    let err = || Frame::err("ERR bit offset is not an integer or out of range");
    let (s, mul) = if hash && a.first() == Some(&b'#') { (&a[1..], bits) } else { (a, 1) };
    let v = crate::util::parse_i64(s).ok_or_else(err)?;
    if v < 0 {
        return Err(err());
    }
    let v = (v as u64).checked_mul(mul).ok_or_else(err)?;
    if (v >> 3) as usize >= ctx.s.cfg.proto_max_bulk_len {
        return Err(err());
    }
    Ok(v)
}

/// Fetch the string for writing, creating/growing it so that `maxbit` is addressable.
fn bits_for_write(ctx: &mut Ctx, key: &Bytes, maxbit: u64) -> Result<bytes::BytesMut, Frame> {
    let db = ctx.db;
    let need = (maxbit >> 3) as usize + 1;
    match ctx.s.get_type_w(db, key, |v| matches!(v, Value::Str(_)))? {
        None => {
            check_len(ctx, need)?;
            let m = bytes::BytesMut::zeroed(need);
            ctx.s.set_key(db, key.clone(), Value::Str(Bytes::new()), false, false);
            Ok(m)
        }
        Some(o) => {
            let mut m = str_mut(&mut o.v);
            if m.len() < need {
                m.resize(need, 0);
            }
            Ok(m)
        }
    }
}

fn store_bits(ctx: &mut Ctx, key: &Bytes, m: bytes::BytesMut) {
    let db = ctx.db;
    if let Some(o) = ctx.s.dbs[db].dict.get_mut(key) {
        o.v = Value::Str(m.freeze());
    }
}

pub fn setbit(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let off = tri!(parse_bit_offset(&args[2], false, 0, ctx));
    let on = match args[3].as_ref() {
        b"1" => true,
        b"0" => false,
        _ => return Frame::err("ERR bit is not an integer or out of range"),
    };
    let mut m = tri!(bits_for_write(ctx, key, off));
    let byte = (off >> 3) as usize;
    let bit = 7 - (off & 7) as u8;
    let old = (m[byte] >> bit) & 1;
    if on {
        m[byte] |= 1 << bit;
    } else {
        m[byte] &= !(1 << bit);
    }
    store_bits(ctx, key, m);
    ctx.modified(key);
    ctx.notify(NOTIFY_STRING, "setbit", key);
    ctx.dirty(1);
    Frame::Integer(old as i64)
}

pub fn getbit(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let off = tri!(parse_bit_offset(&args[2], false, 0, ctx));
    let b = match get_str(ctx, &args[1]) {
        Ok(Some(b)) => b,
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
    };
    let byte = (off >> 3) as usize;
    if byte >= b.len() {
        return Frame::Integer(0);
    }
    Frame::Integer(((b[byte] >> (7 - (off & 7))) & 1) as i64)
}

fn popcount(b: &[u8]) -> u64 {
    let mut n = 0u64;
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        n += u64::from_ne_bytes(c.try_into().unwrap()).count_ones() as u64;
    }
    for &x in chunks.remainder() {
        n += x.count_ones() as u64;
    }
    n
}

pub fn bitcount(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut isbit = false;
    let mut range = None;
    if args.len() == 4 || args.len() == 5 {
        let s = tri!(int(&args[2]));
        let e = tri!(int(&args[3]));
        if args.len() == 5 {
            if eq(&args[4], "bit") {
                isbit = true;
            } else if !eq(&args[4], "byte") {
                return err_syntax();
            }
        }
        range = Some((s, e));
    } else if args.len() == 3 {
        return err_syntax();
    } else if args.len() > 5 {
        return err_syntax();
    }
    let b = match get_str(ctx, &args[1]) {
        Ok(Some(b)) => b,
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
    };
    let Some((s, e)) = range else { return Frame::Integer(popcount(&b) as i64) };
    let total = if isbit { b.len() as i64 * 8 } else { b.len() as i64 };
    let mut s = if s < 0 { total + s } else { s };
    let mut e = if e < 0 { total + e } else { e };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= total {
        e = total - 1;
    }
    if s > e || total == 0 {
        return Frame::Integer(0);
    }
    if !isbit {
        return Frame::Integer(popcount(&b[s as usize..=e as usize]) as i64);
    }
    let (sb, eb) = ((s >> 3) as usize, (e >> 3) as usize);
    let mut n = popcount(&b[sb..=eb]) as i64;
    let first_mask = !(0xFFu8 >> (s & 7));
    let last_mask = (1u16 << (7 - (e & 7))) as u8 - 1;
    n -= (b[sb] & first_mask).count_ones() as i64;
    n -= (b[eb] & last_mask).count_ones() as i64;
    Frame::Integer(n)
}

pub fn bitpos(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let bit = match args[2].as_ref() {
        b"1" => 1u8,
        b"0" => 0u8,
        _ => return Frame::err("ERR The bit argument must be 1 or 0."),
    };
    let mut isbit = false;
    let mut start = 0i64;
    let mut end: Option<i64> = None;
    if args.len() >= 4 {
        start = tri!(int(&args[3]));
    }
    if args.len() >= 5 {
        end = Some(tri!(int(&args[4])));
    }
    if args.len() == 6 {
        if eq(&args[5], "bit") {
            isbit = true;
        } else if !eq(&args[5], "byte") {
            return err_syntax();
        }
    }
    if args.len() > 6 {
        return err_syntax();
    }
    let b = match get_str(ctx, &args[1]) {
        Ok(Some(b)) => b,
        Ok(None) => return Frame::Integer(if bit == 1 { -1 } else { 0 }),
        Err(e) => return e,
    };
    let end_given = end.is_some();
    let total = if isbit { b.len() as i64 * 8 } else { b.len() as i64 };
    let mut s = if start < 0 { total + start } else { start };
    let mut e = match end {
        Some(e) => {
            if e < 0 {
                total + e
            } else {
                e
            }
        }
        None => total - 1,
    };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= total {
        e = total - 1;
    }
    if s > e || total == 0 {
        return Frame::Integer(-1);
    }
    let (sbit, ebit) = if isbit { (s, e) } else { (s * 8, e * 8 + 7) };
    let mut pos = sbit;
    // Byte-at-a-time scan with bit precision at edges.
    while pos <= ebit {
        let byte = b[(pos >> 3) as usize];
        if pos & 7 == 0 && pos + 7 <= ebit {
            let skip = if bit == 1 { byte == 0 } else { byte == 0xFF };
            if skip {
                pos += 8;
                continue;
            }
        }
        let v = (byte >> (7 - (pos & 7))) & 1;
        if v == bit {
            return Frame::Integer(pos);
        }
        pos += 1;
    }
    if bit == 0 && !end_given {
        // Looking for clear bits past the end: the string is padded with zeros.
        return Frame::Integer(ebit + 1);
    }
    Frame::Integer(-1)
}

pub fn bitop(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let op = crate::util::lower(&args[1]);
    if !matches!(op.as_str(), "and" | "or" | "xor" | "not") {
        return err_syntax();
    }
    if op == "not" && args.len() != 4 {
        return Frame::err("ERR BITOP NOT must be called with a single source key.");
    }
    let dest = &args[2];
    let mut srcs = Vec::new();
    for k in &args[3..] {
        match ctx.s.lookup_read(ctx.db, k) {
            None => srcs.push(Bytes::new()),
            Some(o) => match &o.v {
                Value::Str(b) => srcs.push(b.clone()),
                _ => return crate::db::wrongtype(),
            },
        }
    }
    let maxlen = srcs.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut res = vec![0u8; maxlen];
    if op == "not" {
        for (i, r) in res.iter_mut().enumerate() {
            *r = !srcs[0][i];
        }
    } else {
        for i in 0..maxlen {
            let mut acc = if i < srcs[0].len() { srcs[0][i] } else { 0 };
            for s in &srcs[1..] {
                let v = if i < s.len() { s[i] } else { 0 };
                acc = match op.as_str() {
                    "and" => acc & v,
                    "or" => acc | v,
                    _ => acc ^ v,
                };
            }
            res[i] = acc;
        }
    }
    let db = ctx.db;
    if maxlen == 0 {
        if ctx.s.db_delete(db, dest) {
            ctx.notify(NOTIFY_GENERIC, "del", dest);
            ctx.dirty(1);
        }
        return Frame::Integer(0);
    }
    ctx.s.set_key(db, dest.clone(), Value::Str(Bytes::from(res)), false, true);
    ctx.notify(NOTIFY_STRING, "set", dest);
    ctx.dirty(1);
    Frame::Integer(maxlen as i64)
}

#[derive(Clone, Copy)]
enum Ovf {
    Wrap,
    Sat,
    Fail,
}

enum FieldOp {
    Get,
    Set(i64),
    Incr(i64),
}

struct Field {
    op: FieldOp,
    signed: bool,
    bits: u32,
    offset: u64,
    ovf: Ovf,
}

fn parse_type(a: &[u8]) -> Result<(bool, u32), Frame> {
    let err = || Frame::err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
    if a.is_empty() {
        return Err(err());
    }
    let signed = match a[0] {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => return Err(err()),
    };
    let bits = crate::util::parse_i64(&a[1..]).ok_or_else(err)?;
    if bits < 1 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(err());
    }
    Ok((signed, bits as u32))
}

fn get_unsigned(b: &[u8], off: u64, bits: u32) -> u64 {
    let mut v = 0u64;
    for i in 0..bits as u64 {
        let pos = off + i;
        let byte = (pos >> 3) as usize;
        let bit = if byte < b.len() { (b[byte] >> (7 - (pos & 7))) & 1 } else { 0 };
        v = (v << 1) | bit as u64;
    }
    v
}

fn get_signed(b: &[u8], off: u64, bits: u32) -> i64 {
    let u = get_unsigned(b, off, bits);
    if bits == 64 {
        return u as i64;
    }
    if u & (1 << (bits - 1)) != 0 {
        (u | (!0u64 << bits)) as i64
    } else {
        u as i64
    }
}

fn set_bits(b: &mut [u8], off: u64, bits: u32, value: u64) {
    for i in 0..bits as u64 {
        let bitval = (value >> (bits as u64 - 1 - i)) & 1;
        let pos = off + i;
        let byte = (pos >> 3) as usize;
        let shift = 7 - (pos & 7);
        if bitval == 1 {
            b[byte] |= 1 << shift;
        } else {
            b[byte] &= !(1 << shift);
        }
    }
}

/// Returns (value, overflowed). For FAIL, overflowed means don't apply.
fn incr_signed(v: i64, incr: i64, bits: u32, ovf: Ovf) -> (i64, bool) {
    let max: i128 = if bits == 64 { i64::MAX as i128 } else { (1i128 << (bits - 1)) - 1 };
    let min: i128 = -max - 1;
    let r = v as i128 + incr as i128;
    if r > max || r < min {
        match ovf {
            Ovf::Wrap => {
                let m = 1i128 << bits;
                let mut w = (r - min).rem_euclid(m) + min;
                if w > max {
                    w -= m;
                }
                (w as i64, true)
            }
            Ovf::Sat => (if r > max { max as i64 } else { min as i64 }, true),
            Ovf::Fail => (v, true),
        }
    } else {
        (r as i64, false)
    }
}

fn incr_unsigned(v: u64, incr: i64, bits: u32, ovf: Ovf) -> (u64, bool) {
    let max: i128 = (1i128 << bits) - 1;
    let r = v as i128 + incr as i128;
    if r > max || r < 0 {
        match ovf {
            Ovf::Wrap => ((r.rem_euclid(max + 1)) as u64, true),
            Ovf::Sat => (if r > max { max as u64 } else { 0 }, true),
            Ovf::Fail => (v, true),
        }
    } else {
        (r as u64, false)
    }
}

fn bitfield_generic(ctx: &mut Ctx, args: &[Bytes], ro: bool) -> Frame {
    let mut fields = Vec::new();
    let mut ovf = Ovf::Wrap;
    let mut i = 2;
    let mut has_write = false;
    while i < args.len() {
        let a = &args[i];
        let rem = args.len() - i - 1;
        if eq(a, "get") && rem >= 2 {
            let (signed, bits) = tri!(parse_type(&args[i + 1]));
            let off = tri!(parse_bit_offset(&args[i + 2], true, bits as u64, ctx));
            fields.push(Field { op: FieldOp::Get, signed, bits, offset: off, ovf });
            i += 3;
        } else if (eq(a, "set") || eq(a, "incrby")) && rem >= 3 {
            if ro {
                return Frame::err("ERR BITFIELD_RO only supports the GET subcommand");
            }
            let (signed, bits) = tri!(parse_type(&args[i + 1]));
            let off = tri!(parse_bit_offset(&args[i + 2], true, bits as u64, ctx));
            let Some(v) = crate::util::parse_i64(&args[i + 3]) else { return err_int() };
            let op = if eq(a, "set") { FieldOp::Set(v) } else { FieldOp::Incr(v) };
            fields.push(Field { op, signed, bits, offset: off, ovf });
            has_write = true;
            i += 4;
        } else if eq(a, "overflow") && rem >= 1 {
            if ro {
                return Frame::err("ERR BITFIELD_RO only supports the GET subcommand");
            }
            let o = &args[i + 1];
            ovf = if eq(o, "wrap") {
                Ovf::Wrap
            } else if eq(o, "sat") {
                Ovf::Sat
            } else if eq(o, "fail") {
                Ovf::Fail
            } else {
                return Frame::err("ERR Invalid OVERFLOW type specified");
            };
            i += 2;
        } else {
            return err_syntax();
        }
    }
    let key = &args[1];
    if !has_write {
        let b = match get_str(ctx, key) {
            Ok(Some(b)) => b,
            Ok(None) => Bytes::new(),
            Err(e) => return e,
        };
        let out = fields
            .iter()
            .map(|f| {
                Frame::Integer(if f.signed {
                    get_signed(&b, f.offset, f.bits)
                } else {
                    get_unsigned(&b, f.offset, f.bits) as i64
                })
            })
            .collect();
        return Frame::Array(out);
    }
    let maxbit = fields.iter().filter(|f| !matches!(f.op, FieldOp::Get)).map(|f| f.offset + f.bits as u64 - 1).max().unwrap_or(0);
    let mut m = tri!(bits_for_write(ctx, key, maxbit));
    // GET fields beyond the write range read zeros naturally.
    let mut out = Vec::new();
    let mut changes = 0;
    for f in &fields {
        match f.op {
            FieldOp::Get => {
                out.push(Frame::Integer(if f.signed {
                    get_signed(&m, f.offset, f.bits)
                } else {
                    get_unsigned(&m, f.offset, f.bits) as i64
                }));
            }
            FieldOp::Set(v) => {
                if f.signed {
                    let old = get_signed(&m, f.offset, f.bits);
                    let (nv, over) = incr_signed(v, 0, f.bits, f.ovf);
                    // Setting a value out of range wraps/saturates/fails like Redis.
                    let (nv, over) = if f.bits < 64 {
                        let max = (1i64 << (f.bits - 1)) - 1;
                        let min = -max - 1;
                        if v > max || v < min {
                            match f.ovf {
                                Ovf::Wrap => {
                                    let mask = (1u64 << f.bits) - 1;
                                    let u = (v as u64) & mask;
                                    let s = if u & (1 << (f.bits - 1)) != 0 { (u | !mask) as i64 } else { u as i64 };
                                    (s, true)
                                }
                                Ovf::Sat => (if v > max { max } else { min }, true),
                                Ovf::Fail => (old, true),
                            }
                        } else {
                            (nv, over)
                        }
                    } else {
                        (nv, over)
                    };
                    if over && matches!(f.ovf, Ovf::Fail) {
                        out.push(Frame::Null);
                    } else {
                        set_bits(&mut m, f.offset, f.bits, nv as u64);
                        out.push(Frame::Integer(old));
                        changes += 1;
                    }
                } else {
                    let old = get_unsigned(&m, f.offset, f.bits);
                    let max = (1u64 << f.bits) - 1;
                    let (nv, over) = if v < 0 || v as u64 > max {
                        match f.ovf {
                            Ovf::Wrap => ((v as u64) & max, true),
                            Ovf::Sat => (if v < 0 { 0 } else { max }, true),
                            Ovf::Fail => (old, true),
                        }
                    } else {
                        (v as u64, false)
                    };
                    if over && matches!(f.ovf, Ovf::Fail) {
                        out.push(Frame::Null);
                    } else {
                        set_bits(&mut m, f.offset, f.bits, nv);
                        out.push(Frame::Integer(old as i64));
                        changes += 1;
                    }
                }
            }
            FieldOp::Incr(by) => {
                if f.signed {
                    let old = get_signed(&m, f.offset, f.bits);
                    let (nv, over) = incr_signed(old, by, f.bits, f.ovf);
                    if over && matches!(f.ovf, Ovf::Fail) {
                        out.push(Frame::Null);
                    } else {
                        set_bits(&mut m, f.offset, f.bits, nv as u64);
                        out.push(Frame::Integer(nv));
                        changes += 1;
                    }
                } else {
                    let old = get_unsigned(&m, f.offset, f.bits);
                    let (nv, over) = incr_unsigned(old, by, f.bits, f.ovf);
                    if over && matches!(f.ovf, Ovf::Fail) {
                        out.push(Frame::Null);
                    } else {
                        set_bits(&mut m, f.offset, f.bits, nv);
                        out.push(Frame::Integer(nv as i64));
                        changes += 1;
                    }
                }
            }
        }
    }
    store_bits(ctx, key, m);
    if changes > 0 {
        ctx.modified(key);
        ctx.notify(NOTIFY_STRING, "setbit", key);
        ctx.dirty(changes);
    }
    Frame::Array(out)
}

pub fn bitfield(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bitfield_generic(ctx, args, false)
}
pub fn bitfield_ro(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    bitfield_generic(ctx, args, true)
}
