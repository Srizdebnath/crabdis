//! Argument parsing helpers shared by command implementations.

use crate::resp::Frame;
use crate::util;
use bytes::Bytes;

pub type R<T> = Result<T, Frame>;

pub fn err_int() -> Frame {
    Frame::err("ERR value is not an integer or out of range")
}
pub fn err_float() -> Frame {
    Frame::err("ERR value is not a valid float")
}
pub fn err_syntax() -> Frame {
    Frame::err("ERR syntax error")
}
pub fn err_nokey() -> Frame {
    Frame::err("ERR no such key")
}

pub fn int(a: &[u8]) -> R<i64> {
    util::parse_i64(a).ok_or_else(err_int)
}

pub fn int_range(a: &[u8], lo: i64, hi: i64, msg: &str) -> R<i64> {
    match util::parse_i64(a) {
        Some(v) if v >= lo && v <= hi => Ok(v),
        _ => Err(Frame::err(format!("ERR {}", msg))),
    }
}

pub fn positive(a: &[u8], what: &str) -> R<i64> {
    match util::parse_i64(a) {
        Some(v) if v > 0 => Ok(v),
        _ => Err(Frame::err(format!("ERR {} should be greater than 0", what))),
    }
}

pub fn float(a: &[u8]) -> R<f64> {
    util::parse_f64(a).ok_or_else(err_float)
}

pub fn eq(a: &[u8], s: &str) -> bool {
    a.eq_ignore_ascii_case(s.as_bytes())
}

/// Normalize a [start, end] range for a sequence of length `len`.
/// Returns None if the range is empty.
pub fn norm_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if s > e || s >= len {
        return None;
    }
    if e >= len {
        e = len - 1;
    }
    Some((s as usize, e as usize))
}

pub struct ScanOpts {
    pub cursor: u64,
    pub pattern: Option<Bytes>,
    pub count: usize,
    pub type_: Option<String>,
    pub novalues: bool,
    pub noscores: bool,
}

/// Parse "cursor [MATCH pattern] [COUNT count] [TYPE type] [NOVALUES]" starting at args[idx].
pub fn parse_scan(args: &[Bytes], idx: usize, allow_type: bool, allow_novalues: bool) -> R<ScanOpts> {
    let cursor = util::parse_u64(&args[idx]).ok_or_else(|| Frame::err("ERR invalid cursor"))?;
    let mut o = ScanOpts { cursor, pattern: None, count: 10, type_: None, novalues: false, noscores: false };
    let mut i = idx + 1;
    while i < args.len() {
        let a = &args[i];
        if eq(a, "count") && i + 1 < args.len() {
            let c = int(&args[i + 1])?;
            if c < 1 {
                return Err(err_syntax());
            }
            o.count = c as usize;
            i += 2;
        } else if eq(a, "match") && i + 1 < args.len() {
            let p = args[i + 1].clone();
            o.pattern = if p.as_ref() == b"*" { None } else { Some(p) };
            i += 2;
        } else if allow_type && eq(a, "type") && i + 1 < args.len() {
            o.type_ = Some(util::lower(&args[i + 1]));
            i += 2;
        } else if allow_novalues && eq(a, "novalues") {
            o.novalues = true;
            i += 1;
        } else if allow_novalues && eq(a, "noscores") {
            o.noscores = true;
            i += 1;
        } else {
            return Err(err_syntax());
        }
    }
    Ok(o)
}

pub fn help(lines: &[&str]) -> Frame {
    Frame::Array(lines.iter().map(|l| Frame::Simple(l.to_string())).collect())
}

pub fn help_with_header(cmd: &str, lines: &[&str]) -> Frame {
    let mut v = vec![Frame::Simple(format!(
        "{} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        cmd
    ))];
    v.extend(lines.iter().map(|l| Frame::Simple(l.to_string())));
    v.push(Frame::Simple("HELP".into()));
    v.push(Frame::Simple("    Print this help.".into()));
    Frame::Array(v)
}

pub fn bulk(b: &Bytes) -> Frame {
    Frame::Bulk(b.clone())
}

pub fn opt_bulk(b: Option<Bytes>) -> Frame {
    b.map(Frame::Bulk).unwrap_or(Frame::Null)
}

pub fn i64_bytes(v: i64) -> Bytes {
    Bytes::from(v.to_string())
}

/// Count-style reply helper: score as double in RESP3, bulk in RESP2.
pub fn score_frame(v: f64) -> Frame {
    Frame::Double(v)
}

#[macro_export]
macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(f) => return f,
        }
    };
}
