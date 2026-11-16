//! Small helpers: number parsing/formatting identical to Redis, glob matching, clock, RNG.

use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Redis `string2ll`: strict signed 64-bit integer parsing.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s.len() == 1 && s[0] == b'0' {
        return Some(0);
    }
    let (neg, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    if digits.is_empty() || digits[0] < b'1' || digits[0] > b'9' {
        return None;
    }
    let mut v: u64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    if neg {
        if v > (i64::MAX as u64) + 1 {
            None
        } else {
            Some((v as i64).wrapping_neg())
        }
    } else if v > i64::MAX as u64 {
        None
    } else {
        Some(v as i64)
    }
}

pub fn parse_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s.len() > 1 && s[0] == b'0' {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    Some(v)
}

/// Redis `string2d` / `string2ld`: strtod-like, rejecting NaN and surrounding spaces.
pub fn parse_f64(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s.len() > 5120 {
        return None;
    }
    if s[0].is_ascii_whitespace() || s[s.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let st = std::str::from_utf8(s).ok()?;
    let lower = st.to_ascii_lowercase();
    let v = match lower.as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        _ => {
            if lower.contains("nan") || lower.contains("inf") {
                return None;
            }
            // Rust's parser accepts a superset in some places ("1." ".5" ok, same as strtod).
            if let Some(hex) = lower.strip_prefix("0x").or_else(|| lower.strip_prefix("-0x")) {
                let neg = lower.starts_with('-');
                let v = u64::from_str_radix(hex, 16).ok()? as f64;
                if neg { -v } else { v }
            } else {
                if lower.starts_with('+') && lower.len() > 1 && lower.as_bytes()[1] == b'-' {
                    return None;
                }
                lower.parse::<f64>().ok()?
            }
        }
    };
    if v.is_nan() {
        return None;
    }
    Some(v)
}

/// Format like Redis `d2string` (fpconv shortest representation).
pub fn fmt_g17(d: f64) -> String {
    if d.is_nan() {
        return "nan".into();
    }
    if d.is_infinite() {
        return if d > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    if d.fract() == 0.0 && d.abs() < 9.007199254740992e15 {
        return (d as i64).to_string();
    }
    let neg = d < 0.0;
    let sci = format!("{:e}", d.abs()); // e.g. "1.2345e-7"
    let (mant, exp) = sci.split_once('e').unwrap();
    let e: i32 = exp.parse().unwrap();
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let nd = digits.len() as i32;
    let k = e - nd + 1;
    let expv = (k + nd - 1).abs();
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if k >= 0 && expv < nd + 7 {
        out.push_str(digits);
        for _ in 0..k {
            out.push('0');
        }
        return out;
    }
    if k < 0 && (k > -7 || expv < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(digits);
        } else {
            out.push_str(&digits[..offset as usize]);
            out.push('.');
            out.push_str(&digits[offset as usize..]);
        }
        return out;
    }
    let maxd = (18 - neg as i32).min(nd) as usize;
    out.push_str(&digits[..1]);
    if maxd > 1 {
        out.push('.');
        out.push_str(&digits[1..maxd]);
    }
    out.push('e');
    out.push(if k + nd - 1 < 0 { '-' } else { '+' });
    out.push_str(&expv.to_string());
    out
}

/// Human formatting used by INCRBYFLOAT / HINCRBYFLOAT (`%.17Lf` with trailing zeros removed).
pub fn fmt_human(d: f64) -> String {
    if d.is_infinite() {
        return if d > 0.0 { "inf".into() } else { "-inf".into() };
    }
    let s = format!("{}", d);
    if s == "-0" { "0".into() } else { s }
}

/// Redis `stringmatchlen` glob matching.
pub fn glob_match(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    glob_inner(pattern, string, nocase, 0)
}

fn glob_inner(mut p: &[u8], mut s: &[u8], nocase: bool, depth: u32) -> bool {
    if depth > 1000 {
        return false;
    }
    let eq = |a: u8, b: u8| {
        if nocase { a.eq_ignore_ascii_case(&b) } else { a == b }
    };
    while !p.is_empty() && !s.is_empty() {
        match p[0] {
            b'*' => {
                while p.len() > 1 && p[1] == b'*' {
                    p = &p[1..];
                }
                if p.len() == 1 {
                    return true;
                }
                for i in 0..=s.len() {
                    if glob_inner(&p[1..], &s[i..], nocase, depth + 1) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                s = &s[1..];
            }
            b'[' => {
                p = &p[1..];
                let not = !p.is_empty() && p[0] == b'^';
                if not {
                    p = &p[1..];
                }
                let mut matched = false;
                loop {
                    if p.is_empty() {
                        break;
                    }
                    if p[0] == b'\\' && p.len() >= 2 {
                        p = &p[1..];
                        if eq(p[0], s[0]) {
                            matched = true;
                        }
                    } else if p[0] == b']' {
                        break;
                    } else if p.len() >= 3 && p[1] == b'-' {
                        let (mut start, mut end) = (p[0], p[2]);
                        let mut c = s[0];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        if nocase {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        p = &p[2..];
                        if c >= start && c <= end {
                            matched = true;
                        }
                    } else if eq(p[0], s[0]) {
                        matched = true;
                    }
                    p = &p[1..];
                }
                // p now at ']' or empty
                if p.is_empty() {
                    // unterminated: treat as end of pattern
                }
                let matched = if not { !matched } else { matched };
                if !matched {
                    return false;
                }
                s = &s[1..];
            }
            b'\\' if p.len() >= 2 => {
                p = &p[1..];
                if !eq(p[0], s[0]) {
                    return false;
                }
                s = &s[1..];
            }
            c => {
                if !eq(c, s[0]) {
                    return false;
                }
                s = &s[1..];
            }
        }
        if p.is_empty() {
            break;
        }
        p = &p[1..];
        if s.is_empty() {
            while !p.is_empty() && p[0] == b'*' {
                p = &p[1..];
            }
            break;
        }
    }
    p.is_empty() && s.is_empty()
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        let t = now_us() as u64;
        let addr = &t as *const u64 as u64;
        (t ^ addr.rotate_left(32)) | 1
    });
}

/// xorshift64* — fast non-cryptographic RNG.
pub fn rand_u64() -> u64 {
    RNG.with(|r| {
        let mut x = r.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        r.set(x);
        x.wrapping_mul(0x2545F4914F6CDD1D)
    })
}

pub fn rand_range(n: usize) -> usize {
    if n == 0 { 0 } else { (rand_u64() % n as u64) as usize }
}

pub fn rand_f64() -> f64 {
    (rand_u64() >> 11) as f64 / (1u64 << 53) as f64
}

pub fn random_hex(n: usize) -> String {
    const H: &[u8] = b"0123456789abcdef";
    (0..n).map(|_| H[rand_range(16)] as char).collect()
}

pub fn eq_ic(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}

pub fn lower(a: &[u8]) -> String {
    String::from_utf8_lossy(a).to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ints() {
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775808"), None);
        assert_eq!(parse_i64(b"01"), None);
        assert_eq!(parse_i64(b" 1"), None);
        assert_eq!(parse_i64(b"+1"), None);
    }
    #[test]
    fn doubles() {
        assert_eq!(fmt_g17(1.5), "1.5");
        assert_eq!(fmt_g17(3.0), "3");
        assert_eq!(fmt_g17(1e21), "1e+21");
        assert_eq!(fmt_g17(0.0001), "0.0001");
        assert_eq!(fmt_g17(1.5e-7), "1.5e-7");
        assert_eq!(fmt_human(10.5 + 0.1), "10.6");
    }
    #[test]
    fn glob() {
        assert!(glob_match(b"h?llo", b"hello", false));
        assert!(glob_match(b"h*llo", b"heeeello", false));
        assert!(glob_match(b"h[ae]llo", b"hallo", false));
        assert!(!glob_match(b"h[^e]llo", b"hello", false));
        assert!(glob_match(b"h[a-b]llo", b"hbllo", false));
        assert!(!glob_match(b"*", b"", false)); // same as Redis stringmatchlen
        assert!(glob_match(b"a*", b"a", false));
    }
}

/// Redis "human" long double formatting of a double (`%.17Lf`, trailing zeros removed),
/// as used by GEOPOS / WITHCOORD.
pub fn fmt_ld17(d: f64) -> String {
    if d.is_infinite() {
        return if d > 0.0 { "inf".into() } else { "-inf".into() };
    }
    let mut s = format!("{:.17}", d);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" { "0".into() } else { s }
}

/// Timestamp in Redis log format: "16 Nov 2026 10:00:00.123" (UTC).
pub fn log_time() -> String {
    let ms = now_ms();
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    // Civil-from-days (Howard Hinnant).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    const MON: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    format!("{:02} {} {} {:02}:{:02}:{:02}.{:03}", d, MON[(m - 1) as usize], y, rem / 3600, (rem / 60) % 60, rem % 60, ms.rem_euclid(1000))
}

/// Number -> string conversion used for Lua arguments (integers print without decimals).
pub fn fmt_lua_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e17 {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

pub fn fmt_double_resp(d: f64) -> String {
    crate::resp::fmt_double(d)
}
