//! RESP2 / RESP3 protocol: request parsing (multibulk + inline) and reply encoding.

use bytes::{Buf, Bytes, BytesMut};

/// A reply value. Encoded differently depending on the negotiated protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    /// Null bulk string in RESP2 (`$-1`), `_` in RESP3.
    Null,
    /// Null array in RESP2 (`*-1`), `_` in RESP3.
    NullArray,
    Array(Vec<Frame>),
    /// Map: flattened array in RESP2.
    Map(Vec<(Frame, Frame)>),
    /// Set: array in RESP2.
    Set(Vec<Frame>),
    /// Double: bulk string in RESP2.
    Double(f64),
    /// Boolean: integer 0/1 in RESP2.
    Bool(bool),
    /// Verbatim string (format, payload): bulk in RESP2.
    Verbatim(&'static str, Bytes),
    /// Big number: bulk string in RESP2.
    BigNumber(String),
    /// Out-of-band push (pub/sub, invalidation): array in RESP2.
    Push(Vec<Frame>),
    /// Pre-encoded raw bytes, written verbatim.
    Raw(Bytes),
    /// Internal: command must block. Never encoded.
    Block(Box<crate::blocking::BlockSpec>),
    /// Internal: no reply should be written.
    NoReply,
    /// Several consecutive replies (e.g. SUBSCRIBE with many channels).
    Seq(Vec<Frame>),
}

impl Frame {
    pub fn ok() -> Frame {
        Frame::Simple("OK".into())
    }
    pub fn queued() -> Frame {
        Frame::Simple("QUEUED".into())
    }
    pub fn err(s: impl Into<String>) -> Frame {
        Frame::Error(s.into())
    }
    pub fn bulk(b: impl Into<Bytes>) -> Frame {
        Frame::Bulk(b.into())
    }
    pub fn bulk_str(s: &str) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(s.as_bytes()))
    }
    pub fn bulk_int(i: i64) -> Frame {
        Frame::Bulk(Bytes::from(i.to_string()))
    }
    pub fn is_error(&self) -> bool {
        matches!(self, Frame::Error(_))
    }
    pub fn array_of_bulks<I: IntoIterator<Item = Bytes>>(it: I) -> Frame {
        Frame::Array(it.into_iter().map(Frame::Bulk).collect())
    }

    pub fn encode(&self, proto: u8, out: &mut Vec<u8>) {
        match self {
            Frame::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Error(s) => {
                out.push(b'-');
                // Errors must be single-line.
                for &c in s.as_bytes() {
                    out.push(if c == b'\r' || c == b'\n' { b' ' } else { c });
                }
                out.extend_from_slice(b"\r\n");
            }
            Frame::Integer(i) => {
                out.push(b':');
                push_int(out, *i);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Bulk(b) => push_bulk(out, b),
            Frame::Null => {
                if proto >= 3 {
                    out.extend_from_slice(b"_\r\n");
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            Frame::NullArray => {
                if proto >= 3 {
                    out.extend_from_slice(b"_\r\n");
                } else {
                    out.extend_from_slice(b"*-1\r\n");
                }
            }
            Frame::Array(items) => {
                push_len(out, b'*', items.len());
                for i in items {
                    i.encode(proto, out);
                }
            }
            Frame::Map(pairs) => {
                if proto >= 3 {
                    push_len(out, b'%', pairs.len());
                } else {
                    push_len(out, b'*', pairs.len() * 2);
                }
                for (k, v) in pairs {
                    k.encode(proto, out);
                    v.encode(proto, out);
                }
            }
            Frame::Set(items) => {
                push_len(out, if proto >= 3 { b'~' } else { b'*' }, items.len());
                for i in items {
                    i.encode(proto, out);
                }
            }
            Frame::Push(items) => {
                push_len(out, if proto >= 3 { b'>' } else { b'*' }, items.len());
                for i in items {
                    i.encode(proto, out);
                }
            }
            Frame::Double(d) => {
                let s = fmt_double(*d);
                if proto >= 3 {
                    out.push(b',');
                    out.extend_from_slice(s.as_bytes());
                    out.extend_from_slice(b"\r\n");
                } else {
                    push_bulk(out, s.as_bytes());
                }
            }
            Frame::Bool(b) => {
                if proto >= 3 {
                    out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    out.extend_from_slice(if *b { b":1\r\n" } else { b":0\r\n" });
                }
            }
            Frame::Verbatim(fmt, payload) => {
                if proto >= 3 {
                    out.push(b'=');
                    push_int(out, (payload.len() + 4) as i64);
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(fmt.as_bytes());
                    out.push(b':');
                    out.extend_from_slice(payload);
                    out.extend_from_slice(b"\r\n");
                } else {
                    push_bulk(out, payload);
                }
            }
            Frame::BigNumber(n) => {
                if proto >= 3 {
                    out.push(b'(');
                    out.extend_from_slice(n.as_bytes());
                    out.extend_from_slice(b"\r\n");
                } else {
                    push_bulk(out, n.as_bytes());
                }
            }
            Frame::Raw(b) => out.extend_from_slice(b),
            Frame::Seq(v) => {
                for f in v {
                    f.encode(proto, out);
                }
            }
            Frame::Block(_) | Frame::NoReply => {}
        }
    }
}

fn push_int(out: &mut Vec<u8>, i: i64) {
    let mut buf = itoa_buf(i);
    out.append(&mut buf);
}

fn itoa_buf(i: i64) -> Vec<u8> {
    i.to_string().into_bytes()
}

fn push_len(out: &mut Vec<u8>, prefix: u8, len: usize) {
    out.push(prefix);
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn push_bulk(out: &mut Vec<u8>, b: &[u8]) {
    push_len(out, b'$', b.len());
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

/// Format a double the way Redis does for replies (`%.17g` then shortest form).
pub fn fmt_double(d: f64) -> String {
    if d.is_infinite() {
        return if d > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if d.is_nan() {
        return "nan".into();
    }
    crate::util::fmt_g17(d)
}

#[derive(Debug)]
pub enum ProtoError {
    Msg(String),
}

pub const PROTO_INLINE_MAX: usize = 64 * 1024;
pub const PROTO_MBULK_BIG_ARG: usize = 32 * 1024;

/// Parse one complete command from the buffer. Returns Ok(None) if more data is needed.
pub fn parse_command(
    buf: &mut BytesMut,
    max_bulk: usize,
) -> Result<Option<Vec<Bytes>>, ProtoError> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }
    // Multibulk.
    let Some(line_end) = find_crlf(buf, 1) else {
        if buf.len() > PROTO_INLINE_MAX {
            return Err(ProtoError::Msg(
                "Protocol error: too big mbulk count string".into(),
            ));
        }
        return Ok(None);
    };
    let count = parse_len(&buf[1..line_end])
        .filter(|&n| n <= 1024 * 1024 * 1024)
        .ok_or_else(|| ProtoError::Msg("Protocol error: invalid multibulk length".into()))?;
    if count <= 0 {
        buf.advance(line_end + 2);
        return Ok(Some(Vec::new()));
    }
    let count = count as usize;
    let mut pos = line_end + 2;
    // First pass: verify the whole command is present without copying.
    let mut spans = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            return Err(ProtoError::Msg(format!(
                "Protocol error: expected '$', got '{}'",
                buf[pos] as char
            )));
        }
        let Some(le) = find_crlf(buf, pos + 1) else {
            if buf.len() - pos > PROTO_INLINE_MAX {
                return Err(ProtoError::Msg(
                    "Protocol error: too big bulk count string".into(),
                ));
            }
            return Ok(None);
        };
        let len = parse_len(&buf[pos + 1..le])
            .filter(|&n| n >= 0 && (n as usize) <= max_bulk)
            .ok_or_else(|| ProtoError::Msg("Protocol error: invalid bulk length".into()))?
            as usize;
        let start = le + 2;
        if buf.len() < start + len + 2 {
            if buf.capacity() < start + len + 2 {
                buf.reserve(start + len + 2 - buf.len());
            }
            return Ok(None);
        }
        spans.push((start, len));
        pos = start + len + 2;
    }
    let frozen = buf.split_to(pos).freeze();
    let args = spans
        .into_iter()
        .map(|(s, l)| frozen.slice(s..s + l))
        .collect();
    Ok(Some(args))
}

fn parse_inline(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtoError> {
    let Some(nl) = buf.iter().position(|&c| c == b'\n') else {
        if buf.len() > PROTO_INLINE_MAX {
            return Err(ProtoError::Msg(
                "Protocol error: too big inline request".into(),
            ));
        }
        return Ok(None);
    };
    let mut line_end = nl;
    if line_end > 0 && buf[line_end - 1] == b'\r' {
        line_end -= 1;
    }
    let line = buf[..line_end].to_vec();
    buf.advance(nl + 1);
    match split_args(&line) {
        Some(args) => Ok(Some(args.into_iter().map(Bytes::from).collect())),
        None => Err(ProtoError::Msg(
            "Protocol error: unbalanced quotes in request".into(),
        )),
    }
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_len(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    std::str::from_utf8(s).ok()?.parse::<i64>().ok()
}

/// sdssplitargs: split a line into arguments honoring quotes and escapes.
pub fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0;
    let n = line.len();
    loop {
        while i < n && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            return Some(args);
        }
        let mut cur = Vec::new();
        let mut inq = false;
        let mut insq = false;
        let mut done = false;
        while !done {
            if inq {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\'
                    && i + 3 < n
                    && line[i + 1] == b'x'
                    && line[i + 2].is_ascii_hexdigit()
                    && line[i + 3].is_ascii_hexdigit()
                {
                    let h = |c: u8| (c as char).to_digit(16).unwrap() as u8;
                    cur.push(h(line[i + 2]) * 16 + h(line[i + 3]));
                    i += 3;
                } else if line[i] == b'\\' && i + 1 < n {
                    i += 1;
                    cur.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        c => c,
                    });
                } else if line[i] == b'"' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    done = true;
                } else {
                    cur.push(line[i]);
                }
            } else if insq {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    i += 1;
                    cur.push(b'\'');
                } else if line[i] == b'\'' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    done = true;
                } else {
                    cur.push(line[i]);
                }
            } else if i >= n {
                done = true;
            } else {
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' | 0 => done = true,
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    c => cur.push(c),
                }
            }
            if i < n {
                i += 1;
            }
        }
        args.push(cur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multibulk() {
        let mut b = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$4\r\nPI"[..]);
        let c = parse_command(&mut b, 512 << 20).unwrap().unwrap();
        assert_eq!(c, vec![Bytes::from("GET"), Bytes::from("a")]);
        assert!(parse_command(&mut b, 512 << 20).unwrap().is_none());
        b.extend_from_slice(b"NG\r\n");
        let c = parse_command(&mut b, 512 << 20).unwrap().unwrap();
        assert_eq!(c, vec![Bytes::from("PING")]);
    }

    #[test]
    fn inline() {
        let mut b = BytesMut::from(&b"set k \"a b\\n\"\r\n"[..]);
        let c = parse_command(&mut b, 512 << 20).unwrap().unwrap();
        assert_eq!(c[2], Bytes::from("a b\n"));
    }
}
