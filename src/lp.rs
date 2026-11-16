//! Listpack serialization (the format Redis uses inside RDB files for small
//! collections and stream nodes), plus intset decoding.

#[derive(Debug, Clone, PartialEq)]
pub enum LpVal {
    Int(i64),
    Str(Vec<u8>),
}

impl LpVal {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            LpVal::Int(i) => i.to_string().into_bytes(),
            LpVal::Str(s) => s.clone(),
        }
    }
    pub fn as_int(&self) -> Option<i64> {
        match self {
            LpVal::Int(i) => Some(*i),
            LpVal::Str(s) => crate::util::parse_i64(s),
        }
    }
}

fn backlen_size(l: usize) -> usize {
    if l <= 127 {
        1
    } else if l < 16383 {
        2
    } else if l < 2097151 {
        3
    } else if l < 268435455 {
        4
    } else {
        5
    }
}

fn encode_backlen(out: &mut Vec<u8>, l: usize) {
    match backlen_size(l) {
        1 => out.push(l as u8),
        2 => {
            out.push((l >> 7) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        3 => {
            out.push((l >> 14) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        4 => {
            out.push((l >> 21) as u8);
            out.push((((l >> 14) & 127) | 128) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        _ => {
            out.push((l >> 28) as u8);
            out.push((((l >> 21) & 127) | 128) as u8);
            out.push((((l >> 14) & 127) | 128) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
    }
}

pub struct Writer {
    buf: Vec<u8>,
    count: usize,
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer {
    pub fn new() -> Self {
        Writer { buf: vec![0, 0, 0, 0, 0, 0], count: 0 }
    }

    pub fn int(&mut self, v: i64) {
        let start = self.buf.len();
        if (0..=127).contains(&v) {
            self.buf.push(v as u8);
        } else if (-4096..=4095).contains(&v) {
            let u = (v as u64 & 0x1fff) as u16;
            self.buf.push(0xc0 | (u >> 8) as u8);
            self.buf.push((u & 0xff) as u8);
        } else if (-32768..=32767).contains(&v) {
            self.buf.push(0xf1);
            self.buf.extend_from_slice(&(v as i16).to_le_bytes());
        } else if (-8388608..=8388607).contains(&v) {
            self.buf.push(0xf2);
            let b = (v as i32).to_le_bytes();
            self.buf.extend_from_slice(&b[..3]);
        } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
            self.buf.push(0xf3);
            self.buf.extend_from_slice(&(v as i32).to_le_bytes());
        } else {
            self.buf.push(0xf4);
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        let l = self.buf.len() - start;
        encode_backlen(&mut self.buf, l);
        self.count += 1;
    }

    /// Append a string, encoding it as an integer when it is one (like lpAppend).
    pub fn str(&mut self, s: &[u8]) {
        if s.len() <= 20 {
            if let Some(v) = crate::util::parse_i64(s) {
                return self.int(v);
            }
        }
        self.raw_str(s);
    }

    pub fn raw_str(&mut self, s: &[u8]) {
        let start = self.buf.len();
        let l = s.len();
        if l < 64 {
            self.buf.push(0x80 | l as u8);
        } else if l < 4096 {
            self.buf.push(0xe0 | (l >> 8) as u8);
            self.buf.push((l & 0xff) as u8);
        } else {
            self.buf.push(0xf0);
            self.buf.extend_from_slice(&(l as u32).to_le_bytes());
        }
        self.buf.extend_from_slice(s);
        let el = self.buf.len() - start;
        encode_backlen(&mut self.buf, el);
        self.count += 1;
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.buf.push(0xff);
        let total = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&total.to_le_bytes());
        let n = if self.count < 65535 { self.count as u16 } else { 65535 };
        self.buf[4..6].copy_from_slice(&n.to_le_bytes());
        self.buf
    }
}

/// Decode every element of a listpack.
pub fn decode(lp: &[u8]) -> Result<Vec<LpVal>, String> {
    if lp.len() < 7 {
        return Err("listpack too short".into());
    }
    let total = u32::from_le_bytes(lp[0..4].try_into().unwrap()) as usize;
    if total != lp.len() {
        return Err("listpack size mismatch".into());
    }
    let mut out = Vec::new();
    let mut p = 6;
    loop {
        if p >= lp.len() {
            return Err("listpack truncated".into());
        }
        let c = lp[p];
        if c == 0xff {
            break;
        }
        let start = p;
        let v;
        if c & 0x80 == 0 {
            v = LpVal::Int((c & 0x7f) as i64);
            p += 1;
        } else if c & 0xc0 == 0x80 {
            let l = (c & 0x3f) as usize;
            let s = lp.get(p + 1..p + 1 + l).ok_or("listpack truncated")?;
            v = LpVal::Str(s.to_vec());
            p += 1 + l;
        } else if c & 0xe0 == 0xc0 {
            let raw = (((c & 0x1f) as u16) << 8) | *lp.get(p + 1).ok_or("listpack truncated")? as u16;
            let iv = if raw >= 1 << 12 { raw as i64 - (1 << 13) } else { raw as i64 };
            v = LpVal::Int(iv);
            p += 2;
        } else if c & 0xf0 == 0xe0 {
            let l = (((c & 0x0f) as usize) << 8) | *lp.get(p + 1).ok_or("listpack truncated")? as usize;
            let s = lp.get(p + 2..p + 2 + l).ok_or("listpack truncated")?;
            v = LpVal::Str(s.to_vec());
            p += 2 + l;
        } else {
            match c {
                0xf0 => {
                    let l = u32::from_le_bytes(lp.get(p + 1..p + 5).ok_or("listpack truncated")?.try_into().unwrap()) as usize;
                    let s = lp.get(p + 5..p + 5 + l).ok_or("listpack truncated")?;
                    v = LpVal::Str(s.to_vec());
                    p += 5 + l;
                }
                0xf1 => {
                    v = LpVal::Int(i16::from_le_bytes(lp.get(p + 1..p + 3).ok_or("listpack truncated")?.try_into().unwrap()) as i64);
                    p += 3;
                }
                0xf2 => {
                    let b = lp.get(p + 1..p + 4).ok_or("listpack truncated")?;
                    let raw = (b[0] as i32) | ((b[1] as i32) << 8) | ((b[2] as i32) << 16);
                    let iv = (raw << 8) >> 8;
                    v = LpVal::Int(iv as i64);
                    p += 4;
                }
                0xf3 => {
                    v = LpVal::Int(i32::from_le_bytes(lp.get(p + 1..p + 5).ok_or("listpack truncated")?.try_into().unwrap()) as i64);
                    p += 5;
                }
                0xf4 => {
                    v = LpVal::Int(i64::from_le_bytes(lp.get(p + 1..p + 9).ok_or("listpack truncated")?.try_into().unwrap()));
                    p += 9;
                }
                _ => return Err("invalid listpack encoding".into()),
            }
        }
        p += backlen_size(p - start);
        out.push(v);
    }
    Ok(out)
}

/// Decode an intset blob into its integers.
pub fn decode_intset(b: &[u8]) -> Result<Vec<i64>, String> {
    if b.len() < 8 {
        return Err("intset too short".into());
    }
    let enc = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let len = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
    if ![2, 4, 8].contains(&enc) || b.len() != 8 + enc * len {
        return Err("invalid intset".into());
    }
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let s = &b[8 + i * enc..8 + (i + 1) * enc];
        out.push(match enc {
            2 => i16::from_le_bytes(s.try_into().unwrap()) as i64,
            4 => i32::from_le_bytes(s.try_into().unwrap()) as i64,
            _ => i64::from_le_bytes(s.try_into().unwrap()),
        });
    }
    Ok(out)
}

/// Decode a legacy ziplist (RDB types 10/12/13 and quicklist v1 nodes).
pub fn decode_ziplist(z: &[u8]) -> Result<Vec<LpVal>, String> {
    if z.len() < 11 {
        return Err("ziplist too short".into());
    }
    let mut p = 10;
    let mut out = Vec::new();
    loop {
        let c = *z.get(p).ok_or("ziplist truncated")?;
        if c == 0xff {
            break;
        }
        // prevlen
        p += if c < 254 { 1 } else { 5 };
        let e = *z.get(p).ok_or("ziplist truncated")?;
        match e >> 6 {
            0 => {
                let l = (e & 0x3f) as usize;
                out.push(LpVal::Str(z.get(p + 1..p + 1 + l).ok_or("ziplist truncated")?.to_vec()));
                p += 1 + l;
            }
            1 => {
                let l = (((e & 0x3f) as usize) << 8) | *z.get(p + 1).ok_or("ziplist truncated")? as usize;
                out.push(LpVal::Str(z.get(p + 2..p + 2 + l).ok_or("ziplist truncated")?.to_vec()));
                p += 2 + l;
            }
            2 => {
                let l = u32::from_be_bytes(z.get(p + 1..p + 5).ok_or("ziplist truncated")?.try_into().unwrap()) as usize;
                out.push(LpVal::Str(z.get(p + 5..p + 5 + l).ok_or("ziplist truncated")?.to_vec()));
                p += 5 + l;
            }
            _ => {
                let (v, n) = match e {
                    0xc0 => (i16::from_le_bytes(z.get(p + 1..p + 3).ok_or("ziplist truncated")?.try_into().unwrap()) as i64, 2),
                    0xd0 => (i32::from_le_bytes(z.get(p + 1..p + 5).ok_or("ziplist truncated")?.try_into().unwrap()) as i64, 4),
                    0xe0 => (i64::from_le_bytes(z.get(p + 1..p + 9).ok_or("ziplist truncated")?.try_into().unwrap()), 8),
                    0xf0 => {
                        let b = z.get(p + 1..p + 4).ok_or("ziplist truncated")?;
                        let raw = (b[0] as i32) | ((b[1] as i32) << 8) | ((b[2] as i32) << 16);
                        (((raw << 8) >> 8) as i64, 3)
                    }
                    0xfe => (*z.get(p + 1).ok_or("ziplist truncated")? as i8 as i64, 1),
                    0xf1..=0xfd => (((e & 0x0f) as i64) - 1, 0),
                    _ => return Err("invalid ziplist encoding".into()),
                };
                out.push(LpVal::Int(v));
                p += 1 + n;
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let mut w = Writer::new();
        let vals: Vec<i64> = vec![0, 1, 127, 128, -1, -4096, 4095, 30000, -30000, 8000000, -8000000, 2_000_000_000, i64::MAX, i64::MIN];
        for v in &vals {
            w.int(*v);
        }
        w.str(b"hello");
        w.str(&vec![b'x'; 100]);
        w.str(&vec![b'y'; 5000]);
        let lp = w.finish();
        let d = decode(&lp).unwrap();
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(d[i], LpVal::Int(*v));
        }
        assert_eq!(d[vals.len()], LpVal::Str(b"hello".to_vec()));
        assert_eq!(d.len(), vals.len() + 3);
    }
}
