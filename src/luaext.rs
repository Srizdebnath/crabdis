//! The Lua libraries Redis bundles for scripts: cjson, bit (LuaBitOp),
//! cmsgpack and struct, implemented natively.

use mlua::{LightUserData, Lua, MultiValue, Result, Table, Value, Variadic};

fn rt(msg: impl Into<String>) -> mlua::Error {
    mlua::Error::RuntimeError(msg.into())
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Number(n) => Some(*n),
        Value::String(s) => std::str::from_utf8(&s.as_bytes()).ok().and_then(|x| x.trim().parse().ok()),
        _ => None,
    }
}

pub fn register(lua: &Lua) -> Result<()> {
    let g = lua.globals();
    g.raw_set("cjson", cjson(lua)?)?;
    g.raw_set("bit", bit(lua)?)?;
    g.raw_set("cmsgpack", cmsgpack(lua)?)?;
    g.raw_set("struct", structlib(lua)?)?;
    Ok(())
}

// ---------------- number formatting ----------------

/// C printf "%.{prec}g".
pub fn fmt_g(n: f64, prec: usize) -> String {
    if n.is_nan() {
        return if n.is_sign_negative() { "-nan".into() } else { "nan".into() };
    }
    if n.is_infinite() {
        return if n > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if n == 0.0 {
        return if n.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    let p = prec.max(1);
    let e = format!("{:.*e}", p - 1, n);
    let (mant, exp) = e.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    if exp < -4 || exp >= p as i32 {
        let mant = trim_zeros(mant);
        format!("{}e{}{:02}", mant, if exp < 0 { '-' } else { '+' }, exp.abs())
    } else {
        let decimals = (p as i32 - 1 - exp).max(0) as usize;
        trim_zeros(&format!("{:.*}", decimals, n)).to_string()
    }
}

fn trim_zeros(s: &str) -> &str {
    if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.') } else { s }
}

// ---------------- cjson ----------------

fn cjson(lua: &Lua) -> Result<Table> {
    let t = lua.create_table()?;
    let null = Value::LightUserData(LightUserData(std::ptr::null_mut()));
    t.set("null", null)?;
    t.set(
        "encode",
        lua.create_function(|lua, v: Value| {
            let mut out = Vec::new();
            json_encode(lua, &v, &mut out, 0)?;
            lua.create_string(out)
        })?,
    )?;
    t.set(
        "decode",
        lua.create_function(|lua, s: mlua::String| {
            let b = s.as_bytes();
            let mut p = JsonParser { b: &b, i: 0, depth: 0 };
            p.ws();
            let v = p.value(lua)?;
            p.ws();
            if p.i < p.b.len() {
                return Err(rt(format!("Expected the end but found invalid token at character {}", p.i + 1)));
            }
            Ok(v)
        })?,
    )?;
    for name in ["encode_keep_buffer", "encode_max_depth", "decode_max_depth", "encode_number_precision", "encode_invalid_numbers", "decode_invalid_numbers", "encode_sparse_array", "decode_array_with_array_mt", "encode_empty_table_as_object"] {
        t.set(name, lua.create_function(|_, _: MultiValue| Ok(Value::Boolean(true)))?)?;
    }
    Ok(t)
}

fn json_escape(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'/' => out.extend_from_slice(b"\\/"),
            c if c < 0x20 || c == 0x7f => out.extend_from_slice(format!("\\u{:04x}", c).as_bytes()),
            c => out.push(c),
        }
    }
    out.push(b'"');
}

fn json_encode(lua: &Lua, v: &Value, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    match v {
        Value::Nil => out.extend_from_slice(b"null"),
        Value::LightUserData(u) if u.0.is_null() => out.extend_from_slice(b"null"),
        Value::Boolean(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Integer(i) => out.extend_from_slice(i.to_string().as_bytes()),
        Value::Number(n) => {
            if n.is_nan() || n.is_infinite() {
                return Err(rt("Cannot serialise number: must not be NaN or Inf"));
            }
            out.extend_from_slice(fmt_g(*n, 14).as_bytes());
        }
        Value::String(s) => json_escape(&s.as_bytes(), out),
        Value::Table(t) => {
            if depth >= 1000 {
                return Err(rt(format!("Cannot serialise, excessive nesting ({})", depth + 1)));
            }
            let mut max = 0usize;
            let mut count = 0usize;
            let mut is_array = true;
            for pair in t.pairs::<Value, Value>() {
                let (k, _) = pair?;
                count += 1;
                match k {
                    Value::Integer(i) if i >= 1 => max = max.max(i as usize),
                    Value::Number(f) if f >= 1.0 && f.fract() == 0.0 => max = max.max(f as usize),
                    _ => {
                        is_array = false;
                        break;
                    }
                }
            }
            if is_array && count > 0 {
                if max > count * 2 && max > 10 {
                    return Err(rt("Cannot serialise table: excessively sparse array"));
                }
                out.push(b'[');
                for i in 1..=max {
                    if i > 1 {
                        out.push(b',');
                    }
                    let v: Value = t.raw_get(i)?;
                    json_encode(lua, &v, out, depth + 1)?;
                }
                out.push(b']');
            } else {
                out.push(b'{');
                let mut first = true;
                for pair in t.pairs::<Value, Value>() {
                    let (k, v) = pair?;
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    match &k {
                        Value::String(s) => json_escape(&s.as_bytes(), out),
                        Value::Integer(i) => out.extend_from_slice(format!("\"{}\"", i).as_bytes()),
                        Value::Number(n) => out.extend_from_slice(format!("\"{}\"", fmt_g(*n, 14)).as_bytes()),
                        _ => return Err(rt("Cannot serialise table: table key must be a number or string")),
                    }
                    out.push(b':');
                    json_encode(lua, &v, out, depth + 1)?;
                }
                out.push(b'}');
            }
        }
        other => return Err(rt(format!("Cannot serialise {}: type not supported", other.type_name()))),
    }
    Ok(())
}

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
    depth: usize,
}

impl JsonParser<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn err<T>(&self, what: &str) -> Result<T> {
        Err(rt(format!("Expected {} but found invalid token at character {}", what, self.i + 1)))
    }
    fn value(&mut self, lua: &Lua) -> Result<Value> {
        self.ws();
        let Some(&c) = self.b.get(self.i) else { return self.err("value") };
        match c {
            b'{' => {
                self.depth += 1;
                if self.depth > 1000 {
                    return Err(rt(format!("Found too many nested data structures ({}) at character {}", self.depth, self.i + 1)));
                }
                self.i += 1;
                let t = lua.create_table()?;
                self.ws();
                if self.b.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(Value::Table(t));
                }
                loop {
                    self.ws();
                    if self.b.get(self.i) != Some(&b'"') {
                        return self.err("object key string");
                    }
                    let k = self.string()?;
                    self.ws();
                    if self.b.get(self.i) != Some(&b':') {
                        return self.err("colon");
                    }
                    self.i += 1;
                    let v = self.value(lua)?;
                    t.raw_set(lua.create_string(&k)?, v)?;
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            break;
                        }
                        _ => return self.err("comma or object end"),
                    }
                }
                self.depth -= 1;
                Ok(Value::Table(t))
            }
            b'[' => {
                self.depth += 1;
                if self.depth > 1000 {
                    return Err(rt(format!("Found too many nested data structures ({}) at character {}", self.depth, self.i + 1)));
                }
                self.i += 1;
                let t = lua.create_table()?;
                self.ws();
                if self.b.get(self.i) == Some(&b']') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(Value::Table(t));
                }
                let mut n = 1;
                loop {
                    let v = self.value(lua)?;
                    t.raw_set(n, v)?;
                    n += 1;
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            break;
                        }
                        _ => return self.err("comma or array end"),
                    }
                }
                self.depth -= 1;
                Ok(Value::Table(t))
            }
            b'"' => Ok(Value::String(lua.create_string(self.string()?)?)),
            b't' if self.b[self.i..].starts_with(b"true") => {
                self.i += 4;
                Ok(Value::Boolean(true))
            }
            b'f' if self.b[self.i..].starts_with(b"false") => {
                self.i += 5;
                Ok(Value::Boolean(false))
            }
            b'n' if self.b[self.i..].starts_with(b"null") => {
                self.i += 4;
                Ok(Value::LightUserData(LightUserData(std::ptr::null_mut())))
            }
            b'-' | b'0'..=b'9' => {
                let start = self.i;
                self.i += 1;
                while self.i < self.b.len() && matches!(self.b[self.i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-') {
                    self.i += 1;
                }
                let s = std::str::from_utf8(&self.b[start..self.i]).unwrap();
                match s.parse::<f64>() {
                    Ok(n) => Ok(Value::Number(n)),
                    Err(_) => {
                        self.i = start;
                        self.err("value")
                    }
                }
            }
            _ => self.err("value"),
        }
    }
    fn string(&mut self) -> Result<Vec<u8>> {
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.b.get(self.i) else { return self.err("string end") };
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let Some(&e) = self.b.get(self.i) else { return self.err("escape") };
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let hex = |p: &mut Self| -> Result<u32> {
                                let h = p.b.get(p.i..p.i + 4).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u32::from_str_radix(h, 16).ok());
                                match h {
                                    Some(v) => {
                                        p.i += 4;
                                        Ok(v)
                                    }
                                    None => p.err("unicode escape"),
                                }
                            };
                            let mut cp = hex(self)?;
                            if (0xd800..0xdc00).contains(&cp) && self.b.get(self.i) == Some(&b'\\') && self.b.get(self.i + 1) == Some(&b'u') {
                                self.i += 2;
                                let lo = hex(self)?;
                                cp = 0x10000 + ((cp - 0xd800) << 10) + (lo.wrapping_sub(0xdc00) & 0x3ff);
                            }
                            let ch = char::from_u32(cp).unwrap_or('\u{fffd}');
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return self.err("valid escape"),
                    }
                }
                c => out.push(c),
            }
        }
    }
}

// ---------------- bit ----------------

fn tobit(v: &Value) -> Result<i32> {
    let n = num(v).ok_or_else(|| rt("bad argument to bit operation (number expected)"))?;
    // LuaBitOp: add 2^52+2^51 and take the low 32 bits.
    let b = n + 6755399441055744.0;
    Ok(b.to_bits() as u32 as i32)
}

fn bit(lua: &Lua) -> Result<Table> {
    let t = lua.create_table()?;
    t.set("tobit", lua.create_function(|_, v: Value| Ok(tobit(&v)? as f64))?)?;
    t.set("bnot", lua.create_function(|_, v: Value| Ok(!tobit(&v)? as f64))?)?;
    t.set(
        "band",
        lua.create_function(|_, v: Variadic<Value>| {
            let mut r = -1i32;
            for x in v.iter() {
                r &= tobit(x)?;
            }
            Ok(r as f64)
        })?,
    )?;
    t.set(
        "bor",
        lua.create_function(|_, v: Variadic<Value>| {
            let mut r = 0i32;
            for x in v.iter() {
                r |= tobit(x)?;
            }
            Ok(r as f64)
        })?,
    )?;
    t.set(
        "bxor",
        lua.create_function(|_, v: Variadic<Value>| {
            let mut r = 0i32;
            for x in v.iter() {
                r ^= tobit(x)?;
            }
            Ok(r as f64)
        })?,
    )?;
    t.set("lshift", lua.create_function(|_, (a, n): (Value, Value)| Ok(((tobit(&a)? as u32) << (tobit(&n)? as u32 & 31)) as i32 as f64))?)?;
    t.set("rshift", lua.create_function(|_, (a, n): (Value, Value)| Ok(((tobit(&a)? as u32) >> (tobit(&n)? as u32 & 31)) as i32 as f64))?)?;
    t.set("arshift", lua.create_function(|_, (a, n): (Value, Value)| Ok((tobit(&a)? >> (tobit(&n)? as u32 & 31)) as f64))?)?;
    t.set("rol", lua.create_function(|_, (a, n): (Value, Value)| Ok((tobit(&a)? as u32).rotate_left(tobit(&n)? as u32 & 31) as i32 as f64))?)?;
    t.set("ror", lua.create_function(|_, (a, n): (Value, Value)| Ok((tobit(&a)? as u32).rotate_right(tobit(&n)? as u32 & 31) as i32 as f64))?)?;
    t.set("bswap", lua.create_function(|_, a: Value| Ok((tobit(&a)? as u32).swap_bytes() as i32 as f64))?)?;
    t.set(
        "tohex",
        lua.create_function(|lua, (a, n): (Value, Option<Value>)| {
            let x = tobit(&a)? as u32;
            let mut n = match n {
                Some(v) => tobit(&v)?,
                None => 8,
            };
            let upper = n < 0;
            if upper {
                n = -n;
            }
            let n = n.clamp(1, 8) as usize;
            let s = if upper { format!("{:08X}", x) } else { format!("{:08x}", x) };
            lua.create_string(&s[8 - n..])
        })?,
    )?;
    Ok(t)
}

// ---------------- cmsgpack ----------------

fn mp_encode(v: &Value, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    match v {
        Value::Nil => out.push(0xc0),
        Value::LightUserData(_) => out.push(0xc0),
        Value::Boolean(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
        Value::Integer(i) => mp_int(*i, out),
        Value::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() && n.abs() < 9.2e18 {
                mp_int(*n as i64, out)
            } else if (*n as f32) as f64 == *n {
                out.push(0xca);
                out.extend_from_slice(&(*n as f32).to_be_bytes());
            } else {
                out.push(0xcb);
                out.extend_from_slice(&n.to_be_bytes());
            }
        }
        Value::String(s) => {
            let b = s.as_bytes();
            let l = b.len();
            if l < 32 {
                out.push(0xa0 | l as u8);
            } else if l <= 0xff {
                out.push(0xd9);
                out.push(l as u8);
            } else if l <= 0xffff {
                out.push(0xda);
                out.extend_from_slice(&(l as u16).to_be_bytes());
            } else {
                out.push(0xdb);
                out.extend_from_slice(&(l as u32).to_be_bytes());
            }
            out.extend_from_slice(&b);
        }
        Value::Table(t) => {
            if depth >= 16 {
                out.push(0xc0);
                return Ok(());
            }
            let mut count = 0usize;
            let mut max = 0usize;
            let mut is_array = true;
            for pair in t.pairs::<Value, Value>() {
                let (k, _) = pair?;
                count += 1;
                match k {
                    Value::Integer(i) if i >= 1 => max = max.max(i as usize),
                    Value::Number(f) if f >= 1.0 && f.fract() == 0.0 => max = max.max(f as usize),
                    _ => is_array = false,
                }
            }
            if is_array && max == count {
                if count < 16 {
                    out.push(0x90 | count as u8);
                } else if count <= 0xffff {
                    out.push(0xdc);
                    out.extend_from_slice(&(count as u16).to_be_bytes());
                } else {
                    out.push(0xdd);
                    out.extend_from_slice(&(count as u32).to_be_bytes());
                }
                for i in 1..=count {
                    mp_encode(&t.raw_get::<Value>(i)?, out, depth + 1)?;
                }
            } else {
                if count < 16 {
                    out.push(0x80 | count as u8);
                } else if count <= 0xffff {
                    out.push(0xde);
                    out.extend_from_slice(&(count as u16).to_be_bytes());
                } else {
                    out.push(0xdf);
                    out.extend_from_slice(&(count as u32).to_be_bytes());
                }
                for pair in t.pairs::<Value, Value>() {
                    let (k, v) = pair?;
                    mp_encode(&k, out, depth + 1)?;
                    mp_encode(&v, out, depth + 1)?;
                }
            }
        }
        _ => out.push(0xc0),
    }
    Ok(())
}

fn mp_int(i: i64, out: &mut Vec<u8>) {
    if i >= 0 {
        if i <= 127 {
            out.push(i as u8);
        } else if i <= 0xff {
            out.push(0xcc);
            out.push(i as u8);
        } else if i <= 0xffff {
            out.push(0xcd);
            out.extend_from_slice(&(i as u16).to_be_bytes());
        } else if i <= 0xffff_ffff {
            out.push(0xce);
            out.extend_from_slice(&(i as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(i as u64).to_be_bytes());
        }
    } else if i >= -32 {
        out.push(i as i8 as u8);
    } else if i >= -128 {
        out.push(0xd0);
        out.push(i as i8 as u8);
    } else if i >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i32::MIN as i64 {
        out.push(0xd2);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&i.to_be_bytes());
    }
}

struct MpDec<'a> {
    b: &'a [u8],
    i: usize,
}

impl MpDec<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if self.i + n > self.b.len() {
            return Err(rt("Missing bytes in input."));
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn be(&mut self, n: usize) -> Result<u64> {
        Ok(self.take(n)?.iter().fold(0u64, |a, &b| (a << 8) | b as u64))
    }
    fn value(&mut self, lua: &Lua) -> Result<Value> {
        let c = self.take(1)?[0];
        Ok(match c {
            0x00..=0x7f => Value::Number(c as f64),
            0x80..=0x8f => self.map(lua, (c & 0x0f) as usize)?,
            0x90..=0x9f => self.array(lua, (c & 0x0f) as usize)?,
            0xa0..=0xbf => self.str(lua, (c & 0x1f) as usize)?,
            0xc0 => Value::Nil,
            0xc2 => Value::Boolean(false),
            0xc3 => Value::Boolean(true),
            0xc4 | 0xd9 => {
                let n = self.be(1)? as usize;
                self.str(lua, n)?
            }
            0xc5 | 0xda => {
                let n = self.be(2)? as usize;
                self.str(lua, n)?
            }
            0xc6 | 0xdb => {
                let n = self.be(4)? as usize;
                self.str(lua, n)?
            }
            0xca => Value::Number(f32::from_bits(self.be(4)? as u32) as f64),
            0xcb => Value::Number(f64::from_bits(self.be(8)?)),
            0xcc => Value::Number(self.be(1)? as f64),
            0xcd => Value::Number(self.be(2)? as f64),
            0xce => Value::Number(self.be(4)? as f64),
            0xcf => Value::Number(self.be(8)? as f64),
            0xd0 => Value::Number(self.be(1)? as u8 as i8 as f64),
            0xd1 => Value::Number(self.be(2)? as u16 as i16 as f64),
            0xd2 => Value::Number(self.be(4)? as u32 as i32 as f64),
            0xd3 => Value::Number(self.be(8)? as i64 as f64),
            0xdc => {
                let n = self.be(2)? as usize;
                self.array(lua, n)?
            }
            0xdd => {
                let n = self.be(4)? as usize;
                self.array(lua, n)?
            }
            0xde => {
                let n = self.be(2)? as usize;
                self.map(lua, n)?
            }
            0xdf => {
                let n = self.be(4)? as usize;
                self.map(lua, n)?
            }
            0xe0..=0xff => Value::Number(c as i8 as f64),
            _ => return Err(rt("Bad data format in input.")),
        })
    }
    fn str(&mut self, lua: &Lua, n: usize) -> Result<Value> {
        let s = self.take(n)?.to_vec();
        Ok(Value::String(lua.create_string(&s)?))
    }
    fn array(&mut self, lua: &Lua, n: usize) -> Result<Value> {
        let t = lua.create_table()?;
        for i in 1..=n {
            let v = self.value(lua)?;
            t.raw_set(i, v)?;
        }
        Ok(Value::Table(t))
    }
    fn map(&mut self, lua: &Lua, n: usize) -> Result<Value> {
        let t = lua.create_table()?;
        for _ in 0..n {
            let k = self.value(lua)?;
            let v = self.value(lua)?;
            if !k.is_nil() {
                t.raw_set(k, v)?;
            }
        }
        Ok(Value::Table(t))
    }
}

fn cmsgpack(lua: &Lua) -> Result<Table> {
    let t = lua.create_table()?;
    t.set(
        "pack",
        lua.create_function(|lua, args: MultiValue| {
            if args.is_empty() {
                return Err(rt("MessagePack pack needs input."));
            }
            let mut out = Vec::new();
            for a in args.iter() {
                mp_encode(a, &mut out, 0)?;
            }
            lua.create_string(out)
        })?,
    )?;
    let unpack_impl = |lua: &Lua, s: mlua::String, offset: usize, limit: usize, with_offset: bool| -> Result<MultiValue> {
        let b = s.as_bytes();
        let mut d = MpDec { b: &b, i: offset };
        let mut out = Vec::new();
        while d.i < d.b.len() && (limit == 0 || out.len() < limit) {
            out.push(d.value(lua)?);
        }
        let mut mv = Vec::new();
        if with_offset {
            mv.push(Value::Number(if d.i >= d.b.len() { -1.0 } else { d.i as f64 }));
        }
        mv.extend(out);
        Ok(MultiValue::from_iter(mv))
    };
    t.set("unpack", lua.create_function(move |lua, s: mlua::String| unpack_impl(lua, s, 0, 0, false))?)?;
    t.set(
        "unpack_one",
        lua.create_function(move |lua, (s, off): (mlua::String, Option<i64>)| unpack_impl(lua, s, off.unwrap_or(0).max(0) as usize, 1, true))?,
    )?;
    t.set(
        "unpack_limit",
        lua.create_function(move |lua, (s, lim, off): (mlua::String, i64, Option<i64>)| unpack_impl(lua, s, off.unwrap_or(0).max(0) as usize, lim.max(0) as usize, true))?,
    )?;
    Ok(t)
}

// ---------------- struct ----------------

#[derive(Clone, Copy)]
struct Hdr {
    little: bool,
    align: usize,
}

fn getnum(f: &[u8], i: &mut usize, df: usize) -> usize {
    if *i >= f.len() || !f[*i].is_ascii_digit() {
        return df;
    }
    let mut a = 0usize;
    while *i < f.len() && f[*i].is_ascii_digit() {
        a = a * 10 + (f[*i] - b'0') as usize;
        *i += 1;
    }
    a
}

fn optsize(opt: u8, f: &[u8], i: &mut usize) -> Result<usize> {
    Ok(match opt {
        b'B' | b'b' => 1,
        b'H' | b'h' => 2,
        b'L' | b'l' | b'T' => 8,
        b'f' => 4,
        b'd' => 8,
        b'x' => 1,
        b'c' => getnum(f, i, 1),
        b'i' | b'I' => {
            let sz = getnum(f, i, 4);
            if sz > 8 {
                return Err(rt(format!("integral size {} is larger than limit of 8", sz)));
            }
            sz
        }
        _ => 0,
    })
}

fn gettoalign(len: usize, h: &Hdr, opt: u8, size: usize) -> usize {
    if size == 0 || opt == b'c' {
        return 0;
    }
    let size = size.min(h.align);
    (size - (len & (size - 1))) & (size - 1)
}

fn controloptions(opt: u8, f: &[u8], i: &mut usize, h: &mut Hdr) -> Result<bool> {
    match opt {
        b' ' => {}
        b'>' => h.little = false,
        b'<' => h.little = true,
        b'=' => h.little = cfg!(target_endian = "little"),
        b'!' => {
            let a = getnum(f, i, 8);
            if !a.is_power_of_two() {
                return Err(rt(format!("alignment {} is not a power of 2", a)));
            }
            h.align = a;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn put_int(out: &mut Vec<u8>, v: i64, little: bool, size: usize) {
    let bytes = (v as u64).to_le_bytes();
    if little {
        out.extend_from_slice(&bytes[..size]);
    } else {
        out.extend(bytes[..size].iter().rev());
    }
}

fn structlib(lua: &Lua) -> Result<Table> {
    let t = lua.create_table()?;
    t.set(
        "pack",
        lua.create_function(|lua, args: MultiValue| {
            let args: Vec<Value> = args.into_iter().collect();
            let Some(Value::String(fs)) = args.first() else { return Err(rt("bad argument #1 to 'pack' (string expected)")) };
            let f = fs.as_bytes().to_vec();
            let mut h = Hdr { little: cfg!(target_endian = "little"), align: 1 };
            let mut out: Vec<u8> = Vec::new();
            let mut ai = 1;
            let mut i = 0;
            while i < f.len() {
                let opt = f[i];
                i += 1;
                if controloptions(opt, &f, &mut i, &mut h)? {
                    continue;
                }
                let size = optsize(opt, &f, &mut i)?;
                let toalign = gettoalign(out.len(), &h, opt, size);
                out.extend(std::iter::repeat_n(0u8, toalign));
                match opt {
                    b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                        let v = args.get(ai).and_then(num).ok_or_else(|| rt(format!("bad argument #{} to 'pack' (number expected)", ai + 1)))?;
                        ai += 1;
                        put_int(&mut out, v as i64, h.little, size);
                    }
                    b'x' => out.push(0),
                    b'f' => {
                        let v = args.get(ai).and_then(num).ok_or_else(|| rt(format!("bad argument #{} to 'pack' (number expected)", ai + 1)))?;
                        ai += 1;
                        let b = (v as f32).to_bits() as i64;
                        put_int(&mut out, b, h.little, 4);
                    }
                    b'd' => {
                        let v = args.get(ai).and_then(num).ok_or_else(|| rt(format!("bad argument #{} to 'pack' (number expected)", ai + 1)))?;
                        ai += 1;
                        let b = v.to_bits();
                        if h.little {
                            out.extend_from_slice(&b.to_le_bytes());
                        } else {
                            out.extend_from_slice(&b.to_be_bytes());
                        }
                    }
                    b'c' | b's' => {
                        let Some(Value::String(s)) = args.get(ai) else { return Err(rt(format!("bad argument #{} to 'pack' (string expected)", ai + 1))) };
                        ai += 1;
                        let s = s.as_bytes();
                        let mut l = s.len();
                        let mut size = size;
                        if size == 0 {
                            size = l;
                        }
                        if opt == b'c' && l < size {
                            return Err(rt(format!("bad argument #{} to 'pack' (string too short)", ai)));
                        }
                        if opt == b'c' {
                            l = size;
                        }
                        out.extend_from_slice(&s[..l]);
                        if opt == b's' {
                            out.push(0);
                        }
                    }
                    _ => return Err(rt(format!("invalid format option '{}'", opt as char))),
                }
            }
            lua.create_string(out)
        })?,
    )?;
    t.set(
        "unpack",
        lua.create_function(|lua, (fs, data, pos): (mlua::String, mlua::String, Option<i64>)| {
            let f = fs.as_bytes().to_vec();
            let d = data.as_bytes().to_vec();
            let mut pos = pos.unwrap_or(1) as isize - 1;
            if pos < 0 {
                return Err(rt("offset must be 1 or greater"));
            }
            let mut pos = pos as usize;
            let mut h = Hdr { little: cfg!(target_endian = "little"), align: 1 };
            let mut out: Vec<Value> = Vec::new();
            let mut i = 0;
            while i < f.len() {
                let opt = f[i];
                i += 1;
                if controloptions(opt, &f, &mut i, &mut h)? {
                    continue;
                }
                let mut size = optsize(opt, &f, &mut i)?;
                pos += gettoalign(pos, &h, opt, size);
                if pos + size > d.len() && opt != b's' {
                    return Err(rt("bad argument #2 to 'unpack' (data string too short)"));
                }
                match opt {
                    b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                        let bytes = &d[pos..pos + size];
                        let mut v: u64 = 0;
                        if h.little {
                            for (k, &b) in bytes.iter().enumerate() {
                                v |= (b as u64) << (8 * k);
                            }
                        } else {
                            for &b in bytes {
                                v = (v << 8) | b as u64;
                            }
                        }
                        let signed = opt.is_ascii_lowercase();
                        let val = if signed && size < 8 {
                            let shift = 64 - 8 * size;
                            (((v << shift) as i64) >> shift) as f64
                        } else if signed {
                            v as i64 as f64
                        } else {
                            v as f64
                        };
                        out.push(Value::Number(val));
                    }
                    b'x' => {}
                    b'f' => {
                        let mut b = [0u8; 4];
                        b.copy_from_slice(&d[pos..pos + 4]);
                        let v = if h.little { f32::from_le_bytes(b) } else { f32::from_be_bytes(b) };
                        out.push(Value::Number(v as f64));
                    }
                    b'd' => {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(&d[pos..pos + 8]);
                        let v = if h.little { f64::from_le_bytes(b) } else { f64::from_be_bytes(b) };
                        out.push(Value::Number(v));
                    }
                    b'c' => {
                        if size == 0 {
                            let Some(Value::Number(n)) = out.pop() else { return Err(rt("format 'c0' needs a previous size")) };
                            size = n as usize;
                            if pos + size > d.len() {
                                return Err(rt("bad argument #2 to 'unpack' (data string too short)"));
                            }
                        }
                        out.push(Value::String(lua.create_string(&d[pos..pos + size])?));
                    }
                    b's' => {
                        let Some(e) = d[pos..].iter().position(|&c| c == 0) else { return Err(rt("unfinished string in data")) };
                        out.push(Value::String(lua.create_string(&d[pos..pos + e])?));
                        size = e + 1;
                    }
                    _ => return Err(rt(format!("invalid format option '{}'", opt as char))),
                }
                pos += size;
            }
            out.push(Value::Number((pos + 1) as f64));
            Ok(MultiValue::from_iter(out))
        })?,
    )?;
    t.set(
        "size",
        lua.create_function(|_, fs: mlua::String| {
            let f = fs.as_bytes().to_vec();
            let mut h = Hdr { little: true, align: 1 };
            let mut pos = 0usize;
            let mut i = 0;
            while i < f.len() {
                let opt = f[i];
                i += 1;
                if controloptions(opt, &f, &mut i, &mut h)? {
                    continue;
                }
                let size = optsize(opt, &f, &mut i)?;
                pos += gettoalign(pos, &h, opt, size);
                if opt == b's' {
                    return Err(rt("options 'c0' - 's' have undefined sizes"));
                }
                if opt == b'c' && size == 0 {
                    return Err(rt("options 'c0' - 's' have undefined sizes"));
                }
                pos += size;
            }
            Ok(pos as f64)
        })?,
    )?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn g_format() {
        assert_eq!(fmt_g(1.0, 14), "1");
        assert_eq!(fmt_g(0.1, 14), "0.1");
        assert_eq!(fmt_g(1e20, 14), "1e+20");
        assert_eq!(fmt_g(123456.789, 14), "123456.789");
        assert_eq!(fmt_g(1.5e-7, 14), "1.5e-07");
    }
    #[test]
    fn libs() {
        let lua = Lua::new();
        register(&lua).unwrap();
        let r: String = lua.load(r#"return cjson.encode({1,2,{a="x"}})"#).eval().unwrap();
        assert_eq!(r, r#"[1,2,{"a":"x"}]"#);
        let r: f64 = lua.load(r#"return cjson.decode('{"a":[1,2,3]}').a[3]"#).eval().unwrap();
        assert_eq!(r, 3.0);
        let r: f64 = lua.load("return bit.band(0xff, 0x0f)").eval().unwrap();
        assert_eq!(r, 15.0);
        let r: String = lua.load("return bit.tohex(255, 4)").eval().unwrap();
        assert_eq!(r, "00ff");
        let r: f64 = lua.load("local t = cmsgpack.unpack(cmsgpack.pack({1,2,3})) return t[2]").eval().unwrap();
        assert_eq!(r, 2.0);
        let r: f64 = lua.load("local a, b = struct.unpack('>I2', struct.pack('>I2', 513)) return a").eval().unwrap();
        assert_eq!(r, 513.0);
    }
}
