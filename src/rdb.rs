//! RDB persistence, binary compatible with Redis 7.4 (RDB version 12): CRC64,
//! LZF, length/string/int encodings, every value type (including listpack,
//! intset, ziplist and quicklist variants on load), streams with consumer groups,
//! hash field TTLs, function libraries, DUMP/RESTORE payloads and BGSAVE.

use crate::db::Db;
use crate::dict::Dict;
use crate::lp::{self, LpVal};
use crate::server::{Shared, Store};
use crate::stream::{Consumer, Group, Nack, Node, Stream, StreamId};
use crate::value::{HashObj, RObj, Value};
use crate::zset::ZSet;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;
use std::sync::Arc;

pub const RDB_VERSION: u16 = 12;

const T_STRING: u8 = 0;
const T_LIST: u8 = 1;
const T_SET: u8 = 2;
const T_ZSET: u8 = 3;
const T_HASH: u8 = 4;
const T_ZSET_2: u8 = 5;
const T_HASH_ZIPMAP: u8 = 9;
const T_LIST_ZIPLIST: u8 = 10;
const T_SET_INTSET: u8 = 11;
const T_ZSET_ZIPLIST: u8 = 12;
const T_HASH_ZIPLIST: u8 = 13;
const T_LIST_QUICKLIST: u8 = 14;
const T_STREAM_LISTPACKS: u8 = 15;
const T_HASH_LISTPACK: u8 = 16;
const T_ZSET_LISTPACK: u8 = 17;
const T_LIST_QUICKLIST_2: u8 = 18;
const T_STREAM_LISTPACKS_2: u8 = 19;
const T_SET_LISTPACK: u8 = 20;
const T_STREAM_LISTPACKS_3: u8 = 21;
const T_HASH_METADATA_PRE_GA: u8 = 22;
const T_HASH_LISTPACK_EX_PRE_GA: u8 = 23;
const T_HASH_METADATA: u8 = 24;
const T_HASH_LISTPACK_EX: u8 = 25;

const OP_SLOT_INFO: u8 = 244;
const OP_FUNCTION2: u8 = 245;
const OP_FUNCTION_PRE_GA: u8 = 246;
const OP_MODULE_AUX: u8 = 247;
const OP_IDLE: u8 = 248;
const OP_FREQ: u8 = 249;
const OP_AUX: u8 = 250;
const OP_RESIZEDB: u8 = 251;
const OP_EXPIRETIME_MS: u8 = 252;
const OP_EXPIRETIME: u8 = 253;
const OP_SELECTDB: u8 = 254;
const OP_EOF: u8 = 255;

const STREAM_ITEM_FLAG_DELETED: i64 = 1;
const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 2;

// ---------------- CRC64 (Jones, reflected) ----------------

fn crc64_table() -> &'static [[u64; 256]; 8] {
    static T: std::sync::OnceLock<Box<[[u64; 256]; 8]>> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let poly: u64 = 0x95ac9329ac4bc9b5; // reflected 0xad93d23594c935a9
        let mut t = Box::new([[0u64; 256]; 8]);
        for i in 0..256u64 {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 != 0 { (c >> 1) ^ poly } else { c >> 1 };
            }
            t[0][i as usize] = c;
        }
        for i in 0..256 {
            for k in 1..8 {
                let prev = t[k - 1][i];
                t[k][i] = t[0][(prev & 0xff) as usize] ^ (prev >> 8);
            }
        }
        t
    })
}

pub fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    let t = crc64_table();
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        crc ^= u64::from_le_bytes(c.try_into().unwrap());
        crc = t[7][(crc & 0xff) as usize]
            ^ t[6][((crc >> 8) & 0xff) as usize]
            ^ t[5][((crc >> 16) & 0xff) as usize]
            ^ t[4][((crc >> 24) & 0xff) as usize]
            ^ t[3][((crc >> 32) & 0xff) as usize]
            ^ t[2][((crc >> 40) & 0xff) as usize]
            ^ t[1][((crc >> 48) & 0xff) as usize]
            ^ t[0][(crc >> 56) as usize];
    }
    for &b in chunks.remainder() {
        crc = t[0][((crc ^ b as u64) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

// ---------------- LZF ----------------

pub fn lzf_decompress(input: &[u8], out_len: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(out_len);
    let mut i = 0;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            let l = ctrl + 1;
            if i + l > input.len() || out.len() + l > out_len {
                return None;
            }
            out.extend_from_slice(&input[i..i + l]);
            i += l;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(i)? as usize;
                i += 1;
            }
            let back = ((ctrl & 0x1f) << 8) + *input.get(i)? as usize + 1;
            i += 1;
            len += 2;
            if back > out.len() || out.len() + len > out_len {
                return None;
            }
            let start = out.len() - back;
            for k in 0..len {
                let b = out[start + k];
                out.push(b);
            }
        }
    }
    if out.len() != out_len { None } else { Some(out) }
}

/// LZF compressor (liblzf algorithm). Returns None if output wouldn't be smaller.
pub fn lzf_compress(input: &[u8]) -> Option<Vec<u8>> {
    const HLOG: usize = 16;
    const MAX_LIT: usize = 32;
    const MAX_OFF: usize = 1 << 13;
    const MAX_REF: usize = (1 << 8) + (1 << 3);
    let n = input.len();
    if n < 4 {
        return None;
    }
    let max_out = n - 1;
    let mut htab = vec![0usize; 1 << HLOG];
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let hash = |p: usize| -> usize {
        let v = ((input[p] as u32) << 16) | ((input[p + 1] as u32) << 8) | input[p + 2] as u32;
        ((v.wrapping_mul(2654435761)) >> (32 - HLOG as u32)) as usize & ((1 << HLOG) - 1)
    };
    let mut lit = 0usize;
    out.push(0); // literal length placeholder
    let mut ip = 0usize;
    while ip + 2 < n {
        let h = hash(ip);
        let r = htab[h];
        htab[h] = ip + 1;
        let off = if r > 0 { ip - (r - 1) } else { 0 };
        if r > 0 && off > 0 && off <= MAX_OFF && ip + 4 < n && input[r - 1] == input[ip] && input[r] == input[ip + 1] && input[r + 1] == input[ip + 2] {
            let rp = r - 1;
            let mut len = 2;
            let maxlen = (n - ip - len).min(MAX_REF);
            loop {
                len += 1;
                if len >= maxlen || input[rp + len] != input[ip + len] {
                    break;
                }
            }
            len -= 2;
            // close literal run
            if lit > 0 {
                let pos = out.len() - lit - 1;
                out[pos] = (lit - 1) as u8;
            } else {
                out.pop();
            }
            let offv = off - 1;
            if len < 7 {
                out.push(((offv >> 8) + (len << 5)) as u8);
            } else {
                out.push(((offv >> 8) + (7 << 5)) as u8);
                out.push((len - 7) as u8);
            }
            out.push(offv as u8);
            if out.len() >= max_out {
                return None;
            }
            ip += len + 2;
            lit = 0;
            out.push(0);
            if ip + 2 < n {
                // update hash for skipped positions (subset, like liblzf ULTRA_FAST)
                let h1 = hash(ip - 1);
                htab[h1] = ip;
            }
            continue;
        }
        out.push(input[ip]);
        lit += 1;
        ip += 1;
        if lit == MAX_LIT {
            let pos = out.len() - lit - 1;
            out[pos] = (MAX_LIT - 1) as u8;
            lit = 0;
            out.push(0);
        }
        if out.len() >= max_out {
            return None;
        }
    }
    while ip < n {
        out.push(input[ip]);
        lit += 1;
        ip += 1;
        if lit == MAX_LIT {
            let pos = out.len() - lit - 1;
            out[pos] = (MAX_LIT - 1) as u8;
            lit = 0;
            out.push(0);
        }
    }
    if lit > 0 {
        let pos = out.len() - lit - 1;
        out[pos] = (lit - 1) as u8;
    } else {
        out.pop();
    }
    if out.len() >= max_out { None } else { Some(out) }
}

// ---------------- writer ----------------

pub struct W {
    pub buf: Vec<u8>,
    compress: bool,
}

impl W {
    pub fn new(compress: bool) -> Self {
        W { buf: Vec::with_capacity(4096), compress }
    }
    fn u8(&mut self, b: u8) {
        self.buf.push(b);
    }
    fn len(&mut self, l: u64) {
        if l < 1 << 6 {
            self.buf.push(l as u8);
        } else if l < 1 << 14 {
            self.buf.push(((l >> 8) as u8) | 0x40);
            self.buf.push(l as u8);
        } else if l <= u32::MAX as u64 {
            self.buf.push(0x80);
            self.buf.extend_from_slice(&(l as u32).to_be_bytes());
        } else {
            self.buf.push(0x81);
            self.buf.extend_from_slice(&l.to_be_bytes());
        }
    }
    fn millis(&mut self, t: i64) {
        self.buf.extend_from_slice(&t.to_le_bytes());
    }
    fn double(&mut self, d: f64) {
        self.buf.extend_from_slice(&d.to_le_bytes());
    }
    pub fn raw_str(&mut self, s: &[u8]) {
        self.len(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
    pub fn str(&mut self, s: &[u8]) {
        // Integer encoding.
        if s.len() <= 11 {
            if let Some(v) = crate::util::parse_i64(s) {
                if (i8::MIN as i64..=i8::MAX as i64).contains(&v) {
                    self.buf.push(0xc0);
                    self.buf.push(v as i8 as u8);
                    return;
                } else if (i16::MIN as i64..=i16::MAX as i64).contains(&v) {
                    self.buf.push(0xc1);
                    self.buf.extend_from_slice(&(v as i16).to_le_bytes());
                    return;
                } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
                    self.buf.push(0xc2);
                    self.buf.extend_from_slice(&(v as i32).to_le_bytes());
                    return;
                }
            }
        }
        if self.compress && s.len() > 20 {
            if let Some(c) = lzf_compress(s) {
                self.buf.push(0xc3);
                self.len(c.len() as u64);
                self.len(s.len() as u64);
                self.buf.extend_from_slice(&c);
                return;
            }
        }
        self.raw_str(s);
    }
}

fn stream_node_listpack(master: &StreamId, node: &Node) -> Vec<u8> {
    let mut w = lp::Writer::new();
    w.int(node.valid as i64);
    w.int(node.deleted as i64);
    w.int(node.master_fields.len() as i64);
    for f in &node.master_fields {
        w.str(f);
    }
    w.int(0);
    for e in &node.entries {
        let nf = e.fv.len() / 2;
        let same = nf == node.master_fields.len() && e.fv.iter().step_by(2).zip(node.master_fields.iter()).all(|(a, b)| a == b);
        let mut flags = 0;
        if e.deleted {
            flags |= STREAM_ITEM_FLAG_DELETED;
        }
        if same {
            flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
        }
        w.int(flags);
        w.int((e.id.ms - master.ms) as i64);
        w.int(e.id.seq.wrapping_sub(master.seq) as i64);
        if same {
            for v in e.fv.iter().skip(1).step_by(2) {
                w.str(v);
            }
            w.int(nf as i64 + 3);
        } else {
            w.int(nf as i64);
            for x in &e.fv {
                w.str(x);
            }
            w.int(nf as i64 * 2 + 4);
        }
    }
    w.finish()
}

/// Serialize a value: returns the RDB type byte and writes the payload.
pub fn write_value(w: &mut W, v: &Value) -> u8 {
    match v {
        Value::Str(b) => {
            w.u8(T_STRING);
            w.str(b);
            T_STRING
        }
        Value::List(l) => {
            w.u8(T_LIST_QUICKLIST_2);
            // Pack into listpack nodes of up to 128 entries / ~8KB.
            let mut nodes: Vec<Vec<u8>> = Vec::new();
            let mut cur = lp::Writer::new();
            let (mut n, mut bytes) = (0usize, 0usize);
            for e in l {
                if n > 0 && (n >= 128 || bytes + e.len() > 8192) {
                    nodes.push(std::mem::take(&mut cur).finish());
                    n = 0;
                    bytes = 0;
                }
                cur.str(e);
                n += 1;
                bytes += e.len() + 2;
            }
            if n > 0 {
                nodes.push(cur.finish());
            }
            w.len(nodes.len() as u64);
            for node in nodes {
                w.len(2); // QUICKLIST_NODE_CONTAINER_PACKED
                w.str(&node);
            }
            T_LIST_QUICKLIST_2
        }
        Value::Set(s) => {
            w.u8(T_SET);
            w.len(s.len() as u64);
            for k in s.keys() {
                w.str(k);
            }
            T_SET
        }
        Value::ZSet(z) => {
            w.u8(T_ZSET_2);
            w.len(z.len() as u64);
            for (m, sc) in z.iter_rev() {
                w.str(&m);
                w.double(sc);
            }
            T_ZSET_2
        }
        Value::Hash(h) => {
            if let Some(min) = h.min_ttl() {
                w.u8(T_HASH_METADATA);
                w.millis(min);
                w.len(h.map.len() as u64);
                for (f, val) in h.map.iter() {
                    let ttl = match h.field_ttl(f) {
                        Some(t) => (t - min + 1) as u64,
                        None => 0,
                    };
                    w.len(ttl);
                    w.str(f);
                    w.str(val);
                }
                T_HASH_METADATA
            } else {
                w.u8(T_HASH);
                w.len(h.map.len() as u64);
                for (f, val) in h.map.iter() {
                    w.str(f);
                    w.str(val);
                }
                T_HASH
            }
        }
        Value::Stream(s) => {
            w.u8(T_STREAM_LISTPACKS_3);
            w.len(s.nodes.len() as u64);
            for (master, node) in &s.nodes {
                w.raw_str(&master.encode());
                w.raw_str(&stream_node_listpack(master, node));
            }
            w.len(s.length);
            w.len(s.last_id.ms);
            w.len(s.last_id.seq);
            w.len(s.first_id.ms);
            w.len(s.first_id.seq);
            w.len(s.max_deleted_id.ms);
            w.len(s.max_deleted_id.seq);
            w.len(s.entries_added);
            w.len(s.groups.len() as u64);
            for (name, g) in &s.groups {
                w.raw_str(name);
                w.len(g.last_id.ms);
                w.len(g.last_id.seq);
                w.len(g.entries_read as u64);
                w.len(g.pel.len() as u64);
                for (id, n) in &g.pel {
                    w.buf.extend_from_slice(&id.encode());
                    w.millis(n.delivery_time);
                    w.len(n.delivery_count);
                }
                w.len(g.consumers.len() as u64);
                for (cn, c) in &g.consumers {
                    w.raw_str(cn);
                    w.millis(c.seen_time);
                    w.millis(c.active_time);
                    w.len(c.pel.len() as u64);
                    for id in &c.pel {
                        w.buf.extend_from_slice(&id.encode());
                    }
                }
            }
            T_STREAM_LISTPACKS_3
        }
    }
}

// ---------------- reader ----------------

pub struct R<'a> {
    pub b: &'a [u8],
    pub p: usize,
}

type Res<T> = Result<T, String>;

impl<'a> R<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        R { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Res<&'a [u8]> {
        if self.p + n > self.b.len() {
            return Err("Unexpected EOF reading RDB file".into());
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn u8(&mut self) -> Res<u8> {
        Ok(self.take(1)?[0])
    }
    /// Returns (len, is_encoded)
    fn len_enc(&mut self) -> Res<(u64, bool)> {
        let c = self.u8()?;
        match c >> 6 {
            0 => Ok(((c & 0x3f) as u64, false)),
            1 => Ok(((((c & 0x3f) as u64) << 8) | self.u8()? as u64, false)),
            3 => Ok(((c & 0x3f) as u64, true)),
            _ => match c {
                0x80 => Ok((u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64, false)),
                0x81 => Ok((u64::from_be_bytes(self.take(8)?.try_into().unwrap()), false)),
                _ => Err(format!("Unknown length encoding {} in rdbLoadLen()", c)),
            },
        }
    }
    fn len(&mut self) -> Res<u64> {
        let (l, enc) = self.len_enc()?;
        if enc {
            return Err("unexpected encoded length".into());
        }
        Ok(l)
    }
    fn millis(&mut self) -> Res<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn double_bin(&mut self) -> Res<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn double_ascii(&mut self) -> Res<f64> {
        let l = self.u8()?;
        match l {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            _ => {
                let s = self.take(l as usize)?;
                std::str::from_utf8(s).ok().and_then(|x| x.parse().ok()).ok_or_else(|| "bad double".to_string())
            }
        }
    }
    pub fn string(&mut self) -> Res<Vec<u8>> {
        let (l, enc) = self.len_enc()?;
        if enc {
            match l {
                0 => Ok((self.take(1)?[0] as i8).to_string().into_bytes()),
                1 => Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()).to_string().into_bytes()),
                2 => Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()).to_string().into_bytes()),
                3 => {
                    let clen = self.len()? as usize;
                    let ulen = self.len()? as usize;
                    let c = self.take(clen)?;
                    lzf_decompress(c, ulen).ok_or_else(|| "Invalid LZF compressed string".to_string())
                }
                _ => Err(format!("Unknown RDB string encoding type {}", l)),
            }
        } else {
            Ok(self.take(l as usize)?.to_vec())
        }
    }
    fn bytes(&mut self) -> Res<Bytes> {
        Ok(Bytes::from(self.string()?))
    }
}

fn lpv(v: &LpVal) -> Bytes {
    Bytes::from(v.to_bytes())
}

fn decode_stream_node(master: StreamId, blob: &[u8]) -> Res<Node> {
    let els = lp::decode(blob)?;
    let mut it = els.iter();
    let mut next_int = || -> Res<i64> { it.next().and_then(|v| v.as_int()).ok_or_else(|| "Bad stream listpack".to_string()) };
    let valid = next_int()? as usize;
    let deleted = next_int()? as usize;
    let nmf = next_int()? as usize;
    drop(next_int);
    let mut idx = 3;
    let mut master_fields = Vec::with_capacity(nmf);
    for _ in 0..nmf {
        master_fields.push(lpv(els.get(idx).ok_or("Bad stream listpack")?));
        idx += 1;
    }
    // master terminator
    idx += 1;
    let get_int = |i: usize| -> Res<i64> { els.get(i).and_then(|v| v.as_int()).ok_or_else(|| "Bad stream listpack".to_string()) };
    let mut entries = Vec::new();
    while idx < els.len() {
        let flags = get_int(idx)?;
        let msd = get_int(idx + 1)?;
        let seqd = get_int(idx + 2)?;
        idx += 3;
        let id = StreamId { ms: master.ms.wrapping_add(msd as u64), seq: master.seq.wrapping_add(seqd as u64) };
        let mut fv = Vec::new();
        if flags & STREAM_ITEM_FLAG_SAMEFIELDS != 0 {
            for f in &master_fields {
                fv.push(f.clone());
                fv.push(lpv(els.get(idx).ok_or("Bad stream listpack")?));
                idx += 1;
            }
        } else {
            let nf = get_int(idx)? as usize;
            idx += 1;
            for _ in 0..nf * 2 {
                fv.push(lpv(els.get(idx).ok_or("Bad stream listpack")?));
                idx += 1;
            }
        }
        idx += 1; // lp-count
        entries.push(crate::stream::Entry { id, fv, deleted: flags & STREAM_ITEM_FLAG_DELETED != 0 });
    }
    if entries.iter().filter(|e| !e.deleted).count() != valid {
        return Err("Stream listpack valid count mismatch".into());
    }
    Ok(Node { entries, valid, deleted, bytes: blob.len(), master_fields })
}

fn zset_from_pairs(pairs: Vec<(Bytes, f64)>) -> Value {
    let mut z = ZSet::new();
    for (m, s) in pairs {
        z.insert(m, s);
    }
    Value::ZSet(Box::new(z))
}

fn hash_from_pairs(pairs: impl IntoIterator<Item = (Bytes, Bytes)>) -> HashObj {
    let mut h = HashObj::new();
    for (f, v) in pairs {
        h.map.insert(f, v);
    }
    h
}

/// Load a value of the given RDB type.
pub fn read_value(r: &mut R, t: u8) -> Res<Value> {
    Ok(match t {
        T_STRING => Value::Str(r.bytes()?),
        T_LIST => {
            let n = r.len()?;
            let mut l = VecDeque::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                l.push_back(r.bytes()?);
            }
            Value::List(l)
        }
        T_SET => {
            let n = r.len()?;
            let mut d = Dict::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                d.insert(r.bytes()?, ());
            }
            Value::Set(Box::new(d))
        }
        T_ZSET | T_ZSET_2 => {
            let n = r.len()?;
            let mut pairs = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                let m = r.bytes()?;
                let s = if t == T_ZSET_2 { r.double_bin()? } else { r.double_ascii()? };
                if s.is_nan() {
                    return Err("Zset with NAN score detected".into());
                }
                pairs.push((m, s));
            }
            zset_from_pairs(pairs)
        }
        T_HASH => {
            let n = r.len()?;
            let mut h = HashObj::new();
            for _ in 0..n {
                let f = r.bytes()?;
                let v = r.bytes()?;
                h.map.insert(f, v);
            }
            Value::Hash(Box::new(h))
        }
        T_HASH_METADATA | T_HASH_METADATA_PRE_GA => {
            let min = if t == T_HASH_METADATA { r.millis()? } else { 0 };
            let n = r.len()?;
            let mut h = HashObj::new();
            for _ in 0..n {
                let ttl = r.len()?;
                let f = r.bytes()?;
                let v = r.bytes()?;
                let at = if t == T_HASH_METADATA { if ttl != 0 { ttl as i64 + min - 1 } else { 0 } } else { ttl as i64 };
                h.map.insert(f.clone(), v);
                if at != 0 {
                    h.set_ttl(&f, at);
                }
            }
            Value::Hash(Box::new(h))
        }
        T_SET_INTSET => {
            let blob = r.string()?;
            let ints = lp::decode_intset(&blob)?;
            let mut d = Dict::with_capacity(ints.len());
            for i in ints {
                d.insert(Bytes::from(i.to_string()), ());
            }
            Value::Set(Box::new(d))
        }
        T_SET_LISTPACK => {
            let blob = r.string()?;
            let mut d = Dict::new();
            for e in lp::decode(&blob)? {
                d.insert(lpv(&e), ());
            }
            Value::Set(Box::new(d))
        }
        T_LIST_ZIPLIST => {
            let blob = r.string()?;
            Value::List(lp::decode_ziplist(&blob)?.iter().map(lpv).collect())
        }
        T_ZSET_ZIPLIST | T_ZSET_LISTPACK => {
            let blob = r.string()?;
            let els = if t == T_ZSET_LISTPACK { lp::decode(&blob)? } else { lp::decode_ziplist(&blob)? };
            if els.len() % 2 != 0 {
                return Err("Bad zset listpack".into());
            }
            let mut pairs = Vec::new();
            for c in els.chunks(2) {
                let s = match &c[1] {
                    LpVal::Int(i) => *i as f64,
                    LpVal::Str(x) => crate::util::parse_f64(x).ok_or("bad zset score")?,
                };
                pairs.push((lpv(&c[0]), s));
            }
            zset_from_pairs(pairs)
        }
        T_HASH_ZIPLIST | T_HASH_LISTPACK => {
            let blob = r.string()?;
            let els = if t == T_HASH_LISTPACK { lp::decode(&blob)? } else { lp::decode_ziplist(&blob)? };
            if els.len() % 2 != 0 {
                return Err("Bad hash listpack".into());
            }
            Value::Hash(Box::new(hash_from_pairs(els.chunks(2).map(|c| (lpv(&c[0]), lpv(&c[1]))))))
        }
        T_HASH_LISTPACK_EX | T_HASH_LISTPACK_EX_PRE_GA => {
            if t == T_HASH_LISTPACK_EX {
                let _min = r.millis()?;
            }
            let blob = r.string()?;
            let els = lp::decode(&blob)?;
            if els.len() % 3 != 0 {
                return Err("Bad hash listpack-ex".into());
            }
            let mut h = HashObj::new();
            for c in els.chunks(3) {
                let f = lpv(&c[0]);
                h.map.insert(f.clone(), lpv(&c[1]));
                let ttl = c[2].as_int().unwrap_or(0);
                if ttl > 0 {
                    h.set_ttl(&f, ttl);
                }
            }
            Value::Hash(Box::new(h))
        }
        T_HASH_ZIPMAP => return Err("Zipmap hashes are not supported".into()),
        T_LIST_QUICKLIST | T_LIST_QUICKLIST_2 => {
            let n = r.len()?;
            let mut l = VecDeque::new();
            for _ in 0..n {
                let container = if t == T_LIST_QUICKLIST_2 { r.len()? } else { 2 };
                let blob = r.string()?;
                if container == 1 {
                    l.push_back(Bytes::from(blob));
                } else if t == T_LIST_QUICKLIST_2 {
                    for e in lp::decode(&blob)? {
                        l.push_back(lpv(&e));
                    }
                } else {
                    for e in lp::decode_ziplist(&blob)? {
                        l.push_back(lpv(&e));
                    }
                }
            }
            Value::List(l)
        }
        T_STREAM_LISTPACKS | T_STREAM_LISTPACKS_2 | T_STREAM_LISTPACKS_3 => {
            let mut s = Stream::new();
            let nn = r.len()?;
            for _ in 0..nn {
                let key = r.string()?;
                if key.len() != 16 {
                    return Err("Stream node key entry is not the size of a stream ID".into());
                }
                let master = StreamId::decode(&key);
                let blob = r.string()?;
                let node = decode_stream_node(master, &blob)?;
                if node.entries.is_empty() {
                    return Err("Empty listpack inside stream".into());
                }
                s.nodes.insert(master, node);
            }
            s.length = r.len()?;
            s.last_id = StreamId { ms: r.len()?, seq: r.len()? };
            if t >= T_STREAM_LISTPACKS_2 {
                s.first_id = StreamId { ms: r.len()?, seq: r.len()? };
                s.max_deleted_id = StreamId { ms: r.len()?, seq: r.len()? };
                s.entries_added = r.len()?;
            } else {
                s.first_id = s.edge_id(true).unwrap_or_default();
                s.entries_added = s.length;
            }
            let ng = r.len()?;
            for _ in 0..ng {
                let name = r.bytes()?;
                let last_id = StreamId { ms: r.len()?, seq: r.len()? };
                let entries_read = if t >= T_STREAM_LISTPACKS_2 { r.len()? as i64 } else { s.estimate_distance_from_first_ever(&last_id) };
                let mut g = Group { last_id, entries_read, pel: BTreeMap::new(), consumers: BTreeMap::new() };
                let np = r.len()?;
                for _ in 0..np {
                    let id = StreamId::decode(r.take(16)?);
                    let dt = r.millis()?;
                    let dc = r.len()?;
                    g.pel.insert(id, Nack { delivery_time: dt, delivery_count: dc, consumer: None });
                }
                let nc = r.len()?;
                for _ in 0..nc {
                    let cname = r.bytes()?;
                    let seen = r.millis()?;
                    let active = if t >= T_STREAM_LISTPACKS_3 { r.millis()? } else { seen };
                    let ncp = r.len()?;
                    let mut pel = BTreeSet::new();
                    for _ in 0..ncp {
                        let id = StreamId::decode(r.take(16)?);
                        match g.pel.get_mut(&id) {
                            Some(n) => n.consumer = Some(cname.clone()),
                            None => return Err("Consumer entry not found in group global PEL".into()),
                        }
                        pel.insert(id);
                    }
                    g.consumers.insert(cname, Consumer { seen_time: seen, active_time: active, pel });
                }
                if g.pel.values().any(|n| n.consumer.is_none()) {
                    return Err("Group PEL contains entries without consumer".into());
                }
                s.groups.insert(name, g);
            }
            Value::Stream(Box::new(s))
        }
        _ => return Err(format!("Unknown RDB encoding type {}", t)),
    })
}

// ---------------- DUMP / RESTORE ----------------

pub fn dump_payload(v: &Value) -> Vec<u8> {
    let mut w = W::new(true);
    write_value(&mut w, v);
    let mut out = w.buf;
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crc64(0, &out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub fn restore_payload(p: &[u8]) -> Result<Value, String> {
    if p.len() < 10 {
        return Err("ERR DUMP payload version or checksum are wrong".into());
    }
    let n = p.len();
    let ver = u16::from_le_bytes([p[n - 10], p[n - 9]]);
    if ver > RDB_VERSION {
        return Err("ERR DUMP payload version or checksum are wrong".into());
    }
    let crc = u64::from_le_bytes(p[n - 8..].try_into().unwrap());
    if crc != 0 && crc64(0, &p[..n - 8]) != crc {
        return Err("ERR DUMP payload version or checksum are wrong".into());
    }
    let body = &p[..n - 10];
    let mut r = R::new(body);
    let t = r.u8().map_err(|_| "ERR Bad data format".to_string())?;
    let v = read_value(&mut r, t).map_err(|_| "ERR Bad data format".to_string())?;
    if r.p != body.len() || v.is_empty_collection() {
        return Err("ERR Bad data format".into());
    }
    Ok(v)
}

// ---------------- whole file ----------------

/// A point-in-time copy of the dataset for serialization outside the lock.
pub struct Snapshot {
    pub dbs: Vec<Vec<(Bytes, Value, Option<i64>)>>,
    pub functions: Vec<String>,
    pub aux: Vec<(String, String)>,
    pub compress: bool,
    pub checksum: bool,
}

pub fn snapshot(s: &Store) -> Snapshot {
    let dbs = s
        .dbs
        .iter()
        .map(|d| d.dict.iter().map(|(k, o)| (k.clone(), o.v.clone(), d.expire_of(k))).collect())
        .collect();
    let mut aux = vec![
        ("redis-ver".to_string(), crate::server::VERSION.to_string()),
        ("redis-bits".to_string(), "64".to_string()),
        ("ctime".to_string(), (crate::util::now_ms() / 1000).to_string()),
        ("used-mem".to_string(), crate::mem::used_memory().to_string()),
    ];
    let (rid, roff) = s.repl.replid_offset();
    aux.push(("repl-stream-db".into(), "0".into()));
    aux.push(("repl-id".into(), rid));
    aux.push(("repl-offset".into(), roff.to_string()));
    aux.push(("aof-base".into(), "0".into()));
    Snapshot { dbs, functions: s.scripting.library_codes(), aux, compress: s.cfg.rdbcompression, checksum: s.cfg.rdbchecksum }
}

pub fn serialize(snap: &Snapshot) -> Vec<u8> {
    let mut w = W::new(snap.compress);
    w.buf.extend_from_slice(format!("REDIS{:04}", RDB_VERSION).as_bytes());
    for (k, v) in &snap.aux {
        w.u8(OP_AUX);
        w.str(k.as_bytes());
        w.str(v.as_bytes());
    }
    for f in &snap.functions {
        w.u8(OP_FUNCTION2);
        w.str(f.as_bytes());
    }
    for (i, db) in snap.dbs.iter().enumerate() {
        if db.is_empty() {
            continue;
        }
        w.u8(OP_SELECTDB);
        w.len(i as u64);
        w.u8(OP_RESIZEDB);
        w.len(db.len() as u64);
        w.len(db.iter().filter(|e| e.2.is_some()).count() as u64);
        for (k, v, exp) in db {
            if let Some(e) = exp {
                w.u8(OP_EXPIRETIME_MS);
                w.millis(*e);
            }
            // Type byte is emitted by write_value before the key: write into a temp.
            let mut tmp = W::new(snap.compress);
            let t = write_value(&mut tmp, v);
            w.u8(t);
            w.str(k);
            w.buf.extend_from_slice(&tmp.buf[1..]);
        }
    }
    w.u8(OP_EOF);
    let crc = if snap.checksum { crc64(0, &w.buf) } else { 0 };
    w.buf.extend_from_slice(&crc.to_le_bytes());
    w.buf
}

pub struct Loaded {
    pub dbs: Vec<Vec<(Bytes, Value, Option<i64>, Option<u64>, Option<u8>)>>,
    pub functions: Vec<String>,
    pub aux: Vec<(String, String)>,
}

pub fn parse(data: &[u8], ndbs: usize) -> Res<Loaded> {
    if data.len() < 9 || &data[..5] != b"REDIS" {
        return Err("Wrong signature trying to load DB from file".into());
    }
    let ver: u16 = std::str::from_utf8(&data[5..9]).ok().and_then(|v| v.parse().ok()).ok_or("Bad RDB version")?;
    if !(1..=RDB_VERSION).contains(&ver) {
        return Err(format!("Can't handle RDB format version {}", ver));
    }
    let mut r = R::new(data);
    r.p = 9;
    let mut out = Loaded { dbs: (0..ndbs).map(|_| Vec::new()).collect(), functions: Vec::new(), aux: Vec::new() };
    let mut db = 0usize;
    let mut expire: Option<i64> = None;
    let mut idle: Option<u64> = None;
    let mut freq: Option<u8> = None;
    loop {
        let t = r.u8()?;
        match t {
            OP_EXPIRETIME_MS => {
                expire = Some(r.millis()?);
                continue;
            }
            OP_EXPIRETIME => {
                expire = Some(i32::from_le_bytes(r.take(4)?.try_into().unwrap()) as i64 * 1000);
                continue;
            }
            OP_FREQ => {
                freq = Some(r.u8()?);
                continue;
            }
            OP_IDLE => {
                idle = Some(r.len()?);
                continue;
            }
            OP_EOF => break,
            OP_SELECTDB => {
                db = r.len()? as usize;
                if db >= ndbs {
                    return Err(format!("FATAL: Data file was created with a Redis server configured to handle more than {} databases. Exiting\n", ndbs));
                }
                continue;
            }
            OP_RESIZEDB => {
                r.len()?;
                r.len()?;
                continue;
            }
            OP_SLOT_INFO => {
                r.len()?;
                r.len()?;
                r.len()?;
                continue;
            }
            OP_AUX => {
                let k = String::from_utf8_lossy(&r.string()?).into_owned();
                let v = String::from_utf8_lossy(&r.string()?).into_owned();
                out.aux.push((k, v));
                continue;
            }
            OP_MODULE_AUX => return Err("Module auxiliary data is not supported (no modules loaded)".into()),
            OP_FUNCTION_PRE_GA => return Err("Pre-release function format not supported".into()),
            OP_FUNCTION2 => {
                out.functions.push(String::from_utf8_lossy(&r.string()?).into_owned());
                continue;
            }
            _ => {}
        }
        let key = r.bytes()?;
        let v = read_value(&mut r, t)?;
        if !v.is_empty_collection() {
            out.dbs[db].push((key, v, expire, idle, freq));
        }
        expire = None;
        idle = None;
        freq = None;
    }
    if ver >= 5 && r.p + 8 <= data.len() {
        let expected = u64::from_le_bytes(data[r.p..r.p + 8].try_into().unwrap());
        if expected != 0 && crc64(0, &data[..r.p]) != expected {
            return Err("Wrong RDB checksum. Aborting now.".into());
        }
    }
    Ok(out)
}

pub fn rdb_path(s: &Store) -> std::path::PathBuf {
    std::path::Path::new(&s.cfg.dir).join(&s.cfg.dbfilename)
}

fn write_atomic(path: &std::path::Path, data: &[u8], fsync: bool) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(std::path::Path::new("."));
    let tmp = dir.join(format!("temp-{}-{}.rdb", std::process::id(), crate::util::rand_u64() % 100000));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        if fsync {
            f.sync_all()?;
        }
    }
    std::fs::rename(&tmp, path)
}

/// Install loaded data into the store.
pub fn install(s: &mut Store, l: Loaded) -> usize {
    let now = crate::util::now_ms();
    let is_master = !s.repl.is_replica();
    let mut n = 0;
    for f in &l.functions {
        if let Err(e) = crate::scripting::load_library(s, f, true) {
            eprintln!("Failed loading library from RDB: {}", e);
        }
    }
    for (i, db) in l.dbs.into_iter().enumerate() {
        for (k, v, exp, idle, freq) in db {
            if let Some(e) = exp {
                if is_master && e < now {
                    s.stats.rdb_last_load_keys_expired += 1;
                    continue;
                }
            }
            let mut o = RObj::new(v);
            if let Some(f) = freq {
                o.lru = crate::evict::lfu_with_counter(f);
            } else if let Some(sec) = idle {
                o.lru = crate::evict::lru_with_idle(sec * 1000);
            }
            if let Value::Hash(h) = &o.v {
                if h.min_ttl().is_some() {
                    s.dbs[i].hexpires.insert(k.clone(), ());
                }
            }
            s.dbs[i].dict.insert(k.clone(), o);
            if let Some(e) = exp {
                s.dbs[i].expires.insert(k, e);
            }
            n += 1;
        }
    }
    s.stats.rdb_last_load_keys_loaded = n as u64;
    n
}

pub fn load_file(s: &mut Store) -> Result<usize, String> {
    let path = rdb_path(s);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("Fatal error loading the DB, check server logs. Exiting. ({})", e)),
    };
    let l = parse(&data, s.dbs.len())?;
    Ok(install(s, l))
}

pub fn save(s: &mut Store) -> Result<(), String> {
    let snap = snapshot(s);
    let data = serialize(&snap);
    let path = rdb_path(s);
    let start = crate::util::now_ms();
    match write_atomic(&path, &data, true) {
        Ok(()) => {
            s.dirty = 0;
            s.persist.lastsave = crate::util::now_ms() / 1000;
            s.persist.last_bgsave_ok = true;
            s.persist.rdb_saves += 1;
            s.persist.last_bgsave_time_sec = (crate::util::now_ms() - start) / 1000;
            Ok(())
        }
        Err(e) => {
            s.persist.last_bgsave_ok = false;
            Err(format!("Failed opening the temp RDB file temp-{}.rdb (in server root dir {}) for saving: {}", std::process::id(), s.cfg.dir, e))
        }
    }
}

/// Background save: snapshot under the lock, serialize and write on a thread.
pub fn bgsave(s: &mut Store, shared: Arc<Shared>) {
    if s.persist.bgsave_in_progress {
        return;
    }
    let snap = snapshot(s);
    let path = rdb_path(s);
    let dirty_at_start = s.dirty;
    s.persist.bgsave_in_progress = true;
    s.persist.bgsave_started = crate::util::now_ms() / 1000;
    s.stats.total_forks += 1;
    std::thread::spawn(move || {
        let start = crate::util::now_ms();
        let data = serialize(&snap);
        let res = write_atomic(&path, &data, true);
        let mut s = shared.store.lock();
        s.persist.bgsave_in_progress = false;
        s.persist.last_bgsave_time_sec = (crate::util::now_ms() - start) / 1000;
        match res {
            Ok(()) => {
                s.dirty = s.dirty.saturating_sub(dirty_at_start);
                s.persist.lastsave = crate::util::now_ms() / 1000;
                s.persist.last_bgsave_ok = true;
                s.persist.rdb_saves += 1;
                eprintln!("Background saving terminated with success");
            }
            Err(e) => {
                s.persist.last_bgsave_ok = false;
                eprintln!("Background saving error: {}", e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crc() {
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);
    }
    #[test]
    fn lzf_roundtrip() {
        let data: Vec<u8> = b"hello hello hello hello world world world abcabcabcabcabc".repeat(20);
        let c = lzf_compress(&data).unwrap();
        assert!(c.len() < data.len());
        assert_eq!(lzf_decompress(&c, data.len()).unwrap(), data);
        let rnd: Vec<u8> = (0..1000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        if let Some(c) = lzf_compress(&rnd) {
            assert_eq!(lzf_decompress(&c, rnd.len()).unwrap(), rnd);
        }
    }
    #[test]
    fn dump_restore() {
        let vals = vec![
            Value::Str(Bytes::from_static(b"12345")),
            Value::Str(Bytes::from(vec![b'a'; 100])),
            Value::List((0..300).map(|i| Bytes::from(format!("e{}", i))).collect()),
        ];
        for v in vals {
            let p = dump_payload(&v);
            let r = restore_payload(&p).unwrap();
            assert_eq!(format!("{:?}", r.type_name()), format!("{:?}", v.type_name()));
        }
        // Known Redis DUMP of the string "1": \x00\xc0\x01\x0c\x00 + crc
        let p = dump_payload(&Value::Str(Bytes::from_static(b"1")));
        assert_eq!(&p[..5], b"\x00\xc0\x01\x0c\x00");
    }
}
