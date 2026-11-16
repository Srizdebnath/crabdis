//! Stream data type. Entries live in "nodes" that mirror Redis' rax-of-listpacks
//! layout (same node split rules), so approximate trimming (`MAXLEN ~`) and node
//! accounting behave exactly like Redis.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamId { ms, seq }
    }
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }
    pub fn incr(&self) -> Option<StreamId> {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                None
            } else {
                Some(StreamId { ms: self.ms + 1, seq: 0 })
            }
        } else {
            Some(StreamId { ms: self.ms, seq: self.seq + 1 })
        }
    }
    pub fn decr(&self) -> Option<StreamId> {
        if self.seq == 0 {
            if self.ms == 0 {
                None
            } else {
                Some(StreamId { ms: self.ms - 1, seq: u64::MAX })
            }
        } else {
            Some(StreamId { ms: self.ms, seq: self.seq - 1 })
        }
    }
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(format!("{}-{}", self.ms, self.seq))
    }
    pub fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&self.ms.to_be_bytes());
        b[8..].copy_from_slice(&self.seq.to_be_bytes());
        b
    }
    pub fn decode(b: &[u8]) -> StreamId {
        StreamId { ms: u64::from_be_bytes(b[..8].try_into().unwrap()), seq: u64::from_be_bytes(b[8..16].try_into().unwrap()) }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parse "ms-seq", "ms" (seq = missing_seq), "-" and "+". With `strict`, "-"/"+" are
/// rejected. When `seq_given` is Some, "ms-*" is accepted and reported as `false`.
pub fn parse_id(s: &[u8], missing_seq: u64, strict: bool, seq_given: Option<&mut bool>) -> Option<StreamId> {
    if s.len() > 127 || (strict && (s == b"-" || s == b"+")) {
        return None;
    }
    let allow_star = seq_given.is_some();
    let mut given = true;
    let id = if s == b"-" {
        StreamId::MIN
    } else if s == b"+" {
        StreamId::MAX
    } else {
        let (a, b) = match s.iter().position(|&c| c == b'-') {
            Some(p) => (&s[..p], Some(&s[p + 1..])),
            None => (s, None),
        };
        let ms = crate::util::parse_u64(a)?;
        let seq = match b {
            Some(b"*") if allow_star => {
                given = false;
                0
            }
            Some(b) => crate::util::parse_u64(b)?,
            None => missing_seq,
        };
        StreamId { ms, seq }
    };
    if let Some(flag) = seq_given {
        *flag = given;
    }
    Some(id)
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub id: StreamId,
    /// field, value, field, value ...
    pub fv: Vec<Bytes>,
    pub deleted: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Node {
    pub entries: Vec<Entry>,
    pub valid: usize,
    pub deleted: usize,
    pub bytes: usize,
    /// Fields of the master entry (first entry inserted in the node).
    pub master_fields: Vec<Bytes>,
}

fn lp_str_size(len: usize) -> usize {
    let hdr = if len < 64 {
        1
    } else if len < 4096 {
        2
    } else {
        5
    };
    let tot = hdr + len;
    tot + if tot < 128 { 1 } else if tot < 16384 { 2 } else { 3 }
}

fn lp_int_size(v: u64) -> usize {
    if v < 128 {
        2
    } else if v < 4096 {
        3
    } else if v < 32768 {
        4
    } else if v < 1 << 23 {
        5
    } else if v < 1 << 31 {
        6
    } else {
        10
    }
}

#[derive(Clone, Debug)]
pub struct Nack {
    pub delivery_time: i64,
    pub delivery_count: u64,
    pub consumer: Option<Bytes>,
}

#[derive(Clone, Debug)]
pub struct Consumer {
    pub seen_time: i64,
    pub active_time: i64,
    pub pel: BTreeSet<StreamId>,
}

#[derive(Clone, Debug)]
pub struct Group {
    pub last_id: StreamId,
    /// -1 means invalid/unknown.
    pub entries_read: i64,
    pub pel: BTreeMap<StreamId, Nack>,
    pub consumers: BTreeMap<Bytes, Consumer>,
}

pub const INVALID_ENTRIES_READ: i64 = -1;

#[derive(Clone, Debug, Default)]
pub struct Stream {
    pub nodes: BTreeMap<StreamId, Node>,
    pub length: u64,
    pub last_id: StreamId,
    pub first_id: StreamId,
    pub max_deleted_id: StreamId,
    pub entries_added: u64,
    pub groups: BTreeMap<Bytes, Group>,
}

pub enum TrimStrategy {
    None,
    MaxLen(u64),
    MinId(StreamId),
}

pub enum AppendError {
    /// ID equal or smaller than the top item.
    Dom,
    TooLarge,
}

const LISTPACK_MAX_SIZE: usize = 1 << 30;

impl Stream {
    pub fn new() -> Self {
        Stream::default()
    }

    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn next_id(&self) -> StreamId {
        let ms = crate::util::now_ms() as u64;
        if ms > self.last_id.ms {
            StreamId { ms, seq: 0 }
        } else {
            self.last_id.incr().unwrap_or(self.last_id)
        }
    }

    /// streamAppendItem. `use_id` with `seq_given=false` means "ms-*".
    pub fn append(
        &mut self,
        fv: &[Bytes],
        use_id: Option<StreamId>,
        seq_given: bool,
        node_max_bytes: usize,
        node_max_entries: usize,
    ) -> Result<StreamId, AppendError> {
        let id = match use_id {
            Some(u) => {
                if seq_given {
                    u
                } else if self.last_id.ms == u.ms {
                    if self.last_id.seq == u64::MAX {
                        return Err(AppendError::Dom);
                    }
                    StreamId { ms: u.ms, seq: self.last_id.seq + 1 }
                } else {
                    u
                }
            }
            None => {
                let ms = crate::util::now_ms() as u64;
                if ms > self.last_id.ms {
                    StreamId { ms, seq: 0 }
                } else {
                    match self.last_id.incr() {
                        Some(i) => i,
                        None => return Err(AppendError::Dom),
                    }
                }
            }
        };
        if id <= self.last_id {
            return Err(AppendError::Dom);
        }
        let totelelen: usize = fv.iter().map(|b| b.len()).sum();
        if totelelen > LISTPACK_MAX_SIZE {
            return Err(AppendError::TooLarge);
        }
        let max_bytes = if node_max_bytes == 0 || node_max_bytes > LISTPACK_MAX_SIZE { LISTPACK_MAX_SIZE } else { node_max_bytes };
        let mut new_node = true;
        if let Some((_, last)) = self.nodes.iter().next_back() {
            new_node = false;
            if last.bytes + totelelen >= max_bytes {
                new_node = true;
            } else if node_max_entries > 0 && last.valid + last.deleted >= node_max_entries {
                new_node = true;
            }
        }
        if new_node {
            let fields: Vec<Bytes> = fv.iter().step_by(2).cloned().collect();
            let mut bytes = 7 + lp_int_size(1) + lp_int_size(0) + lp_int_size(fields.len() as u64) + lp_int_size(0);
            for f in &fields {
                bytes += lp_str_size(f.len());
            }
            self.nodes.insert(id, Node { entries: Vec::new(), valid: 0, deleted: 0, bytes, master_fields: fields });
        }
        let (master, node) = self.nodes.iter_mut().next_back().unwrap();
        let same = fv.len() / 2 == node.master_fields.len() && fv.iter().step_by(2).zip(node.master_fields.iter()).all(|(a, b)| a == b);
        let mut eb = lp_int_size(0) + lp_int_size(id.ms - master.ms) + lp_int_size(id.seq.wrapping_sub(master.seq)) + lp_int_size(fv.len() as u64);
        if same {
            for v in fv.iter().skip(1).step_by(2) {
                eb += lp_str_size(v.len());
            }
        } else {
            eb += lp_int_size((fv.len() / 2) as u64);
            for x in fv {
                eb += lp_str_size(x.len());
            }
        }
        node.bytes += eb;
        node.entries.push(Entry { id, fv: fv.to_vec(), deleted: false });
        node.valid += 1;
        self.length += 1;
        self.entries_added += 1;
        self.last_id = id;
        if self.length == 1 {
            self.first_id = id;
        }
        Ok(id)
    }

    /// Insert an entry while loading (RDB/RESTORE) into a given node.
    pub fn load_node(&mut self, master: StreamId, node: Node) {
        self.nodes.insert(master, node);
    }

    pub fn entry_exists(&self, id: &StreamId) -> bool {
        self.get(id).is_some()
    }

    pub fn get(&self, id: &StreamId) -> Option<&Entry> {
        let (_, node) = self.nodes.range(..=*id).next_back()?;
        let i = node.entries.binary_search_by(|e| e.id.cmp(id)).ok()?;
        let e = &node.entries[i];
        if e.deleted { None } else { Some(e) }
    }

    /// Iterate entries in [start, end] (inclusive), forward or reverse, up to `count`
    /// (0 = unlimited).
    pub fn range(&self, start: &StreamId, end: &StreamId, rev: bool, count: usize) -> Vec<(StreamId, Vec<Bytes>)> {
        let mut out = Vec::new();
        if start > end {
            return out;
        }
        if !rev {
            let first = self.nodes.range(..=*start).next_back().map(|(k, _)| *k).unwrap_or(*start);
            'outer: for (_, node) in self.nodes.range(first..=*end) {
                for e in &node.entries {
                    if e.deleted || e.id < *start {
                        continue;
                    }
                    if e.id > *end {
                        break 'outer;
                    }
                    out.push((e.id, e.fv.clone()));
                    if count > 0 && out.len() >= count {
                        break 'outer;
                    }
                }
            }
        } else {
            'outer2: for (_, node) in self.nodes.range(..=*end).rev() {
                for e in node.entries.iter().rev() {
                    if e.deleted || e.id > *end {
                        continue;
                    }
                    if e.id < *start {
                        break 'outer2;
                    }
                    out.push((e.id, e.fv.clone()));
                    if count > 0 && out.len() >= count {
                        break 'outer2;
                    }
                }
            }
        }
        out
    }

    /// First / last valid entry ID.
    pub fn edge_id(&self, first: bool) -> Option<StreamId> {
        if first {
            self.nodes.values().flat_map(|n| n.entries.iter()).find(|e| !e.deleted).map(|e| e.id)
        } else {
            self.nodes.values().rev().flat_map(|n| n.entries.iter().rev()).find(|e| !e.deleted).map(|e| e.id)
        }
    }

    /// Max valid ID (streamLastValidID), 0-0 when empty.
    pub fn last_valid_id(&self) -> StreamId {
        self.edge_id(false).unwrap_or_default()
    }

    pub fn delete(&mut self, id: &StreamId) -> bool {
        let Some((&master, _)) = self.nodes.range(..=*id).next_back() else { return false };
        let node = self.nodes.get_mut(&master).unwrap();
        let Ok(i) = node.entries.binary_search_by(|e| e.id.cmp(id)) else { return false };
        if node.entries[i].deleted {
            return false;
        }
        node.entries[i].deleted = true;
        node.valid -= 1;
        node.deleted += 1;
        self.length -= 1;
        if node.valid == 0 {
            self.nodes.remove(&master);
        }
        true
    }

    /// XDEL semantics on top of delete(): track first id and max deleted id.
    pub fn xdel(&mut self, ids: &[StreamId]) -> usize {
        let mut deleted = 0;
        let mut first_entry = false;
        for id in ids {
            if self.delete(id) {
                if *id == self.first_id {
                    first_entry = true;
                }
                if *id > self.max_deleted_id {
                    self.max_deleted_id = *id;
                }
                deleted += 1;
            }
        }
        if deleted > 0 {
            if self.length == 0 {
                self.first_id = StreamId::MIN;
            } else if first_entry {
                self.first_id = self.edge_id(true).unwrap_or_default();
            }
        }
        deleted
    }

    /// streamTrim. `limit` 0 = unlimited.
    pub fn trim(&mut self, strategy: &TrimStrategy, approx: bool, limit: i64) -> i64 {
        if matches!(strategy, TrimStrategy::None) {
            return 0;
        }
        let mut deleted: i64 = 0;
        let keys: Vec<StreamId> = self.nodes.keys().copied().collect();
        for master in keys {
            if let TrimStrategy::MaxLen(m) = strategy {
                if self.length <= *m {
                    break;
                }
            }
            let node = self.nodes.get(&master).unwrap();
            let entries = node.valid as i64;
            if limit > 0 && deleted + entries > limit {
                break;
            }
            let remove_node = match strategy {
                TrimStrategy::MaxLen(m) => self.length - entries as u64 >= *m,
                TrimStrategy::MinId(id) => node.entries.last().map(|e| e.id < *id).unwrap_or(true),
                TrimStrategy::None => false,
            };
            if remove_node {
                self.nodes.remove(&master);
                self.length -= entries as u64;
                deleted += entries;
                continue;
            }
            if approx {
                break;
            }
            let length = &mut self.length;
            let node = self.nodes.get_mut(&master).unwrap();
            let mut from_node = 0;
            for e in node.entries.iter_mut() {
                let stop = match strategy {
                    TrimStrategy::MaxLen(m) => *length <= *m,
                    TrimStrategy::MinId(id) => e.id >= *id,
                    TrimStrategy::None => true,
                };
                if stop {
                    break;
                }
                if !e.deleted {
                    e.deleted = true;
                    from_node += 1;
                    *length -= 1;
                }
            }
            node.valid -= from_node;
            node.deleted += from_node;
            deleted += from_node as i64;
            if node.valid == 0 {
                self.nodes.remove(&master);
            }
            break;
        }
        if self.length == 0 {
            self.first_id = StreamId::MIN;
        } else if deleted > 0 {
            self.first_id = self.edge_id(true).unwrap_or_default();
        }
        deleted
    }

    pub fn range_has_tombstones(&self, start: Option<&StreamId>, end: Option<&StreamId>) -> bool {
        if self.length == 0 || self.max_deleted_id.is_zero() {
            return false;
        }
        if self.first_id > self.max_deleted_id {
            return false;
        }
        let s = start.copied().unwrap_or(StreamId::MIN);
        let e = end.copied().unwrap_or(StreamId::MAX);
        s <= self.max_deleted_id && self.max_deleted_id <= e
    }

    pub fn estimate_distance_from_first_ever(&self, id: &StreamId) -> i64 {
        if self.entries_added == 0 {
            return 0;
        }
        if self.length == 0 && *id <= self.last_id {
            return self.entries_added as i64;
        }
        if *id == self.last_id {
            return self.entries_added as i64;
        } else if *id > self.last_id {
            return INVALID_ENTRIES_READ;
        }
        if self.max_deleted_id.is_zero() || self.max_deleted_id < self.first_id {
            if *id < self.first_id {
                return (self.entries_added - self.length) as i64;
            } else if *id == self.first_id {
                return (self.entries_added - self.length + 1) as i64;
            }
        }
        INVALID_ENTRIES_READ
    }

    /// Consumer group lag (None = cannot be computed).
    pub fn group_lag(&self, g: &Group) -> Option<i64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        if g.entries_read != INVALID_ENTRIES_READ && !self.range_has_tombstones(Some(&g.last_id), None) {
            return Some(self.entries_added as i64 - g.entries_read);
        }
        let er = self.estimate_distance_from_first_ever(&g.last_id);
        if er != INVALID_ENTRIES_READ { Some(self.entries_added as i64 - er) } else { None }
    }

    pub fn mem_usage(&self) -> usize {
        let mut n = 64;
        for node in self.nodes.values() {
            n += node.bytes + 48;
        }
        for (name, g) in &self.groups {
            n += name.len() + 64 + g.pel.len() * 48;
            for (cn, c) in &g.consumers {
                n += cn.len() + 48 + c.pel.len() * 24;
            }
        }
        n
    }

    pub fn digest(&self) -> Vec<u8> {
        let mut h = sha1_smol::Sha1::new();
        for node in self.nodes.values() {
            for e in &node.entries {
                if e.deleted {
                    continue;
                }
                h.update(&e.id.encode());
                for x in &e.fv {
                    h.update(x);
                    h.update(b"\0");
                }
            }
        }
        h.update(&self.last_id.encode());
        h.digest().bytes().to_vec()
    }

    pub fn create_group(&mut self, name: Bytes, id: StreamId, entries_read: i64) -> bool {
        if self.groups.contains_key(&name) {
            return false;
        }
        self.groups.insert(name, Group { last_id: id, entries_read, pel: BTreeMap::new(), consumers: BTreeMap::new() });
        true
    }
}

impl Group {
    /// Create a consumer if missing. Returns true when created.
    pub fn ensure_consumer(&mut self, name: &Bytes, now: i64) -> bool {
        if self.consumers.contains_key(name) {
            return false;
        }
        self.consumers.insert(name.clone(), Consumer { seen_time: now, active_time: -1, pel: BTreeSet::new() });
        true
    }

    pub fn delete_consumer(&mut self, name: &[u8]) -> Option<usize> {
        let c = self.consumers.remove(name)?;
        for id in &c.pel {
            self.pel.remove(id);
        }
        Some(c.pel.len())
    }

    /// Remove a NACK from the group and owning consumer PELs.
    pub fn remove_nack(&mut self, id: &StreamId) -> bool {
        match self.pel.remove(id) {
            Some(n) => {
                if let Some(cn) = n.consumer {
                    if let Some(c) = self.consumers.get_mut(&cn) {
                        c.pel.remove(id);
                    }
                }
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fv() -> Vec<Bytes> {
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"1")]
    }
    #[test]
    fn append_and_range() {
        let mut s = Stream::new();
        for i in 1..=250u64 {
            s.append(&fv(), Some(StreamId::new(i, 0)), true, 4096, 100).ok().unwrap();
        }
        assert_eq!(s.len(), 250);
        assert_eq!(s.nodes.len(), 3);
        let r = s.range(&StreamId::new(10, 0), &StreamId::new(20, 0), false, 0);
        assert_eq!(r.len(), 11);
        let r = s.range(&StreamId::MIN, &StreamId::MAX, true, 3);
        assert_eq!(r[0].0, StreamId::new(250, 0));
        // approx trim removes only whole nodes
        let d = s.trim(&TrimStrategy::MaxLen(120), true, 0);
        assert_eq!(d, 100);
        assert_eq!(s.len(), 150);
        let d = s.trim(&TrimStrategy::MaxLen(120), false, 0);
        assert_eq!(d, 30);
        assert_eq!(s.first_id, StreamId::new(131, 0));
    }
    #[test]
    fn ids() {
        let mut sg = true;
        assert_eq!(parse_id(b"5-*", 0, true, Some(&mut sg)), Some(StreamId::new(5, 0)));
        assert!(!sg);
        assert_eq!(parse_id(b"5", 7, false, None), Some(StreamId::new(5, 7)));
        assert_eq!(parse_id(b"-", 0, true, None), None);
        assert_eq!(parse_id(b"5-*", 0, true, None), None);
    }
}
