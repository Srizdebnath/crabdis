//! Sorted set: dict (member -> score) + skiplist with spans (ranks in O(log n)),
//! a port of Redis' zskiplist design onto an index arena.

use crate::dict::Dict;
use bytes::Bytes;
use std::cmp::Ordering;

const MAX_LEVEL: usize = 32;
const P: f64 = 0.25;
const NIL: u32 = u32::MAX;

#[derive(Clone)]
struct Level {
    forward: u32,
    span: u32,
}

#[derive(Clone)]
struct Node {
    member: Bytes,
    score: f64,
    backward: u32,
    levels: Vec<Level>,
}

#[derive(Clone)]
pub struct SkipList {
    nodes: Vec<Node>,
    free: Vec<u32>,
    tail: u32,
    level: usize,
    length: usize,
}

const HEAD: u32 = 0;

fn cmp(s1: f64, m1: &[u8], s2: f64, m2: &[u8]) -> Ordering {
    match s1.partial_cmp(&s2).unwrap_or(Ordering::Equal) {
        Ordering::Equal => m1.cmp(m2),
        o => o,
    }
}

fn random_level() -> usize {
    let mut l = 1;
    while l < MAX_LEVEL && crate::util::rand_f64() < P {
        l += 1;
    }
    l
}

/// Score range with exclusivity flags.
#[derive(Clone, Copy, Debug)]
pub struct ScoreRange {
    pub min: f64,
    pub max: f64,
    pub minex: bool,
    pub maxex: bool,
}

impl ScoreRange {
    pub fn gte_min(&self, v: f64) -> bool {
        if self.minex { v > self.min } else { v >= self.min }
    }
    pub fn lte_max(&self, v: f64) -> bool {
        if self.maxex { v < self.max } else { v <= self.max }
    }
    pub fn empty(&self) -> bool {
        self.min > self.max || (self.min == self.max && (self.minex || self.maxex))
    }
}

/// Lex range. `None` bound = infinity (min: -, max: +).
#[derive(Clone, Debug)]
pub enum LexBound {
    NegInf,
    PosInf,
    Incl(Bytes),
    Excl(Bytes),
}

#[derive(Clone, Debug)]
pub struct LexRange {
    pub min: LexBound,
    pub max: LexBound,
}

impl LexRange {
    pub fn gte_min(&self, v: &[u8]) -> bool {
        match &self.min {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(m) => v >= m.as_ref(),
            LexBound::Excl(m) => v > m.as_ref(),
        }
    }
    pub fn lte_max(&self, v: &[u8]) -> bool {
        match &self.max {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Incl(m) => v <= m.as_ref(),
            LexBound::Excl(m) => v < m.as_ref(),
        }
    }
    pub fn empty(&self) -> bool {
        match (&self.min, &self.max) {
            (LexBound::PosInf, _) | (_, LexBound::NegInf) => true,
            (LexBound::NegInf, _) | (_, LexBound::PosInf) => false,
            (a, b) => {
                let (av, aex) = match a {
                    LexBound::Incl(x) => (x, false),
                    LexBound::Excl(x) => (x, true),
                    _ => unreachable!(),
                };
                let (bv, bex) = match b {
                    LexBound::Incl(x) => (x, false),
                    LexBound::Excl(x) => (x, true),
                    _ => unreachable!(),
                };
                match av.cmp(bv) {
                    Ordering::Greater => true,
                    Ordering::Equal => aex || bex,
                    Ordering::Less => false,
                }
            }
        }
    }
}

impl SkipList {
    pub fn new() -> Self {
        let head = Node {
            member: Bytes::new(),
            score: 0.0,
            backward: NIL,
            levels: vec![Level { forward: NIL, span: 0 }; MAX_LEVEL],
        };
        SkipList { nodes: vec![head], free: Vec::new(), tail: NIL, level: 1, length: 0 }
    }

    fn alloc(&mut self, member: Bytes, score: f64, level: usize) -> u32 {
        let node = Node {
            member,
            score,
            backward: NIL,
            levels: vec![Level { forward: NIL, span: 0 }; level],
        };
        if let Some(i) = self.free.pop() {
            self.nodes[i as usize] = node;
            i
        } else {
            self.nodes.push(node);
            (self.nodes.len() - 1) as u32
        }
    }

    #[inline]
    fn n(&self, i: u32) -> &Node {
        &self.nodes[i as usize]
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        let mut update = [HEAD; MAX_LEVEL];
        let mut rank = [0u32; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            rank[i] = if i == self.level - 1 { 0 } else { rank[i + 1] };
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && cmp(self.n(f).score, &self.n(f).member, score, &member) == Ordering::Less {
                    rank[i] += self.n(x).levels[i].span;
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let level = random_level();
        if level > self.level {
            for i in self.level..level {
                rank[i] = 0;
                update[i] = HEAD;
                self.nodes[HEAD as usize].levels[i].span = self.length as u32;
            }
            self.level = level;
        }
        let x = self.alloc(member, score, level);
        for i in 0..level {
            let u = update[i] as usize;
            let uf = self.nodes[u].levels[i].forward;
            let uspan = self.nodes[u].levels[i].span;
            self.nodes[x as usize].levels[i].forward = uf;
            self.nodes[u].levels[i].forward = x;
            self.nodes[x as usize].levels[i].span = uspan - (rank[0] - rank[i]);
            self.nodes[u].levels[i].span = (rank[0] - rank[i]) + 1;
        }
        for i in level..self.level {
            self.nodes[update[i] as usize].levels[i].span += 1;
        }
        self.nodes[x as usize].backward = if update[0] == HEAD { NIL } else { update[0] };
        let f0 = self.nodes[x as usize].levels[0].forward;
        if f0 != NIL {
            self.nodes[f0 as usize].backward = x;
        } else {
            self.tail = x;
        }
        self.length += 1;
    }

    fn delete_node(&mut self, x: u32, update: &[u32; MAX_LEVEL]) {
        for i in 0..self.level {
            let u = update[i] as usize;
            if self.nodes[u].levels[i].forward == x {
                self.nodes[u].levels[i].span += self.nodes[x as usize].levels[i].span;
                self.nodes[u].levels[i].span -= 1;
                self.nodes[u].levels[i].forward = self.nodes[x as usize].levels[i].forward;
            } else {
                self.nodes[u].levels[i].span -= 1;
            }
        }
        let f0 = self.nodes[x as usize].levels[0].forward;
        let back = self.nodes[x as usize].backward;
        if f0 != NIL {
            self.nodes[f0 as usize].backward = back;
        } else {
            self.tail = back;
        }
        while self.level > 1 && self.nodes[HEAD as usize].levels[self.level - 1].forward == NIL {
            self.level -= 1;
        }
        self.length -= 1;
        self.nodes[x as usize].member = Bytes::new();
        self.nodes[x as usize].levels = Vec::new();
        self.free.push(x);
    }

    pub fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        let mut update = [HEAD; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && cmp(self.n(f).score, &self.n(f).member, score, member) == Ordering::Less {
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let x = self.n(x).levels[0].forward;
        if x != NIL && self.n(x).score == score && self.n(x).member.as_ref() == member {
            self.delete_node(x, &update);
            true
        } else {
            false
        }
    }

    /// 1-based rank of the element, or 0 if not found.
    pub fn rank(&self, score: f64, member: &[u8]) -> usize {
        let mut rank = 0usize;
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && cmp(self.n(f).score, &self.n(f).member, score, member) != Ordering::Greater {
                    rank += self.n(x).levels[i].span as usize;
                    x = f;
                } else {
                    break;
                }
            }
            if x != HEAD && self.n(x).member.as_ref() == member {
                return rank;
            }
        }
        0
    }

    /// Node at 1-based rank.
    fn by_rank(&self, rank: usize) -> u32 {
        let mut traversed = 0usize;
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && traversed + self.n(x).levels[i].span as usize <= rank {
                    traversed += self.n(x).levels[i].span as usize;
                    x = f;
                } else {
                    break;
                }
            }
            if traversed == rank {
                return x;
            }
        }
        NIL
    }

    fn first_in_score(&self, r: &ScoreRange) -> u32 {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && !r.gte_min(self.n(f).score) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.n(x).levels[0].forward;
        if x == NIL || !r.lte_max(self.n(x).score) { NIL } else { x }
    }

    fn last_in_score(&self, r: &ScoreRange) -> u32 {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && r.lte_max(self.n(f).score) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        if x == HEAD || !r.gte_min(self.n(x).score) { NIL } else { x }
    }

    fn first_in_lex(&self, r: &LexRange) -> u32 {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && !r.gte_min(&self.n(f).member) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.n(x).levels[0].forward;
        if x == NIL || !r.lte_max(&self.n(x).member) { NIL } else { x }
    }

    fn last_in_lex(&self, r: &LexRange) -> u32 {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.n(x).levels[i].forward;
                if f != NIL && r.lte_max(&self.n(f).member) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        if x == HEAD || !r.gte_min(&self.n(x).member) { NIL } else { x }
    }
}

/// Sorted set value.
#[derive(Clone)]
pub struct ZSet {
    pub dict: Dict<f64>,
    pub zsl: SkipList,
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSet {
    pub fn new() -> Self {
        ZSet { dict: Dict::new(), zsl: SkipList::new() }
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    pub fn score(&self, m: &[u8]) -> Option<f64> {
        self.dict.get(m).copied()
    }

    /// Insert or update. Returns true if a new element was added.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        if let Some(old) = self.dict.get(&member).copied() {
            if old != score {
                self.zsl.delete(old, &member);
                self.zsl.insert(score, member.clone());
                *self.dict.get_mut(&member).unwrap() = score;
            }
            false
        } else {
            self.zsl.insert(score, member.clone());
            self.dict.insert(member, score);
            true
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some((_, s)) = self.dict.remove(member) {
            self.zsl.delete(s, member);
            true
        } else {
            false
        }
    }

    /// 0-based rank (ascending).
    pub fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        let s = self.score(member)?;
        let r = self.zsl.rank(s, member);
        if r == 0 {
            return None;
        }
        Some(if rev { self.len() - r } else { r - 1 })
    }

    /// Elements in [start, end] 0-based inclusive ranks (already normalized).
    pub fn range_by_rank(&self, start: usize, end: usize, rev: bool) -> Vec<(Bytes, f64)> {
        let mut out = Vec::new();
        if start > end || start >= self.len() {
            return out;
        }
        let end = end.min(self.len() - 1);
        let mut x = if rev {
            self.zsl.by_rank(self.len() - start)
        } else {
            self.zsl.by_rank(start + 1)
        };
        let mut n = end - start + 1;
        while n > 0 && x != NIL {
            let node = self.zsl.n(x);
            out.push((node.member.clone(), node.score));
            x = if rev { node.backward } else { node.levels[0].forward };
            n -= 1;
        }
        out
    }

    pub fn range_by_score(
        &self,
        r: &ScoreRange,
        rev: bool,
        offset: usize,
        limit: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let mut out = Vec::new();
        if r.empty() {
            return out;
        }
        let mut x = if rev { self.zsl.last_in_score(r) } else { self.zsl.first_in_score(r) };
        let mut skip = offset;
        while x != NIL && skip > 0 {
            let node = self.zsl.n(x);
            x = if rev { node.backward } else { node.levels[0].forward };
            skip -= 1;
        }
        while x != NIL {
            if let Some(l) = limit {
                if out.len() >= l {
                    break;
                }
            }
            let node = self.zsl.n(x);
            let ok = if rev { r.gte_min(node.score) } else { r.lte_max(node.score) };
            if !ok {
                break;
            }
            out.push((node.member.clone(), node.score));
            x = if rev { node.backward } else { node.levels[0].forward };
        }
        out
    }

    pub fn range_by_lex(
        &self,
        r: &LexRange,
        rev: bool,
        offset: usize,
        limit: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let mut out = Vec::new();
        if r.empty() {
            return out;
        }
        let mut x = if rev { self.zsl.last_in_lex(r) } else { self.zsl.first_in_lex(r) };
        let mut skip = offset;
        while x != NIL && skip > 0 {
            let node = self.zsl.n(x);
            x = if rev { node.backward } else { node.levels[0].forward };
            skip -= 1;
        }
        while x != NIL {
            if let Some(l) = limit {
                if out.len() >= l {
                    break;
                }
            }
            let node = self.zsl.n(x);
            let ok = if rev { r.gte_min(&node.member) } else { r.lte_max(&node.member) };
            if !ok {
                break;
            }
            out.push((node.member.clone(), node.score));
            x = if rev { node.backward } else { node.levels[0].forward };
        }
        out
    }

    pub fn count_in_score(&self, r: &ScoreRange) -> usize {
        if r.empty() {
            return 0;
        }
        let first = self.zsl.first_in_score(r);
        if first == NIL {
            return 0;
        }
        let last = self.zsl.last_in_score(r);
        let fr = self.zsl.rank(self.zsl.n(first).score, &self.zsl.n(first).member);
        let lr = self.zsl.rank(self.zsl.n(last).score, &self.zsl.n(last).member);
        lr - fr + 1
    }

    pub fn count_in_lex(&self, r: &LexRange) -> usize {
        if r.empty() {
            return 0;
        }
        let first = self.zsl.first_in_lex(r);
        if first == NIL {
            return 0;
        }
        let last = self.zsl.last_in_lex(r);
        let fr = self.zsl.rank(self.zsl.n(first).score, &self.zsl.n(first).member);
        let lr = self.zsl.rank(self.zsl.n(last).score, &self.zsl.n(last).member);
        lr - fr + 1
    }

    /// Ascending iteration over all elements.
    pub fn iter(&self) -> ZIter<'_> {
        ZIter { z: self, x: self.zsl.n(HEAD).levels[0].forward, rev: false }
    }

    pub fn iter_rev(&self) -> ZIter<'_> {
        ZIter { z: self, x: self.zsl.tail, rev: true }
    }

    pub fn first(&self) -> Option<(Bytes, f64)> {
        self.iter().next()
    }
    pub fn last(&self) -> Option<(Bytes, f64)> {
        self.iter_rev().next()
    }
}

pub struct ZIter<'a> {
    z: &'a ZSet,
    x: u32,
    rev: bool,
}

impl<'a> Iterator for ZIter<'a> {
    type Item = (Bytes, f64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.x == NIL {
            return None;
        }
        let n = self.z.zsl.n(self.x);
        self.x = if self.rev { n.backward } else { n.levels[0].forward };
        Some((n.member.clone(), n.score))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ranks() {
        let mut z = ZSet::new();
        for i in 0..500 {
            z.insert(Bytes::from(format!("m{i:04}")), (i % 50) as f64);
        }
        let all: Vec<_> = z.iter().collect();
        assert_eq!(all.len(), 500);
        for w in all.windows(2) {
            assert!(cmp(w[0].1, &w[0].0, w[1].1, &w[1].0) == Ordering::Less);
        }
        for (i, (m, _)) in all.iter().enumerate() {
            assert_eq!(z.rank(m, false), Some(i));
        }
        let r = z.range_by_rank(10, 12, false);
        assert_eq!(r[0].0, all[10].0);
        for i in 0..250 {
            assert!(z.remove(format!("m{i:04}").as_bytes()));
        }
        let all: Vec<_> = z.iter().collect();
        for (i, (m, _)) in all.iter().enumerate() {
            assert_eq!(z.rank(m, false), Some(i));
        }
        let rng = ScoreRange { min: 10.0, max: 20.0, minex: false, maxex: true };
        assert_eq!(z.count_in_score(&rng), z.range_by_score(&rng, false, 0, None).len());
    }
}
