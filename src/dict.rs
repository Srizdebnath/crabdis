//! A chained power-of-two hash table keyed by bytes, with Redis' reverse-binary
//! SCAN cursor so that SCAN guarantees hold across resizes.

use bytes::Bytes;
use std::hash::BuildHasher;

struct Node<V> {
    key: Bytes,
    val: V,
    next: Option<Box<Node<V>>>,
}

pub struct Dict<V> {
    table: Vec<Option<Box<Node<V>>>>,
    used: usize,
    hasher: ahash::RandomState,
}

impl<V> Default for Dict<V> {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static SEED: ahash::RandomState = ahash::RandomState::new();
}

fn hasher() -> ahash::RandomState {
    // A single process-wide seed keeps hashing consistent (needed by SCAN across instances, debug).
    static GLOBAL: std::sync::OnceLock<(u64, u64, u64, u64)> = std::sync::OnceLock::new();
    let k = GLOBAL.get_or_init(|| {
        let r = crate::util::rand_u64();
        (r, r.rotate_left(17) ^ 0x9E3779B97F4A7C15, r ^ 0xDEADBEEF, r.rotate_left(41))
    });
    ahash::RandomState::with_seeds(k.0, k.1, k.2, k.3)
}

const MIN_SIZE: usize = 4;

impl<V> Dict<V> {
    pub fn new() -> Self {
        let _ = SEED.with(|_| ());
        Dict { table: Vec::new(), used: 0, hasher: hasher() }
    }

    pub fn with_capacity(n: usize) -> Self {
        let mut d = Self::new();
        if n > 0 {
            d.resize(n.next_power_of_two().max(MIN_SIZE));
        }
        d
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.used
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    pub fn buckets(&self) -> usize {
        self.table.len()
    }

    #[inline]
    fn hash(&self, k: &[u8]) -> u64 {
        self.hasher.hash_one(k)
    }

    #[inline]
    fn mask(&self) -> usize {
        self.table.len().wrapping_sub(1)
    }

    pub fn get(&self, k: &[u8]) -> Option<&V> {
        if self.used == 0 {
            return None;
        }
        let idx = (self.hash(k) as usize) & self.mask();
        let mut cur = self.table[idx].as_deref();
        while let Some(n) = cur {
            if n.key.as_ref() == k {
                return Some(&n.val);
            }
            cur = n.next.as_deref();
        }
        None
    }

    pub fn get_key_value(&self, k: &[u8]) -> Option<(&Bytes, &V)> {
        if self.used == 0 {
            return None;
        }
        let idx = (self.hash(k) as usize) & self.mask();
        let mut cur = self.table[idx].as_deref();
        while let Some(n) = cur {
            if n.key.as_ref() == k {
                return Some((&n.key, &n.val));
            }
            cur = n.next.as_deref();
        }
        None
    }

    pub fn get_mut(&mut self, k: &[u8]) -> Option<&mut V> {
        if self.used == 0 {
            return None;
        }
        let idx = (self.hash(k) as usize) & self.mask();
        let mut cur = self.table[idx].as_deref_mut();
        while let Some(n) = cur {
            if n.key.as_ref() == k {
                return Some(&mut n.val);
            }
            cur = n.next.as_deref_mut();
        }
        None
    }

    pub fn contains_key(&self, k: &[u8]) -> bool {
        self.get(k).is_some()
    }

    /// Insert or replace. Returns the previous value.
    pub fn insert(&mut self, k: Bytes, v: V) -> Option<V> {
        if let Some(slot) = self.get_mut(&k) {
            return Some(std::mem::replace(slot, v));
        }
        self.expand_if_needed();
        let idx = (self.hash(&k) as usize) & self.mask();
        let next = self.table[idx].take();
        self.table[idx] = Some(Box::new(Node { key: k, val: v, next }));
        self.used += 1;
        None
    }

    /// Get mutable reference, inserting with `f` if missing. Returns (value, inserted).
    pub fn entry_or_insert_with(&mut self, k: &[u8], f: impl FnOnce() -> V) -> (&mut V, bool) {
        let mut inserted = false;
        if !self.contains_key(k) {
            self.insert(Bytes::copy_from_slice(k), f());
            inserted = true;
        }
        (self.get_mut(k).unwrap(), inserted)
    }

    pub fn remove(&mut self, k: &[u8]) -> Option<(Bytes, V)> {
        if self.used == 0 {
            return None;
        }
        let idx = (self.hash(k) as usize) & self.mask();
        let mut link = &mut self.table[idx];
        loop {
            match link {
                None => return None,
                Some(n) if n.key.as_ref() == k => {
                    let mut node = link.take().unwrap();
                    *link = node.next.take();
                    self.used -= 1;
                    let r = Some((node.key, node.val));
                    self.shrink_if_needed();
                    return r;
                }
                Some(n) => link = &mut n.next,
            }
        }
    }

    pub fn clear(&mut self) {
        // Drop iteratively to avoid deep recursion on long chains.
        for b in self.table.iter_mut() {
            let mut cur = b.take();
            while let Some(mut n) = cur {
                cur = n.next.take();
            }
        }
        self.table = Vec::new();
        self.used = 0;
    }

    fn expand_if_needed(&mut self) {
        if self.table.is_empty() {
            self.resize(MIN_SIZE);
        } else if self.used >= self.table.len() {
            self.resize((self.used + 1).next_power_of_two() * 2);
        }
    }

    fn shrink_if_needed(&mut self) {
        let size = self.table.len();
        if size > MIN_SIZE && self.used * 8 < size {
            self.resize(self.used.next_power_of_two().max(MIN_SIZE));
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.shrink_if_needed();
    }

    fn resize(&mut self, newsize: usize) {
        let mut newt: Vec<Option<Box<Node<V>>>> = Vec::with_capacity(newsize);
        newt.resize_with(newsize, || None);
        let mask = newsize - 1;
        let old = std::mem::take(&mut self.table);
        for mut b in old {
            while let Some(mut n) = b {
                b = n.next.take();
                let idx = (self.hasher.hash_one(n.key.as_ref()) as usize) & mask;
                n.next = newt[idx].take();
                newt[idx] = Some(n);
            }
        }
        self.table = newt;
    }

    pub fn iter(&self) -> DictIter<'_, V> {
        DictIter { dict: self, bucket: 0, node: None }
    }

    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    pub fn retain(&mut self, mut f: impl FnMut(&Bytes, &mut V) -> bool) {
        for i in 0..self.table.len() {
            let mut link = &mut self.table[i];
            loop {
                match link {
                    None => break,
                    Some(n) => {
                        if f(&n.key, &mut n.val) {
                            link = &mut link.as_mut().unwrap().next;
                        } else {
                            let mut node = link.take().unwrap();
                            *link = node.next.take();
                            self.used -= 1;
                        }
                    }
                }
            }
        }
        self.shrink_if_needed();
    }

    /// Redis dictScan: visit one bucket (expanded across sizes) and return the next cursor.
    pub fn scan(&self, cursor: u64, mut f: impl FnMut(&Bytes, &V)) -> u64 {
        if self.table.is_empty() {
            return 0;
        }
        let m0 = self.mask() as u64;
        let mut v = cursor;
        let mut cur = self.table[(v & m0) as usize].as_deref();
        while let Some(n) = cur {
            f(&n.key, &n.val);
            cur = n.next.as_deref();
        }
        v |= !m0;
        v = v.reverse_bits();
        v = v.wrapping_add(1);
        v = v.reverse_bits();
        v
    }

    /// Pick a random entry (bucket-uniform, like dictGetFairRandomKey-ish).
    pub fn random_entry(&self) -> Option<(&Bytes, &V)> {
        if self.used == 0 {
            return None;
        }
        let size = self.table.len();
        loop {
            let idx = crate::util::rand_range(size);
            if let Some(head) = self.table[idx].as_deref() {
                let mut len = 0;
                let mut c = Some(head);
                while let Some(n) = c {
                    len += 1;
                    c = n.next.as_deref();
                }
                let mut pick = crate::util::rand_range(len);
                let mut c = head;
                while pick > 0 {
                    c = c.next.as_deref().unwrap();
                    pick -= 1;
                }
                return Some((&c.key, &c.val));
            }
        }
    }

    /// Sample up to `count` entries starting at a random bucket (dictGetSomeKeys).
    pub fn sample(&self, count: usize) -> Vec<(&Bytes, &V)> {
        let mut out = Vec::with_capacity(count);
        if self.used == 0 {
            return out;
        }
        let size = self.table.len();
        let mut idx = crate::util::rand_range(size);
        let mut steps = 0;
        while out.len() < count && steps < size {
            let mut c = self.table[idx].as_deref();
            while let Some(n) = c {
                out.push((&n.key, &n.val));
                if out.len() >= count {
                    break;
                }
                c = n.next.as_deref();
            }
            idx = (idx + 1) & (size - 1);
            steps += 1;
        }
        out
    }
}

impl<V: Clone> Clone for Dict<V> {
    fn clone(&self) -> Self {
        let mut d = Dict::with_capacity(self.used);
        for (k, v) in self.iter() {
            d.insert(k.clone(), v.clone());
        }
        d
    }
}

impl<V> Drop for Dict<V> {
    fn drop(&mut self) {
        self.clear();
    }
}

pub struct DictIter<'a, V> {
    dict: &'a Dict<V>,
    bucket: usize,
    node: Option<&'a Node<V>>,
}

impl<'a, V> Iterator for DictIter<'a, V> {
    type Item = (&'a Bytes, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(n) = self.node {
                self.node = n.next.as_deref();
                return Some((&n.key, &n.val));
            }
            if self.bucket >= self.dict.table.len() {
                return None;
            }
            self.node = self.dict.table[self.bucket].as_deref();
            self.bucket += 1;
        }
    }
}

impl<V: std::fmt::Debug> std::fmt::Debug for Dict<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn basic_and_scan() {
        let mut d = Dict::new();
        for i in 0..1000 {
            d.insert(Bytes::from(format!("k{i}")), i);
        }
        assert_eq!(d.len(), 1000);
        assert_eq!(d.get(b"k5"), Some(&5));
        let mut seen = std::collections::HashSet::new();
        let mut c = 0;
        let mut rounds = 0;
        loop {
            c = d.scan(c, |k, _| {
                seen.insert(k.clone());
            });
            rounds += 1;
            if rounds == 10 {
                for i in 1000..3000 {
                    d.insert(Bytes::from(format!("k{i}")), i);
                }
            }
            if c == 0 {
                break;
            }
        }
        for i in 0..1000 {
            assert!(seen.contains(format!("k{i}").as_bytes()));
        }
        for i in 0..2900 {
            d.remove(format!("k{i}").as_bytes());
        }
        assert_eq!(d.len(), 100);
    }
}
