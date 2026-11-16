//! Pub/Sub registry, keyspace notifications and MONITOR feed.

use crate::config::*;
use crate::resp::Frame;
use crate::server::Store;
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Default)]
pub struct PubSub {
    pub channels: HashMap<Bytes, Vec<u64>>,
    pub patterns: HashMap<Bytes, Vec<u64>>,
    pub shard: HashMap<Bytes, Vec<u64>>,
}

fn b(s: &'static str) -> Frame {
    Frame::Bulk(Bytes::from_static(s.as_bytes()))
}

pub fn sub_reply(kind: &'static str, ch: Option<&Bytes>, count: usize) -> Frame {
    Frame::Push(vec![
        b(kind),
        ch.map(|c| Frame::Bulk(c.clone())).unwrap_or(Frame::Null),
        Frame::Integer(count as i64),
    ])
}

pub enum Kind {
    Channel,
    Pattern,
    Shard,
}

pub fn subscribe(s: &mut Store, cid: u64, ch: Bytes, kind: Kind) -> usize {
    let Some(c) = s.clients.get_mut(&cid) else { return 0 };
    let (set, map) = match kind {
        Kind::Channel => (&mut c.channels, &mut s.pubsub.channels),
        Kind::Pattern => (&mut c.patterns, &mut s.pubsub.patterns),
        Kind::Shard => (&mut c.shard_channels, &mut s.pubsub.shard),
    };
    if set.insert(ch.clone()) {
        map.entry(ch).or_default().push(cid);
    }
    c.flags |= crate::server::cflags::PUBSUB;
    match kind {
        Kind::Shard => c.shard_channels.len(),
        _ => c.subscriptions(),
    }
}

pub fn unsubscribe(s: &mut Store, cid: u64, ch: &Bytes, kind: Kind) -> (bool, usize) {
    let Some(c) = s.clients.get_mut(&cid) else { return (false, 0) };
    let (set, map) = match kind {
        Kind::Channel => (&mut c.channels, &mut s.pubsub.channels),
        Kind::Pattern => (&mut c.patterns, &mut s.pubsub.patterns),
        Kind::Shard => (&mut c.shard_channels, &mut s.pubsub.shard),
    };
    let removed = set.remove(ch);
    if removed {
        if let Some(v) = map.get_mut(ch) {
            v.retain(|&id| id != cid);
            if v.is_empty() {
                map.remove(ch);
            }
        }
    }
    if !c.in_pubsub() {
        c.flags &= !crate::server::cflags::PUBSUB;
    }
    let n = match kind {
        Kind::Shard => c.shard_channels.len(),
        _ => c.subscriptions(),
    };
    (removed, n)
}

pub fn unsubscribe_all(s: &mut Store, cid: u64) {
    let Some(c) = s.clients.get(&cid) else { return };
    let chans: Vec<Bytes> = c.channels.iter().cloned().collect();
    let pats: Vec<Bytes> = c.patterns.iter().cloned().collect();
    let shards: Vec<Bytes> = c.shard_channels.iter().cloned().collect();
    for ch in chans {
        unsubscribe(s, cid, &ch, Kind::Channel);
    }
    for p in pats {
        unsubscribe(s, cid, &p, Kind::Pattern);
    }
    for p in shards {
        unsubscribe(s, cid, &p, Kind::Shard);
    }
}

/// PUBLISH: returns number of receivers.
pub fn publish(s: &mut Store, ch: &Bytes, msg: &Bytes) -> usize {
    let mut n = 0;
    if let Some(ids) = s.pubsub.channels.get(ch) {
        for id in ids {
            if let Some(c) = s.clients.get(id) {
                c.push(Frame::Push(vec![b("message"), Frame::Bulk(ch.clone()), Frame::Bulk(msg.clone())]));
                n += 1;
            }
        }
    }
    if !s.pubsub.patterns.is_empty() {
        for (pat, ids) in s.pubsub.patterns.iter() {
            if crate::util::glob_match(pat, ch, false) {
                for id in ids {
                    if let Some(c) = s.clients.get(id) {
                        c.push(Frame::Push(vec![
                            b("pmessage"),
                            Frame::Bulk(pat.clone()),
                            Frame::Bulk(ch.clone()),
                            Frame::Bulk(msg.clone()),
                        ]));
                        n += 1;
                    }
                }
            }
        }
    }
    n
}

pub fn spublish(s: &mut Store, ch: &Bytes, msg: &Bytes) -> usize {
    let mut n = 0;
    if let Some(ids) = s.pubsub.shard.get(ch) {
        for id in ids {
            if let Some(c) = s.clients.get(id) {
                c.push(Frame::Push(vec![b("smessage"), Frame::Bulk(ch.clone()), Frame::Bulk(msg.clone())]));
                n += 1;
            }
        }
    }
    n
}

pub fn notify_keyspace_event(s: &mut Store, class: u32, event: &str, key: &Bytes, db: usize) {
    let flags = s.cfg.notify_flags;
    if flags & class == 0 {
        return;
    }
    if s.pubsub.channels.is_empty() && s.pubsub.patterns.is_empty() {
        return;
    }
    let ev = Bytes::copy_from_slice(event.as_bytes());
    if flags & NOTIFY_KEYSPACE != 0 {
        let mut ch = format!("__keyspace@{}__:", db).into_bytes();
        ch.extend_from_slice(key);
        publish(s, &Bytes::from(ch), &ev);
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        let ch = Bytes::from(format!("__keyevent@{}__:{}", db, event));
        publish(s, &ch, key);
    }
}

/// sdscatrepr: quote a binary string Redis-style.
pub fn repr(a: &[u8]) -> String {
    let mut s = String::with_capacity(a.len() + 2);
    s.push('"');
    for &c in a {
        match c {
            b'\\' => s.push_str("\\\\"),
            b'"' => s.push_str("\\\""),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            7 => s.push_str("\\a"),
            8 => s.push_str("\\b"),
            c if c.is_ascii_graphic() || c == b' ' => s.push(c as char),
            c => s.push_str(&format!("\\x{:02x}", c)),
        }
    }
    s.push('"');
    s
}

pub fn feed_monitors(s: &mut Store, cid: u64, db: usize, args: &[Bytes], lua: bool) {
    let now = crate::util::now_us();
    let who = if lua {
        "lua".to_string()
    } else if let Some(c) = s.clients.get(&cid) {
        if c.has(crate::server::cflags::UNIX) {
            format!("unix:{}", c.laddr)
        } else {
            c.addr.clone()
        }
    } else {
        "unknown".into()
    };
    let mut line = format!("{}.{:06} [{} {}]", now / 1_000_000, now % 1_000_000, db, who);
    // AUTH / HELLO AUTH arguments are redacted like Redis does.
    let redact = args.first().map(|a| a.eq_ignore_ascii_case(b"auth") || a.eq_ignore_ascii_case(b"hello")).unwrap_or(false);
    if redact {
        line.push_str(&format!(" {}", repr(&args[0])));
        line.push_str(" (redacted)");
    } else {
        for a in args {
            line.push(' ');
            line.push_str(&repr(a));
        }
    }
    let f = Frame::Simple(line);
    for id in &s.monitors {
        if *id == cid && !lua {
            continue;
        }
        if let Some(c) = s.clients.get(id) {
            c.push(f.clone());
        }
    }
}
