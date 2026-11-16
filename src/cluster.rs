//! Redis Cluster: 16384 hash slots (CRC16 with hash tags), MOVED/ASK/CROSSSLOT
//! redirection, slot migration states, nodes.conf persistence and a cluster bus
//! (on port+10000) carrying gossip, failure detection (PFAIL/FAIL), Pub/Sub
//! propagation and replica failover elections with config epochs.
//!
//! The bus uses crabdis' own binary framing, so crabdis nodes form clusters with
//! each other; clients see standard Redis Cluster behaviour.

use crate::cmd::Cmd;
use crate::resp::Frame;
use crate::server::{Shared, Store, cflags};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

pub const SLOTS: usize = 16384;

// ---------------- CRC16 (XMODEM) ----------------

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
}

pub fn key_slot(key: &[u8]) -> u16 {
    if let Some(s) = key.iter().position(|&c| c == b'{') {
        if let Some(e) = key[s + 1..].iter().position(|&c| c == b'}') {
            if e > 0 {
                return crc16(&key[s + 1..s + 1 + e]) & 0x3fff;
            }
        }
    }
    crc16(key) & 0x3fff
}

// ---------------- node table ----------------

pub mod nflags {
    pub const MYSELF: u16 = 1;
    pub const MASTER: u16 = 2;
    pub const SLAVE: u16 = 4;
    pub const PFAIL: u16 = 8;
    pub const FAIL: u16 = 16;
    pub const HANDSHAKE: u16 = 32;
    pub const NOADDR: u16 = 64;
    pub const MEET: u16 = 128;
    pub const NOFAILOVER: u16 = 256;
}
use nflags as NF;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub flags: u16,
    pub master: Option<String>,
    pub ping_sent: i64,
    pub pong_recv: i64,
    pub data_recv: i64,
    pub config_epoch: u64,
    pub repl_offset: i64,
    pub slots: Vec<u64>,
    pub fail_reports: HashMap<String, i64>,
    pub fail_time: i64,
    pub shard_id: String,
    pub link: Option<mpsc::UnboundedSender<Vec<u8>>>,
    pub link_created: i64,
    pub inbound: bool,
    pub created: i64,
}

impl Node {
    pub fn new(id: String, ip: String, port: u16, cport: u16, flags: u16) -> Node {
        Node {
            id,
            ip,
            port,
            cport,
            flags,
            master: None,
            ping_sent: 0,
            pong_recv: 0,
            data_recv: 0,
            config_epoch: 0,
            repl_offset: 0,
            slots: vec![0; SLOTS / 64],
            fail_reports: HashMap::new(),
            fail_time: 0,
            shard_id: crate::util::random_hex(40),
            link: None,
            link_created: 0,
            inbound: false,
            created: crate::util::now_ms(),
        }
    }
    pub fn has_slot(&self, s: usize) -> bool {
        self.slots[s / 64] & (1 << (s % 64)) != 0
    }
    fn set_slot(&mut self, s: usize, v: bool) {
        if v {
            self.slots[s / 64] |= 1 << (s % 64);
        } else {
            self.slots[s / 64] &= !(1 << (s % 64));
        }
    }
    pub fn slot_count(&self) -> usize {
        self.slots.iter().map(|w| w.count_ones() as usize).sum()
    }
    pub fn is_master(&self) -> bool {
        self.flags & NF::MASTER != 0
    }
    fn flag_str(&self) -> String {
        let mut v = Vec::new();
        if self.flags & NF::MYSELF != 0 {
            v.push("myself");
        }
        if self.flags & NF::MASTER != 0 {
            v.push("master");
        }
        if self.flags & NF::SLAVE != 0 {
            v.push("slave");
        }
        if self.flags & NF::PFAIL != 0 {
            v.push("fail?");
        }
        if self.flags & NF::FAIL != 0 {
            v.push("fail");
        }
        if self.flags & NF::HANDSHAKE != 0 {
            v.push("handshake");
        }
        if self.flags & NF::NOADDR != 0 {
            v.push("noaddr");
        }
        if self.flags & NF::NOFAILOVER != 0 {
            v.push("nofailover");
        }
        if v.is_empty() { "noflags".into() } else { v.join(",") }
    }
}

#[derive(Default, Clone, Copy)]
pub struct MsgStats {
    pub sent: [u64; 10],
    pub recv: [u64; 10],
}

pub struct ClusterState {
    pub myself: String,
    pub nodes: BTreeMap<String, Node>,
    pub slots: Vec<Option<String>>,
    pub migrating: BTreeMap<u16, String>,
    pub importing: BTreeMap<u16, String>,
    pub current_epoch: u64,
    pub last_vote_epoch: u64,
    pub state_ok: bool,
    pub config_file: std::path::PathBuf,
    pub bus_port: u16,
    pub stats: MsgStats,
    pub blacklist: HashMap<String, i64>,
    // failover election (replica side)
    pub election_epoch: u64,
    pub election_votes: usize,
    pub election_start: i64,
    pub mf: Option<ManualFailover>,
    pub todo_save: bool,
}

pub struct ManualFailover {
    pub deadline: i64,
    pub force: bool,
    pub master_offset: Option<i64>,
    /// Master side: replica that requested the manual failover.
    pub replica: Option<String>,
}

const MSG_PING: u8 = 0;
const MSG_PONG: u8 = 1;
const MSG_MEET: u8 = 2;
const MSG_FAIL: u8 = 3;
const MSG_PUBLISH: u8 = 4;
const MSG_AUTH_REQ: u8 = 5;
const MSG_AUTH_ACK: u8 = 6;
const MSG_UPDATE: u8 = 7;
const MSG_MFSTART: u8 = 8;
const MSG_PUBLISHSHARD: u8 = 9;
const MSG_NAMES: [&str; 10] = ["ping", "pong", "meet", "fail", "publish", "auth-req", "auth-ack", "update", "mfstart", "publishshard"];

impl ClusterState {
    pub fn me(&self) -> &Node {
        &self.nodes[&self.myself]
    }
    fn me_mut(&mut self) -> &mut Node {
        self.nodes.get_mut(&self.myself).unwrap()
    }
    pub fn owner(&self, slot: u16) -> Option<&Node> {
        self.slots[slot as usize].as_ref().and_then(|id| self.nodes.get(id))
    }
    pub fn my_master_id(&self) -> String {
        let me = self.me();
        if me.is_master() { me.id.clone() } else { me.master.clone().unwrap_or_default() }
    }

    fn assign(&mut self, slot: usize, id: &str) {
        if let Some(old) = self.slots[slot].take() {
            if let Some(n) = self.nodes.get_mut(&old) {
                n.set_slot(slot, false);
            }
        }
        if let Some(n) = self.nodes.get_mut(id) {
            n.set_slot(slot, true);
            self.slots[slot] = Some(id.to_string());
        }
    }

    fn unassign(&mut self, slot: usize) {
        if let Some(old) = self.slots[slot].take() {
            if let Some(n) = self.nodes.get_mut(&old) {
                n.set_slot(slot, false);
            }
        }
    }

    pub fn update_state(&mut self, require_full: bool) {
        let me_id = self.myself.clone();
        let all_ok = self.slots.iter().all(|o| match o {
            Some(id) => self.nodes.get(id).map(|n| n.flags & NF::FAIL == 0).unwrap_or(false),
            None => false,
        });
        let masters: Vec<&Node> = self.nodes.values().filter(|n| n.is_master() && n.slot_count() > 0).collect();
        let size = masters.len();
        let reachable = masters.iter().filter(|n| n.flags & (NF::FAIL | NF::PFAIL) == 0 || n.id == me_id).count();
        let quorum = size / 2 + 1;
        self.state_ok = (all_ok || !require_full) && (size == 0 || reachable >= quorum) && self.slots.iter().any(|s| s.is_some());
        if !require_full {
            self.state_ok = reachable >= quorum;
        }
    }

    pub fn node_line(&self, n: &Node) -> String {
        let link = if n.flags & NF::MYSELF != 0 || n.link.is_some() { "connected" } else { "disconnected" };
        let mut line = format!(
            "{} {}:{}@{} {} {} {} {} {} {}",
            n.id,
            n.ip,
            n.port,
            n.cport,
            n.flag_str(),
            n.master.clone().unwrap_or_else(|| "-".into()),
            n.ping_sent,
            n.pong_recv,
            n.config_epoch,
            link
        );
        for (a, b) in slot_ranges(n) {
            if a == b {
                line.push_str(&format!(" {}", a));
            } else {
                line.push_str(&format!(" {}-{}", a, b));
            }
        }
        if n.flags & NF::MYSELF != 0 {
            for (s, t) in &self.migrating {
                line.push_str(&format!(" [{}->-{}]", s, t));
            }
            for (s, t) in &self.importing {
                line.push_str(&format!(" [{}-<-{}]", s, t));
            }
        }
        line
    }

    pub fn nodes_text(&self) -> String {
        let mut out = String::new();
        for n in self.nodes.values() {
            if n.flags & NF::HANDSHAKE != 0 {
                continue;
            }
            out.push_str(&self.node_line(n));
            out.push('\n');
        }
        out
    }

    pub fn save_config(&self) -> std::io::Result<()> {
        let mut out = self.nodes_text();
        out.push_str(&format!("vars currentEpoch {} lastVoteEpoch {}\n", self.current_epoch, self.last_vote_epoch));
        let tmp = self.config_file.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, out.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.config_file)
    }

    fn load_config(&mut self, text: &str) -> Result<(), String> {
        for line in text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            if parts[0] == "vars" {
                let mut i = 1;
                while i + 1 < parts.len() {
                    match parts[i] {
                        "currentEpoch" => self.current_epoch = parts[i + 1].parse().unwrap_or(0),
                        "lastVoteEpoch" => self.last_vote_epoch = parts[i + 1].parse().unwrap_or(0),
                        _ => {}
                    }
                    i += 2;
                }
                continue;
            }
            if parts.len() < 8 {
                return Err(format!("Unrecoverable error: corrupted cluster config file \"{}\".", line));
            }
            let id = parts[0].to_string();
            let addr = parts[1].split(',').next().unwrap_or("");
            let (hostport, cport) = addr.split_once('@').unwrap_or((addr, "0"));
            let (ip, port) = hostport.rsplit_once(':').unwrap_or(("", "0"));
            let mut flags = 0;
            for f in parts[2].split(',') {
                flags |= match f {
                    "myself" => NF::MYSELF,
                    "master" => NF::MASTER,
                    "slave" => NF::SLAVE,
                    "fail?" => NF::PFAIL,
                    "fail" => NF::FAIL,
                    "handshake" => NF::HANDSHAKE,
                    "noaddr" => NF::NOADDR,
                    "nofailover" => NF::NOFAILOVER,
                    _ => 0,
                };
            }
            let mut n = Node::new(id.clone(), ip.to_string(), port.parse().unwrap_or(0), cport.parse().unwrap_or(0), flags & !NF::PFAIL);
            if parts[3] != "-" {
                n.master = Some(parts[3].to_string());
            }
            n.config_epoch = parts[6].parse().unwrap_or(0);
            if flags & NF::MYSELF != 0 {
                self.myself = id.clone();
            }
            self.nodes.insert(id.clone(), n);
            for p in &parts[8..] {
                if p.starts_with('[') {
                    let inner = p.trim_matches(|c| c == '[' || c == ']');
                    if let Some((s, t)) = inner.split_once("->-") {
                        if let Ok(s) = s.parse() {
                            self.migrating.insert(s, t.to_string());
                        }
                    } else if let Some((s, t)) = inner.split_once("-<-") {
                        if let Ok(s) = s.parse() {
                            self.importing.insert(s, t.to_string());
                        }
                    }
                    continue;
                }
                let (a, b) = match p.split_once('-') {
                    Some((a, b)) => (a.parse::<usize>().unwrap_or(0), b.parse::<usize>().unwrap_or(0)),
                    None => {
                        let v = p.parse::<usize>().unwrap_or(0);
                        (v, v)
                    }
                };
                for s in a..=b.min(SLOTS - 1) {
                    self.assign(s, &id);
                }
            }
        }
        if self.myself.is_empty() {
            return Err("Unrecoverable error: corrupted cluster config file (no myself node).".into());
        }
        Ok(())
    }
}

pub fn slot_ranges(n: &Node) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for s in 0..=SLOTS {
        let has = s < SLOTS && n.has_slot(s);
        match (has, start) {
            (true, None) => start = Some(s),
            (false, Some(a)) => {
                out.push((a, s - 1));
                start = None;
            }
            _ => {}
        }
    }
    out
}

/// Create (or load from nodes.conf) the cluster state at startup.
pub fn init(s: &mut Store) -> Result<(), String> {
    let path = std::path::Path::new(&s.cfg.dir).join(&s.cfg.cluster_config_file);
    let bus_port = if s.cfg.cluster_port != 0 { s.cfg.cluster_port } else { s.cfg.port.wrapping_add(10000) };
    let mut c = ClusterState {
        myself: String::new(),
        nodes: BTreeMap::new(),
        slots: vec![None; SLOTS],
        migrating: BTreeMap::new(),
        importing: BTreeMap::new(),
        current_epoch: 0,
        last_vote_epoch: 0,
        state_ok: false,
        config_file: path.clone(),
        bus_port,
        stats: MsgStats::default(),
        blacklist: HashMap::new(),
        election_epoch: 0,
        election_votes: 0,
        election_start: 0,
        mf: None,
        todo_save: false,
    };
    match std::fs::read_to_string(&path) {
        Ok(t) if !t.trim().is_empty() => c.load_config(&t)?,
        _ => {
            let id = crate::util::random_hex(40);
            let ip = if s.cfg.cluster_announce_ip.is_empty() { String::new() } else { s.cfg.cluster_announce_ip.clone() };
            let n = Node::new(id.clone(), ip, s.cfg.port, bus_port, NF::MYSELF | NF::MASTER);
            c.myself = id.clone();
            c.nodes.insert(id, n);
            c.save_config().map_err(|e| format!("Can't open {} in order to save the cluster config: {}", path.display(), e))?;
        }
    }
    {
        let port = s.cfg.port;
        let me = c.me_mut();
        me.port = port;
        me.cport = bus_port;
    }
    let req = s.cfg.cluster_require_full_coverage;
    c.update_state(req);
    eprintln!("Cluster node ID: {}", c.myself);
    s.cluster = Some(c);
    Ok(())
}

// ---------------- redirection ----------------

fn moved(n: &Node, slot: u16, ask: bool) -> Frame {
    let ip = if n.ip.is_empty() { "127.0.0.1" } else { &n.ip };
    Frame::err(format!("{} {} {}:{}", if ask { "ASK" } else { "MOVED" }, slot, ip, n.port))
}

/// Decide whether this node can serve the command (getNodeByQuery).
pub fn redirect(s: &mut Store, cid: u64, cmd: &'static Cmd, args: &[Bytes]) -> Option<Frame> {
    let (asking, readonly) = {
        let c = s.clients.get_mut(&cid)?;
        let a = c.has(cflags::ASKING);
        if cmd.name != "asking" {
            c.flags &= !cflags::ASKING;
        }
        (a, c.has(cflags::READONLY))
    };
    let asking = asking || cmd.flags & crate::cmd::flags::ASKING != 0;
    // Commands inside MULTI are checked when queued; EXEC itself has no keys.
    let idx = cmd.key_indexes(args);
    if idx.is_empty() {
        return None;
    }
    let mut slot: Option<u16> = None;
    for &i in &idx {
        let ks = key_slot(&args[i]);
        match slot {
            None => slot = Some(ks),
            Some(sl) if sl != ks => return Some(Frame::err("CROSSSLOT Keys in request don't hash to the same slot")),
            _ => {}
        }
    }
    let slot = slot.unwrap();
    let c = s.cluster.as_ref()?;
    let is_write = cmd.is_write_for(args);
    if !c.state_ok {
        if !s.cfg.cluster_allow_reads_when_down {
            return Some(Frame::err("CLUSTERDOWN The cluster is down"));
        }
        if is_write {
            return Some(Frame::err("CLUSTERDOWN The cluster is down and only accepts read commands"));
        }
    }
    let Some(owner) = c.owner(slot) else {
        return Some(Frame::err(format!("CLUSTERDOWN Hash slot not served")));
    };
    let db = s.clients.get(&cid).map(|c| c.db).unwrap_or(0);
    let missing = idx.iter().filter(|&&i| !s.dbs[db].dict.contains_key(&args[i])).count();
    if owner.id == c.myself {
        if let Some(target) = c.migrating.get(&slot) {
            if missing > 0 {
                if missing < idx.len() || idx.len() == 1 || missing == idx.len() {
                    if idx.len() > 1 && missing < idx.len() {
                        return Some(Frame::err("TRYAGAIN Multiple keys request during rehashing of slot"));
                    }
                    if let Some(t) = c.nodes.get(target) {
                        return Some(moved(t, slot, true));
                    }
                }
            }
        }
        return None;
    }
    if c.importing.contains_key(&slot) && asking {
        if idx.len() > 1 && missing > 0 {
            return Some(Frame::err("TRYAGAIN Multiple keys request during rehashing of slot"));
        }
        return None;
    }
    // Replica serving reads for its master's slots.
    let me = c.me();
    if readonly && !is_write && me.flags & NF::SLAVE != 0 && me.master.as_deref() == Some(owner.id.as_str()) {
        return None;
    }
    Some(moved(owner, slot, false))
}

/// Keys accessed from scripts must be local.
pub fn script_check(s: &Store, _caller: u64, cmd: &Cmd, args: &[Bytes], allow_cross: bool) -> Result<(), String> {
    let Some(c) = s.cluster.as_ref() else { return Ok(()) };
    let idx = cmd.key_indexes(args);
    let mut slot = None;
    for &i in &idx {
        let ks = key_slot(&args[i]);
        if let Some(sl) = slot {
            if sl != ks && !allow_cross {
                return Err(format!("Command '{}' in script attempted to access keys that don't hash to the same slot", cmd.full_name()));
            }
        }
        slot = Some(ks);
        match c.owner(ks) {
            Some(n) if n.id == c.myself => {}
            Some(n) if n.id == c.my_master_id() && !cmd.is_write_for(args) => {}
            None => return Err("Script attempted to access a slot not served".into()),
            _ => return Err("Script attempted to access a non local key in a cluster node".into()),
        }
    }
    if !c.state_ok {
        return Err(if cmd.is_write_for(args) { "Script attempted to execute a write command while the cluster is down and readonly".into() } else { "Script attempted to execute a command while the cluster is down".into() });
    }
    Ok(())
}

// ---------------- bus messages ----------------

struct Msg {
    kind: u8,
    sender: String,
    ip: String,
    port: u16,
    cport: u16,
    flags: u16,
    master: String,
    current_epoch: u64,
    config_epoch: u64,
    offset: i64,
    mflags: u8,
    slots: Vec<u64>,
    gossip: Vec<(String, String, u16, u16, u16, i64, i64)>,
    payload: Vec<Vec<u8>>,
}

fn put_str(b: &mut Vec<u8>, s: &[u8]) {
    b.extend_from_slice(&(s.len() as u32).to_be_bytes());
    b.extend_from_slice(s);
}

fn encode_msg(m: &Msg) -> Vec<u8> {
    let mut b = Vec::with_capacity(2300);
    b.extend_from_slice(b"RCmb");
    b.extend_from_slice(&[0, 0, 0, 0]);
    b.push(m.kind);
    put_str(&mut b, m.sender.as_bytes());
    put_str(&mut b, m.ip.as_bytes());
    b.extend_from_slice(&m.port.to_be_bytes());
    b.extend_from_slice(&m.cport.to_be_bytes());
    b.extend_from_slice(&m.flags.to_be_bytes());
    put_str(&mut b, m.master.as_bytes());
    b.extend_from_slice(&m.current_epoch.to_be_bytes());
    b.extend_from_slice(&m.config_epoch.to_be_bytes());
    b.extend_from_slice(&m.offset.to_be_bytes());
    b.push(m.mflags);
    for w in &m.slots {
        b.extend_from_slice(&w.to_be_bytes());
    }
    b.extend_from_slice(&(m.gossip.len() as u16).to_be_bytes());
    for (id, ip, port, cport, flags, ps, pr) in &m.gossip {
        put_str(&mut b, id.as_bytes());
        put_str(&mut b, ip.as_bytes());
        b.extend_from_slice(&port.to_be_bytes());
        b.extend_from_slice(&cport.to_be_bytes());
        b.extend_from_slice(&flags.to_be_bytes());
        b.extend_from_slice(&ps.to_be_bytes());
        b.extend_from_slice(&pr.to_be_bytes());
    }
    b.extend_from_slice(&(m.payload.len() as u16).to_be_bytes());
    for p in &m.payload {
        put_str(&mut b, p);
    }
    let len = b.len() as u32;
    b[4..8].copy_from_slice(&len.to_be_bytes());
    b
}

struct Rd<'a> {
    b: &'a [u8],
    p: usize,
}
impl Rd<'_> {
    fn n(&mut self, k: usize) -> Option<&[u8]> {
        let s = self.b.get(self.p..self.p + k)?;
        self.p += k;
        Some(s)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.n(2)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.n(8)?.try_into().ok()?))
    }
    fn str(&mut self) -> Option<Vec<u8>> {
        let l = u32::from_be_bytes(self.n(4)?.try_into().ok()?) as usize;
        Some(self.n(l)?.to_vec())
    }
    fn s(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(&self.str()?).into_owned())
    }
}

fn decode_msg(b: &[u8]) -> Option<Msg> {
    let mut r = Rd { b, p: 8 };
    let kind = r.n(1)?[0];
    let sender = r.s()?;
    let ip = r.s()?;
    let port = r.u16()?;
    let cport = r.u16()?;
    let flags = r.u16()?;
    let master = r.s()?;
    let current_epoch = r.u64()?;
    let config_epoch = r.u64()?;
    let offset = r.u64()? as i64;
    let mflags = r.n(1)?[0];
    let mut slots = Vec::with_capacity(SLOTS / 64);
    for _ in 0..SLOTS / 64 {
        slots.push(r.u64()?);
    }
    let ng = r.u16()?;
    let mut gossip = Vec::new();
    for _ in 0..ng {
        gossip.push((r.s()?, r.s()?, r.u16()?, r.u16()?, r.u16()?, r.u64()? as i64, r.u64()? as i64));
    }
    let np = r.u16()?;
    let mut payload = Vec::new();
    for _ in 0..np {
        payload.push(r.str()?);
    }
    Some(Msg { kind, sender, ip, port, cport, flags, master, current_epoch, config_epoch, offset, mflags, slots, gossip, payload })
}

fn build_msg(s: &Store, kind: u8, payload: Vec<Vec<u8>>) -> Vec<u8> {
    let c = s.cluster.as_ref().unwrap();
    let me = c.me();
    // The slots a replica advertises are its master's.
    let (slots, cfg_epoch) = match (me.flags & NF::SLAVE != 0, me.master.as_ref().and_then(|m| c.nodes.get(m))) {
        (true, Some(m)) => (m.slots.clone(), m.config_epoch),
        _ => (me.slots.clone(), me.config_epoch),
    };
    let mut gossip = Vec::new();
    if kind <= MSG_MEET {
        let mut candidates: Vec<&Node> = c.nodes.values().filter(|n| n.flags & (NF::MYSELF | NF::HANDSHAKE | NF::NOADDR) == 0).collect();
        let wanted = (c.nodes.len() / 10).max(3).min(candidates.len());
        // Always include nodes in PFAIL so failure reports spread.
        candidates.sort_by_key(|n| (n.flags & NF::PFAIL == 0, crate::util::rand_u64()));
        for n in candidates.into_iter().take(wanted) {
            gossip.push((n.id.clone(), n.ip.clone(), n.port, n.cport, n.flags, n.ping_sent / 1000, n.pong_recv / 1000));
        }
    }
    let offset = if s.repl.is_replica() { s.repl.master_offset } else { s.repl.master_offset };
    let mflags = if c.mf.as_ref().map(|m| m.replica.is_some()).unwrap_or(false) { 1 } else { 0 };
    encode_msg(&Msg {
        kind,
        sender: me.id.clone(),
        ip: me.ip.clone(),
        port: me.port,
        cport: me.cport,
        flags: me.flags & !(NF::MYSELF | NF::PFAIL | NF::FAIL),
        master: me.master.clone().unwrap_or_default(),
        current_epoch: c.current_epoch,
        config_epoch: cfg_epoch,
        offset,
        mflags,
        slots,
        gossip,
        payload,
    })
}

fn send_to(s: &mut Store, id: &str, kind: u8, payload: Vec<Vec<u8>>) {
    let msg = build_msg(s, kind, payload);
    let c = s.cluster.as_mut().unwrap();
    if let Some(n) = c.nodes.get_mut(id) {
        if let Some(l) = &n.link {
            if l.send(msg).is_ok() {
                c.stats.sent[kind as usize] += 1;
                if kind == MSG_PING || kind == MSG_MEET {
                    if n.ping_sent == 0 {
                        n.ping_sent = crate::util::now_ms();
                    }
                }
            }
        }
    }
}

fn broadcast(s: &mut Store, kind: u8, payload: Vec<Vec<u8>>) {
    let ids: Vec<String> = s.cluster.as_ref().unwrap().nodes.values().filter(|n| n.flags & (NF::MYSELF | NF::HANDSHAKE) == 0 && n.link.is_some()).map(|n| n.id.clone()).collect();
    for id in ids {
        send_to(s, &id, kind, payload.clone());
    }
}

pub fn broadcast_publish(s: &mut Store, ch: &Bytes, msg: &Bytes) {
    broadcast(s, MSG_PUBLISH, vec![ch.to_vec(), msg.to_vec()]);
}

pub fn broadcast_spublish(s: &mut Store, ch: &Bytes, msg: &Bytes) {
    // Shard channels go to nodes of the same shard only.
    let Some(c) = s.cluster.as_ref() else { return };
    let master = c.my_master_id();
    let ids: Vec<String> = c.nodes.values().filter(|n| n.flags & NF::MYSELF == 0 && n.link.is_some() && (n.id == master || n.master.as_deref() == Some(master.as_str()))).map(|n| n.id.clone()).collect();
    for id in ids {
        send_to(s, &id, MSG_PUBLISHSHARD, vec![ch.to_vec(), msg.to_vec()]);
    }
}

fn process_msg(s: &mut Store, shared: &Arc<Shared>, m: Msg, link_tx: &mpsc::UnboundedSender<Vec<u8>>, peer_ip: &str, inbound: bool) -> Option<String> {
    let now = crate::util::now_ms();
    let req_full = s.cfg.cluster_require_full_coverage;
    let node_timeout = s.cfg.cluster_node_timeout as i64;
    let c = s.cluster.as_mut()?;
    c.stats.recv[m.kind as usize % 10] += 1;
    if m.current_epoch > c.current_epoch {
        c.current_epoch = m.current_epoch;
        c.todo_save = true;
    }
    let ip = if m.ip.is_empty() { peer_ip.to_string() } else { m.ip.clone() };
    let known = c.nodes.contains_key(&m.sender);
    // Learn our own IP from the first MEET, like Redis does.
    if m.kind == MSG_MEET && c.me().ip.is_empty() {
        // handled by caller (needs local address); nothing here
    }
    if !known {
        if m.kind == MSG_MEET {
            if c.blacklist.get(&m.sender).map(|&t| t > now).unwrap_or(false) {
                return None;
            }
            let mut n = Node::new(m.sender.clone(), ip.clone(), m.port, m.cport, if m.flags & NF::SLAVE != 0 { NF::SLAVE } else { NF::MASTER });
            n.link = Some(link_tx.clone());
            n.inbound = inbound;
            n.link_created = now;
            c.nodes.insert(m.sender.clone(), n);
            c.todo_save = true;
        } else {
            // A PONG from a node we are handshaking with under a random id.
            let hs = c.nodes.values().find(|n| n.flags & NF::HANDSHAKE != 0 && n.ip == ip && n.port == m.port).map(|n| n.id.clone());
            match hs {
                Some(old) if m.kind == MSG_PONG => {
                    let mut n = c.nodes.remove(&old).unwrap();
                    n.id = m.sender.clone();
                    n.flags &= !(NF::HANDSHAKE | NF::MEET);
                    n.flags |= if m.flags & NF::SLAVE != 0 { NF::SLAVE } else { NF::MASTER };
                    n.cport = m.cport;
                    c.nodes.insert(m.sender.clone(), n);
                    c.todo_save = true;
                    eprintln!("Handshake with node {} completed.", m.sender);
                }
                _ => {
                    if m.kind == MSG_PING {
                        // Unknown sender pinging us: reply so it can finish its handshake.
                        let pong = build_msg(s, MSG_PONG, vec![]);
                        let _ = link_tx.send(pong);
                    }
                    return None;
                }
            }
        }
    }
    let c = s.cluster.as_mut()?;
    let myself = c.myself.clone();
    // Update the sender's view.
    let (sender_was_master, prev_master) = {
        let n = c.nodes.get_mut(&m.sender)?;
        if n.link.is_none() {
            n.link = Some(link_tx.clone());
            n.link_created = now;
            n.inbound = inbound;
        }
        n.data_recv = now;
        if m.kind == MSG_PONG {
            n.pong_recv = now;
            n.ping_sent = 0;
            if n.flags & NF::PFAIL != 0 {
                n.flags &= !NF::PFAIL;
            }
            if n.flags & NF::FAIL != 0 && (!n.is_master() || n.slot_count() == 0 || now - n.fail_time > node_timeout * 2) {
                n.flags &= !NF::FAIL;
                eprintln!("Clear FAIL state for node {}: is reachable again.", n.id);
                c.todo_save = true;
            }
        }
        if n.ip != ip || n.port != m.port {
            n.ip = ip.clone();
            n.port = m.port;
            n.cport = m.cport;
            c.todo_save = true;
        }
        n.repl_offset = m.offset;
        (n.is_master(), n.master.clone())
    };
    // Role changes.
    {
        let n = c.nodes.get_mut(&m.sender).unwrap();
        if m.flags & NF::SLAVE != 0 {
            let new_master = if m.master.is_empty() { None } else { Some(m.master.clone()) };
            if sender_was_master || prev_master != new_master {
                n.flags = (n.flags & !NF::MASTER) | NF::SLAVE;
                n.master = new_master;
                for s in 0..SLOTS {
                    n.set_slot(s, false);
                }
                c.todo_save = true;
            }
        } else if !sender_was_master {
            n.flags = (n.flags & !NF::SLAVE) | NF::MASTER;
            n.master = None;
            c.todo_save = true;
        }
    }
    // Slot ownership: accept claims with a higher (or equal for unowned) config epoch.
    if m.flags & NF::SLAVE == 0 && matches!(m.kind, MSG_PING | MSG_PONG | MSG_MEET | MSG_UPDATE) {
        c.nodes.get_mut(&m.sender).unwrap().config_epoch = m.config_epoch;
        let mut lost_my_slots = false;
        for slot in 0..SLOTS {
            if m.slots[slot / 64] & (1 << (slot % 64)) == 0 {
                continue;
            }
            match &c.slots[slot] {
                Some(o) if *o == m.sender => {}
                Some(o) if *o == myself => {
                    if c.importing.contains_key(&(slot as u16)) {
                        continue;
                    }
                    if m.config_epoch > c.me().config_epoch {
                        lost_my_slots = true;
                        c.assign(slot, &m.sender.clone());
                    }
                }
                Some(o) => {
                    let oe = c.nodes.get(o).map(|n| n.config_epoch).unwrap_or(0);
                    if m.config_epoch > oe {
                        c.assign(slot, &m.sender.clone());
                        c.todo_save = true;
                    }
                }
                None => {
                    if !c.importing.contains_key(&(slot as u16)) {
                        c.assign(slot, &m.sender.clone());
                        c.todo_save = true;
                    }
                }
            }
        }
        // Slots the sender no longer claims (but we think it owns) are released
        // only by a newer claim, matching Redis' epoch-based resolution.
        if lost_my_slots {
            c.todo_save = true;
            if c.me().slot_count() == 0 && c.me().is_master() {
                // We lost all our slots: become a replica of the new owner.
                let target = m.sender.clone();
                if let Some(t) = c.nodes.get(&target) {
                    let (ip, port) = (t.ip.clone(), t.port);
                    let me = c.me_mut();
                    me.flags = (me.flags & !NF::MASTER) | NF::SLAVE;
                    me.master = Some(target);
                    crate::repl::set_master(s, shared.clone(), ip, port);
                }
            }
        }
    }
    let c = s.cluster.as_mut()?;
    // My master failed over to another node? Follow it.
    if m.flags & NF::SLAVE == 0 && m.kind != MSG_MEET {
        let me = c.me();
        if let Some(my_master) = me.master.clone() {
            let my_master_slots = c.nodes.get(&my_master).map(|n| n.slot_count()).unwrap_or(0);
            if my_master_slots == 0 && my_master != m.sender && m.slots.iter().any(|w| *w != 0) && c.nodes.get(&my_master).map(|n| n.flags & NF::SLAVE != 0).unwrap_or(false) {
                let target = m.sender.clone();
                let (ip, port) = (ip.clone(), m.port);
                c.me_mut().master = Some(target);
                c.todo_save = true;
                crate::repl::set_master(s, shared.clone(), ip, port);
            }
        }
    }
    let c = s.cluster.as_mut()?;
    // Gossip.
    if matches!(m.kind, MSG_PING | MSG_PONG | MSG_MEET) {
        let sender_is_master = c.nodes.get(&m.sender).map(|n| n.is_master()).unwrap_or(false);
        for (gid, gip, gport, gcport, gflags, _, _) in &m.gossip {
            if *gid == myself {
                continue;
            }
            match c.nodes.get_mut(gid) {
                Some(n) => {
                    if sender_is_master {
                        if gflags & (NF::PFAIL | NF::FAIL) != 0 {
                            n.fail_reports.insert(m.sender.clone(), now);
                        } else {
                            n.fail_reports.remove(&m.sender);
                        }
                    }
                    if n.flags & NF::NOADDR != 0 && !gip.is_empty() {
                        n.ip = gip.clone();
                        n.port = *gport;
                        n.cport = *gcport;
                        n.flags &= !NF::NOADDR;
                    }
                }
                None => {
                    if gflags & (NF::NOADDR | NF::HANDSHAKE) == 0 && !c.blacklist.get(gid).map(|&t| t > now).unwrap_or(false) && !c.nodes.values().any(|n| n.ip == *gip && n.port == *gport) {
                        let hid = crate::util::random_hex(40);
                        let n = Node::new(hid.clone(), gip.clone(), *gport, *gcport, NF::HANDSHAKE | NF::MEET);
                        c.nodes.insert(hid, n);
                    }
                }
            }
        }
    }
    // Message specific handling.
    let mut reply: Option<(u8, Vec<Vec<u8>>)> = None;
    match m.kind {
        MSG_PING | MSG_MEET => reply = Some((MSG_PONG, vec![])),
        MSG_FAIL => {
            if let Some(id) = m.payload.first().map(|p| String::from_utf8_lossy(p).into_owned()) {
                if let Some(n) = c.nodes.get_mut(&id) {
                    if n.flags & NF::MYSELF == 0 && n.flags & NF::FAIL == 0 {
                        n.flags = (n.flags & !NF::PFAIL) | NF::FAIL;
                        n.fail_time = now;
                        eprintln!("FAIL message received from {} about {}", m.sender, id);
                        c.todo_save = true;
                    }
                }
            }
        }
        MSG_PUBLISH | MSG_PUBLISHSHARD => {
            if m.payload.len() == 2 {
                let ch = Bytes::from(m.payload[0].clone());
                let msg = Bytes::from(m.payload[1].clone());
                if m.kind == MSG_PUBLISH {
                    crate::pubsub::publish(s, &ch, &msg);
                } else {
                    crate::pubsub::spublish(s, &ch, &msg);
                }
            }
        }
        MSG_AUTH_REQ => {
            // Vote for a replica whose master is failed (or manual failover).
            let my_master = c.me().is_master() && c.me().slot_count() > 0;
            let req_epoch = m.current_epoch;
            let forced = m.mflags & 1 != 0;
            let replica = c.nodes.get(&m.sender).cloned();
            if my_master && req_epoch >= c.current_epoch && c.last_vote_epoch < req_epoch {
                if let Some(r) = replica {
                    let master_failed = r.master.as_ref().and_then(|mm| c.nodes.get(mm)).map(|mm| mm.flags & NF::FAIL != 0).unwrap_or(false);
                    // The requested slots must not be claimed by a newer epoch.
                    let mut epochs_ok = true;
                    for slot in 0..SLOTS {
                        if m.slots[slot / 64] & (1 << (slot % 64)) != 0 {
                            if let Some(o) = c.owner(slot as u16) {
                                if o.config_epoch > m.config_epoch {
                                    epochs_ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    if (master_failed || forced) && epochs_ok {
                        c.last_vote_epoch = req_epoch;
                        c.todo_save = true;
                        eprintln!("Failover auth granted to {} for epoch {}", m.sender, req_epoch);
                        reply = Some((MSG_AUTH_ACK, vec![]));
                    }
                }
            }
        }
        MSG_AUTH_ACK => {
            let me = c.me();
            if me.flags & NF::SLAVE != 0 && m.current_epoch >= c.election_epoch && c.election_epoch > 0 {
                c.election_votes += 1;
            }
        }
        MSG_MFSTART => {
            // A replica of ours requests a manual failover: pause writes.
            c.mf = Some(ManualFailover { deadline: now + 5000, force: false, master_offset: None, replica: Some(m.sender.clone()) });
            s.pause = Some((now + 10000, true));
            eprintln!("Manual failover requested by replica {}.", m.sender);
        }
        _ => {}
    }
    let _ = req_full;
    if let Some((k, p)) = reply {
        let msg = build_msg(s, k, p);
        let _ = link_tx.send(msg);
        if let Some(c) = s.cluster.as_mut() {
            c.stats.sent[k as usize] += 1;
        }
    }
    Some(m.sender)
}

// ---------------- bus networking ----------------

async fn read_frames(rd: &mut (impl AsyncReadExt + Unpin), buf: &mut Vec<u8>) -> Option<Vec<Vec<u8>>> {
    let mut tmp = [0u8; 16384];
    let n = rd.read(&mut tmp).await.ok()?;
    if n == 0 {
        return None;
    }
    buf.extend_from_slice(&tmp[..n]);
    let mut out = Vec::new();
    while buf.len() >= 8 {
        if &buf[..4] != b"RCmb" {
            return None;
        }
        let len = u32::from_be_bytes(buf[4..8].try_into().unwrap()) as usize;
        if len > 1 << 24 {
            return None;
        }
        if buf.len() < len {
            break;
        }
        out.push(buf.drain(..len).collect());
    }
    Some(out)
}

async fn run_link(shared: Arc<Shared>, stream: tokio::net::TcpStream, inbound: bool, first: Option<Vec<u8>>, node_id: Option<String>, tx: mpsc::UnboundedSender<Vec<u8>>, mut rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let peer_ip = stream.peer_addr().map(|a| a.ip().to_string()).unwrap_or_default();
    let local_ip = stream.local_addr().map(|a| a.ip().to_string()).unwrap_or_default();
    let (mut rd, mut wr) = stream.into_split();
    if let Some(f) = first {
        if wr.write_all(&f).await.is_err() {
            return;
        }
    }
    let mut buf = Vec::new();
    let mut sender_id = node_id;
    loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(m) = m else { break };
                if wr.write_all(&m).await.is_err() { break; }
            }
            frames = read_frames(&mut rd, &mut buf) => {
                let Some(frames) = frames else { break };
                let mut s = shared.store.lock();
                for f in frames {
                    let Some(msg) = decode_msg(&f) else { continue };
                    if msg.kind == MSG_MEET {
                        if let Some(c) = s.cluster.as_mut() {
                            if c.me().ip.is_empty() {
                                c.me_mut().ip = local_ip.clone();
                                c.todo_save = true;
                            }
                        }
                    }
                    if let Some(id) = process_msg(&mut s, &shared, msg, &tx, &peer_ip, inbound) {
                        sender_id = Some(id);
                    }
                }
            }
        }
    }
    // Link dropped: forget the sender handle so cron reconnects.
    let mut s = shared.store.lock();
    if let (Some(c), Some(id)) = (s.cluster.as_mut(), sender_id) {
        if let Some(n) = c.nodes.get_mut(&id) {
            if n.link.as_ref().map(|l| l.same_channel(&tx)).unwrap_or(false) {
                n.link = None;
            }
        }
    }
}

/// Start the cluster bus listener.
pub async fn listen(shared: Arc<Shared>, bind: String, port: u16) {
    let listener = match tokio::net::TcpListener::bind((bind.as_str(), port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Could not bind the cluster bus port {}: {}", port, e);
            std::process::exit(1);
        }
    };
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let _ = stream.set_nodelay(true);
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_link(shared.clone(), stream, true, None, None, tx, rx));
    }
}

/// Connect a link to `id` (outbound), sending MEET or PING first.
fn connect_node(s: &mut Store, shared: &Arc<Shared>, id: &str) {
    let c = s.cluster.as_mut().unwrap();
    let Some(n) = c.nodes.get_mut(id) else { return };
    if n.link.is_some() || n.ip.is_empty() {
        return;
    }
    let now = crate::util::now_ms();
    if now - n.link_created < 1000 {
        return;
    }
    n.link_created = now;
    let (ip, cport) = (n.ip.clone(), n.cport);
    let kind = if n.flags & NF::MEET != 0 || n.flags & NF::HANDSHAKE != 0 { MSG_MEET } else { MSG_PING };
    let (tx, rx) = mpsc::unbounded_channel();
    n.link = Some(tx.clone());
    if n.ping_sent == 0 {
        n.ping_sent = now;
    }
    let first = build_msg(s, kind, vec![]);
    s.cluster.as_mut().unwrap().stats.sent[kind as usize] += 1;
    let shared2 = shared.clone();
    let id = id.to_string();
    tokio::spawn(async move {
        let fut = tokio::net::TcpStream::connect((ip.as_str(), cport));
        match tokio::time::timeout(std::time::Duration::from_millis(2000), fut).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                run_link(shared2, stream, false, Some(first), Some(id), tx, rx).await;
            }
            _ => {
                let mut s = shared2.store.lock();
                if let Some(c) = s.cluster.as_mut() {
                    if let Some(n) = c.nodes.get_mut(&id) {
                        if n.link.as_ref().map(|l| l.same_channel(&tx)).unwrap_or(false) {
                            n.link = None;
                        }
                    }
                }
            }
        }
    });
}

/// Periodic cluster work (called from the server cron at `hz`).
pub fn cron(s: &mut Store, shared: &Arc<Shared>, tick: u64) {
    if s.cluster.is_none() {
        return;
    }
    let now = crate::util::now_ms();
    let node_timeout = s.cfg.cluster_node_timeout.max(100) as i64;
    let req_full = s.cfg.cluster_require_full_coverage;
    let hz = s.cfg.hz.max(1);
    // Handshake timeouts and connections.
    let ids: Vec<String> = {
        let c = s.cluster.as_mut().unwrap();
        let stale: Vec<String> = c.nodes.values().filter(|n| n.flags & NF::HANDSHAKE != 0 && now - n.created > node_timeout.max(1000)).map(|n| n.id.clone()).collect();
        for id in stale {
            c.nodes.remove(&id);
        }
        c.blacklist.retain(|_, t| *t > now);
        c.nodes.values().filter(|n| n.flags & NF::MYSELF == 0).map(|n| n.id.clone()).collect()
    };
    for id in &ids {
        connect_node(s, shared, id);
    }
    // Pings: every second ping a few random nodes, plus any node not pinged for half the timeout.
    if tick % hz == 0 {
        let c = s.cluster.as_ref().unwrap();
        let mut candidates: Vec<(String, i64)> = c.nodes.values().filter(|n| n.flags & (NF::MYSELF | NF::HANDSHAKE) == 0 && n.link.is_some() && n.ping_sent == 0).map(|n| (n.id.clone(), n.pong_recv)).collect();
        candidates.sort_by_key(|(_, p)| *p);
        let pick: Vec<String> = candidates.into_iter().take(2).map(|(id, _)| id).collect();
        for id in pick {
            send_to(s, &id, MSG_PING, vec![]);
        }
    }
    let late: Vec<String> = {
        let c = s.cluster.as_ref().unwrap();
        c.nodes.values().filter(|n| n.flags & (NF::MYSELF | NF::HANDSHAKE) == 0 && n.link.is_some() && n.ping_sent == 0 && now - n.pong_recv > node_timeout / 2).map(|n| n.id.clone()).collect()
    };
    for id in late {
        send_to(s, &id, MSG_PING, vec![]);
    }
    // Failure detection.
    let mut newly_failed = Vec::new();
    {
        let c = s.cluster.as_mut().unwrap();
        let masters = c.nodes.values().filter(|n| n.is_master() && n.slot_count() > 0).count();
        let quorum = masters / 2 + 1;
        let i_am_master = c.me().is_master();
        let ids: Vec<String> = c.nodes.keys().cloned().collect();
        for id in ids {
            let n = c.nodes.get_mut(&id).unwrap();
            if n.flags & (NF::MYSELF | NF::HANDSHAKE) != 0 {
                continue;
            }
            // Drop links that are not answering.
            if n.ping_sent != 0 && now - n.ping_sent > node_timeout / 2 && n.link.is_some() && now - n.data_recv > node_timeout / 2 {
                n.link = None;
            }
            if n.ping_sent != 0 && now - n.ping_sent > node_timeout && n.flags & (NF::PFAIL | NF::FAIL) == 0 {
                n.flags |= NF::PFAIL;
                eprintln!("*** NODE {} possibly failing", n.id);
            }
            n.fail_reports.retain(|_, t| now - *t <= node_timeout * 2);
            if n.flags & NF::PFAIL != 0 && n.flags & NF::FAIL == 0 {
                let reports = n.fail_reports.len() + i_am_master as usize;
                if reports >= quorum {
                    n.flags = (n.flags & !NF::PFAIL) | NF::FAIL;
                    n.fail_time = now;
                    eprintln!("Marking node {} as failing (quorum reached).", n.id);
                    newly_failed.push(n.id.clone());
                }
            }
        }
    }
    for id in newly_failed {
        broadcast(s, MSG_FAIL, vec![id.into_bytes()]);
        s.cluster.as_mut().unwrap().todo_save = true;
    }
    replica_failover_cron(s, shared, node_timeout);
    let c = s.cluster.as_mut().unwrap();
    c.update_state(req_full);
    if c.todo_save {
        c.todo_save = false;
        if let Err(e) = c.save_config() {
            eprintln!("Error saving cluster config: {}", e);
        }
    }
}

fn replica_failover_cron(s: &mut Store, shared: &Arc<Shared>, node_timeout: i64) {
    let now = crate::util::now_ms();
    let c = s.cluster.as_mut().unwrap();
    // Master side of a manual failover: expire the pause.
    if let Some(mf) = &c.mf {
        if mf.replica.is_some() && now > mf.deadline {
            c.mf = None;
            s.pause = None;
            crate::blocking::unpause(s);
            return;
        }
    }
    let c = s.cluster.as_mut().unwrap();
    let me = c.me().clone();
    if me.flags & NF::SLAVE == 0 || me.flags & NF::NOFAILOVER != 0 {
        return;
    }
    let Some(master) = me.master.as_ref().and_then(|m| c.nodes.get(m)).cloned() else { return };
    let manual = c.mf.as_ref().map(|m| m.replica.is_none()).unwrap_or(false);
    if manual {
        let mf = c.mf.as_ref().unwrap();
        if now > mf.deadline {
            eprintln!("Manual failover timed out.");
            c.mf = None;
            return;
        }
        // Wait until we processed the master's replication offset.
        if !mf.force && s.repl.master_offset < master.repl_offset {
            return;
        }
    } else if master.flags & NF::FAIL == 0 || master.slot_count() == 0 {
        c.election_epoch = 0;
        return;
    }
    let masters = c.nodes.values().filter(|n| n.is_master() && n.slot_count() > 0).count();
    let quorum = masters / 2 + 1;
    if c.election_epoch == 0 || now - c.election_start > node_timeout * 2 {
        // Start an election after a short rank-based delay.
        let delay = if manual { 0 } else { 500 + (crate::util::rand_u64() % 500) as i64 };
        if c.election_start != 0 && now - c.election_start < delay && c.election_epoch == 0 {
            return;
        }
        c.current_epoch += 1;
        c.election_epoch = c.current_epoch;
        c.election_votes = 0;
        c.election_start = now;
        c.todo_save = true;
        eprintln!("Starting a failover election for epoch {}.", c.election_epoch);
        // AUTH_REQ carries the slots we'd claim (the master's) with its epoch.
        broadcast_auth_req(s, &master, manual);
        return;
    }
    if c.election_votes >= quorum {
        eprintln!("Failover election won: I'm the new master.");
        let epoch = c.election_epoch;
        let old = master.id.clone();
        let my_id = c.myself.clone();
        for slot in 0..SLOTS {
            if c.slots[slot].as_deref() == Some(old.as_str()) {
                c.assign(slot, &my_id);
            }
        }
        {
            let me = c.me_mut();
            me.flags = (me.flags & !NF::SLAVE) | NF::MASTER;
            me.master = None;
            me.config_epoch = epoch;
        }
        if let Some(o) = c.nodes.get_mut(&old) {
            o.flags = (o.flags & !NF::MASTER) | NF::SLAVE;
            o.master = Some(my_id.clone());
        }
        c.election_epoch = 0;
        c.mf = None;
        c.todo_save = true;
        crate::repl::unset_master(s);
        broadcast(s, MSG_PONG, vec![]);
        let _ = shared;
    }
}

fn broadcast_auth_req(s: &mut Store, master: &Node, manual: bool) {
    let c = s.cluster.as_ref().unwrap();
    let ids: Vec<String> = c.nodes.values().filter(|n| n.is_master() && n.flags & NF::MYSELF == 0 && n.link.is_some()).map(|n| n.id.clone()).collect();
    for id in ids {
        let mut msg = build_msg(s, MSG_AUTH_REQ, vec![]);
        // Patch slots/epoch/mflags in the encoded header: rebuild explicitly instead.
        if let Some(mut m) = decode_msg(&msg) {
            m.slots = master.slots.clone();
            m.config_epoch = master.config_epoch;
            m.mflags = manual as u8;
            msg = encode_msg(&m);
        }
        let c = s.cluster.as_mut().unwrap();
        if let Some(n) = c.nodes.get(&id) {
            if let Some(l) = &n.link {
                let _ = l.send(msg);
                c.stats.sent[MSG_AUTH_REQ as usize] += 1;
            }
        }
    }
}

// ---------------- helpers used by CLUSTER commands ----------------

pub fn meet(s: &mut Store, ip: String, port: u16, cport: u16) {
    let c = s.cluster.as_mut().unwrap();
    if c.nodes.values().any(|n| n.ip == ip && n.port == port && n.flags & NF::HANDSHAKE != 0) {
        return;
    }
    let id = crate::util::random_hex(40);
    c.nodes.insert(id.clone(), Node::new(id, ip, port, cport, NF::HANDSHAKE | NF::MEET));
}

pub fn count_keys_in_slot(s: &Store, slot: u16) -> usize {
    s.dbs[0].dict.keys().filter(|k| key_slot(k) == slot).count()
}

pub fn keys_in_slot(s: &Store, slot: u16, count: usize) -> Vec<Bytes> {
    s.dbs[0].dict.keys().filter(|k| key_slot(k) == slot).take(count).cloned().collect()
}

pub fn add_slot(c: &mut ClusterState, slot: usize) {
    let me = c.myself.clone();
    c.assign(slot, &me);
    c.importing.remove(&(slot as u16));
}

pub fn del_slot(c: &mut ClusterState, slot: usize) {
    c.unassign(slot);
}

pub fn set_slot_node(c: &mut ClusterState, slot: usize, id: &str) {
    c.assign(slot, id);
}

pub fn msg_stats(c: &ClusterState) -> Vec<(String, u64)> {
    let mut v = Vec::new();
    for (i, n) in MSG_NAMES.iter().enumerate() {
        if c.stats.sent[i] > 0 {
            v.push((format!("cluster_stats_messages_{}_sent", n), c.stats.sent[i]));
        }
    }
    v.push(("cluster_stats_messages_sent".into(), c.stats.sent.iter().sum()));
    for (i, n) in MSG_NAMES.iter().enumerate() {
        if c.stats.recv[i] > 0 {
            v.push((format!("cluster_stats_messages_{}_received", n), c.stats.recv[i]));
        }
    }
    v.push(("cluster_stats_messages_received".into(), c.stats.recv.iter().sum()));
    v
}

pub fn request_manual_failover(s: &mut Store, force: bool) {
    let now = crate::util::now_ms();
    let c = s.cluster.as_mut().unwrap();
    c.mf = Some(ManualFailover { deadline: now + 5000, force, master_offset: None, replica: None });
    c.election_epoch = 0;
    c.election_start = 0;
    if !force {
        if let Some(m) = c.me().master.clone() {
            send_to(s, &m, MSG_MFSTART, vec![]);
        }
    }
}

/// CLUSTER FAILOVER TAKEOVER: claim the master's slots without agreement.
pub fn takeover(s: &mut Store) {
    let c = s.cluster.as_mut().unwrap();
    let Some(old) = c.me().master.clone() else { return };
    c.current_epoch += 1;
    let epoch = c.current_epoch;
    let my_id = c.myself.clone();
    for slot in 0..SLOTS {
        if c.slots[slot].as_deref() == Some(old.as_str()) {
            c.assign(slot, &my_id);
        }
    }
    {
        let me = c.me_mut();
        me.flags = (me.flags & !NF::SLAVE) | NF::MASTER;
        me.master = None;
        me.config_epoch = epoch;
    }
    c.todo_save = true;
    crate::repl::unset_master(s);
    broadcast(s, MSG_PONG, vec![]);
}

pub fn forget(c: &mut ClusterState, id: &str) {
    c.nodes.remove(id);
    for slot in 0..SLOTS {
        if c.slots[slot].as_deref() == Some(id) {
            c.slots[slot] = None;
        }
    }
    c.blacklist.insert(id.to_string(), crate::util::now_ms() + 60_000);
    c.todo_save = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slots() {
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"{user1000}.followers"));
        assert_eq!(key_slot(b"foo{}{bar}"), key_slot(b"foo{}{bar}"));
        assert_eq!(key_slot(b"123456789"), 0x31c3 & 0x3fff);
    }
    #[test]
    fn msg_roundtrip() {
        let m = Msg {
            kind: MSG_PING,
            sender: "a".repeat(40),
            ip: "127.0.0.1".into(),
            port: 7000,
            cport: 17000,
            flags: NF::MASTER,
            master: String::new(),
            current_epoch: 3,
            config_epoch: 2,
            offset: 99,
            mflags: 0,
            slots: vec![1; SLOTS / 64],
            gossip: vec![("b".repeat(40), "127.0.0.1".into(), 7001, 17001, NF::MASTER, 0, 0)],
            payload: vec![b"x".to_vec()],
        };
        let e = encode_msg(&m);
        let d = decode_msg(&e).unwrap();
        assert_eq!(d.sender, m.sender);
        assert_eq!(d.offset, 99);
        assert_eq!(d.gossip.len(), 1);
        assert_eq!(d.payload[0], b"x");
    }
}
