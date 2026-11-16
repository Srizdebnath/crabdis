//! Master/replica replication compatible with the Redis protocol: replication
//! IDs and offsets, a circular backlog for partial resync (PSYNC), full resync
//! with an RDB transfer, the command stream, REPLCONF ACK/GETACK, WAIT/WAITAOF,
//! and the replica side link (handshake, RDB load, stream apply).

use crate::resp::Frame;
use crate::server::{Shared, Store, cflags};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

pub enum SyncPlan {
    /// Full resync: snapshot to serialize, then stream.
    Full(crate::rdb::Snapshot),
    /// Partial resync: bytes from the backlog, then stream.
    Partial(Vec<u8>),
}

pub struct ReplicaLink {
    pub tx: mpsc::UnboundedSender<Bytes>,
    pub rx: Option<mpsc::UnboundedReceiver<Bytes>>,
    pub plan: Option<SyncPlan>,
    pub online: bool,
    pub ack_aof_off: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkState {
    None,
    Connect,
    Connecting,
    Sync,
    Connected,
}

impl LinkState {
    pub fn name(&self) -> &'static str {
        match self {
            LinkState::None => "none",
            LinkState::Connect => "connect",
            LinkState::Connecting => "connecting",
            LinkState::Sync => "sync",
            LinkState::Connected => "connected",
        }
    }
}

pub struct ReplState {
    pub replid: String,
    pub replid2: String,
    pub second_offset: i64,
    pub master_offset: i64,
    backlog: VecDeque<u8>,
    backlog_active: bool,
    /// Offset of the first byte held in the backlog.
    backlog_first: i64,
    pub pending: Vec<(usize, Vec<Bytes>)>,
    pub replicas: HashMap<u64, ReplicaLink>,
    seldb: Option<usize>,
    // replica side
    pub master: Option<(String, u16)>,
    pub state: LinkState,
    pub link_epoch: u64,
    pub master_cid: Option<u64>,
    pub link_down_since: i64,
    pub last_io: i64,
    pub applying_master: bool,
    pub read_offset: i64,
    pub sync_total: i64,
    pub sync_read: i64,
    pub sync_start: i64,
    pub failover_state: &'static str,
    pub failover: Option<Failover>,
    pub aof_fsynced_offset: i64,
}

pub struct Failover {
    pub target: Option<(String, u16)>,
    pub force: bool,
    pub deadline: Option<i64>,
    pub offset: i64,
    pub started: i64,
}

impl ReplState {
    pub fn new(_run_id: &str) -> Self {
        ReplState {
            replid: crate::util::random_hex(40),
            replid2: "0".repeat(40),
            second_offset: -1,
            master_offset: 0,
            backlog: VecDeque::new(),
            backlog_active: false,
            backlog_first: 0,
            pending: Vec::new(),
            replicas: HashMap::new(),
            seldb: None,
            master: None,
            state: LinkState::None,
            link_epoch: 0,
            master_cid: None,
            link_down_since: 0,
            last_io: 0,
            applying_master: false,
            read_offset: 0,
            sync_total: -1,
            sync_read: 0,
            sync_start: 0,
            failover_state: "no-failover",
            failover: None,
            aof_fsynced_offset: 0,
        }
    }

    pub fn is_replica(&self) -> bool {
        self.master.is_some()
    }
    pub fn link_up(&self) -> bool {
        self.state == LinkState::Connected
    }
    pub fn has_backlog(&self) -> bool {
        self.backlog_active
    }
    pub fn backlog_len(&self) -> usize {
        self.backlog.len()
    }
    pub fn replid_offset(&self) -> (String, i64) {
        (self.replid.clone(), self.master_offset)
    }
    pub fn change_replid(&mut self) {
        self.replid = crate::util::random_hex(40);
    }
    /// Keep the current history reachable under replid2 (PSYNC after promotion).
    pub fn shift_replid(&mut self) {
        self.replid2 = self.replid.clone();
        self.second_offset = self.master_offset + 1;
        self.change_replid();
    }
    pub fn master_link_killed(&mut self) {
        if self.state == LinkState::Connected {
            self.state = LinkState::Connect;
            self.link_down_since = crate::util::now_ms();
        }
    }
    fn create_backlog(&mut self) {
        if !self.backlog_active {
            self.backlog_active = true;
            self.backlog.clear();
            self.backlog_first = self.master_offset + 1;
        }
    }

    fn append_backlog(&mut self, data: &[u8], cap: usize) {
        self.master_offset += data.len() as i64;
        if !self.backlog_active {
            return;
        }
        self.backlog.extend(data);
        while self.backlog.len() > cap.max(16 * 1024) {
            let excess = self.backlog.len() - cap.max(16 * 1024);
            self.backlog.drain(..excess);
            self.backlog_first += excess as i64;
        }
        if self.backlog.is_empty() {
            self.backlog_first = self.master_offset + 1;
        }
    }

    /// Bytes from `offset` (1-based, Redis semantics: next byte the replica wants).
    fn backlog_from(&self, offset: i64) -> Option<Vec<u8>> {
        if !self.backlog_active {
            return None;
        }
        let end = self.master_offset + 1;
        if offset < self.backlog_first || offset > end {
            return None;
        }
        let skip = (offset - self.backlog_first) as usize;
        Some(self.backlog.iter().skip(skip).copied().collect())
    }
}

/// Number of replicas with lag <= max_lag seconds.
pub fn count_good(s: &Store, max_lag: u64) -> usize {
    let now = crate::util::now_ms();
    s.repl
        .replicas
        .iter()
        .filter(|(id, l)| l.online && s.clients.get(id).map(|c| (now - c.repl_ack_time) / 1000 <= max_lag as i64).unwrap_or(false))
        .count()
}

/// min-replicas-to-write check.
pub fn good_replicas_ok(s: &Store) -> bool {
    s.repl.is_replica() || s.cfg.min_replicas_to_write == 0 || count_good(s, s.cfg.min_replicas_max_lag) >= s.cfg.min_replicas_to_write
}

fn encode(argv: &[Bytes]) -> Vec<u8> {
    crate::aof::encode_command(argv)
}

fn broadcast(s: &mut Store, data: &[u8]) {
    let cap = s.cfg.repl_backlog_size;
    s.repl.append_backlog(data, cap);
    if s.repl.replicas.is_empty() {
        return;
    }
    let b = Bytes::copy_from_slice(data);
    for l in s.repl.replicas.values() {
        let _ = l.tx.send(b.clone());
    }
}

/// Propagate a command to replicas and the backlog.
pub fn feed(s: &mut Store, db: usize, argv: &[Bytes]) {
    if s.repl.applying_master || (!s.repl.backlog_active && s.repl.replicas.is_empty()) {
        return;
    }
    if s.repl.is_replica() {
        // Replicas only proxy their master's stream (see apply_stream).
        return;
    }
    let mut data = Vec::new();
    if s.repl.seldb != Some(db) {
        data.extend_from_slice(&encode(&[Bytes::from_static(b"SELECT"), Bytes::from(db.to_string())]));
        s.repl.seldb = Some(db);
    }
    data.extend_from_slice(&encode(argv));
    broadcast(s, &data);
}

pub fn on_client_free(s: &mut Store, cid: u64) {
    if s.repl.replicas.remove(&cid).is_some() {
        eprintln!("Connection with replica client id #{} lost.", cid);
    }
    if s.repl.master_cid == Some(cid) {
        s.repl.master_cid = None;
    }
}

// ---------------- master side ----------------

/// PSYNC/SYNC: prepare the replica's stream. Returns the reply header to send.
pub fn start_sync(s: &mut Store, cid: u64, replid: &str, offset: i64, legacy: bool) -> Frame {
    let (tx, rx) = mpsc::unbounded_channel();
    s.repl.create_backlog();
    // Partial resync?
    if !legacy {
        let id_ok = replid == s.repl.replid || (replid == s.repl.replid2 && offset <= s.repl.second_offset);
        if id_ok {
            if let Some(bytes) = s.repl.backlog_from(offset) {
                s.repl.replicas.insert(cid, ReplicaLink { tx, rx: Some(rx), plan: Some(SyncPlan::Partial(bytes)), online: true, ack_aof_off: 0 });
                mark_replica(s, cid);
                s.stats.sync_partial_ok += 1;
                eprintln!("Partial resynchronization request from replica #{} accepted. Sending {} bytes of backlog starting from offset {}.", cid, s.repl.master_offset + 1 - offset, offset);
                return Frame::Raw(Bytes::from(format!("+CONTINUE {}\r\n", s.repl.replid)));
            }
        }
        if replid != "?" {
            s.stats.sync_partial_err += 1;
        }
    }
    s.stats.sync_full += 1;
    // Full resync: snapshot now; the stream from this offset goes to the channel.
    let snap = crate::rdb::snapshot(s);
    s.repl.seldb = None;
    s.repl.replicas.insert(cid, ReplicaLink { tx, rx: Some(rx), plan: Some(SyncPlan::Full(snap)), online: false, ack_aof_off: 0 });
    mark_replica(s, cid);
    s.stats.total_forks += 1;
    if legacy {
        Frame::NoReply
    } else {
        Frame::Raw(Bytes::from(format!("+FULLRESYNC {} {}\r\n", s.repl.replid, s.repl.master_offset)))
    }
}

fn mark_replica(s: &mut Store, cid: u64) {
    if let Some(c) = s.clients.get_mut(&cid) {
        c.flags |= cflags::REPLICA;
        c.repl_ack_time = crate::util::now_ms();
        c.repl_state = "wait_bgsave";
    }
}

/// Drive a replica connection after PSYNC/SYNC.
pub async fn serve_replica<R, W>(shared: Arc<Shared>, cid: u64, mut rd: R, mut wr: W, mut push_rx: mpsc::UnboundedReceiver<Frame>, mut inbuf: BytesMut)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (plan, mut rx, kill) = {
        let mut s = shared.store.lock();
        let kill = s.clients.get(&cid).map(|c| c.kill.clone());
        match (s.repl.replicas.get_mut(&cid), kill) {
            (Some(l), Some(k)) => (l.plan.take(), l.rx.take(), k),
            _ => return,
        }
    };
    let Some(mut rx_ch) = rx.take() else { return };
    match plan {
        Some(SyncPlan::Full(snap)) => {
            let data = tokio::task::spawn_blocking(move || crate::rdb::serialize(&snap)).await.unwrap_or_default();
            {
                let mut s = shared.store.lock();
                if let Some(c) = s.clients.get_mut(&cid) {
                    c.repl_state = "send_bulk";
                }
            }
            let hdr = format!("${}\r\n", data.len());
            if wr.write_all(hdr.as_bytes()).await.is_err() || wr.write_all(&data).await.is_err() {
                return;
            }
            let mut s = shared.store.lock();
            s.stats.net_repl_output += data.len() as u64;
            if let Some(l) = s.repl.replicas.get_mut(&cid) {
                l.online = true;
            }
            if let Some(c) = s.clients.get_mut(&cid) {
                c.repl_state = "online";
                c.repl_ack_time = crate::util::now_ms();
            }
            eprintln!("Synchronization with replica #{} succeeded", cid);
        }
        Some(SyncPlan::Partial(bytes)) => {
            if !bytes.is_empty() && wr.write_all(&bytes).await.is_err() {
                return;
            }
            let mut s = shared.store.lock();
            if let Some(c) = s.clients.get_mut(&cid) {
                c.repl_state = "online";
            }
        }
        None => {}
    }
    let _ = wr.flush().await;
    loop {
        tokio::select! {
            _ = kill.notified() => break,
            m = rx_ch.recv() => {
                let Some(m) = m else { break };
                let mut buf = m.to_vec();
                while let Ok(more) = rx_ch.try_recv() {
                    buf.extend_from_slice(&more);
                }
                if wr.write_all(&buf).await.is_err() {
                    break;
                }
                shared.store.lock().stats.net_repl_output += buf.len() as u64;
            }
            _ = push_rx.recv() => {}
            r = rd.read_buf(&mut inbuf) => {
                match r {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut s = shared.store.lock();
                        s.stats.net_repl_input += n as u64;
                        loop {
                            match crate::resp::parse_command(&mut inbuf, 1 << 20) {
                                Ok(Some(args)) => handle_replica_cmd(&mut s, cid, &args),
                                Ok(None) => break,
                                Err(_) => {
                                    inbuf.clear();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn handle_replica_cmd(s: &mut Store, cid: u64, args: &[Bytes]) {
    if args.len() >= 3 && crate::util::eq_ic(&args[0], "replconf") && crate::util::eq_ic(&args[1], "ack") {
        let off = crate::util::parse_i64(&args[2]).unwrap_or(0);
        let mut fack = None;
        if args.len() >= 5 && crate::util::eq_ic(&args[3], "fack") {
            fack = crate::util::parse_i64(&args[4]);
        }
        if let Some(c) = s.clients.get_mut(&cid) {
            if off > c.repl_ack_off {
                c.repl_ack_off = off;
            }
            c.repl_ack_time = crate::util::now_ms();
        }
        if let (Some(f), Some(l)) = (fack, s.repl.replicas.get_mut(&cid)) {
            if f > l.ack_aof_off {
                l.ack_aof_off = f;
            }
        }
        process_waits(s);
    }
}

fn acked(s: &Store, offset: i64) -> usize {
    s.repl.replicas.keys().filter(|id| s.clients.get(id).map(|c| c.repl_ack_off >= offset).unwrap_or(false)).count()
}

fn aof_acked(s: &Store, offset: i64) -> usize {
    s.repl.replicas.values().filter(|l| l.ack_aof_off >= offset).count()
}

fn local_aof_ok(s: &Store, offset: i64) -> bool {
    s.aof.is_some() && s.repl.aof_fsynced_offset >= offset
}

/// Unblock WAIT/WAITAOF clients whose condition is met.
pub fn process_waits(s: &mut Store) {
    if s.blocking.wait_clients.is_empty() {
        return;
    }
    let ids = s.blocking.wait_clients.clone();
    for id in ids {
        let kind = s.clients.get(&id).and_then(|c| c.blocked.as_ref()).map(|b| b.kind.clone());
        match kind {
            Some(crate::blocking::BlockKind::Wait { offset, numreplicas }) => {
                let n = acked(s, offset);
                if n >= numreplicas {
                    s.unblock_client(id, Some(Frame::Integer(n as i64)));
                }
            }
            Some(crate::blocking::BlockKind::WaitAof { offset, numlocal, numreplicas }) => {
                let local = local_aof_ok(s, offset) as usize;
                let n = aof_acked(s, offset);
                if local >= numlocal && n >= numreplicas {
                    s.unblock_client(id, Some(Frame::Array(vec![Frame::Integer(local as i64), Frame::Integer(n as i64)])));
                }
            }
            _ => {}
        }
    }
}

/// Reply for a WAIT/WAITAOF client whose timeout expired (None for others).
pub fn wait_timeout_reply(s: &mut Store, cid: u64) -> Option<Frame> {
    let kind = s.clients.get(&cid).and_then(|c| c.blocked.as_ref()).map(|b| b.kind.clone())?;
    match kind {
        crate::blocking::BlockKind::Wait { offset, .. } => Some(Frame::Integer(acked(s, offset) as i64)),
        crate::blocking::BlockKind::WaitAof { offset, .. } => {
            Some(Frame::Array(vec![Frame::Integer(local_aof_ok(s, offset) as i64), Frame::Integer(aof_acked(s, offset) as i64)]))
        }
        _ => None,
    }
}

/// Ask replicas for an ACK (part of the stream, like Redis).
pub fn request_acks(s: &mut Store) {
    if s.repl.replicas.is_empty() {
        return;
    }
    let data = encode(&[Bytes::from_static(b"REPLCONF"), Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")]);
    broadcast(s, &data);
}

// ---------------- periodic work ----------------

pub fn cron(s: &mut Store, shared: &Arc<Shared>, tick: u64) {
    let hz = s.cfg.hz.max(1);
    let now = crate::util::now_ms();
    // Track local AOF fsync progress for WAITAOF.
    if s.aof.is_some() {
        crate::aof::flush_now(s);
        s.repl.aof_fsynced_offset = s.repl.master_offset;
    }
    // Ping replicas every repl-ping-replica-period seconds.
    if !s.repl.is_replica() && !s.repl.replicas.is_empty() && tick % (hz * s.cfg.repl_ping_period.max(1)) == 0 {
        let data = encode(&[Bytes::from_static(b"PING")]);
        broadcast(s, &data);
    }
    // Drop replicas that stopped acking.
    let timeout = s.cfg.repl_timeout as i64 * 1000;
    let dead: Vec<u64> = s
        .repl
        .replicas
        .iter()
        .filter(|(id, l)| l.online && s.clients.get(id).map(|c| now - c.repl_ack_time > timeout).unwrap_or(true))
        .map(|(id, _)| *id)
        .collect();
    for id in dead {
        eprintln!("Disconnecting timedout replica (streaming sync): #{}", id);
        if let Some(c) = s.clients.get_mut(&id) {
            c.kill_now();
        }
    }
    process_waits(s);
    failover_cron(s, shared);
    // Replica side: reconnect when the link is down.
    if s.repl.is_replica() && s.repl.state == LinkState::Connect && tick % hz == 0 {
        start_link(s, shared.clone());
    }
}

// ---------------- replica side ----------------

/// REPLICAOF host port.
pub fn set_master(s: &mut Store, shared: Arc<Shared>, host: String, port: u16) {
    if s.repl.master.as_ref() == Some(&(host.clone(), port)) {
        return;
    }
    let was_master = s.repl.master.is_none();
    s.repl.master = Some((host, port));
    if let Some(mcid) = s.repl.master_cid.take() {
        if let Some(c) = s.clients.get_mut(&mcid) {
            c.kill_now();
        }
        s.clients.remove(&mcid);
    }
    // Sub-replicas must resync with the new history.
    for id in s.repl.replicas.keys().copied().collect::<Vec<_>>() {
        if let Some(c) = s.clients.get_mut(&id) {
            c.kill_now();
        }
    }
    s.repl.replicas.clear();
    if was_master {
        // Our current dataset/offset becomes the PSYNC candidate (cached master).
        s.repl.state = LinkState::Connect;
    }
    s.repl.state = LinkState::Connect;
    s.repl.link_down_since = crate::util::now_ms();
    // Blocked clients may now be on a read-only replica.
    let blocked: Vec<u64> = s.clients.values().filter(|c| c.blocked.as_ref().map(|b| b.kind == crate::blocking::BlockKind::Keys).unwrap_or(false)).map(|c| c.id).collect();
    for id in blocked {
        s.unblock_client(id, Some(Frame::err("UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)")));
    }
    start_link(s, shared);
}

/// REPLICAOF NO ONE.
pub fn unset_master(s: &mut Store) {
    if s.repl.master.is_none() {
        return;
    }
    s.repl.master = None;
    s.repl.link_epoch += 1;
    if let Some(mcid) = s.repl.master_cid.take() {
        if let Some(c) = s.clients.get_mut(&mcid) {
            c.kill_now();
        }
        s.clients.remove(&mcid);
    }
    s.repl.shift_replid();
    s.repl.state = LinkState::None;
    s.repl.seldb = None;
    s.repl.create_backlog();
    eprintln!("MASTER MODE enabled (user request from 'id=...')");
}

fn start_link(s: &mut Store, shared: Arc<Shared>) {
    let Some((host, port)) = s.repl.master.clone() else { return };
    s.repl.link_epoch += 1;
    let epoch = s.repl.link_epoch;
    s.repl.state = LinkState::Connecting;
    let auth = (s.cfg.masteruser.clone(), s.cfg.masterauth.clone());
    let my_port = s.cfg.port;
    let (replid, off) = if s.repl.replid2 != "0".repeat(40) || s.repl.master_offset > 0 { (s.repl.replid.clone(), s.repl.master_offset + 1) } else { ("?".to_string(), -1) };
    let timeout = s.cfg.repl_timeout;
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        s.repl.state = LinkState::Connect;
        return;
    };
    rt.spawn(async move {
        let res = run_link(shared.clone(), epoch, host.clone(), port, auth, my_port, replid, off, timeout).await;
        let mut s = shared.store.lock();
        if s.repl.link_epoch == epoch && s.repl.master.is_some() {
            if let Err(e) = res {
                eprintln!("Error condition on socket for SYNC with {}:{}: {}", host, port, e);
            }
            if let Some(mcid) = s.repl.master_cid.take() {
                s.clients.remove(&mcid);
            }
            if s.repl.state == LinkState::Connected {
                s.repl.link_down_since = crate::util::now_ms();
            }
            s.repl.state = LinkState::Connect;
        }
    });
}

async fn read_line<R: AsyncRead + Unpin>(rd: &mut R, buf: &mut BytesMut) -> Result<String, String> {
    loop {
        if let Some(p) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf.split_to(p + 2);
            return Ok(String::from_utf8_lossy(&line[..p]).into_owned());
        }
        let n = rd.read_buf(buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed".into());
        }
    }
}

async fn send_cmd<W: AsyncWrite + Unpin>(wr: &mut W, args: &[&str]) -> Result<(), String> {
    let argv: Vec<Bytes> = args.iter().map(|a| Bytes::copy_from_slice(a.as_bytes())).collect();
    wr.write_all(&encode(&argv)).await.map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn run_link(
    shared: Arc<Shared>,
    epoch: u64,
    host: String,
    port: u16,
    auth: (Option<String>, Option<String>),
    my_port: u16,
    replid: String,
    offset: i64,
    timeout: u64,
) -> Result<(), String> {
    let alive = |shared: &Arc<Shared>| shared.store.lock().repl.link_epoch == epoch;
    eprintln!("Connecting to MASTER {}:{}", host, port);
    let stream = tokio::time::timeout(std::time::Duration::from_secs(timeout.max(1)), tokio::net::TcpStream::connect((host.as_str(), port)))
        .await
        .map_err(|_| "Timeout connecting to the MASTER...".to_string())?
        .map_err(|e| e.to_string())?;
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut buf = BytesMut::with_capacity(64 * 1024);
    eprintln!("MASTER <-> REPLICA sync started");
    send_cmd(&mut wr, &["PING"]).await?;
    let r = read_line(&mut rd, &mut buf).await?;
    if r.starts_with('-') && !r.starts_with("-NOAUTH") && !r.starts_with("-NOPERM") && !r.starts_with("-ERR operation not permitted") {
        return Err(format!("Error reply to PING from master: '{}'", r));
    }
    eprintln!("Master replied to PING, replication can continue...");
    if let Some(pass) = &auth.1 {
        match &auth.0 {
            Some(u) => send_cmd(&mut wr, &["AUTH", u, pass]).await?,
            None => send_cmd(&mut wr, &["AUTH", pass]).await?,
        }
        let r = read_line(&mut rd, &mut buf).await?;
        if r.starts_with('-') {
            return Err(format!("Unable to AUTH to MASTER: {}", r));
        }
    }
    send_cmd(&mut wr, &["REPLCONF", "listening-port", &my_port.to_string()]).await?;
    let r = read_line(&mut rd, &mut buf).await?;
    if r.starts_with('-') {
        eprintln!("(Non critical) Master does not understand REPLCONF listening-port: {}", r);
    }
    send_cmd(&mut wr, &["REPLCONF", "capa", "eof", "capa", "psync2"]).await?;
    let r = read_line(&mut rd, &mut buf).await?;
    if r.starts_with('-') {
        eprintln!("(Non critical) Master does not understand REPLCONF capa: {}", r);
    }
    if !alive(&shared) {
        return Ok(());
    }
    if offset > 0 {
        eprintln!("Trying a partial resynchronization (request {}:{}).", replid, offset);
    }
    send_cmd(&mut wr, &["PSYNC", &replid, &offset.to_string()]).await?;
    let mut r = read_line(&mut rd, &mut buf).await?;
    while r.is_empty() {
        r = read_line(&mut rd, &mut buf).await?;
    }
    if let Some(rest) = r.strip_prefix("+FULLRESYNC ") {
        let mut it = rest.split(' ');
        let new_id = it.next().unwrap_or("").to_string();
        let new_off: i64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
        eprintln!("Full resync from master: {}:{}", new_id, new_off);
        {
            let mut s = shared.store.lock();
            s.repl.state = LinkState::Sync;
            s.repl.sync_start = crate::util::now_ms();
            s.repl.sync_read = 0;
        }
        // Read the bulk header (skipping keepalive newlines).
        let hdr = loop {
            let l = read_line(&mut rd, &mut buf).await?;
            if !l.is_empty() {
                break l;
            }
        };
        let hdr = hdr.strip_prefix('$').ok_or_else(|| format!("Bad protocol from MASTER, the first byte is not '$' (we received '{}'), are you sure the host and port are right?", hdr))?;
        let data: Vec<u8> = if let Some(mark) = hdr.strip_prefix("EOF:") {
            // Diskless transfer terminated by a 40 byte mark.
            let mark = mark.as_bytes().to_vec();
            let mut data = Vec::new();
            loop {
                data.extend_from_slice(&buf.split());
                if data.len() >= mark.len() && data[data.len() - mark.len()..] == mark[..] {
                    data.truncate(data.len() - mark.len());
                    break;
                }
                let n = rd.read_buf(&mut buf).await.map_err(|e| e.to_string())?;
                if n == 0 {
                    return Err("connection lost during diskless transfer".into());
                }
                shared.store.lock().repl.sync_read += n as i64;
            }
            data
        } else {
            let len: usize = hdr.parse().map_err(|_| "Bad bulk length from MASTER".to_string())?;
            shared.store.lock().repl.sync_total = len as i64;
            while buf.len() < len {
                let n = rd.read_buf(&mut buf).await.map_err(|e| e.to_string())?;
                if n == 0 {
                    return Err("connection lost during RDB transfer".into());
                }
                shared.store.lock().repl.sync_read = buf.len() as i64;
            }
            buf.split_to(len).to_vec()
        };
        eprintln!("MASTER <-> REPLICA sync: receiving {} bytes from master to disk", data.len());
        {
            let mut s = shared.store.lock();
            if s.repl.link_epoch != epoch {
                return Ok(());
            }
            eprintln!("MASTER <-> REPLICA sync: Flushing old data");
            for d in s.dbs.iter_mut() {
                d.clear();
            }
            crate::scripting::flush_functions(&mut s);
            eprintln!("MASTER <-> REPLICA sync: Loading DB in memory");
            let ndb = s.dbs.len();
            let parsed = crate::rdb::parse(&data, ndb)?;
            crate::rdb::install(&mut s, parsed);
            // Persist the received dataset like Redis does.
            let path = crate::rdb::rdb_path(&s);
            let _ = std::fs::write(&path, &data);
            s.repl.replid = new_id;
            s.repl.replid2 = "0".repeat(40);
            s.repl.second_offset = -1;
            s.repl.master_offset = new_off;
            s.repl.read_offset = new_off;
            s.repl.backlog_active = false;
            s.repl.create_backlog();
            s.repl.sync_total = -1;
            if s.aof.is_some() {
                crate::aof::start_aof(&mut s);
            }
            eprintln!("MASTER <-> REPLICA sync: Finished with success");
        }
    } else if let Some(rest) = r.strip_prefix("+CONTINUE") {
        let new_id = rest.trim();
        let mut s = shared.store.lock();
        if !new_id.is_empty() && new_id != s.repl.replid {
            s.repl.replid2 = s.repl.replid.clone();
            s.repl.second_offset = s.repl.master_offset + 1;
            s.repl.replid = new_id.to_string();
        }
        s.repl.read_offset = s.repl.master_offset;
        eprintln!("Successful partial resynchronization with master.");
    } else {
        return Err(format!("Unexpected reply to PSYNC from master: {}", r));
    }

    // Create the master pseudo client and enter streaming mode.
    {
        let mut s = shared.store.lock();
        if s.repl.link_epoch != epoch {
            return Ok(());
        }
        let mcid = crate::aof::pseudo_client(&mut s, cflags::MASTER);
        if let Some(c) = s.clients.get_mut(&mcid) {
            c.addr = format!("{}:{}", host, port);
        }
        s.repl.master_cid = Some(mcid);
        s.repl.state = LinkState::Connected;
        s.repl.last_io = crate::util::now_ms();
    }
    eprintln!("MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization or finished full sync.");
    let mut ack_timer = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut multi: Option<(Vec<Vec<Bytes>>, usize)> = None;
    loop {
        // Apply all complete commands in the buffer.
        let mut acks = Vec::new();
        {
            let mut s = shared.store.lock();
            if s.repl.link_epoch != epoch || s.repl.master_cid.is_none() {
                return Ok(());
            }
            loop {
                let before = buf.len();
                let args = match crate::resp::parse_command(&mut buf, 512 << 20) {
                    Ok(Some(a)) => a,
                    Ok(None) => break,
                    Err(_) => return Err("Protocol error from master".into()),
                };
                let consumed = before - buf.len();
                if crate::util::eq_ic(&args[0], "replconf") && args.len() >= 2 && crate::util::eq_ic(&args[1], "getack") {
                    s.repl.read_offset += consumed as i64;
                    proxy_raw(&mut s, &crate::aof::encode_command(&args));
                    acks.push(s.repl.master_offset);
                    continue;
                }
                if crate::util::eq_ic(&args[0], "multi") {
                    multi = Some((Vec::new(), consumed));
                    continue;
                }
                if let Some((q, bytes)) = multi.as_mut() {
                    *bytes += consumed;
                    if crate::util::eq_ic(&args[0], "exec") {
                        let (q, bytes) = multi.take().unwrap();
                        let mcid = s.repl.master_cid.unwrap();
                        let mut raw = encode(&[Bytes::from_static(b"MULTI")]);
                        for a in &q {
                            raw.extend_from_slice(&encode(a));
                        }
                        raw.extend_from_slice(&encode(&[Bytes::from_static(b"EXEC")]));
                        apply_batch(&mut s, mcid, &q);
                        s.repl.read_offset += bytes as i64;
                        proxy_raw(&mut s, &raw);
                    } else {
                        q.push(args);
                    }
                    continue;
                }
                let mcid = s.repl.master_cid.unwrap();
                apply_batch(&mut s, mcid, std::slice::from_ref(&args));
                s.repl.read_offset += consumed as i64;
                proxy_raw(&mut s, &encode(&args));
            }
            s.repl.last_io = crate::util::now_ms();
        }
        for off in acks {
            send_cmd(&mut wr, &["REPLCONF", "ACK", &off.to_string()]).await?;
        }
        tokio::select! {
            r = rd.read_buf(&mut buf) => {
                match r {
                    Ok(0) => return Err("Connection with master lost.".into()),
                    Err(e) => return Err(e.to_string()),
                    Ok(n) => {
                        let mut s = shared.store.lock();
                        s.stats.net_repl_input += n as u64;
                    }
                }
            }
            _ = ack_timer.tick() => {
                let (off, fack, alive, stale) = {
                    let s = shared.store.lock();
                    let stale = crate::util::now_ms() - s.repl.last_io > s.cfg.repl_timeout as i64 * 1000;
                    (s.repl.master_offset, s.repl.aof_fsynced_offset, s.repl.link_epoch == epoch, stale)
                };
                if !alive {
                    return Ok(());
                }
                if stale {
                    return Err("MASTER timeout: no data nor PING received...".into());
                }
                send_cmd(&mut wr, &["REPLCONF", "ACK", &off.to_string(), "FACK", &fack.to_string()]).await?;
            }
        }
    }
}

fn apply_batch(s: &mut Store, mcid: u64, cmds: &[Vec<Bytes>]) {
    s.repl.applying_master = true;
    for args in cmds {
        let db = s.clients.get(&mcid).map(|c| c.db).unwrap_or(0);
        if crate::util::eq_ic(&args[0], "select") {
            if let Some(n) = args.get(1).and_then(|a| crate::util::parse_i64(a)) {
                if (n as usize) < s.dbs.len() {
                    if let Some(c) = s.clients.get_mut(&mcid) {
                        c.db = n as usize;
                    }
                }
            }
            continue;
        }
        if crate::util::eq_ic(&args[0], "ping") {
            continue;
        }
        let prev = s.current_client;
        s.current_client = mcid;
        let r = crate::exec::exec_internal(s, mcid, db, args, crate::exec::CTX_MASTER);
        s.current_client = prev;
        if let Frame::Error(e) = r {
            eprintln!("== CRITICAL == This replica is sending an error to its master: '{}' after processing the command '{}'", e, String::from_utf8_lossy(&args[0]));
        }
    }
    crate::exec::after_command(s);
    s.repl.applying_master = false;
}

/// Replicas proxy the exact master stream to the backlog and sub-replicas.
fn proxy_raw(s: &mut Store, data: &[u8]) {
    broadcast(s, data);
}

// ---------------- FAILOVER ----------------

pub fn failover_cron(s: &mut Store, shared: &Arc<Shared>) {
    let Some(f) = s.repl.failover.as_ref() else { return };
    let now = crate::util::now_ms();
    let target = f.target.clone().or_else(|| {
        // Pick the replica with the highest ack offset.
        s.repl
            .replicas
            .keys()
            .filter_map(|id| s.clients.get(id))
            .max_by_key(|c| c.repl_ack_off)
            .map(|c| (c.addr.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or_default(), c.repl_listening_port))
    });
    let Some((host, port)) = target else { return };
    let caught_up = s
        .repl
        .replicas
        .keys()
        .filter_map(|id| s.clients.get(id))
        .any(|c| c.repl_listening_port == port && c.repl_ack_off >= s.repl.master_offset);
    let timed_out = f.deadline.map(|d| now > d).unwrap_or(false);
    if s.repl.failover_state == "waiting-for-sync" {
        if caught_up || (timed_out && f.force) {
            s.repl.failover_state = "failover-in-progress";
            eprintln!("Failover target {}:{} is synced, failing over.", host, port);
            s.repl.failover = None;
            s.pause = None;
            crate::blocking::unpause(s);
            s.repl.failover_state = "no-failover";
            set_master(s, shared.clone(), host, port);
        } else if timed_out {
            eprintln!("FAILOVER to {}:{} timed out.", host, port);
            abort_failover(s);
        } else {
            request_acks(s);
        }
    }
}

pub fn abort_failover(s: &mut Store) {
    s.repl.failover = None;
    s.repl.failover_state = "no-failover";
    s.pause = None;
    crate::blocking::unpause(s);
}

// ---------------- INFO ----------------

pub fn info(s: &Store) -> String {
    let now = crate::util::now_ms();
    let mut o = String::new();
    if let Some((h, p)) = &s.repl.master {
        o.push_str("role:slave\r\n");
        o.push_str(&format!("master_host:{}\r\nmaster_port:{}\r\n", h, p));
        o.push_str(&format!("master_link_status:{}\r\n", if s.repl.link_up() { "up" } else { "down" }));
        let io = if s.repl.link_up() { (now - s.repl.last_io) / 1000 } else { -1 };
        o.push_str(&format!("master_last_io_seconds_ago:{}\r\n", io));
        let syncing = s.repl.state == LinkState::Sync;
        o.push_str(&format!("master_sync_in_progress:{}\r\n", syncing as u8));
        o.push_str(&format!("slave_read_repl_offset:{}\r\n", s.repl.read_offset.max(s.repl.master_offset)));
        o.push_str(&format!("slave_repl_offset:{}\r\n", s.repl.master_offset));
        if syncing {
            o.push_str(&format!(
                "master_sync_total_bytes:{}\r\nmaster_sync_read_bytes:{}\r\nmaster_sync_left_bytes:{}\r\nmaster_sync_perc:{:.2}\r\nmaster_sync_last_io_seconds_ago:0\r\n",
                s.repl.sync_total,
                s.repl.sync_read,
                if s.repl.sync_total >= 0 { s.repl.sync_total - s.repl.sync_read } else { -1 },
                if s.repl.sync_total > 0 { s.repl.sync_read as f64 * 100.0 / s.repl.sync_total as f64 } else { 0.0 }
            ));
        }
        if !s.repl.link_up() {
            let since = if s.repl.link_down_since > 0 { (now - s.repl.link_down_since) / 1000 } else { -1 };
            o.push_str(&format!("master_link_down_since_seconds:{}\r\n", since));
        }
        o.push_str(&format!("slave_priority:{}\r\nslave_read_only:{}\r\nreplica_announced:1\r\n", s.cfg.replica_priority, s.cfg.replica_read_only as u8));
    } else {
        o.push_str("role:master\r\n");
    }
    let mut lines = Vec::new();
    let mut ids: Vec<&u64> = s.repl.replicas.keys().collect();
    ids.sort();
    for (i, id) in ids.into_iter().enumerate() {
        let Some(c) = s.clients.get(id) else { continue };
        let ip = c.addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("");
        let lag = (now - c.repl_ack_time) / 1000;
        lines.push(format!("slave{}:ip={},port={},state={},offset={},lag={}\r\n", i, ip, c.repl_listening_port, c.repl_state, c.repl_ack_off, lag));
    }
    o.push_str(&format!("connected_slaves:{}\r\n", lines.len()));
    for l in lines {
        o.push_str(&l);
    }
    if s.cfg.min_replicas_to_write > 0 {
        o.push_str(&format!("min_slaves_good_slaves:{}\r\n", count_good(s, s.cfg.min_replicas_max_lag)));
    }
    o.push_str(&format!("master_failover_state:{}\r\n", s.repl.failover_state));
    o.push_str(&format!("master_replid:{}\r\nmaster_replid2:{}\r\n", s.repl.replid, s.repl.replid2));
    o.push_str(&format!("master_repl_offset:{}\r\nsecond_repl_offset:{}\r\n", s.repl.master_offset, s.repl.second_offset));
    o.push_str(&format!(
        "repl_backlog_active:{}\r\nrepl_backlog_size:{}\r\nrepl_backlog_first_byte_offset:{}\r\nrepl_backlog_histlen:{}\r\n",
        s.repl.backlog_active as u8,
        s.cfg.repl_backlog_size,
        if s.repl.backlog_active { s.repl.backlog_first } else { 0 },
        s.repl.backlog.len()
    ));
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backlog_offsets() {
        let mut r = ReplState::new("x");
        r.create_backlog();
        r.append_backlog(b"hello", 1 << 20);
        r.append_backlog(b"world", 1 << 20);
        assert_eq!(r.master_offset, 10);
        assert_eq!(r.backlog_from(1).unwrap(), b"helloworld");
        assert_eq!(r.backlog_from(6).unwrap(), b"world");
        assert_eq!(r.backlog_from(11).unwrap(), b"");
        assert!(r.backlog_from(12).is_none());
    }
}
