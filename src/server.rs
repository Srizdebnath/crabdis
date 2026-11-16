//! Global server state (`Store`), client records, statistics and the server cron.

use crate::blocking::Blocking;
use crate::config::Config;
use crate::db::Db;
use crate::resp::Frame;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Notify, mpsc};

pub const VERSION: &str = "7.4.1";
pub const CRABDIS_VERSION: &str = env!("CARGO_PKG_VERSION");

pub static START_INSTANT: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// LRU clock in seconds, 24 bits like Redis.
pub fn lru_clock() -> u32 {
    ((crate::util::now_ms() / 1000) as u32) & ((1 << 24) - 1)
}

pub fn monotonic_us() -> u64 {
    START_INSTANT.get_or_init(std::time::Instant::now).elapsed().as_micros() as u64
}

pub mod cflags {
    pub const MULTI: u64 = 1 << 0;
    pub const DIRTY_CAS: u64 = 1 << 1;
    pub const DIRTY_EXEC: u64 = 1 << 2;
    pub const PUBSUB: u64 = 1 << 3;
    pub const BLOCKED: u64 = 1 << 4;
    pub const MONITOR: u64 = 1 << 5;
    pub const REPLICA: u64 = 1 << 6;
    pub const MASTER: u64 = 1 << 7;
    pub const CLOSE_AFTER_REPLY: u64 = 1 << 8;
    pub const CLOSE_ASAP: u64 = 1 << 9;
    pub const NO_EVICT: u64 = 1 << 10;
    pub const NO_TOUCH: u64 = 1 << 11;
    pub const READONLY: u64 = 1 << 12;
    pub const ASKING: u64 = 1 << 13;
    pub const TRACKING: u64 = 1 << 14;
    pub const TRACKING_BCAST: u64 = 1 << 15;
    pub const TRACKING_OPTIN: u64 = 1 << 16;
    pub const TRACKING_OPTOUT: u64 = 1 << 17;
    pub const TRACKING_CACHING: u64 = 1 << 18;
    pub const TRACKING_NOLOOP: u64 = 1 << 19;
    pub const TRACKING_BROKEN_REDIR: u64 = 1 << 20;
    pub const REPLY_OFF: u64 = 1 << 21;
    pub const REPLY_SKIP_NEXT: u64 = 1 << 22;
    pub const REPLY_SKIP: u64 = 1 << 23;
    pub const PUSHING: u64 = 1 << 24;
    pub const LUA: u64 = 1 << 25;
    pub const UNIX: u64 = 1 << 26;
    pub const REPLICA_MONITOR_ACK: u64 = 1 << 27;
}

pub struct Blocked {
    pub args: Vec<Bytes>,
    pub db: usize,
    pub keys: Vec<Bytes>,
    pub tx: Option<tokio::sync::oneshot::Sender<Frame>>,
    pub timeout_reply: Frame,
    pub deadline: Option<i64>,
    pub kind: crate::blocking::BlockKind,
    pub since: i64,
}

/// Per-client state kept in the store (visible to CLIENT LIST etc.).
pub struct Client {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    pub name: Option<Bytes>,
    pub lib_name: Option<String>,
    pub lib_ver: Option<String>,
    pub db: usize,
    pub proto: u8,
    pub created_ms: i64,
    pub last_interaction_ms: i64,
    pub last_cmd: String,
    pub flags: u64,
    pub mstate: Vec<Vec<Bytes>>,
    pub watched: Vec<(usize, Bytes)>,
    pub channels: HashSet<Bytes>,
    pub patterns: HashSet<Bytes>,
    pub shard_channels: HashSet<Bytes>,
    pub push_tx: mpsc::UnboundedSender<Frame>,
    pub blocked: Option<Blocked>,
    pub authenticated: bool,
    pub user: String,
    pub kill: Arc<Notify>,
    pub killed: Arc<AtomicBool>,
    pub tracking_redirect: u64,
    pub tracking_prefixes: Vec<Bytes>,
    pub qbuf: usize,
    pub obuf: usize,
    pub tot_net_in: u64,
    pub tot_net_out: u64,
    pub tot_cmds: u64,
    pub multi_mem: usize,
    /// Replication: ack offset reported by a replica.
    pub repl_ack_off: i64,
    pub repl_ack_time: i64,
    pub repl_listening_port: u16,
    pub repl_capa: Vec<String>,
    pub repl_state: &'static str,
}

impl Client {
    pub fn new(id: u64, addr: String, laddr: String, push_tx: mpsc::UnboundedSender<Frame>, authed: bool) -> Self {
        let now = crate::util::now_ms();
        Client {
            id,
            addr,
            laddr,
            name: None,
            lib_name: None,
            lib_ver: None,
            db: 0,
            proto: 2,
            created_ms: now,
            last_interaction_ms: now,
            last_cmd: "NULL".into(),
            flags: 0,
            mstate: Vec::new(),
            watched: Vec::new(),
            channels: HashSet::new(),
            patterns: HashSet::new(),
            shard_channels: HashSet::new(),
            push_tx,
            blocked: None,
            authenticated: authed,
            user: "default".into(),
            kill: Arc::new(Notify::new()),
            killed: Arc::new(AtomicBool::new(false)),
            tracking_redirect: 0,
            tracking_prefixes: Vec::new(),
            qbuf: 0,
            obuf: 0,
            tot_net_in: 0,
            tot_net_out: 0,
            tot_cmds: 0,
            multi_mem: 0,
            repl_ack_off: 0,
            repl_ack_time: 0,
            repl_listening_port: 0,
            repl_capa: Vec::new(),
            repl_state: "",
        }
    }
    pub fn has(&self, f: u64) -> bool {
        self.flags & f != 0
    }
    pub fn subscriptions(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
    pub fn in_pubsub(&self) -> bool {
        self.channels.len() + self.patterns.len() + self.shard_channels.len() > 0
    }
    /// Send an out-of-band frame (pub/sub message, invalidation, monitor line).
    pub fn push(&self, f: Frame) {
        let _ = self.push_tx.send(f);
    }
    pub fn kill_now(&self) {
        self.killed.store(true, Ordering::SeqCst);
        self.kill.notify_one();
    }
}

#[derive(Default, Clone)]
pub struct CmdStat {
    pub calls: u64,
    pub usec: u64,
    pub rejected: u64,
    pub failed: u64,
}

#[derive(Default)]
pub struct Stats {
    pub total_connections: u64,
    pub total_commands: u64,
    pub rejected_connections: u64,
    pub expired_keys: u64,
    pub expired_stale_perc: f64,
    pub expired_time_cap_reached: u64,
    pub evicted_keys: u64,
    pub evicted_clients: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub net_input: u64,
    pub net_output: u64,
    pub net_repl_input: u64,
    pub net_repl_output: u64,
    pub sync_full: u64,
    pub sync_partial_ok: u64,
    pub sync_partial_err: u64,
    pub total_error_replies: u64,
    pub unexpected_error_replies: u64,
    pub dump_payload_sanitizations: u64,
    pub total_reads_processed: u64,
    pub total_writes_processed: u64,
    pub cmdstats: HashMap<String, CmdStat>,
    pub errorstats: HashMap<String, u64>,
    pub ops_samples: VecDeque<(u64, u64)>,
    pub net_in_samples: VecDeque<(u64, u64)>,
    pub net_out_samples: VecDeque<(u64, u64)>,
    pub expired_subkeys: u64,
    pub io_reads: u64,
    pub client_qbuf_limit_disconnections: u64,
    pub client_outbuf_limit_disconnections: u64,
    pub total_blocking_keys: u64,
    pub total_forks: u64,
    pub rdb_last_load_keys_loaded: u64,
    pub rdb_last_load_keys_expired: u64,
}

#[derive(Clone)]
pub struct SlowlogEntry {
    pub id: u64,
    pub time: i64,
    pub duration_us: u64,
    pub args: Vec<Bytes>,
    pub peer: String,
    pub name: String,
}

pub struct PersistState {
    pub lastsave: i64,
    pub last_bgsave_ok: bool,
    pub bgsave_in_progress: bool,
    pub last_bgsave_time_sec: i64,
    pub bgsave_started: i64,
    pub rdb_saves: u64,
    pub aof_rewrite_in_progress: bool,
    pub aof_rewrite_scheduled: bool,
    pub aof_last_rewrite_ok: bool,
    pub aof_last_write_ok: bool,
    pub aof_rewrites: u64,
    pub aof_base_size: u64,
    pub aof_current_size: u64,
    pub loading: bool,
    pub async_loading: bool,
    pub rdb_changes_since_last_save: u64,
    pub bgsave_scheduled: bool,
}

/// The single-lock global state: every command executes with exclusive access,
/// giving Redis' serial execution semantics.
pub struct Store {
    pub dbs: Vec<Db>,
    pub cfg: Config,
    pub clients: HashMap<u64, Client>,
    pub next_client_id: u64,
    pub pubsub: crate::pubsub::PubSub,
    pub watched: HashMap<(usize, Bytes), Vec<u64>>,
    pub blocking: Blocking,
    pub stats: Stats,
    pub dirty: u64,
    pub persist: PersistState,
    pub slowlog: VecDeque<SlowlogEntry>,
    pub slowlog_next_id: u64,
    pub latency: crate::latency::Latency,
    pub start_ms: i64,
    pub run_id: String,
    pub monitors: Vec<u64>,
    pub tracking: crate::tracking::Tracking,
    pub pause: Option<(i64, bool)>,
    pub aof: Option<crate::aof::AofHandle>,
    pub repl: crate::repl::ReplState,
    pub acl: crate::acl::Acl,
    pub scripting: crate::scripting::Scripting,
    pub cluster: Option<crate::cluster::ClusterState>,
    pub executable: String,
    pub shutdown_asap: bool,
    pub shutdown_flags: (bool, bool),
    pub used_memory_peak: usize,
    pub lazyfree_pending: u64,
    pub stat_peak_mem_time: i64,
    pub current_client: u64,
    pub exec_depth: u32,
    pub cmd_time_snapshot: i64,
}

pub struct Shared {
    pub store: Mutex<Store>,
    pub shutdown: Notify,
    pub client_count: AtomicU64,
    pub wakeup_aof: Notify,
}

impl Store {
    pub fn new(cfg: Config) -> Self {
        let dbs = (0..cfg.databases).map(Db::new).collect();
        let run_id = crate::util::random_hex(40);
        let acl = crate::acl::Acl::new(&cfg);
        Store {
            dbs,
            clients: HashMap::new(),
            next_client_id: 1,
            pubsub: Default::default(),
            watched: HashMap::new(),
            blocking: Default::default(),
            stats: Default::default(),
            dirty: 0,
            persist: PersistState {
                lastsave: crate::util::now_ms() / 1000,
                last_bgsave_ok: true,
                bgsave_in_progress: false,
                last_bgsave_time_sec: -1,
                bgsave_started: 0,
                rdb_saves: 0,
                aof_rewrite_in_progress: false,
                aof_rewrite_scheduled: false,
                aof_last_rewrite_ok: true,
                aof_last_write_ok: true,
                aof_rewrites: 0,
                aof_base_size: 0,
                aof_current_size: 0,
                loading: false,
                async_loading: false,
                rdb_changes_since_last_save: 0,
                bgsave_scheduled: false,
            },
            slowlog: VecDeque::new(),
            slowlog_next_id: 0,
            latency: Default::default(),
            start_ms: crate::util::now_ms(),
            repl: crate::repl::ReplState::new(&run_id),
            run_id,
            monitors: Vec::new(),
            tracking: Default::default(),
            pause: None,
            aof: None,
            acl,
            scripting: crate::scripting::Scripting::new(),
            cluster: None,
            executable: std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            shutdown_asap: false,
            shutdown_flags: (false, false),
            used_memory_peak: 0,
            lazyfree_pending: 0,
            stat_peak_mem_time: 0,
            current_client: 0,
            exec_depth: 0,
            cfg,
            cmd_time_snapshot: 0,
        }
    }

    pub fn used_memory(&self) -> usize {
        crate::mem::used_memory()
    }

    pub fn ops_per_sec(samples: &VecDeque<(u64, u64)>) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }
        let (t0, v0) = samples.front().unwrap();
        let (t1, v1) = samples.back().unwrap();
        if t1 <= t0 {
            return 0.0;
        }
        (v1 - v0) as f64 * 1_000_000.0 / (t1 - t0) as f64
    }

    pub fn record_error(&mut self, err: &str) {
        let code = err.split_whitespace().next().unwrap_or("ERR").to_string();
        self.stats.total_error_replies += 1;
        if self.stats.errorstats.len() >= 128 && !self.stats.errorstats.contains_key(&code) {
            return;
        }
        *self.stats.errorstats.entry(code).or_default() += 1;
    }

    pub fn slowlog_push(&mut self, args: &[Bytes], dur: u64, cid: u64) {
        let thr = self.cfg.slowlog_log_slower_than;
        if thr < 0 || (dur as i64) < thr {
            return;
        }
        let (peer, name) = self
            .clients
            .get(&cid)
            .map(|c| (c.addr.clone(), c.name.as_ref().map(|n| String::from_utf8_lossy(n).into_owned()).unwrap_or_default()))
            .unwrap_or_default();
        let mut sargs: Vec<Bytes> = Vec::new();
        let max = 32;
        for (i, a) in args.iter().enumerate() {
            if i == max - 1 && args.len() > max {
                sargs.push(Bytes::from(format!("... ({} more arguments)", args.len() - max + 1)));
                break;
            }
            if a.len() > 128 {
                let mut v = a[..128].to_vec();
                v.extend_from_slice(format!("... ({} more bytes)", a.len() - 128).as_bytes());
                sargs.push(Bytes::from(v));
            } else {
                sargs.push(a.clone());
            }
        }
        let e = SlowlogEntry {
            id: self.slowlog_next_id,
            time: crate::util::now_ms() / 1000,
            duration_us: dur,
            args: sargs,
            peer,
            name,
        };
        self.slowlog_next_id += 1;
        self.slowlog.push_front(e);
        while self.slowlog.len() > self.cfg.slowlog_max_len {
            self.slowlog.pop_back();
        }
    }
}

/// Background cron: active expiry, stats sampling, save points, client timeouts.
pub async fn cron(shared: Arc<Shared>) {
    let mut tick: u64 = 0;
    loop {
        let hz = { shared.store.lock().cfg.hz.max(1) };
        tokio::time::sleep(std::time::Duration::from_millis(1000 / hz)).await;
        tick += 1;
        let mut s = shared.store.lock();
        let now = crate::util::now_ms();
        if s.persist.loading {
            continue;
        }
        // Active expire (fast-ish, bounded).
        if !s.repl.is_replica() {
            let effort = 25 * s.cfg.active_expire_effort as usize;
            crate::expire::active_expire_cycle(&mut s, effort);
        }
        // Stats sampling every ~100ms.
        let t = monotonic_us();
        let (cmds, nin, nout) = (s.stats.total_commands, s.stats.net_input, s.stats.net_output);
        let st = &mut s.stats;
        for (q, v) in [(&mut st.ops_samples, cmds), (&mut st.net_in_samples, nin), (&mut st.net_out_samples, nout)] {
            q.push_back((t, v));
            while q.len() > 16 {
                q.pop_front();
            }
        }
        let mem = s.used_memory();
        if mem > s.used_memory_peak {
            s.used_memory_peak = mem;
            s.stat_peak_mem_time = now;
        }
        // Client timeouts and blocked client timeouts.
        let timeout = s.cfg.timeout;
        if timeout > 0 && tick % hz == 0 {
            let idle: Vec<u64> = s
                .clients
                .values()
                .filter(|c| {
                    !c.has(cflags::REPLICA | cflags::MASTER | cflags::BLOCKED | cflags::PUBSUB)
                        && !c.in_pubsub()
                        && (now - c.last_interaction_ms) as u64 / 1000 > timeout
                })
                .map(|c| c.id)
                .collect();
            for id in idle {
                if let Some(c) = s.clients.get(&id) {
                    c.kill_now();
                }
            }
        }
        crate::blocking::expire_blocked(&mut s, now);
        // Pause expiry.
        if let Some((until, _)) = s.pause {
            if now >= until {
                s.pause = None;
                crate::blocking::unpause(&mut s);
            }
        }
        // Save points.
        if !s.persist.bgsave_in_progress && !s.persist.aof_rewrite_in_progress {
            let elapsed = now / 1000 - s.persist.lastsave;
            let dirty = s.dirty;
            let trigger = s
                .cfg
                .save_params
                .iter()
                .any(|&(secs, changes)| dirty >= changes && elapsed as u64 >= secs && changes > 0);
            let retry_ok = s.persist.last_bgsave_ok || elapsed >= 5;
            if (trigger && retry_ok) || s.persist.bgsave_scheduled {
                s.persist.bgsave_scheduled = false;
                crate::rdb::bgsave(&mut s, shared.clone());
            }
            if s.persist.aof_rewrite_scheduled {
                s.persist.aof_rewrite_scheduled = false;
                crate::aof::bgrewrite(&mut s, shared.clone());
            } else if s.aof.is_some() && s.cfg.auto_aof_rewrite_percentage > 0 {
                let base = s.persist.aof_base_size.max(1);
                let cur = s.persist.aof_current_size;
                if cur >= s.cfg.auto_aof_rewrite_min_size
                    && (cur.saturating_sub(base)) * 100 / base >= s.cfg.auto_aof_rewrite_percentage
                {
                    crate::aof::bgrewrite(&mut s, shared.clone());
                }
            }
        }
        crate::repl::cron(&mut s, &shared, tick);
        crate::cluster::cron(&mut s, &shared, tick);
        crate::evict::maybe_evict(&mut s);
        if s.shutdown_asap {
            drop(s);
            shared.shutdown.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiskErr {
    Rdb,
    Aof,
}

/// writeCommandsDeniedByDiskError()
pub fn disk_error(s: &Store) -> Option<DiskErr> {
    if s.repl.is_replica() {
        return None;
    }
    if s.cfg.stop_writes_on_bgsave_error && !s.cfg.save_params.is_empty() && !s.persist.last_bgsave_ok {
        return Some(DiskErr::Rdb);
    }
    if s.aof.is_some() && !s.persist.aof_last_write_ok {
        return Some(DiskErr::Aof);
    }
    None
}
