//! Server configuration: redis.conf parsing, CONFIG GET / SET / REWRITE.

use std::collections::BTreeMap;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Bool,
    Int(i64, i64),
    Mem,
    Enum(&'static [&'static str]),
    Str,
    Special,
}

struct Def {
    name: &'static str,
    alias: Option<&'static str>,
    kind: Kind,
    default: &'static str,
    /// Can be modified with CONFIG SET.
    mutable: bool,
}

macro_rules! d {
    ($n:expr, $k:expr, $def:expr) => {
        Def { name: $n, alias: None, kind: $k, default: $def, mutable: true }
    };
    ($n:expr, $k:expr, $def:expr, alias = $a:expr) => {
        Def { name: $n, alias: Some($a), kind: $k, default: $def, mutable: true }
    };
    ($n:expr, $k:expr, $def:expr, immutable) => {
        Def { name: $n, alias: None, kind: $k, default: $def, mutable: false }
    };
}

const I64MAX: i64 = i64::MAX;
const POLICIES: &[&str] = &[
    "volatile-lru",
    "volatile-lfu",
    "volatile-random",
    "volatile-ttl",
    "allkeys-lru",
    "allkeys-lfu",
    "allkeys-random",
    "noeviction",
];

static DEFS: &[Def] = &[
    d!("port", Kind::Int(0, 65535), "6379"),
    d!("bind", Kind::Str, "* -::*"),
    d!("protected-mode", Kind::Bool, "no"),
    d!("requirepass", Kind::Str, ""),
    d!("masterauth", Kind::Str, ""),
    d!("masteruser", Kind::Str, ""),
    d!("databases", Kind::Int(1, 1 << 20), "16", immutable),
    d!("dir", Kind::Special, "."),
    d!("dbfilename", Kind::Str, "dump.rdb"),
    d!("appendonly", Kind::Bool, "no"),
    d!("appendfilename", Kind::Str, "appendonly.aof", immutable),
    d!("appenddirname", Kind::Str, "appendonlydir", immutable),
    d!("appendfsync", Kind::Enum(&["always", "everysec", "no"]), "everysec"),
    d!("aof-use-rdb-preamble", Kind::Bool, "yes"),
    d!("aof-timestamp-enabled", Kind::Bool, "no"),
    d!("auto-aof-rewrite-percentage", Kind::Int(0, I64MAX), "100"),
    d!("auto-aof-rewrite-min-size", Kind::Mem, "67108864"),
    d!("aof-load-truncated", Kind::Bool, "yes"),
    d!("no-appendfsync-on-rewrite", Kind::Bool, "no"),
    d!("save", Kind::Special, "3600 1 300 100 60 10000"),
    d!("rdbcompression", Kind::Bool, "yes"),
    d!("rdbchecksum", Kind::Bool, "yes"),
    d!("rdb-del-sync-files", Kind::Bool, "no"),
    d!("stop-writes-on-bgsave-error", Kind::Bool, "yes"),
    d!("maxmemory", Kind::Mem, "0"),
    d!("maxmemory-policy", Kind::Enum(POLICIES), "noeviction"),
    d!("maxmemory-samples", Kind::Int(1, 64), "5"),
    d!("maxmemory-eviction-tenacity", Kind::Int(0, 100), "10"),
    d!("maxmemory-clients", Kind::Mem, "0"),
    d!("maxclients", Kind::Int(1, I64MAX), "10000"),
    d!("timeout", Kind::Int(0, I64MAX), "0"),
    d!("tcp-keepalive", Kind::Int(0, I64MAX), "300"),
    d!("tcp-backlog", Kind::Int(0, I64MAX), "511", immutable),
    d!("loglevel", Kind::Enum(&["debug", "verbose", "notice", "warning", "nothing"]), "notice"),
    d!("logfile", Kind::Str, "", immutable),
    d!("daemonize", Kind::Bool, "no", immutable),
    d!("pidfile", Kind::Str, "", immutable),
    d!("hz", Kind::Int(1, 500), "10"),
    d!("dynamic-hz", Kind::Bool, "yes"),
    d!("notify-keyspace-events", Kind::Special, ""),
    d!("hash-max-listpack-entries", Kind::Int(0, I64MAX), "128", alias = "hash-max-ziplist-entries"),
    d!("hash-max-listpack-value", Kind::Int(0, I64MAX), "64", alias = "hash-max-ziplist-value"),
    d!("list-max-listpack-size", Kind::Int(i64::MIN, I64MAX), "-2", alias = "list-max-ziplist-size"),
    d!("list-compress-depth", Kind::Int(0, I64MAX), "0"),
    d!("set-max-intset-entries", Kind::Int(0, I64MAX), "512"),
    d!("set-max-listpack-entries", Kind::Int(0, I64MAX), "128"),
    d!("set-max-listpack-value", Kind::Int(0, I64MAX), "64"),
    d!("zset-max-listpack-entries", Kind::Int(0, I64MAX), "128", alias = "zset-max-ziplist-entries"),
    d!("zset-max-listpack-value", Kind::Int(0, I64MAX), "64", alias = "zset-max-ziplist-value"),
    d!("hll-sparse-max-bytes", Kind::Mem, "3000"),
    d!("stream-node-max-bytes", Kind::Mem, "4096"),
    d!("stream-node-max-entries", Kind::Int(0, I64MAX), "100"),
    d!("busy-reply-threshold", Kind::Int(0, I64MAX), "5000", alias = "lua-time-limit"),
    d!("slowlog-log-slower-than", Kind::Int(-1, I64MAX), "10000"),
    d!("slowlog-max-len", Kind::Int(0, I64MAX), "128"),
    d!("latency-monitor-threshold", Kind::Int(0, I64MAX), "0"),
    d!("latency-tracking", Kind::Bool, "yes"),
    d!("latency-tracking-info-percentiles", Kind::Str, "50 99 99.9"),
    d!("replicaof", Kind::Special, "", alias = "slaveof"),
    d!("replica-read-only", Kind::Bool, "yes", alias = "slave-read-only"),
    d!("replica-serve-stale-data", Kind::Bool, "yes", alias = "slave-serve-stale-data"),
    d!("replica-priority", Kind::Int(0, I64MAX), "100", alias = "slave-priority"),
    d!("replica-announce-ip", Kind::Str, ""),
    d!("replica-announce-port", Kind::Int(0, 65535), "0"),
    d!("replica-lazy-flush", Kind::Bool, "no"),
    d!("repl-backlog-size", Kind::Mem, "1048576"),
    d!("repl-backlog-ttl", Kind::Int(0, I64MAX), "3600"),
    d!("repl-timeout", Kind::Int(1, I64MAX), "60"),
    d!("repl-ping-replica-period", Kind::Int(1, I64MAX), "10"),
    d!("repl-diskless-sync", Kind::Bool, "yes"),
    d!("repl-diskless-sync-delay", Kind::Int(0, I64MAX), "5"),
    d!("repl-diskless-sync-max-replicas", Kind::Int(0, I64MAX), "0"),
    d!("repl-diskless-load", Kind::Enum(&["disabled", "on-empty-db", "swapdb"]), "disabled"),
    d!("repl-disable-tcp-nodelay", Kind::Bool, "no"),
    d!("min-replicas-to-write", Kind::Int(0, I64MAX), "0", alias = "min-slaves-to-write"),
    d!("min-replicas-max-lag", Kind::Int(0, I64MAX), "10", alias = "min-slaves-max-lag"),
    d!("proto-max-bulk-len", Kind::Mem, "536870912"),
    d!("client-query-buffer-limit", Kind::Mem, "1073741824"),
    d!("client-output-buffer-limit", Kind::Str, "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"),
    d!("lazyfree-lazy-eviction", Kind::Bool, "no"),
    d!("lazyfree-lazy-expire", Kind::Bool, "no"),
    d!("lazyfree-lazy-server-del", Kind::Bool, "no"),
    d!("lazyfree-lazy-user-del", Kind::Bool, "no"),
    d!("lazyfree-lazy-user-flush", Kind::Bool, "no"),
    d!("activerehashing", Kind::Bool, "yes"),
    d!("active-expire-effort", Kind::Int(1, 10), "1"),
    d!("activedefrag", Kind::Bool, "no"),
    d!("lfu-log-factor", Kind::Int(0, I64MAX), "10"),
    d!("lfu-decay-time", Kind::Int(0, I64MAX), "1"),
    d!("enable-debug-command", Kind::Enum(&["yes", "no", "local"]), "yes"),
    d!("enable-protected-configs", Kind::Enum(&["yes", "no", "local"]), "yes"),
    d!("enable-module-command", Kind::Enum(&["yes", "no", "local"]), "no"),
    d!("cluster-enabled", Kind::Bool, "no", immutable),
    d!("cluster-config-file", Kind::Str, "nodes.conf", immutable),
    d!("cluster-node-timeout", Kind::Int(0, I64MAX), "15000"),
    d!("cluster-require-full-coverage", Kind::Bool, "yes"),
    d!("cluster-allow-reads-when-down", Kind::Bool, "no"),
    d!("cluster-announce-ip", Kind::Str, ""),
    d!("cluster-port", Kind::Int(0, 65535), "0", immutable),
    d!("aclfile", Kind::Str, "", immutable),
    d!("acllog-max-len", Kind::Int(0, I64MAX), "128"),
    d!("acl-pubsub-default", Kind::Enum(&["allchannels", "resetchannels"]), "resetchannels"),
    d!("io-threads", Kind::Int(1, 128), "1", immutable),
    d!("io-threads-do-reads", Kind::Bool, "no", immutable),
    d!("tracking-table-max-keys", Kind::Int(0, I64MAX), "1000000"),
    d!("set-proc-title", Kind::Bool, "yes"),
    d!("proc-title-template", Kind::Str, "{title} {listen-addr} {server-mode}"),
    d!("always-show-logo", Kind::Bool, "no"),
    d!("supervised", Kind::Enum(&["upstart", "systemd", "auto", "no"]), "no", immutable),
    d!("unixsocket", Kind::Str, "", immutable),
    d!("unixsocketperm", Kind::Int(0, 0o777), "0", immutable),
    d!("lua-replicate-commands", Kind::Bool, "yes"),
    d!("shutdown-timeout", Kind::Int(0, I64MAX), "10"),
    d!("shutdown-on-sigint", Kind::Str, "default"),
    d!("shutdown-on-sigterm", Kind::Str, "default"),
    d!("crash-log-enabled", Kind::Bool, "yes"),
    d!("crash-memcheck-enabled", Kind::Bool, "yes"),
    d!("oom-score-adj", Kind::Enum(&["no", "yes", "relative", "absolute"]), "no"),
    d!("oom-score-adj-values", Kind::Str, "0 200 800"),
    d!("disable-thp", Kind::Bool, "yes"),
    d!("jemalloc-bg-thread", Kind::Bool, "yes"),
    d!("syslog-enabled", Kind::Bool, "no", immutable),
    d!("syslog-ident", Kind::Str, "redis", immutable),
    d!("rdb-key-save-delay", Kind::Int(i64::MIN, I64MAX), "0"),
    d!("key-load-delay", Kind::Int(i64::MIN, I64MAX), "0"),
    d!("active-defrag-cycle-min", Kind::Int(1, 99), "1"),
    d!("active-defrag-cycle-max", Kind::Int(1, 99), "25"),
    d!("active-defrag-threshold-lower", Kind::Int(0, 1000), "10"),
    d!("active-defrag-threshold-upper", Kind::Int(0, 1000), "100"),
    d!("active-defrag-ignore-bytes", Kind::Mem, "104857600"),
    d!("lazyfree-lazy-user-flush", Kind::Bool, "no"),
    d!("propagation-error-behavior", Kind::Enum(&["ignore", "panic", "panic-on-replicas"]), "ignore"),
    d!("replica-ignore-disk-write-errors", Kind::Bool, "no"),
    d!("hide-user-data-from-log", Kind::Bool, "no"),
    d!("max-new-connections-per-cycle", Kind::Int(1, 1000), "10"),
    d!("max-new-tls-connections-per-cycle", Kind::Int(1, 1000), "1"),
    d!("locale-collate", Kind::Str, ""),
    d!("tls-port", Kind::Int(0, 65535), "0", immutable),
];

/// Typed, hot-path configuration. The string table is the source of truth.
#[derive(Clone)]
pub struct Config {
    values: BTreeMap<&'static str, String>,
    pub port: u16,
    pub bind: Vec<String>,
    pub requirepass: Option<Vec<u8>>,
    pub databases: usize,
    pub dir: String,
    pub dbfilename: String,
    pub appendonly: bool,
    pub appendfilename: String,
    pub appenddirname: String,
    pub appendfsync: String,
    pub aof_use_rdb_preamble: bool,
    pub save_params: Vec<(u64, u64)>,
    pub maxmemory: u64,
    pub maxmemory_policy: String,
    pub maxmemory_samples: usize,
    pub maxclients: usize,
    pub timeout: u64,
    pub hz: u64,
    pub notify_flags: u32,
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_value: usize,
    pub list_max_listpack_size: i64,
    pub set_max_intset_entries: usize,
    pub set_max_listpack_entries: usize,
    pub set_max_listpack_value: usize,
    pub zset_max_listpack_entries: usize,
    pub zset_max_listpack_value: usize,
    pub hll_sparse_max_bytes: usize,
    pub stream_node_max_entries: usize,
    pub stream_node_max_bytes: usize,
    pub busy_reply_threshold: u64,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub latency_monitor_threshold: u64,
    pub replica_read_only: bool,
    pub repl_backlog_size: usize,
    pub proto_max_bulk_len: usize,
    pub client_query_buffer_limit: usize,
    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub acllog_max_len: usize,
    pub tracking_table_max_keys: usize,
    pub min_replicas_to_write: usize,
    pub min_replicas_max_lag: u64,
    pub stop_writes_on_bgsave_error: bool,
    pub active_expire_effort: u64,
    pub masterauth: Option<String>,
    pub masteruser: Option<String>,
    pub loglevel: String,
    pub logfile: String,
    pub cluster_enabled: bool,
    pub cluster_node_timeout: u64,
    pub cluster_config_file: String,
    pub aclfile: String,
    pub repl_timeout: u64,
    pub repl_ping_period: u64,
    pub replica_priority: u64,
    pub replica_announce_ip: String,
    pub replica_serve_stale_data: bool,
    pub enable_debug_command: String,
    pub auto_aof_rewrite_percentage: u64,
    pub auto_aof_rewrite_min_size: u64,
    pub rdb_key_save_delay: i64,
    pub key_load_delay: i64,
    pub acl_pubsub_default_all: bool,
    /// Pending "replicaof" set from config file.
    pub replicaof: Option<(String, u16)>,
    pub config_file: Option<String>,
    /// `user` directives from the config file (ACL rules).
    pub users: Vec<Vec<String>>,
    pub cluster_require_full_coverage: bool,
    pub cluster_allow_reads_when_down: bool,
    pub cluster_port: u16,
    pub cluster_announce_ip: String,
    pub unixsocket: String,
    pub pidfile: String,
    pub aof_timestamp_enabled: bool,
    pub aof_load_truncated: bool,
}

fn find_def(name: &str) -> Option<&'static Def> {
    DEFS.iter().find(|d| d.name == name || d.alias == Some(name))
}

/// Parse memory units like Redis memtoull: 1k=1000, 1kb=1024, etc.
pub fn parse_mem(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mul): (&str, u64) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1 << 30)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1 << 10)
    } else if let Some(n) = s.strip_suffix('g') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1000)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    if num.starts_with('-') {
        // negative memory values are only allowed for some ints; treat via i64
        return None;
    }
    num.parse::<u64>().ok()?.checked_mul(mul)
}

pub const NOTIFY_KEYSPACE: u32 = 1 << 0;
pub const NOTIFY_KEYEVENT: u32 = 1 << 1;
pub const NOTIFY_GENERIC: u32 = 1 << 2;
pub const NOTIFY_STRING: u32 = 1 << 3;
pub const NOTIFY_LIST: u32 = 1 << 4;
pub const NOTIFY_SET: u32 = 1 << 5;
pub const NOTIFY_HASH: u32 = 1 << 6;
pub const NOTIFY_ZSET: u32 = 1 << 7;
pub const NOTIFY_EXPIRED: u32 = 1 << 8;
pub const NOTIFY_EVICTED: u32 = 1 << 9;
pub const NOTIFY_STREAM: u32 = 1 << 10;
pub const NOTIFY_KEY_MISS: u32 = 1 << 11;
pub const NOTIFY_LOADED: u32 = 1 << 12;
pub const NOTIFY_MODULE: u32 = 1 << 13;
pub const NOTIFY_NEW: u32 = 1 << 14;
pub const NOTIFY_ALL: u32 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED
    | NOTIFY_STREAM
    | NOTIFY_MODULE;

pub fn notify_flags_from_str(s: &str) -> Option<u32> {
    let mut f = 0;
    for c in s.chars() {
        f |= match c {
            'A' => NOTIFY_ALL,
            'g' => NOTIFY_GENERIC,
            '$' => NOTIFY_STRING,
            'l' => NOTIFY_LIST,
            's' => NOTIFY_SET,
            'h' => NOTIFY_HASH,
            'z' => NOTIFY_ZSET,
            'x' => NOTIFY_EXPIRED,
            'e' => NOTIFY_EVICTED,
            'K' => NOTIFY_KEYSPACE,
            'E' => NOTIFY_KEYEVENT,
            't' => NOTIFY_STREAM,
            'm' => NOTIFY_KEY_MISS,
            'd' => NOTIFY_MODULE,
            'n' => NOTIFY_NEW,
            _ => return None,
        };
    }
    Some(f)
}

pub fn notify_flags_to_str(f: u32) -> String {
    let mut s = String::new();
    if f & NOTIFY_ALL == NOTIFY_ALL {
        s.push('A');
    } else {
        for (bit, c) in [
            (NOTIFY_GENERIC, 'g'),
            (NOTIFY_STRING, '$'),
            (NOTIFY_LIST, 'l'),
            (NOTIFY_SET, 's'),
            (NOTIFY_HASH, 'h'),
            (NOTIFY_ZSET, 'z'),
            (NOTIFY_EXPIRED, 'x'),
            (NOTIFY_EVICTED, 'e'),
            (NOTIFY_STREAM, 't'),
            (NOTIFY_MODULE, 'd'),
        ] {
            if f & bit != 0 {
                s.push(c);
            }
        }
    }
    if f & NOTIFY_KEYSPACE != 0 {
        s.push('K');
    }
    if f & NOTIFY_KEYEVENT != 0 {
        s.push('E');
    }
    if f & NOTIFY_KEY_MISS != 0 {
        s.push('m');
    }
    if f & NOTIFY_NEW != 0 {
        s.push('n');
    }
    s
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        let mut values = BTreeMap::new();
        for d in DEFS {
            values.insert(d.name, d.default.to_string());
        }
        let mut c = Config {
            values,
            port: 6379,
            bind: vec![],
            requirepass: None,
            databases: 16,
            dir: ".".into(),
            dbfilename: "dump.rdb".into(),
            appendonly: false,
            appendfilename: "appendonly.aof".into(),
            appenddirname: "appendonlydir".into(),
            appendfsync: "everysec".into(),
            aof_use_rdb_preamble: true,
            save_params: vec![],
            maxmemory: 0,
            maxmemory_policy: "noeviction".into(),
            maxmemory_samples: 5,
            maxclients: 10000,
            timeout: 0,
            hz: 10,
            notify_flags: 0,
            hash_max_listpack_entries: 128,
            hash_max_listpack_value: 64,
            list_max_listpack_size: -2,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            hll_sparse_max_bytes: 3000,
            stream_node_max_entries: 100,
            stream_node_max_bytes: 4096,
            busy_reply_threshold: 5000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            replica_read_only: true,
            repl_backlog_size: 1 << 20,
            proto_max_bulk_len: 512 << 20,
            client_query_buffer_limit: 1 << 30,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            rdbcompression: true,
            rdbchecksum: true,
            acllog_max_len: 128,
            tracking_table_max_keys: 1_000_000,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,
            stop_writes_on_bgsave_error: true,
            active_expire_effort: 1,
            masterauth: None,
            masteruser: None,
            loglevel: "notice".into(),
            logfile: String::new(),
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            cluster_config_file: "nodes.conf".into(),
            aclfile: String::new(),
            repl_timeout: 60,
            repl_ping_period: 10,
            replica_priority: 100,
            replica_announce_ip: String::new(),
            replica_serve_stale_data: true,
            enable_debug_command: "yes".into(),
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 << 20,
            rdb_key_save_delay: 0,
            key_load_delay: 0,
            acl_pubsub_default_all: false,
            replicaof: None,
            config_file: None,
            users: Vec::new(),
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_port: 0,
            cluster_announce_ip: String::new(),
            unixsocket: String::new(),
            pidfile: String::new(),
            aof_timestamp_enabled: false,
            aof_load_truncated: true,
        };
        for d in DEFS {
            let _ = c.apply(d.name, d.default);
        }
        c
    }

    pub fn list_max_listpack_size_entries(&self) -> usize {
        if self.list_max_listpack_size > 0 { self.list_max_listpack_size as usize } else { 128 }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        let d = find_def(name)?;
        self.values.get(d.name).map(|s| s.as_str())
    }

    /// CONFIG GET pattern: returns (name, value) for matching params (aliases match too).
    pub fn get_matching(&self, pattern: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let has_glob = pattern.contains(['*', '?', '[']);
        for d in DEFS {
            for name in std::iter::once(d.name).chain(d.alias) {
                let matched = if has_glob {
                    crate::util::glob_match(pattern.as_bytes(), name.as_bytes(), true)
                } else {
                    name.eq_ignore_ascii_case(pattern)
                };
                if matched && !out.iter().any(|(n, _): &(String, String)| n == name) {
                    out.push((name.to_string(), self.values[d.name].clone()));
                }
            }
        }
        out
    }

    pub fn is_mutable(&self, name: &str) -> Option<bool> {
        find_def(name).map(|d| d.mutable)
    }

    pub fn canonical(name: &str) -> Option<&'static str> {
        find_def(name).map(|d| d.name)
    }

    /// Validate and set a parameter. Returns an error message on failure.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), String> {
        let lname = name.to_ascii_lowercase();
        let d = find_def(&lname).ok_or_else(|| format!("Unknown option '{}'", name))?;
        let norm: String = match d.kind {
            Kind::Bool => match value.to_ascii_lowercase().as_str() {
                "yes" => "yes".into(),
                "no" => "no".into(),
                _ => return Err("argument must be 'yes' or 'no'".into()),
            },
            Kind::Int(lo, hi) => {
                let v: i64 = value
                    .parse()
                    .map_err(|_| "argument couldn't be parsed into an integer".to_string())?;
                if v < lo || v > hi {
                    return Err(format!("argument must be between {} and {} inclusive", lo, hi));
                }
                v.to_string()
            }
            Kind::Mem => {
                let v = parse_mem(value)
                    .ok_or_else(|| "argument must be a memory value".to_string())?;
                v.to_string()
            }
            Kind::Enum(opts) => {
                let lv = value.to_ascii_lowercase();
                if !opts.contains(&lv.as_str()) {
                    return Err(format!("argument(s) must be one of the following: {}", opts.join(", ")));
                }
                lv
            }
            Kind::Str => value.to_string(),
            Kind::Special => match d.name {
                "save" => {
                    let v = value.trim();
                    if !v.is_empty() {
                        let parts: Vec<&str> = v.split_whitespace().collect();
                        if parts.len() % 2 != 0 || parts.iter().any(|p| p.parse::<u64>().is_err()) {
                            return Err("Invalid save parameters".into());
                        }
                    }
                    v.split_whitespace().collect::<Vec<_>>().join(" ")
                }
                "notify-keyspace-events" => {
                    let f = notify_flags_from_str(value)
                        .ok_or_else(|| "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.".to_string())?;
                    notify_flags_to_str(f)
                }
                "dir" => {
                    std::env::set_current_dir(value).map_err(|e| e.to_string())?;
                    std::env::current_dir()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| value.to_string())
                }
                "replicaof" => {
                    let v = value.trim();
                    if v.is_empty() || v.eq_ignore_ascii_case("no one") {
                        String::new()
                    } else {
                        let parts: Vec<&str> = v.split_whitespace().collect();
                        if parts.len() != 2 || parts[1].parse::<u16>().is_err() {
                            return Err("Invalid master host/port".into());
                        }
                        format!("{} {}", parts[0], parts[1])
                    }
                }
                _ => value.to_string(),
            },
        };
        self.apply(d.name, &norm)?;
        self.values.insert(d.name, norm);
        Ok(())
    }

    fn apply(&mut self, name: &str, v: &str) -> Result<(), String> {
        let i = || v.parse::<i64>().unwrap_or(0);
        let u = || v.parse::<u64>().unwrap_or(0);
        let b = || v == "yes";
        match name {
            "port" => self.port = u() as u16,
            "bind" => self.bind = v.split_whitespace().map(String::from).collect(),
            "requirepass" => {
                self.requirepass = if v.is_empty() { None } else { Some(v.as_bytes().to_vec()) }
            }
            "masterauth" => self.masterauth = if v.is_empty() { None } else { Some(v.into()) },
            "masteruser" => self.masteruser = if v.is_empty() { None } else { Some(v.into()) },
            "databases" => self.databases = u() as usize,
            "dir" => self.dir = v.into(),
            "dbfilename" => {
                if v.contains('/') {
                    return Err("dbfilename can't be a path, just a filename".into());
                }
                self.dbfilename = v.into()
            }
            "appendonly" => self.appendonly = b(),
            "appendfilename" => self.appendfilename = v.into(),
            "appenddirname" => self.appenddirname = v.into(),
            "appendfsync" => self.appendfsync = v.into(),
            "aof-use-rdb-preamble" => self.aof_use_rdb_preamble = b(),
            "save" => {
                let parts: Vec<u64> = v.split_whitespace().filter_map(|p| p.parse().ok()).collect();
                self.save_params = parts.chunks(2).filter(|c| c.len() == 2).map(|c| (c[0], c[1])).collect();
            }
            "maxmemory" => self.maxmemory = u(),
            "maxmemory-policy" => self.maxmemory_policy = v.into(),
            "maxmemory-samples" => self.maxmemory_samples = u() as usize,
            "maxclients" => self.maxclients = u() as usize,
            "timeout" => self.timeout = u(),
            "hz" => self.hz = u().clamp(1, 500),
            "notify-keyspace-events" => self.notify_flags = notify_flags_from_str(v).unwrap_or(0),
            "hash-max-listpack-entries" => self.hash_max_listpack_entries = u() as usize,
            "hash-max-listpack-value" => self.hash_max_listpack_value = u() as usize,
            "list-max-listpack-size" => self.list_max_listpack_size = i(),
            "set-max-intset-entries" => self.set_max_intset_entries = u() as usize,
            "set-max-listpack-entries" => self.set_max_listpack_entries = u() as usize,
            "set-max-listpack-value" => self.set_max_listpack_value = u() as usize,
            "zset-max-listpack-entries" => self.zset_max_listpack_entries = u() as usize,
            "zset-max-listpack-value" => self.zset_max_listpack_value = u() as usize,
            "hll-sparse-max-bytes" => self.hll_sparse_max_bytes = u() as usize,
            "stream-node-max-entries" => self.stream_node_max_entries = u() as usize,
            "stream-node-max-bytes" => self.stream_node_max_bytes = u() as usize,
            "busy-reply-threshold" => self.busy_reply_threshold = u(),
            "slowlog-log-slower-than" => self.slowlog_log_slower_than = i(),
            "slowlog-max-len" => self.slowlog_max_len = u() as usize,
            "latency-monitor-threshold" => self.latency_monitor_threshold = u(),
            "replica-read-only" => self.replica_read_only = b(),
            "replica-serve-stale-data" => self.replica_serve_stale_data = b(),
            "repl-backlog-size" => self.repl_backlog_size = (u() as usize).max(16 * 1024),
            "proto-max-bulk-len" => self.proto_max_bulk_len = u() as usize,
            "client-query-buffer-limit" => self.client_query_buffer_limit = u() as usize,
            "lfu-log-factor" => self.lfu_log_factor = u() as u32,
            "lfu-decay-time" => self.lfu_decay_time = u() as u32,
            "rdbcompression" => self.rdbcompression = b(),
            "rdbchecksum" => self.rdbchecksum = b(),
            "acllog-max-len" => self.acllog_max_len = u() as usize,
            "tracking-table-max-keys" => self.tracking_table_max_keys = u() as usize,
            "min-replicas-to-write" => self.min_replicas_to_write = u() as usize,
            "min-replicas-max-lag" => self.min_replicas_max_lag = u(),
            "stop-writes-on-bgsave-error" => self.stop_writes_on_bgsave_error = b(),
            "active-expire-effort" => self.active_expire_effort = u(),
            "loglevel" => self.loglevel = v.into(),
            "logfile" => self.logfile = v.into(),
            "cluster-enabled" => self.cluster_enabled = b(),
            "cluster-node-timeout" => self.cluster_node_timeout = u(),
            "cluster-config-file" => self.cluster_config_file = v.into(),
            "aclfile" => self.aclfile = v.into(),
            "repl-timeout" => self.repl_timeout = u(),
            "repl-ping-replica-period" => self.repl_ping_period = u(),
            "replica-priority" => self.replica_priority = u(),
            "replica-announce-ip" => self.replica_announce_ip = v.into(),
            "enable-debug-command" => self.enable_debug_command = v.into(),
            "auto-aof-rewrite-percentage" => self.auto_aof_rewrite_percentage = u(),
            "auto-aof-rewrite-min-size" => self.auto_aof_rewrite_min_size = u(),
            "rdb-key-save-delay" => self.rdb_key_save_delay = i(),
            "key-load-delay" => self.key_load_delay = i(),
            "acl-pubsub-default" => self.acl_pubsub_default_all = v == "allchannels",
            "cluster-require-full-coverage" => self.cluster_require_full_coverage = b(),
            "cluster-allow-reads-when-down" => self.cluster_allow_reads_when_down = b(),
            "cluster-port" => self.cluster_port = u() as u16,
            "cluster-announce-ip" => self.cluster_announce_ip = v.into(),
            "unixsocket" => self.unixsocket = v.into(),
            "pidfile" => self.pidfile = v.into(),
            "aof-timestamp-enabled" => self.aof_timestamp_enabled = b(),
            "aof-load-truncated" => self.aof_load_truncated = b(),
            "replicaof" => {
                let parts: Vec<&str> = v.split_whitespace().collect();
                self.replicaof = if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].parse().unwrap_or(6379)))
                } else {
                    None
                };
            }
            _ => {}
        }
        Ok(())
    }

    /// Load a redis.conf-style file plus command line overrides.
    pub fn load_file(&mut self, path: &str) -> Result<(), String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("Fatal error, can't open config file '{}': {}", path, e))?;
        self.config_file = Some(
            std::fs::canonicalize(path)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| path.to_string()),
        );
        self.load_string(&text)
    }

    pub fn load_string(&mut self, text: &str) -> Result<(), String> {
        let mut save_lines: Vec<String> = Vec::new();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some(args) = crate::resp::split_args(line.as_bytes()) else {
                return Err(format!("Unbalanced quotes in configuration line {}", lineno + 1));
            };
            if args.is_empty() {
                continue;
            }
            let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
            let vals: Vec<String> = args[1..].iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect();
            match name.as_str() {
                "include" => {
                    for v in &vals {
                        let t = std::fs::read_to_string(v).map_err(|e| e.to_string())?;
                        self.load_string(&t)?;
                    }
                    continue;
                }
                "save" => {
                    save_lines.push(vals.join(" "));
                    continue;
                }
                "user" | "rename-command" | "loadmodule" => {
                    // Users are handled by the ACL subsystem; collect them.
                    if name == "user" {
                        self.users.push(vals.clone());
                    }
                    continue;
                }
                "sentinel" => continue,
                _ => {}
            }
            if find_def(&name).is_none() {
                return Err(format!(
                    "*** FATAL CONFIG FILE ERROR ***\nline {}: >>> '{}'\nBad directive or wrong number of arguments",
                    lineno + 1,
                    line
                ));
            }
            self.set(&name, &vals.join(" ")).map_err(|e| {
                format!("*** FATAL CONFIG FILE ERROR ***\nline {}: >>> '{}'\n{}", lineno + 1, line, e)
            })?;
        }
        if !save_lines.is_empty() {
            let joined: Vec<String> = save_lines.into_iter().filter(|s| !s.is_empty() && s != "\"\"").collect();
            self.set("save", &joined.join(" "))?;
        }
        Ok(())
    }

    /// CONFIG REWRITE: rewrite the config file preserving comments/unknown lines.
    pub fn rewrite(&self) -> Result<(), String> {
        let Some(path) = &self.config_file else {
            return Err("The server is running without a config file".into());
        };
        let old = std::fs::read_to_string(path).unwrap_or_default();
        let mut out = Vec::new();
        let mut written = std::collections::HashSet::new();
        for raw in old.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                out.push(raw.to_string());
                continue;
            }
            let name = line.split_whitespace().next().unwrap_or("").to_ascii_lowercase();
            match find_def(&name) {
                Some(d) => {
                    if written.insert(d.name) {
                        out.extend(self.render(d));
                    }
                }
                None => out.push(raw.to_string()),
            }
        }
        for d in DEFS {
            if written.contains(d.name) {
                continue;
            }
            if self.values[d.name] != d.default {
                if out.iter().all(|l| !l.starts_with("# Generated by CONFIG REWRITE")) {
                    out.push("# Generated by CONFIG REWRITE".into());
                }
                out.extend(self.render(d));
            }
        }
        let tmp = format!("{}.tmp-{}", path, std::process::id());
        std::fs::write(&tmp, out.join("\n") + "\n").map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn render(&self, d: &Def) -> Vec<String> {
        let v = &self.values[d.name];
        if d.name == "save" {
            if v.is_empty() {
                return vec!["save \"\"".into()];
            }
            let parts: Vec<&str> = v.split_whitespace().collect();
            return parts.chunks(2).map(|c| format!("save {} {}", c[0], c[1])).collect();
        }
        if d.name == "replicaof" && v.is_empty() {
            return vec![];
        }
        let quoted = if v.is_empty() || v.contains([' ', '"', '\'']) && d.kind != Kind::Special && d.name != "bind" {
            format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
        } else {
            v.clone()
        };
        vec![format!("{} {}", d.name, quoted)]
    }
}

