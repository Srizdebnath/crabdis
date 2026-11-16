//! Command table: names, arity, flags, ACL categories, key specs, handlers.

#[macro_use]
pub mod util;
pub mod acl_cmd;
pub mod admin;
pub mod bitops;
pub mod client;
pub mod cluster_cmd;
pub mod connection;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hll;
pub mod list;
pub mod multi;
pub mod pubsub_cmd;
pub mod repl_cmd;
pub mod script_cmd;
pub mod set;
pub mod sort;
pub mod stream_cmd;
pub mod string;
pub mod zset;

use crate::exec::Ctx;
use crate::resp::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

pub type Handler = fn(&mut Ctx, &[Bytes]) -> Frame;

pub mod flags {
    pub const WRITE: u64 = 1 << 0;
    pub const READONLY: u64 = 1 << 1;
    pub const DENYOOM: u64 = 1 << 2;
    pub const ADMIN: u64 = 1 << 3;
    pub const PUBSUB: u64 = 1 << 4;
    pub const NOSCRIPT: u64 = 1 << 5;
    pub const BLOCKING: u64 = 1 << 6;
    pub const LOADING: u64 = 1 << 7;
    pub const STALE: u64 = 1 << 8;
    pub const SKIP_MONITOR: u64 = 1 << 9;
    pub const NO_SLOWLOG: u64 = 1 << 10;
    pub const FAST: u64 = 1 << 11;
    pub const NO_AUTH: u64 = 1 << 12;
    pub const MAY_REPLICATE: u64 = 1 << 13;
    pub const NO_MULTI: u64 = 1 << 14;
    pub const ALLOW_BUSY: u64 = 1 << 15;
    pub const NO_MANDATORY_KEYS: u64 = 1 << 16;
    pub const PROTECTED: u64 = 1 << 17;
    pub const NO_ASYNC_LOADING: u64 = 1 << 18;
    pub const ASKING: u64 = 1 << 19;
    pub const SENTINEL: u64 = 1 << 20;
    /// Internal: allowed in RESP2 pub/sub context.
    pub const PUBSUB_CTX_OK: u64 = 1 << 40;
    /// Internal: never propagate.
    pub const NO_PROPAGATE: u64 = 1 << 41;
    /// Internal: write-ness depends on arguments (e.g. SORT STORE, EVAL).
    pub const DYN_WRITE: u64 = 1 << 42;
}

pub mod cat {
    pub const KEYSPACE: u64 = 1 << 0;
    pub const READ: u64 = 1 << 1;
    pub const WRITE: u64 = 1 << 2;
    pub const SET: u64 = 1 << 3;
    pub const SORTEDSET: u64 = 1 << 4;
    pub const LIST: u64 = 1 << 5;
    pub const HASH: u64 = 1 << 6;
    pub const STRING: u64 = 1 << 7;
    pub const BITMAP: u64 = 1 << 8;
    pub const HYPERLOGLOG: u64 = 1 << 9;
    pub const GEO: u64 = 1 << 10;
    pub const STREAM: u64 = 1 << 11;
    pub const PUBSUB: u64 = 1 << 12;
    pub const ADMIN: u64 = 1 << 13;
    pub const FAST: u64 = 1 << 14;
    pub const SLOW: u64 = 1 << 15;
    pub const BLOCKING: u64 = 1 << 16;
    pub const DANGEROUS: u64 = 1 << 17;
    pub const CONNECTION: u64 = 1 << 18;
    pub const TRANSACTION: u64 = 1 << 19;
    pub const SCRIPTING: u64 = 1 << 20;
    pub const NAMES: &[(&str, u64)] = &[
        ("keyspace", KEYSPACE),
        ("read", READ),
        ("write", WRITE),
        ("set", SET),
        ("sortedset", SORTEDSET),
        ("list", LIST),
        ("hash", HASH),
        ("string", STRING),
        ("bitmap", BITMAP),
        ("hyperloglog", HYPERLOGLOG),
        ("geo", GEO),
        ("stream", STREAM),
        ("pubsub", PUBSUB),
        ("admin", ADMIN),
        ("fast", FAST),
        ("slow", SLOW),
        ("blocking", BLOCKING),
        ("dangerous", DANGEROUS),
        ("connection", CONNECTION),
        ("transaction", TRANSACTION),
        ("scripting", SCRIPTING),
    ];
}

#[derive(Clone, Copy)]
pub enum KeySpec {
    None,
    /// first, last (negative = from end), step
    Range(i32, i32, i32),
    /// numkeys argument index, keys start right after it, step 1
    NumKeys(usize),
    /// Custom key extraction.
    Custom(fn(&[Bytes]) -> Vec<usize>),
}

pub struct Cmd {
    pub name: &'static str,
    pub parent: Option<&'static str>,
    pub arity: i32,
    pub flags: u64,
    pub cats: u64,
    pub keys: KeySpec,
    pub f: Handler,
    pub subs: &'static [Cmd],
    pub group: &'static str,
}

impl Cmd {
    pub fn full_name(&self) -> String {
        match self.parent {
            Some(p) => format!("{}|{}", p, self.name),
            None => self.name.to_string(),
        }
    }

    pub fn is_write_for(&self, args: &[Bytes]) -> bool {
        if self.flags & flags::WRITE != 0 {
            return true;
        }
        if self.flags & flags::DYN_WRITE != 0 {
            return match self.name {
                "sort" => args.iter().skip(2).any(|a| a.eq_ignore_ascii_case(b"store")),
                "georadius" | "georadiusbymember" => args
                    .iter()
                    .skip(3)
                    .any(|a| a.eq_ignore_ascii_case(b"store") || a.eq_ignore_ascii_case(b"storedist")),
                _ => false,
            };
        }
        false
    }

    /// ACL categories, including the ones Redis derives from flags.
    pub fn acl_categories(&self) -> u64 {
        let mut c = self.cats;
        if self.flags & flags::WRITE != 0 {
            c |= cat::WRITE;
        }
        if self.flags & flags::READONLY != 0 && c & cat::SCRIPTING == 0 {
            c |= cat::READ;
        }
        if self.flags & flags::ADMIN != 0 {
            c |= cat::ADMIN | cat::DANGEROUS;
        }
        if self.flags & flags::PUBSUB != 0 {
            c |= cat::PUBSUB;
        }
        if self.flags & flags::FAST != 0 {
            c |= cat::FAST;
        }
        if self.flags & flags::BLOCKING != 0 {
            c |= cat::BLOCKING;
        }
        if c & cat::FAST == 0 {
            c |= cat::SLOW;
        }
        c
    }

    pub fn movable_keys(&self) -> bool {
        matches!(self.keys, KeySpec::NumKeys(_) | KeySpec::Custom(_))
    }

    /// (first, last, step) for COMMAND INFO legacy fields.
    pub fn legacy_range(&self) -> (i64, i64, i64) {
        match self.keys {
            KeySpec::None => (0, 0, 0),
            KeySpec::Range(a, b, c) => (a as i64, b as i64, c as i64),
            KeySpec::NumKeys(_) | KeySpec::Custom(_) => (0, 0, 0),
        }
    }

    pub fn key_indexes(&self, args: &[Bytes]) -> Vec<usize> {
        let n = args.len() as i32;
        match self.keys {
            KeySpec::None => vec![],
            KeySpec::Range(first, last, step) => {
                if first == 0 || first >= n {
                    return vec![];
                }
                let last = if last < 0 { n + last } else { last.min(n - 1) };
                let mut v = Vec::new();
                let mut i = first;
                while i <= last && i < n {
                    v.push(i as usize);
                    i += step.max(1);
                }
                v
            }
            KeySpec::NumKeys(idx) => {
                let Some(nk) = args.get(idx).and_then(|a| crate::util::parse_i64(a)) else { return vec![] };
                if nk <= 0 {
                    return vec![];
                }
                (idx + 1..(idx + 1 + nk as usize).min(args.len())).collect()
            }
            KeySpec::Custom(f) => f(args),
        }
    }
}

use cat as C;
use flags as F;

macro_rules! cmd {
    ($name:literal, $arity:expr, $flags:expr, $cats:expr, $keys:expr, $f:expr, $group:literal) => {
        Cmd { name: $name, parent: None, arity: $arity, flags: $flags, cats: $cats, keys: $keys, f: $f, subs: &[], group: $group }
    };
}
macro_rules! sub {
    ($parent:literal, $name:literal, $arity:expr, $flags:expr, $cats:expr, $keys:expr, $f:expr) => {
        Cmd { name: $name, parent: Some($parent), arity: $arity, flags: $flags, cats: $cats, keys: $keys, f: $f, subs: &[], group: "" }
    };
}
macro_rules! container {
    ($name:literal, $group:literal, $subs:expr) => {
        Cmd { name: $name, parent: None, arity: -2, flags: 0, cats: 0, keys: KeySpec::None, f: container_help, subs: $subs, group: $group }
    };
}

fn container_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::err("ERR container command")
}

const K1: KeySpec = KeySpec::Range(1, 1, 1);
const K12: KeySpec = KeySpec::Range(1, 2, 1);
const KALL: KeySpec = KeySpec::Range(1, -1, 1);
const KN: KeySpec = KeySpec::None;

const W: u64 = F::WRITE;
const R: u64 = F::READONLY;
const WD: u64 = F::WRITE | F::DENYOOM;
const WDF: u64 = F::WRITE | F::DENYOOM | F::FAST;
const WF: u64 = F::WRITE | F::FAST;
const RF: u64 = F::READONLY | F::FAST;

use acl_cmd as acl;
use admin as adm;
use bitops as bit;
use client as cli;
use cluster_cmd as clu;
use connection as con;
use generic as gn;
use pubsub_cmd as ps;
use repl_cmd as rep;
use script_cmd as scr;
use stream_cmd as xs;
use string as st;

static CONFIG_SUBS: &[Cmd] = &[
    sub!("config", "get", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::config_get),
    sub!("config", "set", -4, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::config_set),
    sub!("config", "resetstat", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::config_resetstat),
    sub!("config", "rewrite", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::config_rewrite),
    sub!("config", "help", 2, F::LOADING | F::STALE, 0, KN, adm::config_help),
];

static CLIENT_SUBS: &[Cmd] = &[
    sub!("client", "caching", 3, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::caching),
    sub!("client", "getname", 2, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::getname),
    sub!("client", "getredir", 2, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::getredir),
    sub!("client", "id", 2, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::id),
    sub!("client", "info", 2, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::info),
    sub!("client", "kill", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::kill),
    sub!("client", "list", -2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::list),
    sub!("client", "no-evict", 3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::no_evict),
    sub!("client", "no-touch", 3, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::no_touch),
    sub!("client", "pause", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::pause),
    sub!("client", "reply", 3, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::reply),
    sub!("client", "setinfo", 4, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::setinfo),
    sub!("client", "setname", 3, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::setname),
    sub!("client", "tracking", -3, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::tracking),
    sub!("client", "trackinginfo", 2, F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::trackinginfo),
    sub!("client", "unblock", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::unblock),
    sub!("client", "unpause", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, C::CONNECTION, KN, cli::unpause),
    sub!("client", "help", 2, F::LOADING | F::STALE, C::CONNECTION, KN, cli::help),
];

static OBJECT_SUBS: &[Cmd] = &[
    sub!("object", "encoding", 3, R, C::KEYSPACE, K12_OBJ, gn::object_encoding),
    sub!("object", "freq", 3, R, C::KEYSPACE, K12_OBJ, gn::object_freq),
    sub!("object", "idletime", 3, R, C::KEYSPACE, K12_OBJ, gn::object_idletime),
    sub!("object", "refcount", 3, R, C::KEYSPACE, K12_OBJ, gn::object_refcount),
    sub!("object", "help", 2, F::LOADING | F::STALE, C::KEYSPACE, KN, gn::object_help),
];
const K12_OBJ: KeySpec = KeySpec::Range(2, 2, 1);

static MEMORY_SUBS: &[Cmd] = &[
    sub!("memory", "doctor", 2, 0, 0, KN, adm::memory_doctor),
    sub!("memory", "malloc-stats", 2, 0, 0, KN, adm::memory_malloc_stats),
    sub!("memory", "purge", 2, 0, 0, KN, adm::memory_purge),
    sub!("memory", "stats", 2, 0, 0, KN, adm::memory_stats),
    sub!("memory", "usage", -3, R, C::KEYSPACE, K12_OBJ, adm::memory_usage),
    sub!("memory", "help", 2, F::LOADING | F::STALE, 0, KN, adm::memory_help),
];

static SLOWLOG_SUBS: &[Cmd] = &[
    sub!("slowlog", "get", -2, F::ADMIN | F::LOADING | F::STALE, 0, KN, adm::slowlog_get),
    sub!("slowlog", "len", 2, F::ADMIN | F::LOADING | F::STALE, 0, KN, adm::slowlog_len),
    sub!("slowlog", "reset", 2, F::ADMIN | F::LOADING | F::STALE, 0, KN, adm::slowlog_reset),
    sub!("slowlog", "help", 2, F::LOADING | F::STALE, 0, KN, adm::slowlog_help),
];

static LATENCY_SUBS: &[Cmd] = &[
    sub!("latency", "doctor", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_doctor),
    sub!("latency", "graph", 3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_graph),
    sub!("latency", "histogram", -2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_histogram),
    sub!("latency", "history", 3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_history),
    sub!("latency", "latest", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_latest),
    sub!("latency", "reset", -2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::latency_reset),
    sub!("latency", "help", 2, F::LOADING | F::STALE, 0, KN, adm::latency_help),
];

static COMMAND_SUBS: &[Cmd] = &[
    sub!("command", "count", 2, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_count),
    sub!("command", "docs", -2, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_docs),
    sub!("command", "getkeys", -3, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_getkeys),
    sub!("command", "getkeysandflags", -3, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_getkeysandflags),
    sub!("command", "info", -2, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_info),
    sub!("command", "list", -2, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_list),
    sub!("command", "help", 2, F::LOADING | F::STALE, C::CONNECTION, KN, adm::command_help),
];

static PUBSUB_SUBS: &[Cmd] = &[
    sub!("pubsub", "channels", -2, F::PUBSUB | F::LOADING | F::STALE, 0, KN, ps::channels),
    sub!("pubsub", "numpat", 2, F::PUBSUB | F::LOADING | F::STALE, 0, KN, ps::numpat),
    sub!("pubsub", "numsub", -2, F::PUBSUB | F::LOADING | F::STALE, 0, KN, ps::numsub),
    sub!("pubsub", "shardchannels", -2, F::PUBSUB | F::LOADING | F::STALE, 0, KN, ps::shardchannels),
    sub!("pubsub", "shardnumsub", -2, F::PUBSUB | F::LOADING | F::STALE, 0, KN, ps::shardnumsub),
    sub!("pubsub", "help", 2, F::LOADING | F::STALE, 0, KN, ps::help),
];

static SCRIPT_SUBS: &[Cmd] = &[
    sub!("script", "debug", 3, F::NOSCRIPT, C::SCRIPTING, KN, scr::script_debug),
    sub!("script", "exists", -3, F::NOSCRIPT, C::SCRIPTING, KN, scr::script_exists),
    sub!("script", "flush", -2, F::NOSCRIPT, C::SCRIPTING, KN, scr::script_flush),
    sub!("script", "kill", 2, F::NOSCRIPT | F::ALLOW_BUSY, C::SCRIPTING, KN, scr::script_kill),
    sub!("script", "load", 3, F::NOSCRIPT | F::STALE, C::SCRIPTING, KN, scr::script_load),
    sub!("script", "help", 2, F::NOSCRIPT, C::SCRIPTING, KN, scr::script_help),
];

static FUNCTION_SUBS: &[Cmd] = &[
    sub!("function", "delete", 3, F::WRITE | F::NOSCRIPT, C::SCRIPTING, KN, scr::function_delete),
    sub!("function", "dump", 2, F::NOSCRIPT, C::SCRIPTING, KN, scr::function_dump),
    sub!("function", "flush", -2, F::WRITE | F::NOSCRIPT, C::SCRIPTING, KN, scr::function_flush),
    sub!("function", "kill", 2, F::NOSCRIPT | F::ALLOW_BUSY, C::SCRIPTING, KN, scr::function_kill),
    sub!("function", "list", -2, F::NOSCRIPT, C::SCRIPTING, KN, scr::function_list),
    sub!("function", "load", -3, F::WRITE | F::DENYOOM | F::NOSCRIPT, C::SCRIPTING, KN, scr::function_load),
    sub!("function", "restore", -3, F::WRITE | F::DENYOOM | F::NOSCRIPT, C::SCRIPTING, KN, scr::function_restore),
    sub!("function", "stats", 2, F::NOSCRIPT | F::ALLOW_BUSY, C::SCRIPTING, KN, scr::function_stats),
    sub!("function", "help", 2, F::LOADING | F::STALE, C::SCRIPTING, KN, scr::function_help),
];

static ACL_SUBS: &[Cmd] = &[
    sub!("acl", "cat", -2, F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::cat),
    sub!("acl", "deluser", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::deluser),
    sub!("acl", "dryrun", -4, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::dryrun),
    sub!("acl", "genpass", -2, F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::genpass),
    sub!("acl", "getuser", 3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::getuser),
    sub!("acl", "list", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::list),
    sub!("acl", "load", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::load),
    sub!("acl", "log", -2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::log),
    sub!("acl", "save", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::save),
    sub!("acl", "setuser", -3, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::setuser),
    sub!("acl", "users", 2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::users),
    sub!("acl", "whoami", 2, F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, acl::whoami),
    sub!("acl", "help", 2, F::LOADING | F::STALE, 0, KN, acl::help),
];

static XGROUP_SUBS: &[Cmd] = &[
    sub!("xgroup", "create", -5, WD, C::STREAM, KeySpec::Range(2, 2, 1), xs::xgroup_create),
    sub!("xgroup", "createconsumer", 5, WD, C::STREAM, KeySpec::Range(2, 2, 1), xs::xgroup_createconsumer),
    sub!("xgroup", "delconsumer", 5, W, C::STREAM, KeySpec::Range(2, 2, 1), xs::xgroup_delconsumer),
    sub!("xgroup", "destroy", 4, W, C::STREAM, KeySpec::Range(2, 2, 1), xs::xgroup_destroy),
    sub!("xgroup", "setid", -5, W, C::STREAM, KeySpec::Range(2, 2, 1), xs::xgroup_setid),
    sub!("xgroup", "help", 2, F::LOADING | F::STALE, C::STREAM, KN, xs::xgroup_help),
];

static XINFO_SUBS: &[Cmd] = &[
    sub!("xinfo", "consumers", 4, R, C::STREAM, KeySpec::Range(2, 2, 1), xs::xinfo_consumers),
    sub!("xinfo", "groups", 3, R, C::STREAM, KeySpec::Range(2, 2, 1), xs::xinfo_groups),
    sub!("xinfo", "stream", -3, R, C::STREAM, KeySpec::Range(2, 2, 1), xs::xinfo_stream),
    sub!("xinfo", "help", 2, F::LOADING | F::STALE, C::STREAM, KN, xs::xinfo_help),
];

static CLUSTER_SUBS: &[Cmd] = &[
    sub!("cluster", "addslots", -3, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::addslots),
    sub!("cluster", "addslotsrange", -4, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::addslotsrange),
    sub!("cluster", "bumpepoch", 2, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::bumpepoch),
    sub!("cluster", "count-failure-reports", 3, F::ADMIN | F::STALE, 0, KN, clu::count_failure_reports),
    sub!("cluster", "countkeysinslot", 3, F::STALE, 0, KN, clu::countkeysinslot),
    sub!("cluster", "delslots", -3, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::delslots),
    sub!("cluster", "delslotsrange", -4, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::delslotsrange),
    sub!("cluster", "failover", -2, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::failover),
    sub!("cluster", "flushslots", 2, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::flushslots),
    sub!("cluster", "forget", 3, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::forget),
    sub!("cluster", "getkeysinslot", 4, F::STALE, 0, KN, clu::getkeysinslot),
    sub!("cluster", "info", 2, F::STALE, 0, KN, clu::info),
    sub!("cluster", "keyslot", 3, F::STALE, 0, KN, clu::keyslot),
    sub!("cluster", "links", 2, F::STALE, 0, KN, clu::links),
    sub!("cluster", "meet", -4, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::meet),
    sub!("cluster", "myid", 2, F::STALE, 0, KN, clu::myid),
    sub!("cluster", "myshardid", 2, F::STALE, 0, KN, clu::myshardid),
    sub!("cluster", "nodes", 2, F::STALE, 0, KN, clu::nodes),
    sub!("cluster", "replicas", 3, F::ADMIN | F::STALE, 0, KN, clu::replicas),
    sub!("cluster", "replicate", 3, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::replicate),
    sub!("cluster", "reset", -2, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::reset),
    sub!("cluster", "saveconfig", 2, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::saveconfig),
    sub!("cluster", "set-config-epoch", 3, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::set_config_epoch),
    sub!("cluster", "setslot", -4, F::ADMIN | F::STALE | F::NOSCRIPT, 0, KN, clu::setslot),
    sub!("cluster", "shards", 2, F::STALE, 0, KN, clu::shards),
    sub!("cluster", "slaves", 3, F::ADMIN | F::STALE, 0, KN, clu::replicas),
    sub!("cluster", "slots", 2, F::STALE, 0, KN, clu::slots),
    sub!("cluster", "help", 2, F::LOADING | F::STALE, 0, KN, clu::help),
];

static MODULE_SUBS: &[Cmd] = &[
    sub!("module", "list", 2, F::ADMIN | F::NOSCRIPT, 0, KN, adm::module_list),
    sub!("module", "load", -3, F::ADMIN | F::NOSCRIPT | F::PROTECTED, 0, KN, adm::module_load),
    sub!("module", "loadex", -3, F::ADMIN | F::NOSCRIPT | F::PROTECTED, 0, KN, adm::module_load),
    sub!("module", "unload", 3, F::ADMIN | F::NOSCRIPT | F::PROTECTED, 0, KN, adm::module_unload),
    sub!("module", "help", 2, F::LOADING | F::STALE, 0, KN, adm::module_help),
];

const PSC: u64 = F::PUBSUB | F::NOSCRIPT | F::LOADING | F::STALE | F::PUBSUB_CTX_OK;

pub static COMMANDS: &[Cmd] = &[
    // connection
    cmd!("auth", -2, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::NO_AUTH | F::SENTINEL | F::ALLOW_BUSY | F::NO_SLOWLOG | F::SKIP_MONITOR, C::CONNECTION, KN, con::auth, "connection"),
    cmd!("echo", 2, F::LOADING | F::STALE | F::FAST, C::CONNECTION, KN, con::echo, "connection"),
    cmd!("hello", -1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::NO_AUTH | F::SENTINEL | F::ALLOW_BUSY | F::NO_SLOWLOG | F::SKIP_MONITOR, C::CONNECTION, KN, con::hello, "connection"),
    cmd!("ping", -1, F::FAST | F::SENTINEL | F::PUBSUB_CTX_OK, C::CONNECTION, KN, con::ping, "connection"),
    cmd!("quit", -1, F::ALLOW_BUSY | F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::NO_AUTH | F::PUBSUB_CTX_OK, C::CONNECTION, KN, con::quit, "connection"),
    cmd!("reset", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::NO_AUTH | F::ALLOW_BUSY | F::PUBSUB_CTX_OK, C::CONNECTION, KN, con::reset, "connection"),
    cmd!("select", 2, F::LOADING | F::STALE | F::FAST, C::CONNECTION, KN, con::select, "connection"),
    container!("client", "connection", CLIENT_SUBS),
    // generic
    cmd!("copy", -3, WD, C::KEYSPACE, K12, gn::copy, "generic"),
    cmd!("del", -2, W, C::KEYSPACE, KALL, gn::del, "generic"),
    cmd!("dump", 2, R, C::KEYSPACE, K1, gn::dump, "generic"),
    cmd!("exists", -2, RF, C::KEYSPACE, KALL, gn::exists, "generic"),
    cmd!("expire", -3, WF, C::KEYSPACE, K1, gn::expire, "generic"),
    cmd!("expireat", -3, WF, C::KEYSPACE, K1, gn::expireat, "generic"),
    cmd!("expiretime", 2, RF, C::KEYSPACE, K1, gn::expiretime, "generic"),
    cmd!("keys", 2, R, C::KEYSPACE | C::DANGEROUS, KN, gn::keys, "generic"),
    cmd!("migrate", -6, W, C::KEYSPACE | C::DANGEROUS, KeySpec::Custom(gn::migrate_keys), gn::migrate, "generic"),
    cmd!("move", 3, WF, C::KEYSPACE, K1, gn::move_, "generic"),
    container!("object", "generic", OBJECT_SUBS),
    cmd!("persist", 2, WF, C::KEYSPACE, K1, gn::persist, "generic"),
    cmd!("pexpire", -3, WF, C::KEYSPACE, K1, gn::pexpire, "generic"),
    cmd!("pexpireat", -3, WF, C::KEYSPACE, K1, gn::pexpireat, "generic"),
    cmd!("pexpiretime", 2, RF, C::KEYSPACE, K1, gn::pexpiretime, "generic"),
    cmd!("pttl", 2, RF, C::KEYSPACE, K1, gn::pttl, "generic"),
    cmd!("randomkey", 1, R, C::KEYSPACE, KN, gn::randomkey, "generic"),
    cmd!("rename", 3, W, C::KEYSPACE, K12, gn::rename, "generic"),
    cmd!("renamenx", 3, WF, C::KEYSPACE, K12, gn::renamenx, "generic"),
    cmd!("restore", -4, WD, C::KEYSPACE | C::DANGEROUS, K1, gn::restore, "generic"),
    cmd!("restore-asking", -4, WD | F::ASKING, C::KEYSPACE | C::DANGEROUS, K1, gn::restore, "generic"),
    cmd!("scan", -2, R, C::KEYSPACE, KN, gn::scan, "generic"),
    cmd!("sort", -2, WD | F::DYN_WRITE, C::SET | C::SORTEDSET | C::LIST, KeySpec::Custom(sort::sort_keys), sort::sort, "generic"),
    cmd!("sort_ro", -2, R, C::SET | C::SORTEDSET | C::LIST, K1, sort::sort_ro, "generic"),
    cmd!("touch", -2, RF, C::KEYSPACE, KALL, gn::touch, "generic"),
    cmd!("ttl", 2, RF, C::KEYSPACE, K1, gn::ttl, "generic"),
    cmd!("type", 2, RF, C::KEYSPACE, K1, gn::type_, "generic"),
    cmd!("unlink", -2, WF, C::KEYSPACE, KALL, gn::unlink, "generic"),
    cmd!("wait", 3, 0, C::CONNECTION, KN, rep::wait, "generic"),
    cmd!("waitaof", 4, F::NOSCRIPT, C::CONNECTION, KN, rep::waitaof, "generic"),
    // string
    cmd!("append", 3, WDF, C::STRING, K1, st::append, "string"),
    cmd!("decr", 2, WDF, C::STRING, K1, st::decr, "string"),
    cmd!("decrby", 3, WDF, C::STRING, K1, st::decrby, "string"),
    cmd!("get", 2, RF, C::STRING, K1, st::get, "string"),
    cmd!("getdel", 2, WF, C::STRING, K1, st::getdel, "string"),
    cmd!("getex", -2, WF, C::STRING, K1, st::getex, "string"),
    cmd!("getrange", 4, R, C::STRING, K1, st::getrange, "string"),
    cmd!("getset", 3, WDF, C::STRING, K1, st::getset, "string"),
    cmd!("incr", 2, WDF, C::STRING, K1, st::incr, "string"),
    cmd!("incrby", 3, WDF, C::STRING, K1, st::incrby, "string"),
    cmd!("incrbyfloat", 3, WDF, C::STRING, K1, st::incrbyfloat, "string"),
    cmd!("lcs", -3, R, C::STRING, K12, st::lcs, "string"),
    cmd!("mget", -2, RF, C::STRING, KALL, st::mget, "string"),
    cmd!("mset", -3, WD, C::STRING, KeySpec::Range(1, -1, 2), st::mset, "string"),
    cmd!("msetnx", -3, WD, C::STRING, KeySpec::Range(1, -1, 2), st::msetnx, "string"),
    cmd!("psetex", 4, WD, C::STRING, K1, st::psetex, "string"),
    cmd!("set", -3, WD, C::STRING, K1, st::set, "string"),
    cmd!("setex", 4, WD, C::STRING, K1, st::setex, "string"),
    cmd!("setnx", 3, WDF, C::STRING, K1, st::setnx, "string"),
    cmd!("setrange", 4, WD, C::STRING, K1, st::setrange, "string"),
    cmd!("strlen", 2, RF, C::STRING, K1, st::strlen, "string"),
    cmd!("substr", 4, R, C::STRING, K1, st::getrange, "string"),
    // bitmap
    cmd!("bitcount", -2, R, C::BITMAP, K1, bit::bitcount, "bitmap"),
    cmd!("bitfield", -2, WD, C::BITMAP, K1, bit::bitfield, "bitmap"),
    cmd!("bitfield_ro", -2, RF, C::BITMAP, K1, bit::bitfield_ro, "bitmap"),
    cmd!("bitop", -4, WD, C::BITMAP, KeySpec::Range(2, -1, 1), bit::bitop, "bitmap"),
    cmd!("bitpos", -3, R, C::BITMAP, K1, bit::bitpos, "bitmap"),
    cmd!("getbit", 3, RF, C::BITMAP, K1, bit::getbit, "bitmap"),
    cmd!("setbit", 4, WD, C::BITMAP, K1, bit::setbit, "bitmap"),
    // list
    cmd!("blmove", 6, W | F::DENYOOM | F::BLOCKING, C::LIST, K12, list::blmove, "list"),
    cmd!("blmpop", -5, W | F::BLOCKING, C::LIST, KeySpec::NumKeys(2), list::blmpop, "list"),
    cmd!("blpop", -3, W | F::BLOCKING, C::LIST, KeySpec::Range(1, -2, 1), list::blpop, "list"),
    cmd!("brpop", -3, W | F::BLOCKING, C::LIST, KeySpec::Range(1, -2, 1), list::brpop, "list"),
    cmd!("brpoplpush", 4, W | F::DENYOOM | F::BLOCKING, C::LIST, K12, list::brpoplpush, "list"),
    cmd!("lindex", 3, R, C::LIST, K1, list::lindex, "list"),
    cmd!("linsert", 5, WD, C::LIST, K1, list::linsert, "list"),
    cmd!("llen", 2, RF, C::LIST, K1, list::llen, "list"),
    cmd!("lmove", 5, WD, C::LIST, K12, list::lmove, "list"),
    cmd!("lmpop", -4, W, C::LIST, KeySpec::NumKeys(1), list::lmpop, "list"),
    cmd!("lpop", -2, WF, C::LIST, K1, list::lpop, "list"),
    cmd!("lpos", -3, R, C::LIST, K1, list::lpos, "list"),
    cmd!("lpush", -3, WDF, C::LIST, K1, list::lpush, "list"),
    cmd!("lpushx", -3, WDF, C::LIST, K1, list::lpushx, "list"),
    cmd!("lrange", 4, R, C::LIST, K1, list::lrange, "list"),
    cmd!("lrem", 4, W, C::LIST, K1, list::lrem, "list"),
    cmd!("lset", 4, WD, C::LIST, K1, list::lset, "list"),
    cmd!("ltrim", 4, W, C::LIST, K1, list::ltrim, "list"),
    cmd!("rpop", -2, WF, C::LIST, K1, list::rpop, "list"),
    cmd!("rpoplpush", 3, WD, C::LIST, K12, list::rpoplpush, "list"),
    cmd!("rpush", -3, WDF, C::LIST, K1, list::rpush, "list"),
    cmd!("rpushx", -3, WDF, C::LIST, K1, list::rpushx, "list"),
    // hash
    cmd!("hdel", -3, WF, C::HASH, K1, hash::hdel, "hash"),
    cmd!("hexists", 3, RF, C::HASH, K1, hash::hexists, "hash"),
    cmd!("hexpire", -6, WDF, C::HASH, K1, hash::hexpire, "hash"),
    cmd!("hexpireat", -6, WDF, C::HASH, K1, hash::hexpireat, "hash"),
    cmd!("hexpiretime", -5, RF, C::HASH, K1, hash::hexpiretime, "hash"),
    cmd!("hget", 3, RF, C::HASH, K1, hash::hget, "hash"),
    cmd!("hgetall", 2, R, C::HASH, K1, hash::hgetall, "hash"),
    cmd!("hincrby", 4, WDF, C::HASH, K1, hash::hincrby, "hash"),
    cmd!("hincrbyfloat", 4, WDF, C::HASH, K1, hash::hincrbyfloat, "hash"),
    cmd!("hkeys", 2, R, C::HASH, K1, hash::hkeys, "hash"),
    cmd!("hlen", 2, RF, C::HASH, K1, hash::hlen, "hash"),
    cmd!("hmget", -3, RF, C::HASH, K1, hash::hmget, "hash"),
    cmd!("hmset", -4, WDF, C::HASH, K1, hash::hmset, "hash"),
    cmd!("hpersist", -5, WF, C::HASH, K1, hash::hpersist, "hash"),
    cmd!("hpexpire", -6, WDF, C::HASH, K1, hash::hpexpire, "hash"),
    cmd!("hpexpireat", -6, WDF, C::HASH, K1, hash::hpexpireat, "hash"),
    cmd!("hpexpiretime", -5, RF, C::HASH, K1, hash::hpexpiretime, "hash"),
    cmd!("hpttl", -5, RF, C::HASH, K1, hash::hpttl, "hash"),
    cmd!("hrandfield", -2, R, C::HASH, K1, hash::hrandfield, "hash"),
    cmd!("hscan", -3, R, C::HASH, K1, hash::hscan, "hash"),
    cmd!("hset", -4, WDF, C::HASH, K1, hash::hset, "hash"),
    cmd!("hsetnx", 4, WDF, C::HASH, K1, hash::hsetnx, "hash"),
    cmd!("hstrlen", 3, RF, C::HASH, K1, hash::hstrlen, "hash"),
    cmd!("httl", -5, RF, C::HASH, K1, hash::httl, "hash"),
    cmd!("hvals", 2, R, C::HASH, K1, hash::hvals, "hash"),
    // set
    cmd!("sadd", -3, WDF, C::SET, K1, set::sadd, "set"),
    cmd!("scard", 2, RF, C::SET, K1, set::scard, "set"),
    cmd!("sdiff", -2, R, C::SET, KALL, set::sdiff, "set"),
    cmd!("sdiffstore", -3, WD, C::SET, KALL, set::sdiffstore, "set"),
    cmd!("sinter", -2, R, C::SET, KALL, set::sinter, "set"),
    cmd!("sintercard", -3, R, C::SET, KeySpec::NumKeys(1), set::sintercard, "set"),
    cmd!("sinterstore", -3, WD, C::SET, KALL, set::sinterstore, "set"),
    cmd!("sismember", 3, RF, C::SET, K1, set::sismember, "set"),
    cmd!("smembers", 2, R, C::SET, K1, set::smembers, "set"),
    cmd!("smismember", -3, RF, C::SET, K1, set::smismember, "set"),
    cmd!("smove", 4, WF, C::SET, K12, set::smove, "set"),
    cmd!("spop", -2, WF, C::SET, K1, set::spop, "set"),
    cmd!("srandmember", -2, R, C::SET, K1, set::srandmember, "set"),
    cmd!("srem", -3, WF, C::SET, K1, set::srem, "set"),
    cmd!("sscan", -3, R, C::SET, K1, set::sscan, "set"),
    cmd!("sunion", -2, R, C::SET, KALL, set::sunion, "set"),
    cmd!("sunionstore", -3, WD, C::SET, KALL, set::sunionstore, "set"),
    // sorted set
    cmd!("bzmpop", -5, W | F::BLOCKING, C::SORTEDSET, KeySpec::NumKeys(2), zset::bzmpop, "sorted_set"),
    cmd!("bzpopmax", -3, WF | F::BLOCKING, C::SORTEDSET, KeySpec::Range(1, -2, 1), zset::bzpopmax, "sorted_set"),
    cmd!("bzpopmin", -3, WF | F::BLOCKING, C::SORTEDSET, KeySpec::Range(1, -2, 1), zset::bzpopmin, "sorted_set"),
    cmd!("zadd", -4, WDF, C::SORTEDSET, K1, zset::zadd, "sorted_set"),
    cmd!("zcard", 2, RF, C::SORTEDSET, K1, zset::zcard, "sorted_set"),
    cmd!("zcount", 4, RF, C::SORTEDSET, K1, zset::zcount, "sorted_set"),
    cmd!("zdiff", -3, R, C::SORTEDSET, KeySpec::NumKeys(1), zset::zdiff, "sorted_set"),
    cmd!("zdiffstore", -4, WD, C::SORTEDSET, KeySpec::Custom(zset::store_keys), zset::zdiffstore, "sorted_set"),
    cmd!("zincrby", 4, WDF, C::SORTEDSET, K1, zset::zincrby, "sorted_set"),
    cmd!("zinter", -3, R, C::SORTEDSET, KeySpec::NumKeys(1), zset::zinter, "sorted_set"),
    cmd!("zintercard", -3, R, C::SORTEDSET, KeySpec::NumKeys(1), zset::zintercard, "sorted_set"),
    cmd!("zinterstore", -4, WD, C::SORTEDSET, KeySpec::Custom(zset::store_keys), zset::zinterstore, "sorted_set"),
    cmd!("zlexcount", 4, RF, C::SORTEDSET, K1, zset::zlexcount, "sorted_set"),
    cmd!("zmpop", -4, W, C::SORTEDSET, KeySpec::NumKeys(1), zset::zmpop, "sorted_set"),
    cmd!("zmscore", -3, RF, C::SORTEDSET, K1, zset::zmscore, "sorted_set"),
    cmd!("zpopmax", -2, WF, C::SORTEDSET, K1, zset::zpopmax, "sorted_set"),
    cmd!("zpopmin", -2, WF, C::SORTEDSET, K1, zset::zpopmin, "sorted_set"),
    cmd!("zrandmember", -2, R, C::SORTEDSET, K1, zset::zrandmember, "sorted_set"),
    cmd!("zrange", -4, R, C::SORTEDSET, K1, zset::zrange, "sorted_set"),
    cmd!("zrangebylex", -4, R, C::SORTEDSET, K1, zset::zrangebylex, "sorted_set"),
    cmd!("zrangebyscore", -4, R, C::SORTEDSET, K1, zset::zrangebyscore, "sorted_set"),
    cmd!("zrangestore", -5, WD, C::SORTEDSET, K12, zset::zrangestore, "sorted_set"),
    cmd!("zrank", -3, RF, C::SORTEDSET, K1, zset::zrank, "sorted_set"),
    cmd!("zrem", -3, WF, C::SORTEDSET, K1, zset::zrem, "sorted_set"),
    cmd!("zremrangebylex", 4, W, C::SORTEDSET, K1, zset::zremrangebylex, "sorted_set"),
    cmd!("zremrangebyrank", 4, W, C::SORTEDSET, K1, zset::zremrangebyrank, "sorted_set"),
    cmd!("zremrangebyscore", 4, W, C::SORTEDSET, K1, zset::zremrangebyscore, "sorted_set"),
    cmd!("zrevrange", -4, R, C::SORTEDSET, K1, zset::zrevrange, "sorted_set"),
    cmd!("zrevrangebylex", -4, R, C::SORTEDSET, K1, zset::zrevrangebylex, "sorted_set"),
    cmd!("zrevrangebyscore", -4, R, C::SORTEDSET, K1, zset::zrevrangebyscore, "sorted_set"),
    cmd!("zrevrank", -3, RF, C::SORTEDSET, K1, zset::zrevrank, "sorted_set"),
    cmd!("zscan", -3, R, C::SORTEDSET, K1, zset::zscan, "sorted_set"),
    cmd!("zscore", 3, RF, C::SORTEDSET, K1, zset::zscore, "sorted_set"),
    cmd!("zunion", -3, R, C::SORTEDSET, KeySpec::NumKeys(1), zset::zunion, "sorted_set"),
    cmd!("zunionstore", -4, WD, C::SORTEDSET, KeySpec::Custom(zset::store_keys), zset::zunionstore, "sorted_set"),
    // hyperloglog
    cmd!("pfadd", -2, WDF, C::HYPERLOGLOG, K1, hll::pfadd, "hyperloglog"),
    cmd!("pfcount", -2, R | F::MAY_REPLICATE, C::HYPERLOGLOG, KALL, hll::pfcount, "hyperloglog"),
    cmd!("pfdebug", 3, W | F::DENYOOM | F::ADMIN, C::HYPERLOGLOG, KeySpec::Range(2, 2, 1), hll::pfdebug, "hyperloglog"),
    cmd!("pfmerge", -2, WD, C::HYPERLOGLOG, KALL, hll::pfmerge, "hyperloglog"),
    cmd!("pfselftest", 1, F::ADMIN, C::HYPERLOGLOG, KN, hll::pfselftest, "hyperloglog"),
    // geo
    cmd!("geoadd", -5, WD, C::GEO, K1, geo::geoadd, "geo"),
    cmd!("geodist", -4, R, C::GEO, K1, geo::geodist, "geo"),
    cmd!("geohash", -2, R, C::GEO, K1, geo::geohash, "geo"),
    cmd!("geopos", -2, R, C::GEO, K1, geo::geopos, "geo"),
    cmd!("georadius", -6, WD | F::DYN_WRITE, C::GEO, KeySpec::Custom(geo::georadius_keys), geo::georadius, "geo"),
    cmd!("georadius_ro", -6, R, C::GEO, K1, geo::georadius_ro, "geo"),
    cmd!("georadiusbymember", -5, WD | F::DYN_WRITE, C::GEO, KeySpec::Custom(geo::georadius_keys), geo::georadiusbymember, "geo"),
    cmd!("georadiusbymember_ro", -5, R, C::GEO, K1, geo::georadiusbymember_ro, "geo"),
    cmd!("geosearch", -7, R, C::GEO, K1, geo::geosearch, "geo"),
    cmd!("geosearchstore", -8, WD, C::GEO, K12, geo::geosearchstore, "geo"),
    // stream
    cmd!("xack", -4, WF, C::STREAM, K1, xs::xack, "stream"),
    cmd!("xadd", -5, WDF, C::STREAM, K1, xs::xadd, "stream"),
    cmd!("xautoclaim", -6, WF, C::STREAM, K1, xs::xautoclaim, "stream"),
    cmd!("xclaim", -6, WF, C::STREAM, K1, xs::xclaim, "stream"),
    cmd!("xdel", -3, WF, C::STREAM, K1, xs::xdel, "stream"),
    container!("xgroup", "stream", XGROUP_SUBS),
    container!("xinfo", "stream", XINFO_SUBS),
    cmd!("xlen", 2, RF, C::STREAM, K1, xs::xlen, "stream"),
    cmd!("xpending", -3, R, C::STREAM, K1, xs::xpending, "stream"),
    cmd!("xrange", -4, R, C::STREAM, K1, xs::xrange, "stream"),
    cmd!("xread", -4, R | F::BLOCKING, C::STREAM, KeySpec::Custom(xs::xread_keys), xs::xread, "stream"),
    cmd!("xreadgroup", -7, W | F::BLOCKING, C::STREAM, KeySpec::Custom(xs::xread_keys), xs::xreadgroup, "stream"),
    cmd!("xrevrange", -4, R, C::STREAM, K1, xs::xrevrange, "stream"),
    cmd!("xsetid", -3, WDF, C::STREAM, K1, xs::xsetid, "stream"),
    cmd!("xtrim", -4, W, C::STREAM, K1, xs::xtrim, "stream"),
    // pubsub
    cmd!("psubscribe", -2, PSC, 0, KN, ps::psubscribe, "pubsub"),
    cmd!("publish", 3, F::PUBSUB | F::LOADING | F::STALE | F::FAST | F::MAY_REPLICATE, 0, KN, ps::publish, "pubsub"),
    container!("pubsub", "pubsub", PUBSUB_SUBS),
    cmd!("punsubscribe", -1, PSC, 0, KN, ps::punsubscribe, "pubsub"),
    cmd!("spublish", 3, F::PUBSUB | F::LOADING | F::STALE | F::FAST | F::MAY_REPLICATE, 0, K1, ps::spublish, "pubsub"),
    cmd!("ssubscribe", -2, PSC, 0, KALL, ps::ssubscribe, "pubsub"),
    cmd!("subscribe", -2, PSC, 0, KN, ps::subscribe, "pubsub"),
    cmd!("sunsubscribe", -1, PSC, 0, KALL, ps::sunsubscribe, "pubsub"),
    cmd!("unsubscribe", -1, PSC, 0, KN, ps::unsubscribe, "pubsub"),
    // transactions
    cmd!("discard", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::ALLOW_BUSY, C::TRANSACTION, KN, multi::discard, "transactions"),
    cmd!("exec", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::SKIP_MONITOR | F::NO_SLOWLOG, C::TRANSACTION, KN, multi::exec, "transactions"),
    cmd!("multi", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::ALLOW_BUSY, C::TRANSACTION, KN, multi::multi, "transactions"),
    cmd!("unwatch", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::ALLOW_BUSY, C::TRANSACTION, KN, multi::unwatch, "transactions"),
    cmd!("watch", -2, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::ALLOW_BUSY | F::NO_MULTI, C::TRANSACTION, KALL, multi::watch, "transactions"),
    // scripting
    cmd!("eval", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::MAY_REPLICATE | F::NO_MANDATORY_KEYS | F::STALE, C::SCRIPTING, KeySpec::NumKeys(2), scr::eval, "scripting"),
    cmd!("eval_ro", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::NO_MANDATORY_KEYS | F::STALE | F::READONLY, C::SCRIPTING, KeySpec::NumKeys(2), scr::eval_ro, "scripting"),
    cmd!("evalsha", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::MAY_REPLICATE | F::NO_MANDATORY_KEYS | F::STALE, C::SCRIPTING, KeySpec::NumKeys(2), scr::evalsha, "scripting"),
    cmd!("evalsha_ro", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::NO_MANDATORY_KEYS | F::STALE | F::READONLY, C::SCRIPTING, KeySpec::NumKeys(2), scr::evalsha_ro, "scripting"),
    cmd!("fcall", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::MAY_REPLICATE | F::NO_MANDATORY_KEYS | F::STALE, C::SCRIPTING, KeySpec::NumKeys(2), scr::fcall, "scripting"),
    cmd!("fcall_ro", -3, F::NOSCRIPT | F::SKIP_MONITOR | F::NO_MANDATORY_KEYS | F::STALE | F::READONLY, C::SCRIPTING, KeySpec::NumKeys(2), scr::fcall_ro, "scripting"),
    container!("function", "scripting", FUNCTION_SUBS),
    container!("script", "scripting", SCRIPT_SUBS),
    // server
    container!("acl", "server", ACL_SUBS),
    cmd!("bgrewriteaof", 1, F::NOSCRIPT | F::ADMIN | F::NO_ASYNC_LOADING, 0, KN, adm::bgrewriteaof, "server"),
    cmd!("bgsave", -1, F::NOSCRIPT | F::ADMIN | F::NO_ASYNC_LOADING, 0, KN, adm::bgsave, "server"),
    container!("command", "server", COMMAND_SUBS),
    container!("config", "server", CONFIG_SUBS),
    cmd!("dbsize", 1, RF, C::KEYSPACE, KN, adm::dbsize, "server"),
    cmd!("debug", -2, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE | F::PROTECTED, 0, KN, adm::debug, "server"),
    cmd!("failover", -1, F::ADMIN | F::NOSCRIPT | F::STALE, 0, KN, rep::failover, "server"),
    cmd!("flushall", -1, W, C::KEYSPACE | C::DANGEROUS, KN, adm::flushall, "server"),
    cmd!("flushdb", -1, W, C::KEYSPACE | C::DANGEROUS, KN, adm::flushdb, "server"),
    cmd!("info", -1, F::LOADING | F::STALE | F::SENTINEL, C::DANGEROUS, KN, adm::info, "server"),
    cmd!("lastsave", 1, F::LOADING | F::STALE | F::FAST, C::ADMIN | C::DANGEROUS, KN, adm::lastsave, "server"),
    container!("latency", "server", LATENCY_SUBS),
    cmd!("lolwut", -1, R | F::FAST, 0, KN, adm::lolwut, "server"),
    container!("memory", "server", MEMORY_SUBS),
    container!("module", "server", MODULE_SUBS),
    cmd!("monitor", 1, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE, 0, KN, adm::monitor, "server"),
    cmd!("psync", -3, F::ADMIN | F::NOSCRIPT | F::NO_ASYNC_LOADING | F::NO_MULTI, 0, KN, rep::psync, "server"),
    cmd!("replconf", -1, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE | F::ALLOW_BUSY, 0, KN, rep::replconf, "server"),
    cmd!("replicaof", 3, F::ADMIN | F::NOSCRIPT | F::STALE | F::NO_ASYNC_LOADING, 0, KN, rep::replicaof, "server"),
    cmd!("restore-asking-internal", -4, WD | F::ASKING, C::KEYSPACE | C::DANGEROUS, K1, gn::restore, "server"),
    cmd!("role", 1, F::NOSCRIPT | F::LOADING | F::STALE | F::FAST | F::SENTINEL, C::ADMIN | C::DANGEROUS, KN, rep::role, "server"),
    cmd!("save", 1, F::ADMIN | F::NOSCRIPT | F::NO_ASYNC_LOADING | F::NO_MULTI, 0, KN, adm::save, "server"),
    cmd!("shutdown", -1, F::ADMIN | F::NOSCRIPT | F::LOADING | F::STALE | F::NO_MULTI | F::SENTINEL | F::ALLOW_BUSY, 0, KN, adm::shutdown, "server"),
    cmd!("slaveof", 3, F::ADMIN | F::NOSCRIPT | F::STALE | F::NO_ASYNC_LOADING, 0, KN, rep::replicaof, "server"),
    container!("slowlog", "server", SLOWLOG_SUBS),
    cmd!("swapdb", 3, WF, C::KEYSPACE | C::DANGEROUS, KN, adm::swapdb, "server"),
    cmd!("sync", 1, F::ADMIN | F::NOSCRIPT | F::NO_ASYNC_LOADING | F::NO_MULTI, 0, KN, rep::sync, "server"),
    cmd!("time", 1, F::LOADING | F::STALE | F::FAST, 0, KN, adm::time, "server"),
    // cluster
    cmd!("asking", 1, F::FAST, C::CONNECTION, KN, clu::asking, "cluster"),
    container!("cluster", "cluster", CLUSTER_SUBS),
    cmd!("readonly", 1, F::LOADING | F::STALE | F::FAST, C::CONNECTION, KN, clu::readonly, "cluster"),
    cmd!("readwrite", 1, F::LOADING | F::STALE | F::FAST, C::CONNECTION, KN, clu::readwrite, "cluster"),
];

fn table() -> &'static HashMap<String, &'static Cmd> {
    static T: OnceLock<HashMap<String, &'static Cmd>> = OnceLock::new();
    T.get_or_init(|| COMMANDS.iter().map(|c| (c.name.to_string(), c)).collect())
}

#[inline]
fn lower_buf(name: &[u8], buf: &mut [u8; 64]) -> Option<usize> {
    if name.len() > 64 {
        return None;
    }
    for (i, c) in name.iter().enumerate() {
        buf[i] = c.to_ascii_lowercase();
    }
    Some(name.len())
}

pub fn lookup(name: &[u8]) -> Option<&'static Cmd> {
    let mut buf = [0u8; 64];
    let n = lower_buf(name, &mut buf)?;
    let s = std::str::from_utf8(&buf[..n]).ok()?;
    table().get(s).copied()
}

pub fn lookup_sub(c: &'static Cmd, name: &[u8]) -> Option<&'static Cmd> {
    c.subs.iter().find(|s| s.name.as_bytes().eq_ignore_ascii_case(name))
}

/// Find a command by "name" or "name|sub".
pub fn lookup_full(full: &str) -> Option<&'static Cmd> {
    match full.split_once('|') {
        Some((p, sname)) => {
            let c = lookup(p.as_bytes())?;
            lookup_sub(c, sname.as_bytes())
        }
        None => lookup(full.as_bytes()),
    }
}

pub fn all_commands() -> impl Iterator<Item = &'static Cmd> {
    COMMANDS.iter()
}
