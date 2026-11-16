//! Server administration commands: INFO CONFIG COMMAND DEBUG MEMORY SLOWLOG LATENCY
//! FLUSHALL FLUSHDB SWAPDB SAVE BGSAVE BGREWRITEAOF SHUTDOWN TIME LASTSAVE MONITOR …

use super::util::*;
use super::{Cmd, KeySpec, cat, flags as F};
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::server::{CRABDIS_VERSION, Store, VERSION, cflags};
use crate::tri;
use crate::value::Value;
use bytes::Bytes;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};

pub static ACTIVE_EXPIRE: AtomicBool = AtomicBool::new(true);

// ---------------- CONFIG ----------------

pub fn config_get(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut out: Vec<(String, String)> = Vec::new();
    for p in &args[2..] {
        let pat = String::from_utf8_lossy(p).to_ascii_lowercase();
        for (k, v) in ctx.s.cfg.get_matching(&pat) {
            if !out.iter().any(|(n, _)| *n == k) {
                out.push((k, v));
            }
        }
    }
    Frame::Map(out.into_iter().map(|(k, v)| (Frame::bulk(Bytes::from(k)), Frame::bulk(Bytes::from(v)))).collect())
}

pub fn config_set(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 != 0 {
        return Frame::err("ERR wrong number of arguments for 'config|set' command");
    }
    let mut seen = std::collections::HashSet::new();
    let mut pairs = Vec::new();
    for kv in args[2..].chunks(2) {
        let name = String::from_utf8_lossy(&kv[0]).to_ascii_lowercase();
        let Some(canon) = Config::canonical(&name) else {
            return Frame::err(format!("ERR Unknown option or number of arguments for CONFIG SET - '{}'", name));
        };
        if !seen.insert(canon) {
            return Frame::err(format!("ERR Duplicate parameter - {}", name));
        }
        if ctx.s.cfg.is_mutable(&name) == Some(false) {
            return Frame::err(format!("ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config", name));
        }
        pairs.push((name, String::from_utf8_lossy(&kv[1]).into_owned()));
    }
    // Validate all on a copy first (atomic semantics), then apply.
    let mut test = ctx.s.cfg.clone();
    for (n, v) in &pairs {
        if n == "dir" {
            if !std::path::Path::new(v).is_dir() {
                return Frame::err(format!("ERR CONFIG SET failed (possibly related to argument '{}') - No such file or directory", n));
            }
            continue;
        }
        if let Err(e) = test.set(n, v) {
            return Frame::err(format!("ERR CONFIG SET failed (possibly related to argument '{}') - {}", n, e));
        }
    }
    let old_aof = ctx.s.cfg.appendonly;
    for (n, v) in &pairs {
        if let Err(e) = ctx.s.cfg.set(n, v) {
            return Frame::err(format!("ERR CONFIG SET failed (possibly related to argument '{}') - {}", n, e));
        }
    }
    // Side effects.
    for (n, _) in &pairs {
        match n.as_str() {
            "appendonly" => {
                let now = ctx.s.cfg.appendonly;
                if now && !old_aof {
                    crate::aof::start_aof(ctx.s);
                } else if !now && old_aof {
                    crate::aof::stop_aof(ctx.s);
                }
            }
            "maxmemory" => {
                crate::evict::perform_evictions(ctx.s);
            }
            "requirepass" => {
                let p = ctx.s.cfg.requirepass.clone();
                ctx.s.acl.set_default_password(p);
            }
            "replicaof" => {}
            "appendfsync" => crate::aof::set_fsync(ctx.s),
            _ => {}
        }
    }
    Frame::ok()
}

pub fn config_resetstat(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let s = &mut ctx.s;
    s.stats = Default::default();
    s.dirty = s.dirty; // keep
    s.slowlog.clear();
    s.latency.events.clear();
    s.acl.log.clear();
    for c in s.clients.values_mut() {
        c.tot_net_in = 0;
        c.tot_net_out = 0;
        c.tot_cmds = 0;
    }
    Frame::ok()
}

pub fn config_rewrite(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    match ctx.s.cfg.rewrite() {
        Ok(()) => Frame::ok(),
        Err(e) => Frame::err(format!("ERR Rewriting config file: {}", e)),
    }
}

pub fn config_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "CONFIG",
        &[
            "GET <pattern>",
            "    Return parameters matching the glob-like <pattern> and their values.",
            "SET <directive> <value>",
            "    Set the configuration <directive> to <value>.",
            "RESETSTAT",
            "    Reset statistics reported by the INFO command.",
            "REWRITE",
            "    Rewrite the configuration file.",
        ],
    )
}

// ---------------- simple server commands ----------------

pub fn dbsize(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.s.dbs[ctx.db].len() as i64)
}

pub fn time(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let us = crate::util::now_us();
    Frame::Array(vec![Frame::bulk_int(us / 1_000_000), Frame::bulk_int(us % 1_000_000)])
}

pub fn lastsave(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.s.persist.lastsave)
}

fn parse_flush_mode(args: &[Bytes]) -> Result<(), Frame> {
    if args.len() > 2 {
        return Err(err_syntax());
    }
    if args.len() == 2 && !eq(&args[1], "sync") && !eq(&args[1], "async") {
        return Err(err_syntax());
    }
    Ok(())
}

pub fn flushdb(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(parse_flush_mode(args));
    let db = ctx.db;
    let n = ctx.s.dbs[db].len() as u64;
    ctx.s.signal_flushed_db(Some(db));
    let old = std::mem::replace(&mut ctx.s.dbs[db], crate::db::Db::new(db));
    std::thread::spawn(move || drop(old));
    ctx.dirty(n.max(1));
    ctx.propagate_as(args.to_vec());
    Frame::ok()
}

pub fn flushall(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(parse_flush_mode(args));
    let n: u64 = ctx.s.dbs.iter().map(|d| d.len() as u64).sum();
    ctx.s.signal_flushed_db(None);
    let count = ctx.s.dbs.len();
    let old: Vec<crate::db::Db> = std::mem::replace(&mut ctx.s.dbs, (0..count).map(crate::db::Db::new).collect());
    std::thread::spawn(move || drop(old));
    ctx.dirty(n.max(1));
    ctx.propagate_as(args.to_vec());
    if !ctx.s.cfg.save_params.is_empty() && !ctx.s.persist.bgsave_in_progress {
        let saved_dirty = ctx.s.dirty;
        let _ = crate::rdb::save(ctx.s);
        // FLUSHALL still needs to be propagated even though save reset dirty.
        ctx.s.dirty = saved_dirty + 1;
    }
    Frame::ok()
}

pub fn swapdb(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_some() {
        return Frame::err("ERR SWAPDB is not allowed in cluster mode");
    }
    let (Some(a), Some(b)) = (crate::util::parse_i64(&args[1]), crate::util::parse_i64(&args[2])) else {
        return Frame::err("ERR invalid first DB index");
    };
    let n = ctx.s.dbs.len() as i64;
    if a < 0 || a >= n {
        return Frame::err("ERR DB index is out of range");
    }
    if b < 0 || b >= n {
        return Frame::err("ERR DB index is out of range");
    }
    let (a, b) = (a as usize, b as usize);
    if a != b {
        ctx.s.dbs.swap(a, b);
        ctx.s.dbs[a].id = a;
        ctx.s.dbs[b].id = b;
        // Watched keys in either DB are touched; blocked clients may now be served.
        ctx.s.signal_flushed_db(Some(a));
        ctx.s.signal_flushed_db(Some(b));
        let waiting: Vec<(usize, Bytes)> = ctx.s.blocking.waiting.keys().cloned().collect();
        for (d, k) in waiting {
            if (d == a || d == b) && ctx.s.dbs[d].dict.contains_key(&k) {
                ctx.s.signal_ready(d, &k);
            }
        }
    }
    ctx.dirty(1);
    Frame::ok()
}

pub fn save(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    if ctx.s.persist.bgsave_in_progress {
        return Frame::err("ERR Background save already in progress");
    }
    match crate::rdb::save(ctx.s) {
        Ok(()) => Frame::ok(),
        Err(e) => Frame::err(format!("ERR {}", e)),
    }
}

pub fn bgsave(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let schedule = args.len() == 2 && eq(&args[1], "schedule");
    if args.len() == 2 && !schedule {
        return err_syntax();
    }
    if args.len() > 2 {
        return err_syntax();
    }
    if ctx.s.persist.bgsave_in_progress {
        if schedule {
            ctx.s.persist.bgsave_scheduled = true;
            return Frame::Simple("Background saving scheduled".into());
        }
        return Frame::err("ERR Background save already in progress");
    }
    if ctx.s.persist.aof_rewrite_in_progress {
        if schedule {
            ctx.s.persist.bgsave_scheduled = true;
            return Frame::Simple("Background saving scheduled".into());
        }
        return Frame::err("ERR Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.");
    }
    let shared = crate::SHARED.get().cloned().unwrap();
    crate::rdb::bgsave(ctx.s, shared);
    Frame::Simple("Background saving started".into())
}

pub fn bgrewriteaof(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    if ctx.s.persist.aof_rewrite_in_progress {
        return Frame::err("ERR Background append only file rewriting already in progress");
    }
    if ctx.s.persist.bgsave_in_progress {
        ctx.s.persist.aof_rewrite_scheduled = true;
        return Frame::Simple("Background append only file rewriting scheduled".into());
    }
    let shared = crate::SHARED.get().cloned().unwrap();
    crate::aof::bgrewrite(ctx.s, shared);
    Frame::Simple("Background append only file rewriting started".into())
}

pub fn shutdown(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (mut nosave, mut save, mut abort) = (false, false, false);
    for a in &args[1..] {
        if eq(a, "nosave") {
            nosave = true;
        } else if eq(a, "save") {
            save = true;
        } else if eq(a, "now") || eq(a, "force") {
        } else if eq(a, "abort") {
            abort = true;
        } else {
            return err_syntax();
        }
    }
    if (nosave && save) || (abort && (nosave || save)) {
        return err_syntax();
    }
    if abort {
        return Frame::err("ERR No shutdown in progress.");
    }
    if ctx.flags & crate::exec::CTX_LUA != 0 {
        return Frame::err("ERR SHUTDOWN is not allowed from scripts");
    }
    match crate::shutdown(ctx.s, nosave, save) {
        Ok(()) => std::process::exit(0),
        Err(e) => Frame::err(format!("ERR Errors trying to SHUTDOWN. Check logs. ({})", e)),
    }
}

pub fn monitor(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let cid = ctx.cid;
    let c = ctx.client();
    if c.has(cflags::REPLICA) {
        return Frame::NoReply;
    }
    if !ctx.s.monitors.contains(&cid) {
        ctx.s.monitors.push(cid);
    }
    Frame::ok()
}

pub fn lolwut(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut i = 1;
    let mut version = 7;
    while i < args.len() {
        if eq(&args[i], "version") && i + 1 < args.len() {
            version = crate::util::parse_i64(&args[i + 1]).unwrap_or(7);
            i += 2;
        } else {
            i += 1;
        }
    }
    let _ = version;
    let mut s = String::new();
    // Draw a small Rust crab with Braille-ish ASCII.
    s.push_str("   _~^~^~_\n");
    s.push_str("\\) /  o o  \\ (/\n");
    s.push_str("  '_   -   _'\n");
    s.push_str("  / '-----' \\\n\n");
    let _ = write!(s, "crabdis {} -- Redis ver. {}\n", CRABDIS_VERSION, VERSION);
    Frame::Verbatim("txt", Bytes::from(s))
}

// ---------------- INFO ----------------

fn section_wanted(sections: &[String], name: &str, default: bool) -> bool {
    if sections.is_empty() {
        return default;
    }
    sections.iter().any(|s| s == name || s == "all" || s == "everything" || (s == "default" && default))
}

pub fn info(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let sections: Vec<String> = args[1..].iter().map(|a| crate::util::lower(a)).collect();
    let text = gen_info(ctx.s, &sections);
    Frame::Verbatim("txt", Bytes::from(text))
}

const DEFAULT_SECTIONS: &[&str] = &[
    "server", "clients", "memory", "persistence", "stats", "replication", "cpu", "modules", "errorstats", "cluster", "keyspace",
];

pub fn gen_info(s: &mut Store, sections: &[String]) -> String {
    let mut out = String::new();
    let want = |name: &str| {
        let default = DEFAULT_SECTIONS.contains(&name);
        let everything_only = name == "commandstats" || name == "latencystats";
        if sections.is_empty() {
            return default;
        }
        if everything_only {
            return sections.iter().any(|s| s == name || s == "all" || s == "everything");
        }
        section_wanted(sections, name, default)
    };
    let now = crate::util::now_ms();
    let uptime = (now - s.start_ms) / 1000;
    let mut first = true;
    let mut header = |out: &mut String, name: &str| {
        if !first {
            out.push_str("\r\n");
        }
        first = false;
        let _ = write!(out, "# {}\r\n", name);
    };
    if want("server") {
        header(&mut out, "Server");
        let _ = write!(
            out,
            "redis_version:{}\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:crabdis-{}\r\nredis_mode:{}\r\nos:{} {}\r\narch_bits:64\r\nmonotonic_clock:POSIX clock_gettime\r\nmultiplexing_api:epoll\r\natomicvar_api:c11-builtin\r\ngcc_version:0.0.0\r\nprocess_id:{}\r\nprocess_supervised:no\r\nrun_id:{}\r\ntcp_port:{}\r\nserver_time_usec:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nhz:{}\r\nconfigured_hz:{}\r\nlru_clock:{}\r\nexecutable:{}\r\nconfig_file:{}\r\nio_threads_active:0\r\nlistener0:name=tcp,bind={},port={}\r\ncrabdis_version:{}\r\n",
            VERSION,
            CRABDIS_VERSION,
            if s.cluster.is_some() { "cluster" } else { "standalone" },
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::process::id(),
            s.run_id,
            s.cfg.port,
            crate::util::now_us(),
            uptime,
            uptime / 86400,
            s.cfg.hz,
            s.cfg.hz,
            crate::server::lru_clock(),
            s.executable,
            s.cfg.config_file.clone().unwrap_or_default(),
            if s.cfg.bind.is_empty() { "*".to_string() } else { s.cfg.bind.join(",") },
            s.cfg.port,
            CRABDIS_VERSION,
        );
    }
    if want("clients") {
        header(&mut out, "Clients");
        let normal = s.clients.values().filter(|c| !c.has(cflags::REPLICA | cflags::MASTER)).count();
        let pubsub_clients = s.clients.values().filter(|c| c.in_pubsub()).count();
        let watching = s.clients.values().filter(|c| !c.watched.is_empty()).count();
        let tracking = s.clients.values().filter(|c| c.has(cflags::TRACKING)).count();
        let maxq = s.clients.values().map(|c| c.qbuf).max().unwrap_or(0);
        let _ = write!(
            out,
            "connected_clients:{}\r\ncluster_connections:0\r\nmaxclients:{}\r\nclient_recent_max_input_buffer:{}\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:{}\r\ntracking_clients:{}\r\npubsub_clients:{}\r\nwatching_clients:{}\r\nclients_in_timeout_table:{}\r\ntotal_watched_keys:{}\r\ntotal_blocking_keys:{}\r\ntotal_blocking_keys_on_nokey:0\r\n",
            normal,
            s.cfg.maxclients,
            maxq,
            s.blocking.blocked_clients - s.blocking.paused.len(),
            tracking,
            pubsub_clients,
            watching,
            s.clients.values().filter(|c| c.blocked.as_ref().is_some_and(|b| b.deadline.is_some())).count(),
            s.watched.len(),
            s.blocking.waiting.len(),
        );
    }
    if want("memory") {
        header(&mut out, "Memory");
        let used = s.used_memory();
        let rss = crate::mem::rss();
        let peak = s.used_memory_peak.max(used);
        let total = crate::mem::total_system_memory();
        let _ = write!(
            out,
            "used_memory:{}\r\nused_memory_human:{}\r\nused_memory_rss:{}\r\nused_memory_rss_human:{}\r\nused_memory_peak:{}\r\nused_memory_peak_human:{}\r\nused_memory_peak_perc:{:.2}%\r\nused_memory_overhead:{}\r\nused_memory_startup:0\r\nused_memory_dataset:{}\r\nused_memory_dataset_perc:100.00%\r\nallocator_allocated:{}\r\nallocator_active:{}\r\nallocator_resident:{}\r\ntotal_system_memory:{}\r\ntotal_system_memory_human:{}\r\nused_memory_lua:{}\r\nused_memory_vm_eval:{}\r\nused_memory_lua_human:{}\r\nused_memory_scripts_eval:0\r\nnumber_of_cached_scripts:{}\r\nnumber_of_functions:{}\r\nnumber_of_libraries:{}\r\nused_memory_vm_functions:0\r\nused_memory_vm_total:{}\r\nused_memory_vm_total_human:{}\r\nused_memory_functions:0\r\nused_memory_scripts:0\r\nused_memory_scripts_human:0B\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nmaxmemory_policy:{}\r\nallocator_frag_ratio:1.00\r\nallocator_frag_bytes:0\r\nallocator_rss_ratio:1.00\r\nallocator_rss_bytes:0\r\nrss_overhead_ratio:1.00\r\nrss_overhead_bytes:0\r\nmem_fragmentation_ratio:{:.2}\r\nmem_fragmentation_bytes:{}\r\nmem_not_counted_for_evict:0\r\nmem_replication_backlog:{}\r\nmem_total_replication_buffers:{}\r\nmem_clients_slaves:0\r\nmem_clients_normal:{}\r\nmem_cluster_links:0\r\nmem_aof_buffer:0\r\nmem_allocator:crabdis-counting\r\nmem_overhead_db_hashtable_rehashing:0\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n",
            used,
            crate::mem::human(used),
            rss,
            crate::mem::human(rss),
            peak,
            crate::mem::human(peak),
            used as f64 * 100.0 / peak.max(1) as f64,
            s.clients.len() * 1024,
            used,
            used,
            used,
            rss,
            total,
            crate::mem::human(total),
            s.scripting.memory(),
            s.scripting.memory(),
            crate::mem::human(s.scripting.memory()),
            s.scripting.script_count(),
            s.scripting.function_count(),
            s.scripting.library_count(),
            s.scripting.memory(),
            crate::mem::human(s.scripting.memory()),
            s.cfg.maxmemory,
            crate::mem::human(s.cfg.maxmemory as usize),
            s.cfg.maxmemory_policy,
            rss as f64 / used.max(1) as f64,
            rss.saturating_sub(used),
            s.repl.backlog_len(),
            s.repl.backlog_len(),
            s.clients.len() * 1024,
        );
    }
    if want("persistence") {
        header(&mut out, "Persistence");
        let p = &s.persist;
        let _ = write!(
            out,
            "loading:{}\r\nasync_loading:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_fork_perc:0.00\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\nrdb_last_bgsave_time_sec:{}\r\nrdb_current_bgsave_time_sec:{}\r\nrdb_saves:{}\r\nrdb_last_cow_size:0\r\nrdb_last_load_keys_expired:0\r\nrdb_last_load_keys_loaded:0\r\naof_enabled:{}\r\naof_rewrite_in_progress:{}\r\naof_rewrite_scheduled:{}\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:{}\r\naof_rewrites:{}\r\naof_rewrites_consecutive_failures:0\r\naof_last_write_status:{}\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n",
            p.loading as u8,
            s.dirty,
            p.bgsave_in_progress as u8,
            p.lastsave,
            if p.last_bgsave_ok { "ok" } else { "err" },
            p.last_bgsave_time_sec,
            if p.bgsave_in_progress { (now / 1000 - p.bgsave_started).max(0) } else { -1 },
            p.rdb_saves,
            s.aof.is_some() as u8,
            p.aof_rewrite_in_progress as u8,
            p.aof_rewrite_scheduled as u8,
            if p.aof_last_rewrite_ok { "ok" } else { "err" },
            p.aof_rewrites,
            if p.aof_last_write_ok { "ok" } else { "err" },
        );
        if s.aof.is_some() {
            let _ = write!(
                out,
                "aof_current_size:{}\r\naof_base_size:{}\r\naof_pending_rewrite:0\r\naof_buffer_length:0\r\naof_pending_bio_fsync:0\r\naof_delayed_fsync:0\r\n",
                p.aof_current_size, p.aof_base_size
            );
        }
    }
    if want("stats") {
        header(&mut out, "Stats");
        let st = &s.stats;
        let _ = write!(
            out,
            "total_connections_received:{}\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\ntotal_net_repl_input_bytes:{}\r\ntotal_net_repl_output_bytes:{}\r\ninstantaneous_input_kbps:{:.2}\r\ninstantaneous_output_kbps:{:.2}\r\ninstantaneous_input_repl_kbps:0.00\r\ninstantaneous_output_repl_kbps:0.00\r\nrejected_connections:{}\r\nsync_full:{}\r\nsync_partial_ok:{}\r\nsync_partial_err:{}\r\nexpired_subkeys:{}\r\nexpired_keys:{}\r\nexpired_stale_perc:{:.2}\r\nexpired_time_cap_reached_count:{}\r\nexpire_cycle_cpu_milliseconds:0\r\nevicted_keys:{}\r\nevicted_clients:{}\r\nevicted_scripts:0\r\ntotal_eviction_exceeded_time:0\r\ncurrent_eviction_exceeded_time:0\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\npubsubshard_channels:{}\r\nlatest_fork_usec:0\r\ntotal_forks:0\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntotal_active_defrag_time:0\r\ncurrent_active_defrag_time:0\r\ntracking_total_keys:{}\r\ntracking_total_items:{}\r\ntracking_total_prefixes:{}\r\nunexpected_error_replies:{}\r\ntotal_error_replies:{}\r\ndump_payload_sanitizations:{}\r\ntotal_reads_processed:{}\r\ntotal_writes_processed:{}\r\nio_threaded_reads_processed:0\r\nio_threaded_writes_processed:0\r\nclient_query_buffer_limit_disconnections:{}\r\nclient_output_buffer_limit_disconnections:{}\r\nreply_buffer_shrinks:0\r\nreply_buffer_expands:0\r\neventloop_cycles:0\r\neventloop_duration_sum:0\r\neventloop_duration_cmd_sum:0\r\ninstantaneous_eventloop_cycles_per_sec:0\r\ninstantaneous_eventloop_duration_usec:0\r\nacl_access_denied_auth:{}\r\nacl_access_denied_cmd:{}\r\nacl_access_denied_key:{}\r\nacl_access_denied_channel:{}\r\n",
            st.total_connections,
            st.total_commands,
            Store::ops_per_sec(&st.ops_samples) as u64,
            st.net_input,
            st.net_output,
            st.net_repl_input,
            st.net_repl_output,
            Store::ops_per_sec(&st.net_in_samples) / 1024.0,
            Store::ops_per_sec(&st.net_out_samples) / 1024.0,
            st.rejected_connections,
            st.sync_full,
            st.sync_partial_ok,
            st.sync_partial_err,
            st.expired_subkeys,
            st.expired_keys,
            st.expired_stale_perc * 100.0,
            st.expired_time_cap_reached,
            st.evicted_keys,
            st.evicted_clients,
            st.keyspace_hits,
            st.keyspace_misses,
            s.pubsub.channels.len(),
            s.pubsub.patterns.len(),
            s.pubsub.shard.len(),
            s.tracking.table.len(),
            s.tracking.total_items,
            s.tracking.prefixes.len(),
            st.unexpected_error_replies,
            st.total_error_replies,
            st.dump_payload_sanitizations,
            st.total_reads_processed,
            st.total_writes_processed,
            st.client_qbuf_limit_disconnections,
            st.client_outbuf_limit_disconnections,
            s.acl.denied_auth,
            s.acl.denied_cmd,
            s.acl.denied_key,
            s.acl.denied_channel,
        );
    }
    if want("replication") {
        header(&mut out, "Replication");
        out.push_str(&crate::repl::info(s));
    }
    if want("cpu") {
        header(&mut out, "CPU");
        let (u, sy) = cpu_times();
        let _ = write!(
            out,
            "used_cpu_sys:{:.6}\r\nused_cpu_user:{:.6}\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.000000\r\nused_cpu_sys_main_thread:{:.6}\r\nused_cpu_user_main_thread:{:.6}\r\n",
            sy, u, sy, u
        );
    }
    if want("modules") {
        header(&mut out, "Modules");
    }
    if want("commandstats") {
        header(&mut out, "Commandstats");
        let mut names: Vec<&String> = s.stats.cmdstats.keys().collect();
        names.sort();
        for n in names {
            let c = &s.stats.cmdstats[n];
            if c.calls == 0 && c.rejected == 0 && c.failed == 0 {
                continue;
            }
            let _ = write!(
                out,
                "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={}\r\n",
                n,
                c.calls,
                c.usec,
                if c.calls > 0 { c.usec as f64 / c.calls as f64 } else { 0.0 },
                c.rejected,
                c.failed
            );
        }
    }
    if want("errorstats") {
        header(&mut out, "Errorstats");
        let mut names: Vec<&String> = s.stats.errorstats.keys().collect();
        names.sort();
        for n in names {
            let _ = write!(out, "errorstat_{}:count={}\r\n", n, s.stats.errorstats[n]);
        }
    }
    if want("latencystats") {
        header(&mut out, "Latencystats");
        let mut names: Vec<&String> = s.stats.cmdstats.keys().collect();
        names.sort();
        for n in names {
            let c = &s.stats.cmdstats[n];
            if c.calls == 0 {
                continue;
            }
            let avg = c.usec as f64 / c.calls as f64;
            let _ = write!(out, "latency_percentiles_usec_{}:p50={:.3},p99={:.3},p99.9={:.3}\r\n", n, avg, avg, avg);
        }
    }
    if want("cluster") {
        header(&mut out, "Cluster");
        let _ = write!(out, "cluster_enabled:{}\r\n", s.cluster.is_some() as u8);
    }
    if want("keyspace") {
        header(&mut out, "Keyspace");
        for d in &s.dbs {
            if d.len() > 0 {
                let _ = write!(out, "db{}:keys={},expires={},avg_ttl={},subexpiry={}\r\n", d.id, d.len(), d.expires.len(), d.avg_ttl, d.hexpires.len());
            }
        }
    }
    out
}

fn cpu_times() -> (f64, f64) {
    // /proc/self/stat fields 14 (utime) and 15 (stime) in clock ticks.
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let rest = s.rsplit_once(')')?.1;
            let f: Vec<&str> = rest.split_whitespace().collect();
            let u: f64 = f.get(11)?.parse().ok()?;
            let sy: f64 = f.get(12)?.parse().ok()?;
            Some((u / 100.0, sy / 100.0))
        })
        .unwrap_or((0.0, 0.0))
}

// ---------------- COMMAND ----------------

fn flag_names(c: &Cmd) -> Vec<Frame> {
    let mut v = Vec::new();
    let table = [
        (F::WRITE, "write"),
        (F::READONLY, "readonly"),
        (F::DENYOOM, "denyoom"),
        (F::ADMIN, "admin"),
        (F::PUBSUB, "pubsub"),
        (F::NOSCRIPT, "noscript"),
        (F::BLOCKING, "blocking"),
        (F::LOADING, "loading"),
        (F::STALE, "stale"),
        (F::SKIP_MONITOR, "skip_monitor"),
        (F::NO_SLOWLOG, "skip_slowlog"),
        (F::ASKING, "asking"),
        (F::FAST, "fast"),
        (F::NO_AUTH, "no_auth"),
        (F::MAY_REPLICATE, "may_replicate"),
        (F::SENTINEL, "sentinel"),
        (F::NO_MANDATORY_KEYS, "no_mandatory_keys"),
        (F::PROTECTED, "protected"),
        (F::NO_ASYNC_LOADING, "no_async_loading"),
        (F::NO_MULTI, "no_multi"),
        (F::ALLOW_BUSY, "allow_busy"),
    ];
    for (f, n) in table {
        if c.flags & f != 0 {
            v.push(Frame::Simple(n.into()));
        }
    }
    if c.movable_keys() {
        v.push(Frame::Simple("movablekeys".into()));
    }
    v
}

fn cat_names(c: &Cmd) -> Vec<Frame> {
    let cats = c.acl_categories();
    cat::NAMES.iter().filter(|(_, b)| cats & b != 0).map(|(n, _)| Frame::Simple(format!("@{}", n))).collect()
}

fn key_specs(c: &Cmd) -> Vec<Frame> {
    let flags = if c.flags & F::WRITE != 0 {
        vec![Frame::Simple("RW".into()), Frame::Simple("UPDATE".into())]
    } else {
        vec![Frame::Simple("RO".into()), Frame::Simple("ACCESS".into())]
    };
    let mk = |begin: Frame, find: Frame| {
        Frame::Map(vec![
            (Frame::bulk_str("flags"), Frame::Set(flags.clone())),
            (Frame::bulk_str("begin_search"), begin),
            (Frame::bulk_str("find_keys"), find),
        ])
    };
    match c.keys {
        KeySpec::None => vec![],
        KeySpec::Range(first, last, step) => {
            let begin = Frame::Map(vec![
                (Frame::bulk_str("type"), Frame::bulk_str("index")),
                (Frame::bulk_str("spec"), Frame::Map(vec![(Frame::bulk_str("index"), Frame::Integer(first as i64))])),
            ]);
            let lastkey = if last < 0 { last as i64 } else { (last - first) as i64 };
            let find = Frame::Map(vec![
                (Frame::bulk_str("type"), Frame::bulk_str("range")),
                (
                    Frame::bulk_str("spec"),
                    Frame::Map(vec![
                        (Frame::bulk_str("lastkey"), Frame::Integer(lastkey)),
                        (Frame::bulk_str("keystep"), Frame::Integer(step as i64)),
                        (Frame::bulk_str("limit"), Frame::Integer(0)),
                    ]),
                ),
            ]);
            vec![mk(begin, find)]
        }
        KeySpec::NumKeys(idx) => {
            let begin = Frame::Map(vec![
                (Frame::bulk_str("type"), Frame::bulk_str("index")),
                (Frame::bulk_str("spec"), Frame::Map(vec![(Frame::bulk_str("index"), Frame::Integer(idx as i64))])),
            ]);
            let find = Frame::Map(vec![
                (Frame::bulk_str("type"), Frame::bulk_str("keynum")),
                (
                    Frame::bulk_str("spec"),
                    Frame::Map(vec![
                        (Frame::bulk_str("keynumidx"), Frame::Integer(0)),
                        (Frame::bulk_str("firstkey"), Frame::Integer(1)),
                        (Frame::bulk_str("keystep"), Frame::Integer(1)),
                    ]),
                ),
            ]);
            vec![mk(begin, find)]
        }
        KeySpec::Custom(_) => vec![Frame::Map(vec![
            (Frame::bulk_str("flags"), Frame::Set(flags.clone())),
            (Frame::bulk_str("begin_search"), Frame::Map(vec![(Frame::bulk_str("type"), Frame::bulk_str("unknown")), (Frame::bulk_str("spec"), Frame::Map(vec![]))])),
            (Frame::bulk_str("find_keys"), Frame::Map(vec![(Frame::bulk_str("type"), Frame::bulk_str("unknown")), (Frame::bulk_str("spec"), Frame::Map(vec![]))])),
        ])],
    }
}

pub fn command_entry(c: &Cmd) -> Frame {
    let (first, last, step) = c.legacy_range();
    let (first, last, step) = if let KeySpec::Range(..) = c.keys { (first, last, step) } else { (first, last, step) };
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(c.full_name())),
        Frame::Integer(c.arity as i64),
        Frame::Set(flag_names(c)),
        Frame::Integer(first),
        Frame::Integer(last),
        Frame::Integer(step),
        Frame::Set(cat_names(c)),
        Frame::Set(vec![]),
        Frame::Array(key_specs(c)),
        Frame::Array(c.subs.iter().map(command_entry).collect()),
    ])
}

pub fn command(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Array(super::all_commands().map(command_entry).collect())
}

pub fn command_count(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(super::all_commands().count() as i64)
}

pub fn command_info(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() == 2 {
        return Frame::Array(super::all_commands().map(command_entry).collect());
    }
    Frame::Array(
        args[2..]
            .iter()
            .map(|n| match super::lookup_full(&String::from_utf8_lossy(n).to_ascii_lowercase()) {
                Some(c) => command_entry(c),
                None => Frame::NullArray,
            })
            .collect(),
    )
}

fn doc_entry(c: &Cmd) -> Frame {
    let mut fields = vec![
        (Frame::bulk_str("summary"), Frame::Bulk(Bytes::from(format!("{} command", c.full_name().to_uppercase())))),
        (Frame::bulk_str("since"), Frame::bulk_str("1.0.0")),
        (Frame::bulk_str("group"), Frame::bulk_str(if c.group.is_empty() { "server" } else { c.group })),
    ];
    if !c.subs.is_empty() {
        fields.push((
            Frame::bulk_str("subcommands"),
            Frame::Map(c.subs.iter().map(|s| (Frame::Bulk(Bytes::from(s.full_name())), doc_entry(s))).collect()),
        ));
    }
    Frame::Map(fields)
}

pub fn command_docs(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() == 2 {
        return Frame::Map(super::all_commands().map(|c| (Frame::bulk_str(c.name), doc_entry(c))).collect());
    }
    Frame::Map(
        args[2..]
            .iter()
            .filter_map(|n| {
                let name = String::from_utf8_lossy(n).to_ascii_lowercase();
                super::lookup_full(&name).map(|c| (Frame::Bulk(Bytes::from(c.full_name())), doc_entry(c)))
            })
            .collect(),
    )
}

pub fn command_list(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut names: Vec<String> = Vec::new();
    for c in super::all_commands() {
        names.push(c.name.to_string());
        for s in c.subs {
            names.push(s.full_name());
        }
    }
    if args.len() == 5 && eq(&args[2], "filterby") {
        let v = String::from_utf8_lossy(&args[4]).to_ascii_lowercase();
        if eq(&args[3], "pattern") {
            names.retain(|n| crate::util::glob_match(v.as_bytes(), n.as_bytes(), true));
        } else if eq(&args[3], "aclcat") {
            let Some((_, bit)) = cat::NAMES.iter().find(|(n, _)| *n == v) else { return Frame::Array(vec![]) };
            names.retain(|n| super::lookup_full(n).map(|c| c.acl_categories() & bit != 0).unwrap_or(false));
        } else if eq(&args[3], "module") {
            names.clear();
        } else {
            return err_syntax();
        }
    } else if args.len() != 2 {
        return err_syntax();
    }
    Frame::Array(names.into_iter().map(|n| Frame::Bulk(Bytes::from(n))).collect())
}

fn getkeys(args: &[Bytes]) -> Result<(&'static Cmd, Vec<usize>), Frame> {
    let sub = &args[2..];
    let Some(c) = super::lookup(&sub[0]) else { return Err(Frame::err("ERR Invalid command specified")) };
    let c = if !c.subs.is_empty() && sub.len() > 1 {
        super::lookup_sub(c, &sub[1]).unwrap_or(c)
    } else {
        c
    };
    let n = sub.len() as i32;
    if (c.arity > 0 && n != c.arity) || (c.arity < 0 && n < -c.arity) {
        return Err(Frame::err("ERR Invalid number of arguments specified for command"));
    }
    let idx = c.key_indexes(sub);
    if idx.is_empty() {
        return Err(Frame::err("ERR The command has no key arguments"));
    }
    Ok((c, idx))
}

pub fn command_getkeys(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match getkeys(args) {
        Ok((_, idx)) => Frame::Array(idx.into_iter().map(|i| Frame::Bulk(args[2 + i].clone())).collect()),
        Err(e) => e,
    }
}

pub fn command_getkeysandflags(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match getkeys(args) {
        Ok((c, idx)) => {
            let flags = if c.flags & F::WRITE != 0 {
                vec![Frame::Simple("RW".into()), Frame::Simple("UPDATE".into())]
            } else {
                vec![Frame::Simple("RO".into()), Frame::Simple("ACCESS".into())]
            };
            Frame::Array(idx.into_iter().map(|i| Frame::Array(vec![Frame::Bulk(args[2 + i].clone()), Frame::Set(flags.clone())])).collect())
        }
        Err(e) => e,
    }
}

pub fn command_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "COMMAND",
        &[
            "(no subcommand)",
            "    Return details about all Redis commands.",
            "COUNT",
            "    Return the total number of commands in this Redis server.",
            "LIST",
            "    Return a list of all commands in this Redis server.",
            "INFO [<command-name> ...]",
            "    Return details about multiple Redis commands.",
            "    If no command names are given, documentation details for all",
            "    commands are returned.",
            "DOCS [<command-name> ...]",
            "    Return documentation details about multiple Redis commands.",
            "    If no command names are given, documentation details for all",
            "    commands are returned.",
            "GETKEYS <full-command>",
            "    Return the keys from a full Redis command.",
            "GETKEYSANDFLAGS <full-command>",
            "    Return the keys and the access flags from a full Redis command.",
        ],
    )
}

// ---------------- MEMORY ----------------

pub fn memory_usage(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 3 {
        if args.len() != 5 || !eq(&args[3], "samples") {
            return err_syntax();
        }
        if crate::util::parse_i64(&args[4]).is_none() {
            return err_int();
        }
    }
    match ctx.s.lookup(ctx.db, &args[2], crate::db::LOOKUP_NOTOUCH) {
        Some(o) => Frame::Integer((o.v.mem_usage() + args[2].len() + 56) as i64),
        None => Frame::Null,
    }
}

pub fn memory_stats(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let used = ctx.s.used_memory();
    let keys = ctx.s.total_keys();
    let mut v = vec![
        (Frame::bulk_str("peak.allocated"), Frame::Integer(ctx.s.used_memory_peak.max(used) as i64)),
        (Frame::bulk_str("total.allocated"), Frame::Integer(used as i64)),
        (Frame::bulk_str("startup.allocated"), Frame::Integer(0)),
        (Frame::bulk_str("replication.backlog"), Frame::Integer(ctx.s.repl.backlog_len() as i64)),
        (Frame::bulk_str("clients.slaves"), Frame::Integer(0)),
        (Frame::bulk_str("clients.normal"), Frame::Integer((ctx.s.clients.len() * 1024) as i64)),
        (Frame::bulk_str("cluster.links"), Frame::Integer(0)),
        (Frame::bulk_str("aof.buffer"), Frame::Integer(0)),
        (Frame::bulk_str("lua.caches"), Frame::Integer(0)),
        (Frame::bulk_str("functions.caches"), Frame::Integer(0)),
    ];
    for d in &ctx.s.dbs {
        if d.len() > 0 {
            v.push((
                Frame::Bulk(Bytes::from(format!("db.{}", d.id))),
                Frame::Map(vec![
                    (Frame::bulk_str("overhead.hashtable.main"), Frame::Integer((d.dict.buckets() * 8 + d.len() * 24) as i64)),
                    (Frame::bulk_str("overhead.hashtable.expires"), Frame::Integer((d.expires.buckets() * 8 + d.expires.len() * 24) as i64)),
                ]),
            ));
        }
    }
    v.extend([
        (Frame::bulk_str("overhead.total"), Frame::Integer((ctx.s.clients.len() * 1024) as i64)),
        (Frame::bulk_str("db.dict.rehashing.count"), Frame::Integer(0)),
        (Frame::bulk_str("keys.count"), Frame::Integer(keys as i64)),
        (Frame::bulk_str("keys.bytes-per-key"), Frame::Integer(if keys > 0 { (used / keys) as i64 } else { 0 })),
        (Frame::bulk_str("dataset.bytes"), Frame::Integer(used as i64)),
        (Frame::bulk_str("dataset.percentage"), Frame::Double(100.0)),
        (Frame::bulk_str("peak.percentage"), Frame::Double(used as f64 * 100.0 / ctx.s.used_memory_peak.max(used).max(1) as f64)),
        (Frame::bulk_str("fragmentation"), Frame::Double(crate::mem::rss() as f64 / used.max(1) as f64)),
    ]);
    Frame::Map(v)
}

pub fn memory_doctor(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Verbatim("txt", Bytes::from_static(b"Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base."))
}

pub fn memory_malloc_stats(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Verbatim("txt", Bytes::from(format!("Stats not supported for the current allocator (allocated: {} bytes)", crate::mem::used_memory())))
}

pub fn memory_purge(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::ok()
}

pub fn memory_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "MEMORY",
        &[
            "DOCTOR",
            "    Return memory problems reports.",
            "MALLOC-STATS",
            "    Return internal statistics report from the memory allocator.",
            "PURGE",
            "    Attempt to purge dirty pages for reclamation by the allocator.",
            "STATS",
            "    Return information about the memory usage of the server.",
            "USAGE <key> [SAMPLES <count>]",
            "    Return memory in bytes used by <key> and its value. Nested values are",
            "    sampled up to <count> times (default: 5, 0 means sample all).",
        ],
    )
}

// ---------------- SLOWLOG ----------------

pub fn slowlog_get(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let count = if args.len() == 3 {
        match crate::util::parse_i64(&args[2]) {
            Some(-1) => usize::MAX,
            Some(v) if v >= 0 => v as usize,
            _ => return Frame::err("ERR count should be greater than or equal to -1"),
        }
    } else if args.len() > 3 {
        return err_syntax();
    } else {
        10
    };
    Frame::Array(
        ctx.s
            .slowlog
            .iter()
            .take(count)
            .map(|e| {
                Frame::Array(vec![
                    Frame::Integer(e.id as i64),
                    Frame::Integer(e.time),
                    Frame::Integer(e.duration_us as i64),
                    Frame::Array(e.args.iter().map(|a| Frame::Bulk(a.clone())).collect()),
                    Frame::Bulk(Bytes::from(e.peer.clone())),
                    Frame::Bulk(Bytes::from(e.name.clone())),
                ])
            })
            .collect(),
    )
}

pub fn slowlog_len(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.s.slowlog.len() as i64)
}

pub fn slowlog_reset(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    ctx.s.slowlog.clear();
    Frame::ok()
}

pub fn slowlog_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "SLOWLOG",
        &[
            "GET [<count>]",
            "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
            "    Entries are made of:",
            "    id, timestamp, time in microseconds, arguments array, client IP and port,",
            "    client name",
            "LEN",
            "    Return the length of the slowlog.",
            "RESET",
            "    Reset the slowlog.",
        ],
    )
}

// ---------------- LATENCY ----------------

pub fn latency_latest(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Array(
        ctx.s
            .latency
            .events
            .iter()
            .filter_map(|(n, e)| {
                let last = e.samples.back()?;
                Some(Frame::Array(vec![
                    Frame::Bulk(Bytes::from(n.clone())),
                    Frame::Integer(last.0),
                    Frame::Integer(last.1 as i64),
                    Frame::Integer(e.max as i64),
                ]))
            })
            .collect(),
    )
}

pub fn latency_history(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let ev = String::from_utf8_lossy(&args[2]).into_owned();
    match ctx.s.latency.events.get(&ev) {
        None => Frame::Array(vec![]),
        Some(e) => Frame::Array(e.samples.iter().map(|(t, v)| Frame::Array(vec![Frame::Integer(*t), Frame::Integer(*v as i64)])).collect()),
    }
}

pub fn latency_reset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let evs: Vec<String> = args[2..].iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect();
    Frame::Integer(ctx.s.latency.reset(&evs) as i64)
}

pub fn latency_doctor(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let mut s = String::new();
    if ctx.s.latency.events.is_empty() {
        s.push_str("Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sleep better.\n");
    } else {
        s.push_str("Dave, I have observed latency spikes in this Redis instance. You don't mind talking about it, do you Dave?\n\n");
        for (i, (n, e)) in ctx.s.latency.events.iter().enumerate() {
            let avg = e.samples.iter().map(|x| x.1).sum::<u64>() / e.samples.len().max(1) as u64;
            let _ = writeln!(s, "{}. {}: {} latency spikes (average {}ms, max {}ms).", i + 1, n, e.samples.len(), avg, e.max);
        }
    }
    Frame::Verbatim("txt", Bytes::from(s))
}

pub fn latency_graph(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let ev = String::from_utf8_lossy(&args[2]).into_owned();
    let Some(e) = ctx.s.latency.events.get(&ev) else {
        return Frame::err(format!("ERR No samples available for event '{}'", ev));
    };
    let mut s = format!("{} - high {} ms, low {} ms (all time high {} ms)\n", ev, e.samples.iter().map(|x| x.1).max().unwrap_or(0), e.samples.iter().map(|x| x.1).min().unwrap_or(0), e.max);
    s.push_str(&"-".repeat(80));
    s.push('\n');
    let max = e.max.max(1);
    for row in (1..=4).rev() {
        for (_, v) in &e.samples {
            s.push(if *v * 4 / max >= row { '#' } else { ' ' });
        }
        s.push('\n');
    }
    Frame::Verbatim("txt", Bytes::from(s))
}

pub fn latency_histogram(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let filter: Vec<String> = args[2..].iter().map(|a| crate::util::lower(a)).collect();
    let mut out = Vec::new();
    let mut names: Vec<&String> = ctx.s.stats.cmdstats.keys().collect();
    names.sort();
    for n in names {
        if !filter.is_empty() && !filter.contains(n) {
            continue;
        }
        let c = &ctx.s.stats.cmdstats[n];
        if c.calls == 0 {
            continue;
        }
        let avg = (c.usec / c.calls.max(1)).max(1);
        let bucket = avg.next_power_of_two() as i64;
        out.push((
            Frame::Bulk(Bytes::from(n.clone())),
            Frame::Map(vec![
                (Frame::bulk_str("calls"), Frame::Integer(c.calls as i64)),
                (Frame::bulk_str("histogram_usec"), Frame::Map(vec![(Frame::Integer(bucket), Frame::Integer(c.calls as i64))])),
            ]),
        ));
    }
    Frame::Map(out)
}

pub fn latency_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "LATENCY",
        &[
            "DOCTOR",
            "    Return a human readable latency analysis report.",
            "GRAPH <event>",
            "    Return an ASCII latency graph for the <event> class.",
            "HISTORY <event>",
            "    Return time-latency samples for the <event> class.",
            "LATEST",
            "    Return the latest latency samples for all events.",
            "RESET [<event> ...]",
            "    Reset latency data of one or more <event> classes.",
            "    (default: reset all data for all event classes)",
            "HISTOGRAM [COMMAND ...]",
            "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
            "    If no commands are specified then all histograms are replied.",
        ],
    )
}

// ---------------- MODULE ----------------

pub fn module_list(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Array(vec![])
}
pub fn module_load(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::err("ERR Error loading the extension. Please check the server logs.")
}
pub fn module_unload(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::err("ERR Error unloading module: no such module with that name")
}
pub fn module_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header("MODULE", &["LIST", "    Return a list of loaded modules.", "LOAD <path> [<arg> ...]", "    Load a module library from <path>, passing to it any optional arguments.", "UNLOAD <name>", "    Unload a module."])
}

// ---------------- DEBUG ----------------

fn digest_value(v: &Value) -> [u8; 20] {
    let mut h = sha1_smol::Sha1::new();
    match v {
        Value::Str(b) => h.update(b),
        Value::List(l) => {
            for e in l {
                h.update(e);
                h.update(b"\0");
            }
        }
        Value::Set(s) => {
            let mut acc = [0u8; 20];
            for k in s.keys() {
                let d = sha1_smol::Sha1::from(k).digest().bytes();
                for i in 0..20 {
                    acc[i] ^= d[i];
                }
            }
            h.update(&acc);
        }
        Value::Hash(hs) => {
            let mut acc = [0u8; 20];
            for (k, val) in hs.map.iter() {
                let mut x = sha1_smol::Sha1::new();
                x.update(k);
                x.update(b"\0");
                x.update(val);
                let d = x.digest().bytes();
                for i in 0..20 {
                    acc[i] ^= d[i];
                }
            }
            h.update(&acc);
        }
        Value::ZSet(z) => {
            for (m, s) in z.iter() {
                h.update(&m);
                h.update(crate::util::fmt_g17(s).as_bytes());
            }
        }
        Value::Stream(st) => {
            h.update(&st.digest());
        }
    }
    h.digest().bytes()
}

pub fn dataset_digest(s: &Store) -> String {
    let mut acc = [0u8; 20];
    let mut any = false;
    for d in &s.dbs {
        for (k, o) in d.dict.iter() {
            any = true;
            let mut x = sha1_smol::Sha1::new();
            x.update(&(d.id as u64).to_le_bytes());
            x.update(k);
            x.update(o.v.type_name().as_bytes());
            x.update(&digest_value(&o.v));
            if let Some(e) = d.expire_of(k) {
                x.update(&e.to_le_bytes());
            }
            let dg = x.digest().bytes();
            for i in 0..20 {
                acc[i] ^= dg[i];
            }
        }
    }
    if !any {
        return "0".repeat(40);
    }
    acc.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn debug(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cfg.enable_debug_command == "no" {
        return Frame::err("ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.");
    }
    let sub = crate::util::lower(&args[1]);
    match sub.as_str() {
        "help" => help_with_header("DEBUG", &["SLEEP <seconds>", "RELOAD [MERGE] [NOFLUSH] [NOSAVE]", "LOADAOF", "OBJECT <key>", "DIGEST", "DIGEST-VALUE <key> [<key> ...]", "SET-ACTIVE-EXPIRE <0|1>", "PROTOCOL <type>", "JMAP", "CHANGE-REPL-ID", "STRINGMATCH-LEN <pattern> <string>"]),
        "sleep" => {
            let secs = args.get(2).and_then(|a| crate::util::parse_f64(a)).unwrap_or(0.0);
            std::thread::sleep(std::time::Duration::from_micros((secs * 1e6) as u64));
            Frame::ok()
        }
        "set-active-expire" => {
            let on = args.get(2).map(|a| a.as_ref() == b"1").unwrap_or(true);
            ACTIVE_EXPIRE.store(on, Ordering::SeqCst);
            Frame::ok()
        }
        "reload" => {
            let mut nosave = false;
            let mut noflush = false;
            for a in &args[2..] {
                if eq(a, "nosave") {
                    nosave = true;
                } else if eq(a, "noflush") {
                    noflush = true;
                } else if eq(a, "merge") {
                    noflush = true;
                } else {
                    return Frame::err("ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.");
                }
            }
            if !nosave {
                if let Err(e) = crate::rdb::save(ctx.s) {
                    return Frame::err(format!("ERR Error trying to save the DB: {}", e));
                }
            }
            if !noflush {
                let n = ctx.s.dbs.len();
                ctx.s.dbs = (0..n).map(crate::db::Db::new).collect();
            }
            match crate::rdb::load_file(ctx.s) {
                Ok(_) => Frame::ok(),
                Err(e) => Frame::err(format!("ERR Error trying to load the RDB dump: {}", e)),
            }
        }
        "loadaof" => {
            if ctx.s.aof.is_some() {
                crate::aof::flush_now(ctx.s);
            }
            let n = ctx.s.dbs.len();
            ctx.s.dbs = (0..n).map(crate::db::Db::new).collect();
            match crate::aof::load(ctx.s) {
                Ok(_) => Frame::ok(),
                Err(e) => Frame::err(format!("ERR Error loading the AOF: {}", e)),
            }
        }
        "digest" => Frame::Simple(dataset_digest(ctx.s)),
        "digest-value" => Frame::Array(
            args[2..]
                .iter()
                .map(|k| {
                    let d = ctx.s.dbs[ctx.db].dict.get(k).map(|o| digest_value(&o.v));
                    Frame::Simple(match d {
                        Some(d) => d.iter().map(|b| format!("{:02x}", b)).collect(),
                        None => "0".repeat(40),
                    })
                })
                .collect(),
        ),
        "object" => {
            let Some(k) = args.get(2) else { return err_syntax() };
            let cfg = ctx.s.cfg.clone();
            match ctx.s.dbs[ctx.db].dict.get(k) {
                None => err_nokey(),
                Some(o) => {
                    let ql = match &o.v {
                        Value::List(l) => format!(" ql_nodes:{} ql_avg_node:{:.2} ql_listpack_max:{} ql_compressed:0 ql_uncompressed_size:{}", (l.len() / 128).max(1), l.len() as f64 / (l.len() / 128).max(1) as f64, cfg.list_max_listpack_size, l.iter().map(|e| e.len()).sum::<usize>()),
                        _ => String::new(),
                    };
                    Frame::Simple(format!(
                        "Value at:0x{:x} refcount:1 encoding:{} serializedlength:{} lru:{} lru_seconds_idle:{}{}",
                        o as *const _ as usize,
                        o.v.encoding(&cfg),
                        crate::rdb::dump_payload(&o.v).len().saturating_sub(10),
                        o.lru,
                        crate::evict::idle_ms(o.lru) / 1000,
                        ql
                    ))
                }
            }
        }
        "protocol" => {
            let t = args.get(2).map(|a| crate::util::lower(a)).unwrap_or_default();
            match t.as_str() {
                "string" => Frame::bulk_str("Hello World"),
                "integer" => Frame::Integer(12345),
                "double" => Frame::Double(3.141),
                "bignum" => Frame::BigNumber("1234567999999999999999999999999999999".into()),
                "null" => Frame::Null,
                "array" => Frame::Array(vec![Frame::Integer(0), Frame::Integer(1), Frame::Integer(2)]),
                "set" => Frame::Set(vec![Frame::Integer(0), Frame::Integer(1), Frame::Integer(2)]),
                "map" => Frame::Map(vec![(Frame::Integer(0), Frame::Bool(false)), (Frame::Integer(1), Frame::Bool(true)), (Frame::Integer(2), Frame::Bool(false))]),
                "attrib" => {
                    if ctx.proto() >= 3 {
                        let mut out = Vec::new();
                        out.extend_from_slice(b"|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n");
                        Frame::Seq(vec![Frame::Raw(Bytes::from(out)), Frame::bulk_str("Some real reply following the attribute")])
                    } else {
                        Frame::bulk_str("Some real reply following the attribute")
                    }
                }
                "push" => {
                    if ctx.proto() < 3 {
                        return Frame::err("ERR RESP2 is not supported by this command");
                    }
                    Frame::Seq(vec![Frame::Push(vec![Frame::bulk_str("server-cpu-usage"), Frame::Integer(42)]), Frame::bulk_str("Some real reply following the push reply")])
                }
                "true" => Frame::Bool(true),
                "false" => Frame::Bool(false),
                "verbatim" => Frame::Verbatim("txt", Bytes::from_static(b"This is a verbatim\nstring")),
                _ => Frame::err("ERR Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false"),
            }
        }
        "jmap" => Frame::ok(),
        "change-repl-id" => {
            ctx.s.repl.change_replid();
            Frame::ok()
        }
        "stringmatch-len" => {
            if args.len() < 4 {
                return err_syntax();
            }
            Frame::Integer(crate::util::glob_match(&args[2], &args[3], false) as i64)
        }
        "error" => Frame::Error(String::from_utf8_lossy(args.get(2).map(|a| a.as_ref()).unwrap_or(b"ERR")).into_owned()),
        "log" => {
            eprintln!("DEBUG LOG: {}", String::from_utf8_lossy(args.get(2).map(|a| a.as_ref()).unwrap_or(b"")));
            Frame::ok()
        }
        "quicklist-packed-threshold" | "listpack" | "quicklist" | "replybuffer" | "dict-resizing" | "pause-cron" | "set-skip-checksum-validation" | "set-disable-deny-scripts" | "client-eviction" | "script" | "mallctl" | "mallctl-str" | "lazyfree-free-effort" => Frame::ok(),
        "htstats" => Frame::Verbatim("txt", Bytes::from(format!("[Dictionary HT]\nHash table 0 stats (main hash table):\n table size: {}\n number of elements: {}\n", ctx.s.dbs[ctx.db].dict.buckets(), ctx.s.dbs[ctx.db].len()))),
        "htstats-key" => match args.get(2).and_then(|k| ctx.s.dbs[ctx.db].dict.get(k)) {
            None => err_nokey(),
            Some(o) => match &o.v {
                Value::Hash(_) | Value::Set(_) | Value::ZSet(_) => Frame::Verbatim("txt", Bytes::from_static(b"Hash table 0 stats (main hash table):\n")),
                _ => Frame::err("ERR The value stored at the specified key is not represented using an hash table"),
            },
        },
        "segfault" | "panic" | "assert" | "oom" => {
            eprintln!("DEBUG {} requested: exiting", sub);
            std::process::abort();
        }
        _ => Frame::err(format!("ERR unknown subcommand '{}'. Try DEBUG HELP.", String::from_utf8_lossy(&args[1]))),
    }
}
