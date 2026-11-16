//! Command processing pipeline: lookup, arity, auth/ACL, state checks, MULTI
//! queueing, execution, statistics, propagation and blocked-client handling.

use crate::cmd::{self, Cmd, flags as F};
use crate::resp::Frame;
use crate::server::{Store, cflags};
use bytes::Bytes;

pub const CTX_MULTI: u32 = 1;
pub const CTX_LUA: u32 = 2;
pub const CTX_REPROCESS: u32 = 4;
pub const CTX_LOADING: u32 = 8;
pub const CTX_MASTER: u32 = 16;
pub const CTX_NO_BLOCK: u32 = CTX_MULTI | CTX_LUA | CTX_LOADING | CTX_MASTER;

/// Execution context handed to every command handler.
pub struct Ctx<'a> {
    pub s: &'a mut Store,
    pub cid: u64,
    pub db: usize,
    pub flags: u32,
    /// Replace the propagated argv for this command.
    pub rewrite: Option<Vec<Bytes>>,
    /// Suppress propagation of this command (even if dirty changed).
    pub no_prop: bool,
    /// Additional commands to propagate after this one.
    pub also: Vec<(usize, Vec<Bytes>)>,
    pub cmd: &'static Cmd,
}

impl<'a> Ctx<'a> {
    pub fn can_block(&self) -> bool {
        self.flags & CTX_NO_BLOCK == 0
            && !self
                .s
                .clients
                .get(&self.cid)
                .map(|c| c.has(cflags::LUA))
                .unwrap_or(false)
    }
    pub fn client(&mut self) -> &mut crate::server::Client {
        self.s.clients.get_mut(&self.cid).expect("client")
    }
    pub fn client_ref(&self) -> Option<&crate::server::Client> {
        self.s.clients.get(&self.cid)
    }
    pub fn proto(&self) -> u8 {
        self.s.clients.get(&self.cid).map(|c| c.proto).unwrap_or(2)
    }
    pub fn dirty(&mut self, n: u64) {
        self.s.dirty += n;
    }
    pub fn notify(&mut self, class: u32, event: &str, key: &Bytes) {
        let db = self.db;
        self.s.notify(class, event, key, db);
    }
    pub fn modified(&mut self, key: &Bytes) {
        let db = self.db;
        self.s.signal_modified(db, key);
    }
    pub fn propagate_as(&mut self, argv: Vec<Bytes>) {
        self.rewrite = Some(argv);
    }
    pub fn also_propagate(&mut self, argv: Vec<Bytes>) {
        let db = self.db;
        self.also.push((db, argv));
    }
}

pub enum Outcome {
    Reply(Frame),
    Block(tokio::sync::oneshot::Receiver<Frame>),
    /// Reply then close the connection.
    Close(Frame),
}

fn args_preview(args: &[Bytes]) -> String {
    let mut s = String::new();
    for a in args.iter().skip(1).take(10) {
        let a = String::from_utf8_lossy(a);
        let a: String = a.chars().take(128).collect();
        s.push('\'');
        s.push_str(&a);
        s.push_str("' ");
    }
    s
}

pub fn unknown_command_error(args: &[Bytes]) -> Frame {
    let name: String = String::from_utf8_lossy(&args[0]).chars().take(128).collect();
    Frame::err(format!(
        "ERR unknown command '{}', with args beginning with: {}",
        name,
        args_preview(args)
    ))
}

/// Resolve command and subcommand. Returns Err(reply) for unknown/arity errors.
pub fn resolve(args: &[Bytes]) -> Result<&'static Cmd, Frame> {
    let Some(c) = cmd::lookup(&args[0]) else {
        return Err(unknown_command_error(args));
    };
    if !c.subs.is_empty() {
        if args.len() < 2 {
            return Err(Frame::err(format!("ERR wrong number of arguments for '{}' command", c.name)));
        }
        let Some(sub) = cmd::lookup_sub(c, &args[1]) else {
            let sname: String = String::from_utf8_lossy(&args[1]).chars().take(128).collect();
            return Err(Frame::err(format!(
                "ERR unknown subcommand '{}'. Try {} HELP.",
                sname,
                c.name.to_ascii_uppercase()
            )));
        };
        if !arity_ok(sub.arity, args.len()) {
            return Err(Frame::err(format!(
                "ERR wrong number of arguments for '{}|{}' command",
                c.name, sub.name
            )));
        }
        return Ok(sub);
    }
    if !arity_ok(c.arity, args.len()) {
        return Err(Frame::err(format!("ERR wrong number of arguments for '{}' command", c.name)));
    }
    Ok(c)
}

#[inline]
fn arity_ok(arity: i32, n: usize) -> bool {
    (arity > 0 && n as i32 == arity) || (arity < 0 && n as i32 >= -arity)
}

fn reject(s: &mut Store, c: Option<&'static Cmd>, f: Frame) -> Outcome {
    if let Frame::Error(e) = &f {
        s.record_error(e);
        if let Some(c) = c {
            s.stats.cmdstats.entry(c.full_name()).or_default().rejected += 1;
        }
    }
    Outcome::Reply(f)
}

/// Process a command from a regular client connection.
pub fn process(s: &mut Store, cid: u64, args: Vec<Bytes>) -> Outcome {
    if args.is_empty() {
        return Outcome::Reply(Frame::NoReply);
    }
    s.current_client = cid;
    let (flags, authed, in_multi, db) = match s.clients.get_mut(&cid) {
        Some(c) => {
            c.last_interaction_ms = crate::util::now_ms();
            (c.flags, c.authenticated, c.has(cflags::MULTI), c.db)
        }
        None => return Outcome::Close(Frame::NoReply),
    };
    let cmd = match resolve(&args) {
        Ok(c) => c,
        Err(e) => {
            if in_multi {
                if let Some(c) = s.clients.get_mut(&cid) {
                    c.flags |= cflags::DIRTY_EXEC;
                }
            }
            let c = cmd::lookup(&args[0]);
            return reject(s, c, e);
        }
    };
    if let Some(c) = s.clients.get_mut(&cid) {
        c.last_cmd = cmd.full_name();
    }
    // Authentication.
    if !authed && cmd.flags & F::NO_AUTH == 0 {
        let e = Frame::err("NOAUTH Authentication required.");
        if in_multi {
            s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
        }
        return reject(s, Some(cmd), e);
    }
    // ACL permission check.
    if let Err(e) = crate::acl::check_command(s, cid, cmd, &args) {
        if in_multi {
            s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
        }
        return reject(s, Some(cmd), e);
    }
    let is_exec_family = matches!(cmd.name, "exec" | "discard" | "multi" | "watch" | "quit" | "reset");
    // Cluster redirection.
    if s.cluster.is_some() && flags & cflags::MASTER == 0 {
        if let Some(redir) = crate::cluster::redirect(s, cid, cmd, &args) {
            if in_multi {
                s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
            }
            return reject(s, Some(cmd), redir);
        }
    }
    // Pub/Sub context restriction (RESP2 only).
    let (proto, in_pubsub) = {
        let c = &s.clients[&cid];
        (c.proto, c.in_pubsub())
    };
    if in_pubsub && proto == 2 && cmd.flags & F::PUBSUB_CTX_OK == 0 {
        return reject(
            s,
            Some(cmd),
            Frame::err(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd.full_name()
            )),
        );
    }
    let is_write = cmd.is_write_for(&args);
    // Loading.
    if s.persist.loading && cmd.flags & F::LOADING == 0 {
        return reject(s, Some(cmd), Frame::err("LOADING Redis is loading the dataset in memory"));
    }
    // Replica checks.
    if s.repl.is_replica() && flags & cflags::MASTER == 0 {
        if is_write && s.cfg.replica_read_only {
            if in_multi {
                s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
            }
            return reject(s, Some(cmd), Frame::err("READONLY You can't write against a read only replica."));
        }
        if !s.cfg.replica_serve_stale_data && !s.repl.link_up() && cmd.flags & F::STALE == 0 {
            return reject(
                s,
                Some(cmd),
                Frame::err("MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."),
            );
        }
    }
    if !in_multi || is_exec_family {
        // OOM.
        if s.cfg.maxmemory > 0 && cmd.flags & F::DENYOOM != 0 && !s.persist.loading {
            crate::evict::perform_evictions(s);
            if s.used_memory() as u64 > s.cfg.maxmemory {
                if in_multi {
                    s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
                }
                return reject(s, Some(cmd), Frame::err("OOM command not allowed when used memory > 'maxmemory'."));
            }
        }
        // Persistence errors.
        if is_write
            && s.cfg.stop_writes_on_bgsave_error
            && !s.persist.last_bgsave_ok
            && !s.cfg.save_params.is_empty()
            && !s.repl.is_replica()
        {
            return reject(
                s,
                Some(cmd),
                Frame::err("MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error."),
            );
        }
        if is_write && !s.repl.is_replica() && s.cfg.min_replicas_to_write > 0 && crate::repl::count_good(s, s.cfg.min_replicas_max_lag) < s.cfg.min_replicas_to_write {
            if in_multi {
                s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
            }
            return reject(s, Some(cmd), Frame::err("NOREPLICAS Not enough good replicas to write."));
        }
    }
    // MULTI queueing.
    if in_multi && !is_exec_family {
        if cmd.flags & F::NO_MULTI != 0 {
            s.clients.get_mut(&cid).unwrap().flags |= cflags::DIRTY_EXEC;
            return reject(s, Some(cmd), Frame::err("ERR Command not allowed inside a transaction"));
        }
        let c = s.clients.get_mut(&cid).unwrap();
        c.multi_mem += args.iter().map(|a| a.len()).sum::<usize>();
        c.mstate.push(args);
        return Outcome::Reply(Frame::queued());
    }
    // CLIENT PAUSE.
    if let Some((_, write_only)) = s.pause {
        let pausable = !write_only || is_write || cmd.flags & F::MAY_REPLICATE != 0;
        if pausable && flags & (cflags::MASTER | cflags::REPLICA) == 0 && !matches!(cmd.name, "client" | "unpause") {
            let spec = crate::blocking::BlockSpec { rewrite: None,
                keys: vec![],
                deadline: None,
                timeout_reply: Frame::NoReply,
                kind: crate::blocking::BlockKind::Pause,
            };
            let rx = s.block_client(cid, args, db, spec);
            return Outcome::Block(rx);
        }
    }
    let reply = call(s, cid, db, cmd, &args, 0);
    let out = match reply {
        Frame::Block(mut spec) => {
            let args = spec.rewrite.take().unwrap_or(args);
            let rx = s.block_client(cid, args, db, *spec);
            Outcome::Block(rx)
        }
        r => {
            let close = s.clients.get(&cid).map(|c| c.has(cflags::CLOSE_AFTER_REPLY)).unwrap_or(true);
            if close { Outcome::Close(r) } else { Outcome::Reply(r) }
        }
    };
    after_command(s);
    out
}

/// Work done after a top-level command: serve blocked clients, flush propagation.
pub fn after_command(s: &mut Store) {
    if !s.blocking.ready.is_empty() {
        s.handle_ready_keys();
    }
    flush_propagation(s);
}

/// Execute a resolved command. `ctx_flags` describes nesting (MULTI/Lua/...).
pub fn call(s: &mut Store, cid: u64, db: usize, cmd: &'static Cmd, args: &[Bytes], ctx_flags: u32) -> Frame {
    let dirty_before = s.dirty;
    let top = s.exec_depth == 0;
    s.exec_depth += 1;
    if top {
        s.cmd_time_snapshot = crate::util::now_ms();
    }
    if !s.monitors.is_empty() && cmd.flags & (F::SKIP_MONITOR | F::ADMIN) == 0 && ctx_flags & CTX_LOADING == 0 {
        crate::pubsub::feed_monitors(s, cid, db, args, ctx_flags & CTX_LUA != 0);
    }
    let start = std::time::Instant::now();
    let mut ctx = Ctx { s, cid, db, flags: ctx_flags, rewrite: None, no_prop: false, also: Vec::new(), cmd };
    let reply = (cmd.f)(&mut ctx, args);
    let Ctx { rewrite, no_prop, also, .. } = ctx;
    let dur = start.elapsed().as_micros() as u64;
    s.exec_depth -= 1;
    if top {
        s.cmd_time_snapshot = 0;
    }
    if matches!(reply, Frame::Block(_)) {
        return reply;
    }
    // Stats.
    if ctx_flags & CTX_LOADING == 0 {
        s.stats.total_commands += 1;
        let st = s.stats.cmdstats.entry(cmd.full_name()).or_default();
        st.calls += 1;
        st.usec += dur;
        if let Frame::Error(e) = &reply {
            st.failed += 1;
            let e = e.clone();
            s.record_error(&e);
        }
        if let Some(c) = s.clients.get_mut(&cid) {
            c.tot_cmds += 1;
        }
        if cmd.flags & F::NO_SLOWLOG == 0 && ctx_flags & CTX_LUA == 0 {
            s.slowlog_push(args, dur, cid);
        }
        if s.cfg.latency_monitor_threshold > 0 && dur / 1000 >= s.cfg.latency_monitor_threshold {
            let ev = if cmd.flags & F::FAST != 0 { "fast-command" } else { "command" };
            s.latency.add(ev, dur / 1000);
        }
    }
    // Propagation.
    if ctx_flags & CTX_LOADING == 0 {
        let pdb = db;
        if s.dirty != dirty_before && !no_prop && cmd.flags & F::NO_PROPAGATE == 0 {
            let argv = rewrite.unwrap_or_else(|| args.to_vec());
            s.propagate(pdb, argv);
        }
        for (adb, argv) in also {
            s.propagate(adb, argv);
        }
    }
    // Client side caching: remember keys read by this client.
    if s.tracking.enabled() && cmd.flags & F::READONLY != 0 && ctx_flags & CTX_LOADING == 0 {
        crate::tracking::remember_keys(s, cid, cmd, args);
    }
    reply
}

/// Re-run a blocked client's command when one of its keys is ready.
pub fn reexecute_blocked(s: &mut Store, cid: u64, db: usize, args: &[Bytes]) -> Frame {
    let Ok(cmd) = resolve(args) else { return Frame::err("ERR internal") };
    let prev = s.current_client;
    s.current_client = cid;
    let r = call(s, cid, db, cmd, args, CTX_REPROCESS);
    s.current_client = prev;
    r
}

/// Execute a command on behalf of an internal pseudo-client (AOF loading, master link, Lua).
pub fn exec_internal(s: &mut Store, cid: u64, db: usize, args: &[Bytes], ctx_flags: u32) -> Frame {
    match resolve(args) {
        Ok(cmd) => call(s, cid, db, cmd, args, ctx_flags),
        Err(e) => e,
    }
}

impl Store {
    /// Queue a command for propagation (AOF + replicas).
    pub fn propagate(&mut self, db: usize, argv: Vec<Bytes>) {
        if self.aof.is_none() && !self.repl.has_backlog() {
            return;
        }
        self.repl.pending.push((db, argv));
    }

    /// Propagate immediately as its own unit (used outside command execution).
    pub fn propagate_now(&mut self, db: usize, argv: Vec<Bytes>) {
        self.propagate(db, argv);
        if self.exec_depth == 0 {
            flush_propagation(self);
        }
    }
}

/// Emit queued propagated commands; wrap in MULTI/EXEC when there is more than one.
pub fn flush_propagation(s: &mut Store) {
    if s.repl.pending.is_empty() {
        return;
    }
    let ops = std::mem::take(&mut s.repl.pending);
    let wrap = ops.len() > 1;
    if wrap {
        let db0 = ops[0].0;
        feed(s, db0, vec![Bytes::from_static(b"MULTI")]);
    }
    let mut last_db = 0;
    for (db, argv) in ops {
        last_db = db;
        feed(s, db, argv);
    }
    if wrap {
        feed(s, last_db, vec![Bytes::from_static(b"EXEC")]);
    }
}

fn feed(s: &mut Store, db: usize, argv: Vec<Bytes>) {
    crate::aof::feed(s, db, &argv);
    crate::repl::feed(s, db, &argv);
}

