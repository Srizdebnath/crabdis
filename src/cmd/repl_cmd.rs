//! Replication commands.

use super::util::*;
use crate::blocking::{BlockKind, BlockSpec};
use crate::exec::Ctx;
use crate::repl::{self, Failover};
use crate::resp::Frame;
use crate::server::cflags;
use bytes::Bytes;

pub fn replicaof(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_some() {
        return Frame::err("ERR REPLICAOF not allowed in cluster mode.");
    }
    if ctx.s.repl.failover.is_some() {
        return Frame::err("ERR REPLICAOF not allowed while failing over.");
    }
    if eq(&args[1], "no") && eq(&args[2], "one") {
        if ctx.s.repl.is_replica() {
            repl::unset_master(ctx.s);
        }
        return Frame::ok();
    }
    if ctx.client().has(cflags::REPLICA) {
        return Frame::err("ERR Command is not valid when client is a replica.");
    }
    let port = match crate::util::parse_i64(&args[2]) {
        Some(p) if (0..=65535).contains(&p) => p as u16,
        _ => return Frame::err("ERR Invalid master port"),
    };
    let host = String::from_utf8_lossy(&args[1]).into_owned();
    if ctx.s.repl.master.as_ref() == Some(&(host.clone(), port)) {
        return Frame::Simple("OK Already connected to specified master".into());
    }
    let Some(shared) = crate::SHARED.get().cloned() else { return Frame::err("ERR server not ready") };
    eprintln!("REPLICAOF {}:{} enabled (user request)", host, port);
    repl::set_master(ctx.s, shared, host, port);
    Frame::ok()
}

pub fn role(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let s = &ctx.s;
    if let Some((h, p)) = &s.repl.master {
        let state = match s.repl.state {
            repl::LinkState::Connected => "connected",
            repl::LinkState::Sync => "sync",
            repl::LinkState::Connecting => "connecting",
            _ => "connect",
        };
        return Frame::Array(vec![
            Frame::bulk_str("slave"),
            Frame::bulk(Bytes::from(h.clone())),
            Frame::Integer(*p as i64),
            Frame::bulk_str(state),
            Frame::Integer(if s.repl.link_up() { s.repl.master_offset } else { -1 }),
        ]);
    }
    let mut ids: Vec<&u64> = s.repl.replicas.keys().collect();
    ids.sort();
    let reps = ids
        .into_iter()
        .filter_map(|id| s.clients.get(id))
        .map(|c| {
            let ip = c.addr.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or_default();
            Frame::Array(vec![Frame::bulk(Bytes::from(ip)), Frame::bulk(Bytes::from(c.repl_listening_port.to_string())), Frame::bulk(Bytes::from(c.repl_ack_off.to_string()))])
        })
        .collect();
    Frame::Array(vec![Frame::bulk_str("master"), Frame::Integer(s.repl.master_offset), Frame::Array(reps)])
}

pub fn replconf(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 == 0 {
        return err_syntax();
    }
    let mut i = 1;
    while i < args.len() {
        let opt = &args[i];
        let val = &args[i + 1];
        if eq(opt, "listening-port") {
            let p = tri!(int(val));
            ctx.client().repl_listening_port = p as u16;
        } else if eq(opt, "ip-address") {
            let ip = String::from_utf8_lossy(val).into_owned();
            let c = ctx.client();
            let port = c.addr.rsplit_once(':').map(|(_, p)| p.to_string()).unwrap_or_default();
            c.addr = format!("{}:{}", ip, port);
        } else if eq(opt, "capa") {
            let v = String::from_utf8_lossy(val).to_ascii_lowercase();
            ctx.client().repl_capa.push(v);
        } else if eq(opt, "ack") {
            // ACKs normally arrive on the replication stream; accept them here too.
            if !ctx.client().has(cflags::REPLICA) {
                return Frame::NoReply;
            }
            let off = tri!(int(val));
            let c = ctx.client();
            c.repl_ack_off = c.repl_ack_off.max(off);
            c.repl_ack_time = crate::util::now_ms();
            repl::process_waits(ctx.s);
            return Frame::NoReply;
        } else if eq(opt, "getack") {
            if let Some(m) = ctx.s.repl.master_cid {
                if m == ctx.cid {
                    return Frame::Array(vec![Frame::bulk_str("REPLCONF"), Frame::bulk_str("ACK"), Frame::bulk(Bytes::from(ctx.s.repl.master_offset.to_string()))]);
                }
            }
            return Frame::NoReply;
        } else if eq(opt, "rdb-only") || eq(opt, "rdb-filter-only") || eq(opt, "rdb-channel") || eq(opt, "main-ch-client-id") {
            // Accepted for compatibility with redis-cli --rdb.
        } else {
            return Frame::err(format!("ERR Unrecognized REPLCONF option: {}", String::from_utf8_lossy(opt)));
        }
        i += 2;
    }
    Frame::ok()
}

fn sync_common(ctx: &mut Ctx, replid: &str, offset: i64, legacy: bool) -> Frame {
    if ctx.client().has(cflags::REPLICA) {
        return Frame::NoReply;
    }
    if ctx.s.repl.is_replica() && !ctx.s.repl.link_up() {
        return Frame::err("NOMASTERLINK Can't SYNC while not connected with my master");
    }
    if ctx.client().has(cflags::MULTI) {
        return Frame::err("ERR Replica can't be in a transaction");
    }
    let cid = ctx.cid;
    repl::start_sync(ctx.s, cid, replid, offset, legacy)
}

pub fn psync(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let replid = String::from_utf8_lossy(&args[1]).into_owned();
    let off = tri!(int(&args[2]));
    // FAILOVER handshake: the old master asks us (its replica) to take over.
    if args.len() > 3 && eq(&args[3], "failover") {
        if !ctx.s.repl.is_replica() {
            return Frame::err("ERR PSYNC FAILOVER can't be sent to a master.");
        }
        if replid != ctx.s.repl.replid {
            return Frame::err("ERR PSYNC FAILOVER replid must match my replid.");
        }
        eprintln!("Failover request received for replid {}.", replid);
        repl::unset_master(ctx.s);
    }
    sync_common(ctx, &replid, off, false)
}

pub fn sync(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    sync_common(ctx, "?", -1, true)
}

pub fn wait(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.repl.is_replica() {
        return Frame::err("ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
    }
    let numreplicas = tri!(int(&args[1]));
    let timeout = tri!(timeout_ms(&args[2]));
    let offset = ctx.s.repl.master_offset;
    let acked = ctx.s.repl.replicas.keys().filter(|id| ctx.s.clients.get(id).map(|c| c.repl_ack_off >= offset).unwrap_or(false)).count();
    if acked as i64 >= numreplicas || !ctx.can_block() {
        return Frame::Integer(acked as i64);
    }
    repl::request_acks(ctx.s);
    Frame::Block(Box::new(BlockSpec {
        keys: vec![],
        deadline: if timeout > 0 { Some(crate::util::now_ms() + timeout) } else { None },
        timeout_reply: Frame::Integer(acked as i64),
        kind: BlockKind::Wait { offset, numreplicas: numreplicas.max(0) as usize },
        rewrite: None,
    }))
}

pub fn waitaof(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let numlocal = tri!(int(&args[1]));
    let numreplicas = tri!(int(&args[2]));
    let timeout = tri!(timeout_ms(&args[3]));
    if ctx.s.repl.is_replica() {
        return Frame::err("ERR WAITAOF cannot be used with replica instances. Please also note that writes to replicas are just local and are not propagated.");
    }
    if numlocal > 0 && ctx.s.aof.is_none() {
        return Frame::err("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
    }
    let offset = ctx.s.repl.master_offset;
    if ctx.s.aof.is_some() {
        crate::aof::flush_now(ctx.s);
        ctx.s.repl.aof_fsynced_offset = offset;
    }
    let local = (ctx.s.aof.is_some() && ctx.s.repl.aof_fsynced_offset >= offset) as i64;
    let reps = ctx.s.repl.replicas.values().filter(|l| l.ack_aof_off >= offset).count() as i64;
    let reply = Frame::Array(vec![Frame::Integer(local), Frame::Integer(reps)]);
    if (local >= numlocal && reps >= numreplicas) || !ctx.can_block() {
        return reply;
    }
    repl::request_acks(ctx.s);
    Frame::Block(Box::new(BlockSpec {
        keys: vec![],
        deadline: if timeout > 0 { Some(crate::util::now_ms() + timeout) } else { None },
        timeout_reply: reply,
        kind: BlockKind::WaitAof { offset, numlocal: numlocal.max(0) as usize, numreplicas: numreplicas.max(0) as usize },
        rewrite: None,
    }))
}

fn timeout_ms(b: &[u8]) -> Result<i64, Frame> {
    match crate::util::parse_i64(b) {
        Some(t) if t >= 0 => Ok(t),
        Some(_) => Err(Frame::err("ERR timeout is negative")),
        None => Err(Frame::err("ERR timeout is not an integer or out of range")),
    }
}

pub fn failover(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut target: Option<(String, u16)> = None;
    let (mut force, mut abort, mut timeout) = (false, false, 0i64);
    let mut i = 1;
    while i < args.len() {
        if eq(&args[i], "to") && i + 2 < args.len() {
            let port = match crate::util::parse_i64(&args[i + 2]) {
                Some(p) if (0..=65535).contains(&p) => p as u16,
                _ => return Frame::err("ERR Invalid port"),
            };
            target = Some((String::from_utf8_lossy(&args[i + 1]).into_owned(), port));
            i += 3;
        } else if eq(&args[i], "force") {
            force = true;
            i += 1;
        } else if eq(&args[i], "abort") {
            abort = true;
            i += 1;
        } else if eq(&args[i], "timeout") && i + 1 < args.len() {
            timeout = tri!(int(&args[i + 1]));
            if timeout <= 0 {
                return Frame::err("ERR FAILOVER timeout must be greater than 0");
            }
            i += 2;
        } else {
            return err_syntax();
        }
    }
    if abort {
        if force || timeout > 0 || target.is_some() {
            return Frame::err("ERR FAILOVER ABORT can't be used with other arguments.");
        }
        if ctx.s.repl.failover.is_none() {
            return Frame::err("ERR No failover in progress.");
        }
        repl::abort_failover(ctx.s);
        return Frame::ok();
    }
    if force && (target.is_none() || timeout == 0) {
        return Frame::err("ERR FAILOVER with force option requires both a timeout and target HOST and IP.");
    }
    if ctx.s.cluster.is_some() {
        return Frame::err("ERR FAILOVER not allowed in cluster mode.");
    }
    if ctx.s.repl.is_replica() {
        return Frame::err("ERR FAILOVER is not valid when server is a replica.");
    }
    if ctx.s.repl.replicas.is_empty() {
        return Frame::err("ERR FAILOVER requires connected replicas.");
    }
    if ctx.s.repl.failover.is_some() {
        return Frame::err("ERR FAILOVER already in progress.");
    }
    if let Some((h, p)) = &target {
        let found = ctx.s.repl.replicas.keys().filter_map(|id| ctx.s.clients.get(id)).any(|c| c.repl_listening_port == *p && c.addr.rsplit_once(':').map(|(ip, _)| ip == h).unwrap_or(false));
        if !found {
            return Frame::err("ERR FAILOVER target HOST and PORT is not a replica.");
        }
    }
    let now = crate::util::now_ms();
    ctx.s.repl.failover = Some(Failover { target, force, deadline: if timeout > 0 { Some(now + timeout) } else { None }, offset: ctx.s.repl.master_offset, started: now });
    ctx.s.repl.failover_state = "waiting-for-sync";
    // Pause writes while the target catches up.
    ctx.s.pause = Some((i64::MAX, true));
    repl::request_acks(ctx.s);
    Frame::ok()
}
