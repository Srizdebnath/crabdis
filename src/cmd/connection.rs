//! Connection commands: AUTH HELLO PING ECHO QUIT RESET SELECT.

use super::util::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::server::{VERSION, cflags};
use bytes::Bytes;

pub fn auth(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 3 {
        return err_syntax();
    }
    let (user, pass) = if args.len() == 2 {
        (Bytes::from_static(b"default"), args[1].clone())
    } else {
        (args[1].clone(), args[2].clone())
    };
    if args.len() == 2 && crate::acl::default_user_nopass(ctx.s) {
        return Frame::err("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
    }
    match crate::acl::authenticate(ctx.s, ctx.cid, &user, &pass) {
        Ok(()) => Frame::ok(),
        Err(e) => e,
    }
}

pub fn echo(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Bulk(args[1].clone())
}

pub fn ping(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 2 {
        return Frame::err("ERR wrong number of arguments for 'ping' command");
    }
    let c = ctx.client_ref().unwrap();
    if c.proto == 2 && c.in_pubsub() {
        let msg = args.get(1).cloned().unwrap_or_default();
        return Frame::Array(vec![Frame::bulk_str("pong"), Frame::Bulk(msg)]);
    }
    match args.get(1) {
        Some(m) => Frame::Bulk(m.clone()),
        None => Frame::Simple("PONG".into()),
    }
}

pub fn quit(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    ctx.client().flags |= cflags::CLOSE_AFTER_REPLY;
    Frame::ok()
}

pub fn reset(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let cid = ctx.cid;
    super::multi::discard_transaction(ctx.s, cid);
    super::multi::unwatch_all(ctx.s, cid);
    crate::pubsub::unsubscribe_all(ctx.s, cid);
    crate::tracking::disable(ctx.s, cid);
    ctx.s.monitors.retain(|&id| id != cid);
    let needs_auth = !crate::acl::default_user_authenticated(ctx.s);
    let c = ctx.client();
    c.db = 0;
    c.proto = 2;
    c.name = None;
    c.flags &= !(cflags::REPLY_OFF
        | cflags::REPLY_SKIP
        | cflags::REPLY_SKIP_NEXT
        | cflags::NO_EVICT
        | cflags::NO_TOUCH
        | cflags::MONITOR
        | cflags::READONLY
        | cflags::ASKING);
    c.user = "default".into();
    c.authenticated = !needs_auth;
    ctx.db = 0;
    Frame::Simple("RESET".into())
}

pub fn select(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let id = match crate::util::parse_i64(&args[1]) {
        Some(v) => v,
        None => return err_int(),
    };
    if ctx.s.cluster.is_some() && id != 0 {
        return Frame::err("ERR SELECT is not allowed in cluster mode");
    }
    if id < 0 || id as usize >= ctx.s.dbs.len() {
        return Frame::err("ERR DB index is out of range");
    }
    ctx.client().db = id as usize;
    ctx.db = id as usize;
    Frame::ok()
}

pub fn hello(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut proto = None;
    let mut i = 1;
    if args.len() >= 2 {
        match crate::util::parse_i64(&args[1]) {
            Some(v) => {
                if !(2..=3).contains(&v) {
                    return Frame::err("NOPROTO unsupported protocol version");
                }
                proto = Some(v as u8);
            }
            None => return Frame::err("ERR Protocol version is not an integer or out of range"),
        }
        i = 2;
    }
    let mut setname = None;
    while i < args.len() {
        if eq(&args[i], "auth") && i + 2 < args.len() {
            let (u, p) = (args[i + 1].clone(), args[i + 2].clone());
            if let Err(e) = crate::acl::authenticate(ctx.s, ctx.cid, &u, &p) {
                return e;
            }
            i += 3;
        } else if eq(&args[i], "setname") && i + 1 < args.len() {
            if let Err(e) = super::client::validate_name(&args[i + 1]) {
                return e;
            }
            setname = Some(args[i + 1].clone());
            i += 2;
        } else {
            return Frame::err(format!(
                "ERR Syntax error in HELLO option '{}'",
                String::from_utf8_lossy(&args[i])
            ));
        }
    }
    if !ctx.client_ref().map(|c| c.authenticated).unwrap_or(false) {
        return Frame::err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    let role = if ctx.s.repl.is_replica() { "replica" } else { "master" };
    let mode = if ctx.s.cluster.is_some() { "cluster" } else { "standalone" };
    let id = ctx.cid;
    let c = ctx.client();
    if let Some(p) = proto {
        c.proto = p;
    }
    if let Some(n) = setname {
        c.name = if n.is_empty() { None } else { Some(n) };
    }
    Frame::Map(vec![
        (Frame::bulk_str("server"), Frame::bulk_str("redis")),
        (Frame::bulk_str("version"), Frame::bulk_str(VERSION)),
        (Frame::bulk_str("proto"), Frame::Integer(c.proto as i64)),
        (Frame::bulk_str("id"), Frame::Integer(id as i64)),
        (Frame::bulk_str("mode"), Frame::bulk_str(mode)),
        (Frame::bulk_str("role"), Frame::bulk_str(role)),
        (Frame::bulk_str("modules"), Frame::Array(vec![])),
    ])
}
