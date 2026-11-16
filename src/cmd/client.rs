//! CLIENT subcommands.

use super::util::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::server::{Client, Store, cflags};
use crate::tri;
use bytes::Bytes;

pub fn validate_name(n: &[u8]) -> Result<(), Frame> {
    if n.iter().any(|&c| c < b'!' || c > b'~') {
        return Err(Frame::err("ERR Client names cannot contain spaces, newlines or special characters."));
    }
    Ok(())
}

pub fn flags_string(c: &Client, s: &Store) -> String {
    let mut f = String::new();
    if c.has(cflags::REPLICA) {
        if c.has(cflags::REPLICA_MONITOR_ACK) {
            f.push('O');
        }
        f.push('S');
    }
    if c.has(cflags::MASTER) {
        f.push('M');
    }
    if c.in_pubsub() {
        f.push('P');
    }
    if c.has(cflags::MULTI) {
        f.push('x');
    }
    if c.has(cflags::BLOCKED) {
        f.push('b');
    }
    if c.has(cflags::TRACKING) {
        f.push('t');
    }
    if c.has(cflags::TRACKING_BROKEN_REDIR) {
        f.push('R');
    }
    if c.has(cflags::TRACKING_BCAST) {
        f.push('B');
    }
    if c.has(cflags::DIRTY_CAS) {
        f.push('d');
    }
    if c.has(cflags::CLOSE_AFTER_REPLY) {
        f.push('c');
    }
    if c.has(cflags::UNIX) {
        f.push('U');
    }
    if c.has(cflags::READONLY) {
        f.push('r');
    }
    if c.has(cflags::NO_EVICT) {
        f.push('e');
    }
    if c.has(cflags::NO_TOUCH) {
        f.push('T');
    }
    if s.monitors.contains(&c.id) {
        f.push('O');
    }
    if f.is_empty() {
        f.push('N');
    }
    f
}

pub fn client_line(c: &Client, s: &Store) -> String {
    let now = crate::util::now_ms();
    let name = c.name.as_ref().map(|n| String::from_utf8_lossy(n).into_owned()).unwrap_or_default();
    let multi = if c.has(cflags::MULTI) { c.mstate.len() as i64 } else { -1 };
    let redir = if c.has(cflags::TRACKING) { c.tracking_redirect as i64 } else { -1 };
    let events = if c.obuf > 0 { "rw" } else { "r" };
    let argv_mem = 0;
    let tot_mem = 1024 + c.qbuf + c.obuf + c.multi_mem + 20000;
    format!(
        "id={} addr={} laddr={} fd={} name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi={} watch={} qbuf={} qbuf-free={} argv-mem={} multi-mem={} rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem={} events={} cmd={} user={} redir={} resp={} lib-name={} lib-ver={}",
        c.id,
        c.addr,
        c.laddr,
        c.id + 7,
        name,
        (now - c.created_ms) / 1000,
        (now - c.last_interaction_ms) / 1000,
        flags_string(c, s),
        c.db,
        c.channels.len(),
        c.patterns.len(),
        c.shard_channels.len(),
        multi,
        c.watched.len(),
        c.qbuf,
        16384usize.saturating_sub(c.qbuf),
        argv_mem,
        c.multi_mem,
        tot_mem,
        events,
        c.last_cmd,
        c.user,
        redir,
        c.proto,
        c.lib_name.clone().unwrap_or_default(),
        c.lib_ver.clone().unwrap_or_default(),
    )
}

fn client_type(c: &Client) -> &'static str {
    if c.has(cflags::MASTER) {
        "master"
    } else if c.has(cflags::REPLICA) {
        "replica"
    } else if c.in_pubsub() {
        "pubsub"
    } else {
        "normal"
    }
}

pub fn list(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut ty: Option<String> = None;
    let mut ids: Option<Vec<u64>> = None;
    let mut i = 2;
    while i < args.len() {
        if eq(&args[i], "type") && i + 1 < args.len() {
            let t = crate::util::lower(&args[i + 1]);
            let t = if t == "slave" { "replica".to_string() } else { t };
            if !matches!(t.as_str(), "normal" | "master" | "replica" | "pubsub") {
                return Frame::err(format!("ERR Unknown client type '{}'", String::from_utf8_lossy(&args[i + 1])));
            }
            ty = Some(t);
            i += 2;
        } else if eq(&args[i], "id") && i + 1 < args.len() {
            let mut v = Vec::new();
            for a in &args[i + 1..] {
                match crate::util::parse_i64(a) {
                    Some(n) if n > 0 => v.push(n as u64),
                    _ => return Frame::err("ERR Invalid client ID"),
                }
            }
            ids = Some(v);
            i = args.len();
        } else {
            return err_syntax();
        }
    }
    let mut clients: Vec<&Client> = ctx.s.clients.values().collect();
    clients.sort_by_key(|c| c.id);
    let mut out = String::new();
    for c in clients {
        if let Some(t) = &ty {
            if client_type(c) != t {
                continue;
            }
        }
        if let Some(ids) = &ids {
            if !ids.contains(&c.id) {
                continue;
            }
        }
        out.push_str(&client_line(c, ctx.s));
        out.push('\n');
    }
    Frame::Verbatim("txt", Bytes::from(out))
}

pub fn info(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = ctx.s.clients.get(&ctx.cid).unwrap();
    Frame::Verbatim("txt", Bytes::from(client_line(c, ctx.s) + "\n"))
}

pub fn id(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.cid as i64)
}

pub fn getname(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    opt_bulk(ctx.client().name.clone())
}

pub fn setname(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(validate_name(&args[2]));
    let n = args[2].clone();
    ctx.client().name = if n.is_empty() { None } else { Some(n) };
    Frame::ok()
}

pub fn setinfo(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let attr = crate::util::lower(&args[2]);
    if attr != "lib-name" && attr != "lib-ver" {
        return Frame::err(format!("ERR Unrecognized option '{}'", String::from_utf8_lossy(&args[2])));
    }
    if args[3].iter().any(|&c| c < b'!' || c > b'~') {
        return Frame::err(format!("ERR {} cannot contain spaces, newlines or special characters.", attr));
    }
    let v = String::from_utf8_lossy(&args[3]).into_owned();
    let v = if v.is_empty() { None } else { Some(v) };
    let c = ctx.client();
    if attr == "lib-name" {
        c.lib_name = v;
    } else {
        c.lib_ver = v;
    }
    Frame::ok()
}

pub fn kill(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    // Old style: CLIENT KILL addr:port
    if args.len() == 3 {
        let addr = String::from_utf8_lossy(&args[2]).into_owned();
        let target = ctx.s.clients.values().find(|c| c.addr == addr).map(|c| c.id);
        return match target {
            Some(id) => {
                kill_client(ctx, id);
                Frame::ok()
            }
            None => Frame::err("ERR No such client"),
        };
    }
    let mut id: Option<u64> = None;
    let mut ty: Option<String> = None;
    let mut user: Option<String> = None;
    let mut addr: Option<String> = None;
    let mut laddr: Option<String> = None;
    let mut maxage: Option<i64> = None;
    let mut skipme = true;
    let mut i = 2;
    while i + 1 < args.len() {
        let (k, v) = (&args[i], &args[i + 1]);
        if eq(k, "id") {
            match crate::util::parse_i64(v) {
                Some(n) if n > 0 => id = Some(n as u64),
                _ => return Frame::err("ERR client-id should be greater than 0"),
            }
        } else if eq(k, "type") {
            let t = crate::util::lower(v);
            let t = if t == "slave" { "replica".to_string() } else { t };
            if !matches!(t.as_str(), "normal" | "master" | "replica" | "pubsub") {
                return Frame::err(format!("ERR Unknown client type '{}'", String::from_utf8_lossy(v)));
            }
            ty = Some(t);
        } else if eq(k, "user") {
            let u = String::from_utf8_lossy(v).into_owned();
            if !ctx.s.acl.users.contains_key(&u) {
                return Frame::err(format!("ERR No such user '{}'", u));
            }
            user = Some(u);
        } else if eq(k, "addr") {
            addr = Some(String::from_utf8_lossy(v).into_owned());
        } else if eq(k, "laddr") {
            laddr = Some(String::from_utf8_lossy(v).into_owned());
        } else if eq(k, "skipme") {
            if eq(v, "yes") {
                skipme = true;
            } else if eq(v, "no") {
                skipme = false;
            } else {
                return err_syntax();
            }
        } else if eq(k, "maxage") {
            match crate::util::parse_i64(v) {
                Some(n) if n >= 0 => maxage = Some(n),
                _ => return err_syntax(),
            }
        } else {
            return err_syntax();
        }
        i += 2;
    }
    if i != args.len() {
        return err_syntax();
    }
    let now = crate::util::now_ms();
    let targets: Vec<u64> = ctx
        .s
        .clients
        .values()
        .filter(|c| {
            id.is_none_or(|x| c.id == x)
                && ty.as_ref().is_none_or(|t| client_type(c) == t)
                && user.as_ref().is_none_or(|u| &c.user == u)
                && addr.as_ref().is_none_or(|a| &c.addr == a)
                && laddr.as_ref().is_none_or(|a| &c.laddr == a)
                && maxage.is_none_or(|m| (now - c.created_ms) / 1000 >= m)
                && !(skipme && c.id == ctx.cid)
        })
        .map(|c| c.id)
        .collect();
    for t in &targets {
        kill_client(ctx, *t);
    }
    Frame::Integer(targets.len() as i64)
}

fn kill_client(ctx: &mut Ctx, id: u64) {
    if id == ctx.cid {
        ctx.client().flags |= cflags::CLOSE_AFTER_REPLY;
        return;
    }
    if let Some(c) = ctx.s.clients.get(&id) {
        c.kill_now();
    }
    let is_master = ctx.s.clients.get(&id).map(|c| c.has(cflags::MASTER)).unwrap_or(false);
    if is_master {
        ctx.s.repl.master_link_killed();
    }
}

pub fn pause(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let Some(ms) = crate::util::parse_i64(&args[2]) else {
        return Frame::err("ERR timeout is not an integer or out of range");
    };
    if ms < 0 {
        return Frame::err("ERR timeout is negative");
    }
    let write_only = if args.len() == 4 {
        if eq(&args[3], "write") {
            true
        } else if eq(&args[3], "all") {
            false
        } else {
            return Frame::err("ERR CLIENT PAUSE mode must be WRITE or ALL");
        }
    } else if args.len() > 4 {
        return err_syntax();
    } else {
        false
    };
    let until = crate::util::now_ms() + ms;
    // A stricter pause (ALL) or later deadline wins.
    let (nu, nw) = match ctx.s.pause {
        Some((u, w)) => (u.max(until), w && write_only),
        None => (until, write_only),
    };
    ctx.s.pause = Some((nu, nw));
    Frame::ok()
}

pub fn unpause(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    ctx.s.pause = None;
    crate::blocking::unpause(ctx.s);
    Frame::ok()
}

pub fn reply(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if eq(&args[2], "on") {
        c.flags &= !(cflags::REPLY_OFF | cflags::REPLY_SKIP_NEXT);
        Frame::ok()
    } else if eq(&args[2], "off") {
        c.flags |= cflags::REPLY_OFF;
        Frame::NoReply
    } else if eq(&args[2], "skip") {
        if !c.has(cflags::REPLY_OFF) {
            c.flags |= cflags::REPLY_SKIP_NEXT;
        }
        Frame::NoReply
    } else {
        err_syntax()
    }
}

pub fn no_evict(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if eq(&args[2], "on") {
        c.flags |= cflags::NO_EVICT;
    } else if eq(&args[2], "off") {
        c.flags &= !cflags::NO_EVICT;
    } else {
        return err_syntax();
    }
    Frame::ok()
}

pub fn no_touch(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if eq(&args[2], "on") {
        c.flags |= cflags::NO_TOUCH;
    } else if eq(&args[2], "off") {
        c.flags &= !cflags::NO_TOUCH;
    } else {
        return err_syntax();
    }
    Frame::ok()
}

pub fn unblock(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let Some(id) = crate::util::parse_i64(&args[2]) else { return err_int() };
    let use_error = if args.len() == 4 {
        if eq(&args[3], "error") {
            true
        } else if eq(&args[3], "timeout") {
            false
        } else {
            return Frame::err("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
        }
    } else {
        false
    };
    let id = id as u64;
    let blocked = ctx
        .s
        .clients
        .get(&id)
        .and_then(|c| c.blocked.as_ref())
        .map(|b| b.kind != crate::blocking::BlockKind::Pause)
        .unwrap_or(false);
    if !blocked {
        return Frame::Integer(0);
    }
    let reply = if use_error {
        Some(Frame::err("UNBLOCKED client unblocked via CLIENT UNBLOCK"))
    } else {
        crate::repl::wait_timeout_reply(ctx.s, id)
    };
    ctx.s.unblock_client(id, reply);
    Frame::Integer(1)
}

pub fn tracking(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let on = if eq(&args[2], "on") {
        true
    } else if eq(&args[2], "off") {
        false
    } else {
        return err_syntax();
    };
    let (mut bcast, mut optin, mut optout, mut noloop) = (false, false, false, false);
    let mut redirect = 0u64;
    let mut prefixes = Vec::new();
    let mut i = 3;
    while i < args.len() {
        let a = &args[i];
        if eq(a, "redirect") && i + 1 < args.len() {
            if redirect != 0 {
                return Frame::err("ERR A client can only redirect to a single other client");
            }
            let Some(r) = crate::util::parse_i64(&args[i + 1]) else { return err_int() };
            redirect = r as u64;
            if redirect == ctx.cid {
                return Frame::err("ERR You can't redirect to yourself");
            }
            if !ctx.s.clients.contains_key(&redirect) {
                return Frame::err("ERR The client ID you want redirect to does not exist");
            }
            i += 2;
        } else if eq(a, "bcast") {
            bcast = true;
            i += 1;
        } else if eq(a, "optin") {
            optin = true;
            i += 1;
        } else if eq(a, "optout") {
            optout = true;
            i += 1;
        } else if eq(a, "noloop") {
            noloop = true;
            i += 1;
        } else if eq(a, "prefix") && i + 1 < args.len() {
            prefixes.push(args[i + 1].clone());
            i += 2;
        } else {
            return err_syntax();
        }
    }
    let cid = ctx.cid;
    if !on {
        crate::tracking::disable(ctx.s, cid);
        return Frame::ok();
    }
    let c = ctx.client();
    if c.has(cflags::TRACKING) {
        let was_bcast = c.has(cflags::TRACKING_BCAST);
        if was_bcast != bcast {
            return Frame::err("ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.");
        }
        if (c.has(cflags::TRACKING_OPTIN) && optout) || (c.has(cflags::TRACKING_OPTOUT) && optin) {
            return Frame::err("ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.");
        }
    }
    if !prefixes.is_empty() && !bcast {
        return Frame::err("ERR PREFIX option requires BCAST mode to be enabled");
    }
    if optin && optout {
        return Frame::err("ERR You can't use OPTIN and OPTOUT at the same time");
    }
    if bcast && (optin || optout) {
        return Frame::err("ERR OPTIN and OPTOUT are not compatible with BCAST");
    }
    if bcast {
        let mut all: Vec<Bytes> = c.tracking_prefixes.clone();
        all.extend(prefixes.iter().cloned());
        for (a, p) in all.iter().enumerate() {
            for (b, q) in all.iter().enumerate() {
                if a != b && (p.starts_with(q) || q.starts_with(p)) && p != q {
                    return Frame::err(format!(
                        "ERR Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a single client must not overlap.",
                        String::from_utf8_lossy(if a > b { p } else { q }),
                        String::from_utf8_lossy(if a > b { q } else { p })
                    ));
                }
            }
        }
    }
    crate::tracking::enable(ctx.s, cid, redirect, bcast, optin, optout, noloop, prefixes);
    Frame::ok()
}

pub fn caching(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if !c.has(cflags::TRACKING) {
        return Frame::err("ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled");
    }
    if eq(&args[2], "yes") {
        if !c.has(cflags::TRACKING_OPTIN) {
            return Frame::err("ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
        }
        c.flags |= cflags::TRACKING_CACHING;
    } else if eq(&args[2], "no") {
        if !c.has(cflags::TRACKING_OPTOUT) {
            return Frame::err("ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
        }
        c.flags |= cflags::TRACKING_CACHING;
    } else {
        return err_syntax();
    }
    Frame::ok()
}

pub fn getredir(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = ctx.client();
    if c.has(cflags::TRACKING) { Frame::Integer(c.tracking_redirect as i64) } else { Frame::Integer(-1) }
}

pub fn trackinginfo(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = ctx.client();
    let mut flags = Vec::new();
    if !c.has(cflags::TRACKING) {
        flags.push("off");
    } else {
        flags.push("on");
        if c.has(cflags::TRACKING_BCAST) {
            flags.push("bcast");
        }
        if c.has(cflags::TRACKING_OPTIN) {
            flags.push("optin");
            if c.has(cflags::TRACKING_CACHING) {
                flags.push("caching-yes");
            }
        }
        if c.has(cflags::TRACKING_OPTOUT) {
            flags.push("optout");
            if c.has(cflags::TRACKING_CACHING) {
                flags.push("caching-no");
            }
        }
        if c.has(cflags::TRACKING_NOLOOP) {
            flags.push("noloop");
        }
        if c.has(cflags::TRACKING_BROKEN_REDIR) {
            flags.push("broken_redirect");
        }
    }
    let redir = if c.has(cflags::TRACKING) { c.tracking_redirect as i64 } else { -1 };
    Frame::Map(vec![
        (Frame::bulk_str("flags"), Frame::Set(flags.into_iter().map(Frame::bulk_str).collect())),
        (Frame::bulk_str("redirect"), Frame::Integer(redir)),
        (Frame::bulk_str("prefixes"), Frame::Array(c.tracking_prefixes.iter().map(|p| Frame::Bulk(p.clone())).collect())),
    ])
}

pub fn help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "CLIENT",
        &[
            "CACHING (YES|NO)",
            "    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
            "GETREDIR",
            "    Return the client ID we are redirecting to when tracking is enabled.",
            "GETNAME",
            "    Return the name of the current connection.",
            "ID",
            "    Return the ID of the current connection.",
            "INFO",
            "    Return information about the current client connection.",
            "KILL <ip:port>",
            "    Kill connection made from <ip:port>.",
            "KILL <option> <value> [<option> <value> [...]]",
            "    Kill connections. Options are:",
            "    * ADDR (<ip:port>|<unixsocket>:0)",
            "      Kill connections made from the specified address",
            "    * LADDR (<ip:port>|<unixsocket>:0)",
            "      Kill connections made to specified local address",
            "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
            "      Kill connections by type.",
            "    * USER <username>",
            "      Kill connections authenticated by <username>.",
            "    * SKIPME (YES|NO)",
            "      Skip killing current connection (default: yes).",
            "    * ID <client-id>",
            "      Kill connections by client id.",
            "    * MAXAGE <maxage>",
            "      Kill connections older than the specified age.",
            "LIST [options ...]",
            "    Return information about client connections. Options:",
            "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
            "      Return clients of specified type.",
            "UNPAUSE",
            "    Stop the current client pause, resuming traffic.",
            "PAUSE <timeout> [WRITE|ALL]",
            "    Suspend all, or just write, clients for <timeout> milliseconds.",
            "REPLY (ON|OFF|SKIP)",
            "    Control the replies sent to the current connection.",
            "SETNAME <name>",
            "    Assign the name <name> to the current connection.",
            "SETINFO <option> <value>",
            "    Set client meta attr. Options are:",
            "    * LIB-NAME: the client lib name.",
            "    * LIB-VER: the client lib version.",
            "UNBLOCK <clientid> [TIMEOUT|ERROR]",
            "    Unblock the specified blocked client.",
            "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
            "         [OPTIN] [OPTOUT] [NOLOOP]",
            "    Control server assisted client side caching.",
            "TRACKINGINFO",
            "    Report tracking status for the current connection.",
            "NO-EVICT (ON|OFF)",
            "    Protect current client connection from eviction.",
            "NO-TOUCH (ON|OFF)",
            "    Will not touch LRU/LFU stats when this mode is on.",
        ],
    )
}
