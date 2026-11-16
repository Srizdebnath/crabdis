//! Stream commands (t_stream.c).

use super::util::*;
use crate::blocking::BlockSpec;
use crate::config::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::stream::*;
use crate::value::Value;
use bytes::Bytes;

fn invalid_id() -> Frame {
    Frame::err("ERR Invalid stream ID specified as stream command argument")
}

fn strict_id(a: &[u8], missing_seq: u64) -> Result<StreamId, Frame> {
    parse_id(a, missing_seq, true, None).ok_or_else(invalid_id)
}

fn loose_id(a: &[u8], missing_seq: u64) -> Result<StreamId, Frame> {
    parse_id(a, missing_seq, false, None).ok_or_else(invalid_id)
}

/// Interval ID: "(" prefix means exclusive (strict parsing of the rest).
fn interval_id(a: &[u8], missing_seq: u64) -> Result<(StreamId, bool), Frame> {
    if a.len() > 1 && a[0] == b'(' {
        Ok((strict_id(&a[1..], missing_seq)?, true))
    } else {
        Ok((loose_id(a, missing_seq)?, false))
    }
}

fn is_stream(v: &Value) -> bool {
    matches!(v, Value::Stream(_))
}

fn stream_ref<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut Stream>, Frame> {
    match ctx.s.get_type(ctx.db, key, is_stream)? {
        Some(o) => match &mut o.v {
            Value::Stream(s) => Ok(Some(s)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

fn stream_mut<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<Option<&'a mut Stream>, Frame> {
    match ctx.s.get_type_w(ctx.db, key, is_stream)? {
        Some(o) => match &mut o.v {
            Value::Stream(s) => Ok(Some(s)),
            _ => unreachable!(),
        },
        None => Ok(None),
    }
}

pub fn entry_frame(id: StreamId, fv: Vec<Bytes>) -> Frame {
    Frame::Array(vec![Frame::Bulk(id.to_bytes()), Frame::Array(fv.into_iter().map(Frame::Bulk).collect())])
}

fn id_frame(id: &StreamId) -> Frame {
    Frame::Bulk(id.to_bytes())
}

// ---------------- XADD / XTRIM ----------------

struct AddTrim {
    strategy: TrimStrategy,
    approx: bool,
    limit: i64,
    strategy_arg_idx: usize,
    nomkstream: bool,
    id: Option<StreamId>,
    seq_given: bool,
}

fn parse_add_trim(ctx: &Ctx, args: &[Bytes], xadd: bool) -> Result<(AddTrim, usize), Frame> {
    let mut a = AddTrim { strategy: TrimStrategy::None, approx: false, limit: 0, strategy_arg_idx: 0, nomkstream: false, id: None, seq_given: true };
    let mut limit_given = false;
    let mut i = 2;
    while i < args.len() {
        let more = args.len() - 1 - i;
        let opt = &args[i];
        if xadd && opt.as_ref() == b"*" {
            break;
        } else if (eq(opt, "maxlen") || eq(opt, "minid")) && more > 0 {
            if !matches!(a.strategy, TrimStrategy::None) {
                return Err(Frame::err("ERR syntax error, MAXLEN and MINID options at the same time are not compatible"));
            }
            let is_max = eq(opt, "maxlen");
            a.approx = false;
            let next = &args[i + 1];
            if more >= 2 && next.as_ref() == b"~" {
                a.approx = true;
                i += 1;
            } else if more >= 2 && next.as_ref() == b"=" {
                i += 1;
            }
            if is_max {
                let m = int(&args[i + 1])?;
                if m < 0 {
                    return Err(Frame::err("ERR The MAXLEN argument must be >= 0."));
                }
                a.strategy = TrimStrategy::MaxLen(m as u64);
            } else {
                a.strategy = TrimStrategy::MinId(strict_id(&args[i + 1], 0)?);
            }
            i += 1;
            a.strategy_arg_idx = i;
        } else if eq(opt, "limit") && more > 0 {
            a.limit = int(&args[i + 1])?;
            if a.limit < 0 {
                return Err(Frame::err("ERR The LIMIT argument must be >= 0."));
            }
            limit_given = true;
            i += 1;
        } else if xadd && eq(opt, "nomkstream") {
            a.nomkstream = true;
        } else if xadd {
            let mut sg = true;
            a.id = Some(parse_id(opt, 0, true, Some(&mut sg)).ok_or_else(invalid_id)?);
            a.seq_given = sg;
            break;
        } else {
            return Err(err_syntax());
        }
        i += 1;
    }
    if a.limit != 0 && matches!(a.strategy, TrimStrategy::None) {
        return Err(Frame::err("ERR syntax error, LIMIT cannot be used without specifying a trimming strategy"));
    }
    if !xadd && matches!(a.strategy, TrimStrategy::None) {
        return Err(Frame::err("ERR syntax error, XTRIM must be called with a trimming strategy"));
    }
    let must_obey = ctx.flags & (crate::exec::CTX_MASTER | crate::exec::CTX_LOADING) != 0;
    if must_obey {
        a.limit = 0;
    } else if limit_given {
        if !a.approx {
            return Err(Frame::err("ERR syntax error, LIMIT cannot be used without the special ~ option"));
        }
    } else if a.approx {
        a.limit = 100 * ctx.s.cfg.stream_node_max_entries as i64;
        if a.limit <= 0 {
            a.limit = 10000;
        }
        if a.limit > 1_000_000 {
            a.limit = 1_000_000;
        }
    } else {
        a.limit = 0;
    }
    Ok((a, i))
}

fn rewrite_trim(argv: &mut [Bytes], s: &Stream, a: &AddTrim) {
    argv[a.strategy_arg_idx - 1] = Bytes::from_static(b"=");
    argv[a.strategy_arg_idx] = match a.strategy {
        TrimStrategy::MaxLen(_) => Bytes::from(s.length.to_string()),
        _ => s.edge_id(true).unwrap_or_default().to_bytes(),
    };
}

pub fn xadd(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (a, idpos) = match parse_add_trim(ctx, args, true) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let field_pos = idpos + 1;
    if args.len() < field_pos + 2 || (args.len() - field_pos) % 2 == 1 {
        return Frame::err("ERR wrong number of arguments for 'xadd' command");
    }
    if a.id.is_some() && a.seq_given && a.id.unwrap().is_zero() {
        return Frame::err("ERR The ID specified in XADD must be greater than 0-0");
    }
    let key = &args[1];
    let db = ctx.db;
    match stream_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => {
            if a.nomkstream {
                return Frame::Null;
            }
            ctx.s.db_add(db, key.clone(), Value::Stream(Box::new(Stream::new())));
        }
        Ok(Some(_)) => {}
    }
    let (nmb, nme) = (ctx.s.cfg.stream_node_max_bytes, ctx.s.cfg.stream_node_max_entries);
    let s = stream_mut(ctx, key).unwrap().unwrap();
    if s.last_id == StreamId::MAX {
        return Frame::err("ERR The stream has exhausted the last possible ID, unable to add more items");
    }
    let id = match s.append(&args[field_pos..], a.id, a.seq_given, nmb, nme) {
        Ok(id) => id,
        Err(AppendError::Dom) => return Frame::err("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
        Err(AppendError::TooLarge) => return Frame::err("ERR Elements are too large to be stored"),
    };
    let mut argv = args.to_vec();
    let mut trimmed = false;
    if !matches!(a.strategy, TrimStrategy::None) {
        trimmed = s.trim(&a.strategy, a.approx, a.limit) > 0;
        if a.approx {
            rewrite_trim(&mut argv, s, &a);
        }
    }
    ctx.notify(NOTIFY_STREAM, "xadd", key);
    ctx.dirty(1);
    if trimmed {
        ctx.notify(NOTIFY_STREAM, "xtrim", key);
    }
    ctx.modified(key);
    if a.id.is_none() || !a.seq_given {
        argv[idpos] = id.to_bytes();
    }
    ctx.propagate_as(argv);
    ctx.s.signal_ready(db, key);
    Frame::Bulk(id.to_bytes())
}

pub fn xtrim(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let (a, _) = match parse_add_trim(ctx, args, false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let key = &args[1];
    let s = match stream_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(s)) => s,
    };
    let deleted = s.trim(&a.strategy, a.approx, a.limit);
    if deleted > 0 {
        if a.approx {
            let mut argv = args.to_vec();
            rewrite_trim(&mut argv, s, &a);
            ctx.propagate_as(argv);
        }
        ctx.notify(NOTIFY_STREAM, "xtrim", key);
        ctx.modified(key);
        ctx.dirty(deleted as u64);
    }
    Frame::Integer(deleted)
}

// ---------------- XRANGE / XLEN / XDEL ----------------

fn xrange_generic(ctx: &mut Ctx, args: &[Bytes], rev: bool) -> Frame {
    let (sa, ea) = if rev { (&args[3], &args[2]) } else { (&args[2], &args[3]) };
    let (mut start, sex) = tri!(interval_id(sa, 0));
    if sex {
        match start.incr() {
            Some(i) => start = i,
            None => return Frame::err("ERR invalid start ID for the interval"),
        }
    }
    let (mut end, eex) = tri!(interval_id(ea, u64::MAX));
    if eex {
        match end.decr() {
            Some(i) => end = i,
            None => return Frame::err("ERR invalid end ID for the interval"),
        }
    }
    let mut count: i64 = -1;
    let mut j = 4;
    while j < args.len() {
        if eq(&args[j], "count") && j + 1 < args.len() {
            count = tri!(int(&args[j + 1])).max(0);
            j += 2;
        } else {
            return err_syntax();
        }
    }
    let s = match stream_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(None) => return Frame::Array(vec![]),
        Ok(Some(s)) => s,
    };
    if count == 0 {
        return Frame::NullArray;
    }
    let c = if count == -1 { 0 } else { count as usize };
    Frame::Array(s.range(&start, &end, rev, c).into_iter().map(|(id, fv)| entry_frame(id, fv)).collect())
}

pub fn xrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    xrange_generic(ctx, args, false)
}

pub fn xrevrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    xrange_generic(ctx, args, true)
}

pub fn xlen(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match stream_ref(ctx, &args[1]) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(s)) => Frame::Integer(s.length as i64),
    }
}

pub fn xdel(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let mut ids = Vec::with_capacity(args.len() - 2);
    // Redis looks the key up first (missing key → 0 without parsing IDs).
    match stream_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return Frame::Integer(0),
        Ok(Some(_)) => {}
    }
    for a in &args[2..] {
        ids.push(tri!(strict_id(a, 0)));
    }
    let s = stream_mut(ctx, key).unwrap().unwrap();
    let deleted = s.xdel(&ids);
    if deleted > 0 {
        ctx.modified(key);
        ctx.notify(NOTIFY_STREAM, "xdel", key);
        ctx.dirty(deleted as u64);
    }
    Frame::Integer(deleted as i64)
}

pub fn xsetid(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let id = tri!(strict_id(&args[2], 0));
    let mut entries_added: i64 = -1;
    let mut max_xdel = StreamId::MIN;
    let mut i = 3;
    while i < args.len() {
        let more = args.len() - 1 - i;
        if eq(&args[i], "entriesadded") && more > 0 {
            entries_added = tri!(int(&args[i + 1]));
            if entries_added < 0 {
                return Frame::err("ERR entries_added must be positive");
            }
            i += 2;
        } else if eq(&args[i], "maxdeletedid") && more > 0 {
            max_xdel = tri!(strict_id(&args[i + 1], 0));
            if id < max_xdel {
                return Frame::err("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id");
            }
            i += 2;
        } else {
            return err_syntax();
        }
    }
    let key = &args[1];
    let s = match stream_mut(ctx, key) {
        Err(e) => return e,
        Ok(None) => return err_nokey(),
        Ok(Some(s)) => s,
    };
    if id < s.max_deleted_id {
        return Frame::err("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id");
    }
    if s.length > 0 {
        if id < s.last_valid_id() {
            return Frame::err("ERR The ID specified in XSETID is smaller than the target stream top item");
        }
        if entries_added != -1 && s.length > entries_added as u64 {
            return Frame::err("ERR The entries_added specified in XSETID is smaller than the target stream length");
        }
    }
    s.last_id = id;
    if entries_added != -1 {
        s.entries_added = entries_added as u64;
    }
    if !max_xdel.is_zero() {
        s.max_deleted_id = max_xdel;
    }
    ctx.dirty(1);
    ctx.notify(NOTIFY_STREAM, "xsetid", key);
    Frame::ok()
}

// ---------------- XREAD / XREADGROUP ----------------

pub fn xread_keys(args: &[Bytes]) -> Vec<usize> {
    let Some(pos) = args.iter().position(|a| eq(a, "streams")) else { return vec![] };
    let n = (args.len() - pos - 1) / 2;
    (pos + 1..pos + 1 + n).collect()
}

fn xclaim_argv(key: &Bytes, group: &Bytes, id: &StreamId, nack: &Nack, last_id: &StreamId) -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"XCLAIM"),
        key.clone(),
        group.clone(),
        nack.consumer.clone().unwrap_or_default(),
        Bytes::from_static(b"0"),
        id.to_bytes(),
        Bytes::from_static(b"TIME"),
        Bytes::from(nack.delivery_time.to_string()),
        Bytes::from_static(b"RETRYCOUNT"),
        Bytes::from(nack.delivery_count.to_string()),
        Bytes::from_static(b"FORCE"),
        Bytes::from_static(b"JUSTID"),
        Bytes::from_static(b"LASTID"),
        last_id.to_bytes(),
    ]
}

fn setid_argv(key: &Bytes, group: &Bytes, g: &Group) -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"XGROUP"),
        Bytes::from_static(b"SETID"),
        key.clone(),
        group.clone(),
        g.last_id.to_bytes(),
        Bytes::from_static(b"ENTRIESREAD"),
        Bytes::from(g.entries_read.to_string()),
    ]
}

pub fn xread(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    xread_generic(ctx, args, false)
}

pub fn xreadgroup(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    xread_generic(ctx, args, true)
}

fn xread_generic(ctx: &mut Ctx, args: &[Bytes], group_mode: bool) -> Frame {
    let mut timeout: Option<Option<i64>> = None; // None = no BLOCK
    let mut count: usize = 0;
    let mut streams_arg = 0;
    let mut streams_count = 0;
    let mut noack = false;
    let mut groupname: Option<Bytes> = None;
    let mut consumername: Option<Bytes> = None;
    let now = crate::util::now_ms();
    let mut i = 1;
    while i < args.len() {
        let more = args.len() - i - 1;
        let o = &args[i];
        if eq(o, "block") && more > 0 {
            i += 1;
            timeout = Some(tri!(crate::blocking::parse_timeout_ms(&args[i], now)));
        } else if eq(o, "count") && more > 0 {
            i += 1;
            count = tri!(int(&args[i])).max(0) as usize;
        } else if eq(o, "streams") && more > 0 {
            streams_arg = i + 1;
            streams_count = args.len() - streams_arg;
            if streams_count % 2 != 0 {
                let sym = if group_mode { '>' } else { '$' };
                return Frame::err(format!(
                    "ERR Unbalanced '{}' list of streams: for each stream key an ID or '{}' must be specified.",
                    if group_mode { "xreadgroup" } else { "xread" },
                    sym
                ));
            }
            streams_count /= 2;
            break;
        } else if eq(o, "group") && more >= 2 {
            if !group_mode {
                return Frame::err("ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.");
            }
            groupname = Some(args[i + 1].clone());
            consumername = Some(args[i + 2].clone());
            i += 2;
        } else if eq(o, "noack") {
            if !group_mode {
                return Frame::err("ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.");
            }
            noack = true;
        } else {
            return err_syntax();
        }
        i += 1;
    }
    if streams_arg == 0 {
        return err_syntax();
    }
    if group_mode && groupname.is_none() {
        return Frame::err("ERR Missing GROUP option for XREADGROUP");
    }
    let db = ctx.db;
    let reprocess = ctx.flags & crate::exec::CTX_REPROCESS != 0;
    let keys: Vec<Bytes> = args[streams_arg..streams_arg + streams_count].to_vec();
    let mut ids: Vec<StreamId> = Vec::with_capacity(streams_count);
    for (k, key) in keys.iter().enumerate() {
        let idarg = &args[streams_arg + streams_count + k];
        let s = match stream_ref(ctx, key) {
            Err(e) => return e,
            Ok(s) => s,
        };
        if let Some(g) = &groupname {
            let has = s.as_ref().map(|s| s.groups.contains_key(g)).unwrap_or(false);
            if !has {
                if reprocess {
                    if s.is_none() {
                        return Frame::err("UNBLOCKED the stream key no longer exists");
                    }
                    return Frame::err("UNBLOCKED the consumer group this client was blocked on no longer exists");
                }
                return Frame::err(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(g)
                ));
            }
        }
        let idv: &[u8] = idarg;
        if idv == b"$" {
            if group_mode {
                return Frame::err("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.");
            }
            ids.push(s.map(|s| s.last_id).unwrap_or_default());
        } else if idv == b"+" {
            if group_mode {
                return Frame::err("ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set.");
            }
            match s {
                Some(s) => match s.last_id.decr() {
                    Some(d) => ids.push(d),
                    None => return Frame::err("ERR the stream last element ID is 0-0"),
                },
                None => ids.push(StreamId::MIN),
            }
        } else if idv == b">" {
            if !group_mode {
                return Frame::err("ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.");
            }
            ids.push(StreamId::MAX);
        } else {
            ids.push(tri!(strict_id(idv, 0)));
        }
    }
    let mut results: Vec<(Bytes, Frame)> = Vec::new();
    let mut props: Vec<Vec<Bytes>> = Vec::new();
    let mut dirty = 0;
    let cmd_now = ctx.s.now_ms();
    for (k, key) in keys.iter().enumerate() {
        let Ok(Some(s)) = stream_mut(ctx, key) else { continue };
        let mut gt = ids[k];
        let mut serve = false;
        let mut history = false;
        if let Some(gname) = &groupname {
            let cname = consumername.clone().unwrap();
            let maxid = s.last_valid_id();
            let has_len = s.length > 0;
            let Stream { groups, .. } = &mut *s;
            let g = groups.get_mut(gname).unwrap();
            if gt != StreamId::MAX {
                serve = true;
                history = true;
            } else if has_len && maxid > g.last_id {
                serve = true;
                gt = g.last_id;
            }
            if g.ensure_consumer(&cname, cmd_now) {
                dirty += 1;
                if noack {
                    props.push(vec![Bytes::from_static(b"XGROUP"), Bytes::from_static(b"CREATECONSUMER"), key.clone(), gname.clone(), cname.clone()]);
                }
                let db = ctx.db;
                ctx.s.notify(NOTIFY_STREAM, "xgroup-createconsumer", key, db);
            }
            let s = stream_mut(ctx, key).unwrap().unwrap();
            let g = s.groups.get_mut(gname).unwrap();
            g.consumers.get_mut(&cname).unwrap().seen_time = cmd_now;
            if !serve {
                continue;
            }
            let start = gt.incr().unwrap_or(StreamId::MAX);
            let frame = if history {
                // Serve from the consumer's own PEL.
                let pel: Vec<StreamId> = g.consumers[&cname].pel.range(start..).take(if count == 0 { usize::MAX } else { count }).copied().collect();
                let mut out = Vec::new();
                for id in pel {
                    let found = s.get(&id).map(|e| e.fv.clone());
                    let g = s.groups.get_mut(gname).unwrap();
                    match found {
                        Some(fv) => {
                            let n = g.pel.get_mut(&id).unwrap();
                            n.delivery_time = cmd_now;
                            n.delivery_count += 1;
                            out.push(entry_frame(id, fv));
                        }
                        None => out.push(Frame::Array(vec![id_frame(&id), Frame::NullArray])),
                    }
                }
                Frame::Array(out)
            } else {
                let entries = s.range(&start, &StreamId::MAX, false, count);
                let mut out = Vec::with_capacity(entries.len());
                let mut prop_last = false;
                for (id, fv) in entries {
                    let has_tomb = s.range_has_tombstones(Some(&id), None);
                    let est = if s.entries_added > 0 { Some(s.estimate_distance_from_first_ever(&id)) } else { None };
                    let g = s.groups.get_mut(gname).unwrap();
                    if id > g.last_id {
                        if g.entries_read != INVALID_ENTRIES_READ && !has_tomb {
                            g.entries_read += 1;
                        } else if let Some(e) = est {
                            g.entries_read = e;
                        }
                        g.last_id = id;
                        prop_last = true;
                    }
                    out.push(entry_frame(id, fv));
                    if !noack {
                        // Create / transfer the NACK.
                        if let Some(old) = g.pel.get(&id).and_then(|n| n.consumer.clone()) {
                            if let Some(oc) = g.consumers.get_mut(&old) {
                                oc.pel.remove(&id);
                            }
                        }
                        g.pel.insert(id, Nack { delivery_time: cmd_now, delivery_count: 1, consumer: Some(cname.clone()) });
                        let c = g.consumers.get_mut(&cname).unwrap();
                        c.pel.insert(id);
                        c.active_time = cmd_now;
                        let last = g.last_id;
                        props.push(xclaim_argv(key, gname, &id, &g.pel[&id], &last));
                        dirty += 1;
                    }
                }
                if prop_last {
                    let g = &s.groups[gname];
                    props.push(setid_argv(key, gname, g));
                    dirty += 1;
                }
                Frame::Array(out)
            };
            results.push((key.clone(), frame));
        } else {
            if s.length > 0 && s.last_valid_id() > gt {
                serve = true;
            }
            if !serve {
                continue;
            }
            let start = gt.incr().unwrap_or(StreamId::MAX);
            let entries = s.range(&start, &StreamId::MAX, false, count);
            results.push((key.clone(), Frame::Array(entries.into_iter().map(|(id, fv)| entry_frame(id, fv)).collect())));
        }
    }
    // XREAD(GROUP) itself is never propagated; group effects are, as XCLAIM / SETID.
    ctx.no_prop = true;
    for p in props {
        ctx.also_propagate(p);
    }
    if dirty > 0 {
        ctx.dirty(dirty);
    }
    if !results.is_empty() {
        let _ = db;
        return if ctx.proto() >= 3 {
            Frame::Map(results.into_iter().map(|(k, v)| (Frame::Bulk(k), v)).collect())
        } else {
            Frame::Array(results.into_iter().map(|(k, v)| Frame::Array(vec![Frame::Bulk(k), v])).collect())
        };
    }
    if let Some(deadline) = timeout {
        if !ctx.can_block() {
            return Frame::NullArray;
        }
        // Rewrite "$" into the concrete last ID so the re-executed command keeps
        // waiting for entries after the one current at blocking time.
        let mut argv = args.to_vec();
        for k in 0..streams_count {
            let idx = streams_arg + streams_count + k;
            if argv[idx].as_ref() == b"$" {
                argv[idx] = ids[k].to_bytes();
            }
        }
        if let Frame::Block(mut spec) = BlockSpec::keys(keys, deadline, Frame::NullArray) {
            spec.rewrite = Some(argv);
            return Frame::Block(spec);
        }
        unreachable!()
    }
    Frame::NullArray
}

// ---------------- XGROUP ----------------

fn nogroup(group: &[u8], key: &[u8]) -> Frame {
    Frame::err(format!("NOGROUP No such consumer group '{}' for key name '{}'", String::from_utf8_lossy(group), String::from_utf8_lossy(key)))
}

fn xgroup_prelude(ctx: &mut Ctx, args: &[Bytes], create: bool, setid: bool) -> Result<(bool, i64), Frame> {
    let mut mkstream = false;
    let mut entries_read = INVALID_ENTRIES_READ;
    let mut i = 5;
    while i < args.len() {
        if create && eq(&args[i], "mkstream") {
            mkstream = true;
            i += 1;
        } else if (create || setid) && eq(&args[i], "entriesread") && i + 1 < args.len() {
            entries_read = int(&args[i + 1])?;
            if entries_read < 0 && entries_read != INVALID_ENTRIES_READ {
                return Err(Frame::err("ERR value for ENTRIESREAD must be positive or -1"));
            }
            i += 2;
        } else {
            return Err(Frame::err(format!("ERR unknown subcommand or wrong number of arguments for '{}'. Try XGROUP HELP.", String::from_utf8_lossy(&args[1]))));
        }
    }
    let exists = stream_mut(ctx, &args[2])?.is_some();
    if !mkstream && !exists {
        return Err(Frame::err("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."));
    }
    Ok((mkstream, entries_read))
}

fn group_must_exist(ctx: &mut Ctx, args: &[Bytes]) -> Result<(), Frame> {
    let s = stream_mut(ctx, &args[2])?.unwrap();
    if !s.groups.contains_key(&args[3]) {
        return Err(nogroup(&args[3], &args[2]));
    }
    Ok(())
}

pub fn xgroup_create(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 8 {
        return Frame::err("ERR unknown subcommand or wrong number of arguments for 'CREATE'. Try XGROUP HELP.");
    }
    let (_mk, er) = tri!(xgroup_prelude(ctx, args, true, false));
    let key = &args[2];
    let db = ctx.db;
    let last = match stream_mut(ctx, key) {
        Err(e) => return e,
        Ok(s) => s.map(|s| s.last_id),
    };
    let id = if args[4].as_ref() == b"$" { last.unwrap_or_default() } else { tri!(strict_id(&args[4], 0)) };
    if last.is_none() {
        ctx.s.db_add(db, key.clone(), Value::Stream(Box::new(Stream::new())));
        ctx.modified(key);
    }
    let s = stream_mut(ctx, key).unwrap().unwrap();
    if s.create_group(args[3].clone(), id, er) {
        ctx.dirty(1);
        ctx.notify(NOTIFY_STREAM, "xgroup-create", key);
        Frame::ok()
    } else {
        Frame::err("BUSYGROUP Consumer Group name already exists")
    }
}

pub fn xgroup_setid(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() != 5 && args.len() != 7 {
        return Frame::err("ERR unknown subcommand or wrong number of arguments for 'SETID'. Try XGROUP HELP.");
    }
    let (_, er) = tri!(xgroup_prelude(ctx, args, false, true));
    tri!(group_must_exist(ctx, args));
    let key = &args[2];
    let s = stream_mut(ctx, key).unwrap().unwrap();
    let id = if args[4].as_ref() == b"$" { s.last_id } else { tri!(loose_id(&args[4], 0)) };
    let g = s.groups.get_mut(&args[3]).unwrap();
    g.last_id = id;
    g.entries_read = er;
    ctx.dirty(1);
    ctx.notify(NOTIFY_STREAM, "xgroup-setid", key);
    Frame::ok()
}

pub fn xgroup_destroy(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(xgroup_prelude(ctx, args, false, false));
    let key = &args[2];
    let db = ctx.db;
    let s = stream_mut(ctx, key).unwrap().unwrap();
    if s.groups.remove(&args[3]).is_some() {
        ctx.dirty(1);
        ctx.notify(NOTIFY_STREAM, "xgroup-destroy", key);
        ctx.s.signal_ready(db, key);
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

pub fn xgroup_createconsumer(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(xgroup_prelude(ctx, args, false, false));
    tri!(group_must_exist(ctx, args));
    let key = &args[2];
    let now = ctx.s.now_ms();
    let s = stream_mut(ctx, key).unwrap().unwrap();
    let created = s.groups.get_mut(&args[3]).unwrap().ensure_consumer(&args[4], now);
    if created {
        ctx.dirty(1);
        ctx.notify(NOTIFY_STREAM, "xgroup-createconsumer", key);
    }
    Frame::Integer(created as i64)
}

pub fn xgroup_delconsumer(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    tri!(xgroup_prelude(ctx, args, false, false));
    tri!(group_must_exist(ctx, args));
    let key = &args[2];
    let s = stream_mut(ctx, key).unwrap().unwrap();
    match s.groups.get_mut(&args[3]).unwrap().delete_consumer(&args[4]) {
        Some(p) => {
            ctx.dirty(1);
            ctx.notify(NOTIFY_STREAM, "xgroup-delconsumer", key);
            Frame::Integer(p as i64)
        }
        None => Frame::Integer(0),
    }
}

pub fn xgroup_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "XGROUP",
        &[
            "CREATE <key> <groupname> <id|$> [option]",
            "    Create a new consumer group. Options are:",
            "    * MKSTREAM",
            "      Create the empty stream if it does not exist.",
            "    * ENTRIESREAD entries_read",
            "      Set the group's entries_read counter (internal use).",
            "CREATECONSUMER <key> <groupname> <consumer>",
            "    Create a new consumer in the specified group.",
            "DELCONSUMER <key> <groupname> <consumer>",
            "    Remove the specified consumer.",
            "DESTROY <key> <groupname>",
            "    Remove the specified group.",
            "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
            "    Set the current group ID and entries_read counter.",
        ],
    )
}

// ---------------- XACK / XPENDING / XCLAIM / XAUTOCLAIM ----------------

pub fn xack(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = &args[1];
    let has = match stream_ref(ctx, key) {
        Err(e) => return e,
        Ok(s) => s.map(|s| s.groups.contains_key(&args[2])).unwrap_or(false),
    };
    if !has {
        return Frame::Integer(0);
    }
    let mut ids = Vec::new();
    for a in &args[3..] {
        ids.push(tri!(strict_id(a, 0)));
    }
    let s = stream_ref(ctx, key).unwrap().unwrap();
    let g = s.groups.get_mut(&args[2]).unwrap();
    let mut n = 0;
    for id in ids {
        if g.remove_nack(&id) {
            n += 1;
        }
    }
    if n > 0 {
        ctx.dirty(n);
    }
    Frame::Integer(n as i64)
}

pub fn xpending(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let justinfo = args.len() == 3;
    if args.len() != 3 && (args.len() < 6 || args.len() > 9) {
        return err_syntax();
    }
    let (mut start, mut end, mut count, mut minidle) = (StreamId::MIN, StreamId::MAX, 0i64, 0i64);
    let mut consumer: Option<Bytes> = None;
    if args.len() >= 6 {
        let mut si = 3;
        if eq(&args[3], "idle") {
            minidle = tri!(int(&args[4]));
            if args.len() < 8 {
                return err_syntax();
            }
            si += 2;
        }
        count = tri!(int(&args[si + 2])).max(0);
        let (s, sex) = tri!(interval_id(&args[si], 0));
        start = s;
        if sex {
            match start.incr() {
                Some(i) => start = i,
                None => return Frame::err("ERR invalid start ID for the interval"),
            }
        }
        let (e, eex) = tri!(interval_id(&args[si + 1], u64::MAX));
        end = e;
        if eex {
            match end.decr() {
                Some(i) => end = i,
                None => return Frame::err("ERR invalid end ID for the interval"),
            }
        }
        if si + 3 < args.len() {
            consumer = Some(args[si + 3].clone());
        }
    }
    let now = ctx.s.now_ms();
    let s = match stream_ref(ctx, &args[1]) {
        Err(e) => return e,
        Ok(s) => s,
    };
    let Some(g) = s.and_then(|s| s.groups.get(&args[2])) else {
        return Frame::err(format!("NOGROUP No such key '{}' or consumer group '{}'", String::from_utf8_lossy(&args[1]), String::from_utf8_lossy(&args[2])));
    };
    if justinfo {
        if g.pel.is_empty() {
            return Frame::Array(vec![Frame::Integer(0), Frame::Null, Frame::Null, Frame::NullArray]);
        }
        let first = g.pel.keys().next().unwrap();
        let last = g.pel.keys().next_back().unwrap();
        let cons: Vec<Frame> = g
            .consumers
            .iter()
            .filter(|(_, c)| !c.pel.is_empty())
            .map(|(n, c)| Frame::Array(vec![Frame::Bulk(n.clone()), Frame::bulk_int(c.pel.len() as i64)]))
            .collect();
        return Frame::Array(vec![Frame::Integer(g.pel.len() as i64), id_frame(first), id_frame(last), Frame::Array(cons)]);
    }
    let ids: Vec<StreamId> = match &consumer {
        Some(c) => match g.consumers.get(c) {
            None => return Frame::Array(vec![]),
            Some(c) => c.pel.range(start..=end.max(start)).copied().collect(),
        },
        None => g.pel.range(start..=end.max(start)).map(|(k, _)| *k).collect(),
    };
    let mut out = Vec::new();
    for id in ids {
        if count == 0 {
            break;
        }
        if id > end {
            break;
        }
        let n = &g.pel[&id];
        if minidle > 0 && now - n.delivery_time < minidle {
            continue;
        }
        count -= 1;
        out.push(Frame::Array(vec![
            id_frame(&id),
            Frame::Bulk(n.consumer.clone().unwrap_or_default()),
            Frame::Integer((now - n.delivery_time).max(0)),
            Frame::Integer(n.delivery_count as i64),
        ]));
    }
    Frame::Array(out)
}

fn nogroup_key(key: &[u8], g: &[u8]) -> Frame {
    Frame::err(format!("NOGROUP No such key '{}' or consumer group '{}'", String::from_utf8_lossy(key), String::from_utf8_lossy(g)))
}

fn int_msg(a: &[u8], msg: &str) -> Result<i64, Frame> {
    crate::util::parse_i64(a).ok_or_else(|| Frame::err(format!("ERR {}", msg)))
}

pub fn xclaim(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = args[1].clone();
    let gname = args[2].clone();
    let has = match stream_ref(ctx, &key) {
        Err(e) => return e,
        Ok(s) => s.map(|s| s.groups.contains_key(&gname)).unwrap_or(false),
    };
    if !has {
        return nogroup_key(&key, &gname);
    }
    let minidle = tri!(int_msg(&args[4], "Invalid min-idle-time argument for XCLAIM")).max(0);
    let mut ids = Vec::new();
    let mut j = 5;
    while j < args.len() {
        match parse_id(&args[j], 0, true, None) {
            Some(id) => ids.push(id),
            None => break,
        }
        j += 1;
    }
    let now = ctx.s.now_ms();
    let (mut force, mut justid) = (false, false);
    let mut deliverytime: i64 = -1;
    let mut retrycount: i64 = -1;
    let mut last_id = StreamId::MIN;
    while j < args.len() {
        let more = args.len() - 1 - j;
        let o = &args[j];
        if eq(o, "force") {
            force = true;
        } else if eq(o, "justid") {
            justid = true;
        } else if eq(o, "idle") && more > 0 {
            j += 1;
            deliverytime = now - tri!(int_msg(&args[j], "Invalid IDLE option argument for XCLAIM"));
        } else if eq(o, "time") && more > 0 {
            j += 1;
            deliverytime = tri!(int_msg(&args[j], "Invalid TIME option argument for XCLAIM"));
        } else if eq(o, "retrycount") && more > 0 {
            j += 1;
            retrycount = tri!(int_msg(&args[j], "Invalid RETRYCOUNT option argument for XCLAIM"));
        } else if eq(o, "lastid") && more > 0 {
            j += 1;
            last_id = tri!(strict_id(&args[j], 0));
        } else {
            return Frame::err(format!("ERR Unrecognized XCLAIM option '{}'", String::from_utf8_lossy(o)));
        }
        j += 1;
    }
    let cname = args[3].clone();
    let db = ctx.db;
    let mut props: Vec<Vec<Bytes>> = Vec::new();
    let mut dirty = 0u64;
    let s = stream_ref(ctx, &key).unwrap().unwrap();
    let mut propagate_last_id = false;
    {
        let g = s.groups.get_mut(&gname).unwrap();
        if last_id > g.last_id {
            g.last_id = last_id;
            propagate_last_id = true;
        }
    }
    if deliverytime != -1 {
        if deliverytime < 0 || deliverytime > now {
            deliverytime = now;
        }
    } else {
        deliverytime = now;
    }
    let created = s.groups.get_mut(&gname).unwrap().ensure_consumer(&cname, now);
    s.groups.get_mut(&gname).unwrap().consumers.get_mut(&cname).unwrap().seen_time = now;
    let mut out = Vec::new();
    for id in ids {
        let exists = s.get(&id).map(|e| e.fv.clone());
        let g = s.groups.get_mut(&gname).unwrap();
        let glast = g.last_id;
        match exists {
            None => {
                if let Some(n) = g.pel.get(&id).cloned() {
                    props.push(xclaim_argv(&key, &gname, &id, &n, &glast));
                    propagate_last_id = false;
                    dirty += 1;
                    g.remove_nack(&id);
                }
                continue;
            }
            Some(fv) => {
                if force && !g.pel.contains_key(&id) {
                    g.pel.insert(id, Nack { delivery_time: now, delivery_count: 1, consumer: None });
                }
                let Some(n) = g.pel.get(&id).cloned() else { continue };
                if n.consumer.is_some() && minidle > 0 && now - n.delivery_time < minidle {
                    continue;
                }
                if n.consumer.as_ref() != Some(&cname) {
                    if let Some(oc) = n.consumer.as_ref().and_then(|c| g.consumers.get_mut(c)) {
                        oc.pel.remove(&id);
                    }
                }
                let nn = g.pel.get_mut(&id).unwrap();
                nn.delivery_time = deliverytime;
                if retrycount >= 0 {
                    nn.delivery_count = retrycount as u64;
                } else if !justid {
                    nn.delivery_count += 1;
                }
                nn.consumer = Some(cname.clone());
                let nn = nn.clone();
                let c = g.consumers.get_mut(&cname).unwrap();
                c.pel.insert(id);
                c.active_time = now;
                out.push(if justid { id_frame(&id) } else { entry_frame(id, fv) });
                props.push(xclaim_argv(&key, &gname, &id, &nn, &glast));
                propagate_last_id = false;
                dirty += 1;
            }
        }
    }
    if propagate_last_id {
        let g = &s.groups[&gname];
        props.push(setid_argv(&key, &gname, g));
        dirty += 1;
    }
    if created {
        dirty += 1;
        ctx.s.notify(NOTIFY_STREAM, "xgroup-createconsumer", &key, db);
    }
    ctx.no_prop = true;
    for p in props {
        ctx.also_propagate(p);
    }
    if dirty > 0 {
        ctx.dirty(dirty);
    }
    Frame::Array(out)
}

pub fn xautoclaim(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let key = args[1].clone();
    let gname = args[2].clone();
    let cname = args[3].clone();
    let minidle = tri!(int_msg(&args[4], "Invalid min-idle-time argument for XAUTOCLAIM")).max(0);
    let (mut start, sex) = tri!(interval_id(&args[5], 0));
    if sex {
        match start.incr() {
            Some(i) => start = i,
            None => return Frame::err("ERR invalid start ID for the interval"),
        }
    }
    let mut count: i64 = 100;
    let mut justid = false;
    let mut j = 6;
    while j < args.len() {
        let more = args.len() - 1 - j;
        if eq(&args[j], "count") && more > 0 {
            let max_count = i64::MAX / 16;
            match crate::util::parse_i64(&args[j + 1]) {
                Some(c) if (1..=max_count).contains(&c) => count = c,
                _ => return Frame::err("ERR COUNT must be > 0"),
            }
            j += 1;
        } else if eq(&args[j], "justid") {
            justid = true;
        } else {
            return err_syntax();
        }
        j += 1;
    }
    let has = match stream_ref(ctx, &key) {
        Err(e) => return e,
        Ok(s) => s.map(|s| s.groups.contains_key(&gname)).unwrap_or(false),
    };
    if !has {
        return nogroup_key(&key, &gname);
    }
    let now = ctx.s.now_ms();
    let db = ctx.db;
    let s = stream_ref(ctx, &key).unwrap().unwrap();
    let created = s.groups.get_mut(&gname).unwrap().ensure_consumer(&cname, now);
    s.groups.get_mut(&gname).unwrap().consumers.get_mut(&cname).unwrap().seen_time = now;
    let mut attempts = count * 10;
    let mut props = Vec::new();
    let mut dirty = 0u64;
    let mut claimed = Vec::new();
    let mut deleted = Vec::new();
    let candidates: Vec<StreamId> = s.groups[&gname].pel.range(start..).map(|(k, _)| *k).collect();
    let mut idx = 0;
    while attempts > 0 && count > 0 && idx < candidates.len() {
        attempts -= 1;
        let id = candidates[idx];
        idx += 1;
        let exists = s.get(&id).map(|e| e.fv.clone());
        let g = s.groups.get_mut(&gname).unwrap();
        let glast = g.last_id;
        let n = g.pel[&id].clone();
        let Some(fv) = exists else {
            props.push(xclaim_argv(&key, &gname, &id, &n, &glast));
            dirty += 1;
            g.remove_nack(&id);
            deleted.push(id);
            count -= 1;
            continue;
        };
        if minidle > 0 && now - n.delivery_time < minidle {
            continue;
        }
        if n.consumer.as_ref() != Some(&cname) {
            if let Some(oc) = n.consumer.as_ref().and_then(|c| g.consumers.get_mut(c)) {
                oc.pel.remove(&id);
            }
        }
        let nn = g.pel.get_mut(&id).unwrap();
        nn.delivery_time = now;
        if !justid {
            nn.delivery_count += 1;
        }
        nn.consumer = Some(cname.clone());
        let nn = nn.clone();
        let c = g.consumers.get_mut(&cname).unwrap();
        c.pel.insert(id);
        c.active_time = now;
        claimed.push(if justid { id_frame(&id) } else { entry_frame(id, fv) });
        count -= 1;
        props.push(xclaim_argv(&key, &gname, &id, &nn, &glast));
        dirty += 1;
    }
    // The cursor is the next PEL entry after the last one examined.
    let endid = candidates.get(idx).copied().unwrap_or_default();
    if created {
        dirty += 1;
        ctx.s.notify(NOTIFY_STREAM, "xgroup-createconsumer", &key, db);
    }
    ctx.no_prop = true;
    for p in props {
        ctx.also_propagate(p);
    }
    if dirty > 0 {
        ctx.dirty(dirty);
    }
    Frame::Array(vec![id_frame(&endid), Frame::Array(claimed), Frame::Array(deleted.iter().map(id_frame).collect())])
}

// ---------------- XINFO ----------------

pub fn xinfo_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "XINFO",
        &[
            "CONSUMERS <key> <groupname>",
            "    Show consumers of <groupname>.",
            "GROUPS <key>",
            "    Show the stream consumer groups.",
            "STREAM <key> [FULL [COUNT <count>]",
            "    Show information about the stream.",
        ],
    )
}

fn kv(k: &str, v: Frame) -> (Frame, Frame) {
    (Frame::bulk_str(k), v)
}

fn opt_int(v: Option<i64>) -> Frame {
    match v {
        Some(v) => Frame::Integer(v),
        None => Frame::Null,
    }
}

fn entries_read_frame(g: &Group) -> Frame {
    if g.entries_read != INVALID_ENTRIES_READ { Frame::Integer(g.entries_read) } else { Frame::Null }
}

pub fn xinfo_consumers(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let now = ctx.s.now_ms();
    let s = match stream_ref(ctx, &args[2]) {
        Err(e) => return e,
        Ok(None) => return err_nokey(),
        Ok(Some(s)) => s,
    };
    let Some(g) = s.groups.get(&args[3]) else { return nogroup(&args[3], &args[2]) };
    Frame::Array(
        g.consumers
            .iter()
            .map(|(n, c)| {
                let inactive = if c.active_time != -1 { now - c.active_time } else { -1 };
                Frame::Map(vec![
                    kv("name", Frame::Bulk(n.clone())),
                    kv("pending", Frame::Integer(c.pel.len() as i64)),
                    kv("idle", Frame::Integer((now - c.seen_time).max(0))),
                    kv("inactive", Frame::Integer(inactive)),
                ])
            })
            .collect(),
    )
}

pub fn xinfo_groups(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let s = match stream_ref(ctx, &args[2]) {
        Err(e) => return e,
        Ok(None) => return err_nokey(),
        Ok(Some(s)) => s,
    };
    Frame::Array(
        s.groups
            .iter()
            .map(|(n, g)| {
                Frame::Map(vec![
                    kv("name", Frame::Bulk(n.clone())),
                    kv("consumers", Frame::Integer(g.consumers.len() as i64)),
                    kv("pending", Frame::Integer(g.pel.len() as i64)),
                    kv("last-delivered-id", id_frame(&g.last_id)),
                    kv("entries-read", entries_read_frame(g)),
                    kv("lag", opt_int(s.group_lag(g))),
                ])
            })
            .collect(),
    )
}

pub fn xinfo_stream(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let optc = args.len() - 3;
    let mut full = true;
    let mut count: i64 = 10;
    let suberr = || Frame::err("ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.");
    if optc == 0 {
        full = false;
    } else {
        if optc != 1 && optc != 3 {
            return suberr();
        }
        if !eq(&args[3], "full") {
            return suberr();
        }
        if optc == 3 {
            if !eq(&args[4], "count") {
                return suberr();
            }
            count = tri!(int(&args[5]));
            if count < 0 {
                count = 10;
            }
        }
    }
    let s = match stream_ref(ctx, &args[2]) {
        Err(e) => return e,
        Ok(None) => return err_nokey(),
        Ok(Some(s)) => s,
    };
    let mut m = vec![
        kv("length", Frame::Integer(s.length as i64)),
        kv("radix-tree-keys", Frame::Integer(s.nodes.len() as i64)),
        kv("radix-tree-nodes", Frame::Integer((s.nodes.len() as i64 * 2).max(1))),
        kv("last-generated-id", id_frame(&s.last_id)),
        kv("max-deleted-entry-id", id_frame(&s.max_deleted_id)),
        kv("entries-added", Frame::Integer(s.entries_added as i64)),
        kv("recorded-first-entry-id", id_frame(&s.first_id)),
    ];
    if !full {
        m.push(kv("groups", Frame::Integer(s.groups.len() as i64)));
        let first = s.range(&StreamId::MIN, &StreamId::MAX, false, 1).pop();
        let last = s.range(&StreamId::MIN, &StreamId::MAX, true, 1).pop();
        m.push(kv("first-entry", first.map(|(i, f)| entry_frame(i, f)).unwrap_or(Frame::Null)));
        m.push(kv("last-entry", last.map(|(i, f)| entry_frame(i, f)).unwrap_or(Frame::Null)));
        return Frame::Map(m);
    }
    let c = count as usize;
    m.push(kv("entries", Frame::Array(s.range(&StreamId::MIN, &StreamId::MAX, false, c).into_iter().map(|(i, f)| entry_frame(i, f)).collect())));
    let groups: Vec<Frame> = s
        .groups
        .iter()
        .map(|(n, g)| {
            let pending: Vec<Frame> = g
                .pel
                .iter()
                .take(if c == 0 { usize::MAX } else { c })
                .map(|(id, nk)| {
                    Frame::Array(vec![
                        id_frame(id),
                        Frame::Bulk(nk.consumer.clone().unwrap_or_default()),
                        Frame::Integer(nk.delivery_time),
                        Frame::Integer(nk.delivery_count as i64),
                    ])
                })
                .collect();
            let consumers: Vec<Frame> = g
                .consumers
                .iter()
                .map(|(cn, cs)| {
                    let p: Vec<Frame> = cs
                        .pel
                        .iter()
                        .take(if c == 0 { usize::MAX } else { c })
                        .map(|id| {
                            let nk = &g.pel[id];
                            Frame::Array(vec![id_frame(id), Frame::Integer(nk.delivery_time), Frame::Integer(nk.delivery_count as i64)])
                        })
                        .collect();
                    Frame::Map(vec![
                        kv("name", Frame::Bulk(cn.clone())),
                        kv("seen-time", Frame::Integer(cs.seen_time)),
                        kv("active-time", Frame::Integer(cs.active_time)),
                        kv("pel-count", Frame::Integer(cs.pel.len() as i64)),
                        kv("pending", Frame::Array(p)),
                    ])
                })
                .collect();
            Frame::Map(vec![
                kv("name", Frame::Bulk(n.clone())),
                kv("last-delivered-id", id_frame(&g.last_id)),
                kv("entries-read", entries_read_frame(g)),
                kv("lag", opt_int(s.group_lag(g))),
                kv("pel-count", Frame::Integer(g.pel.len() as i64)),
                kv("pending", Frame::Array(pending)),
                kv("consumers", Frame::Array(consumers)),
            ])
        })
        .collect();
    m.push(kv("groups", Frame::Array(groups)));
    Frame::Map(m)
}
