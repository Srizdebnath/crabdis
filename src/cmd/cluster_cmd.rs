//! CLUSTER subcommands plus ASKING / READONLY / READWRITE.

use super::util::*;
use crate::cluster::{self, ClusterState, SLOTS, nflags as NF};
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::server::cflags;
use bytes::Bytes;

const DISABLED: &str = "ERR This instance has cluster support disabled";

macro_rules! cl {
    ($ctx:expr) => {
        match $ctx.s.cluster.as_mut() {
            Some(c) => c,
            None => return Frame::err(DISABLED),
        }
    };
}

fn parse_slot(b: &[u8]) -> Result<usize, Frame> {
    match crate::util::parse_i64(b) {
        Some(v) if (0..SLOTS as i64).contains(&v) => Ok(v as usize),
        _ => Err(Frame::err("ERR Invalid or out of range slot")),
    }
}

fn save(c: &mut ClusterState) {
    c.todo_save = false;
    if let Err(e) = c.save_config() {
        eprintln!("Error saving cluster config: {}", e);
    }
}

pub fn asking(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    ctx.client().flags |= cflags::ASKING;
    Frame::ok()
}

pub fn readonly(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    ctx.client().flags |= cflags::READONLY;
    Frame::ok()
}

pub fn readwrite(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    ctx.client().flags &= !cflags::READONLY;
    Frame::ok()
}

pub fn myid(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    Frame::bulk(Bytes::from(c.myself.clone()))
}

pub fn myshardid(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let mid = c.my_master_id();
    let sid = c.nodes.get(&mid).map(|n| n.shard_id.clone()).unwrap_or_else(|| c.me().shard_id.clone());
    Frame::bulk(Bytes::from(sid))
}

pub fn keyslot(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Integer(cluster::key_slot(&args[2]) as i64)
}

pub fn info(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let req = ctx.s.cfg.cluster_require_full_coverage;
    let c = cl!(ctx);
    c.update_state(req);
    let assigned = c.slots.iter().filter(|s| s.is_some()).count();
    let (mut pfail, mut fail) = (0, 0);
    for o in c.slots.iter().flatten() {
        if let Some(n) = c.nodes.get(o) {
            if n.flags & NF::FAIL != 0 {
                fail += 1;
            } else if n.flags & NF::PFAIL != 0 {
                pfail += 1;
            }
        }
    }
    let size = c.nodes.values().filter(|n| n.is_master() && n.slot_count() > 0).count();
    let my_epoch = {
        let me = c.me();
        if me.is_master() { me.config_epoch } else { me.master.as_ref().and_then(|m| c.nodes.get(m)).map(|n| n.config_epoch).unwrap_or(0) }
    };
    let mut out = format!(
        "cluster_state:{}\r\ncluster_slots_assigned:{}\r\ncluster_slots_ok:{}\r\ncluster_slots_pfail:{}\r\ncluster_slots_fail:{}\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n",
        if c.state_ok { "ok" } else { "fail" },
        assigned,
        assigned - pfail - fail,
        pfail,
        fail,
        c.nodes.len(),
        size,
        c.current_epoch,
        my_epoch
    );
    for (k, v) in cluster::msg_stats(c) {
        out.push_str(&format!("{}:{}\r\n", k, v));
    }
    out.push_str("total_cluster_links_buffer_limit_exceeded:0\r\n");
    Frame::Verbatim("txt", Bytes::from(out))
}

pub fn nodes(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    Frame::Verbatim("txt", Bytes::from(c.nodes_text()))
}

pub fn replicas(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let id = String::from_utf8_lossy(&args[2]).into_owned();
    let Some(n) = c.nodes.get(&id) else { return Frame::err(format!("ERR Unknown node {}", id)) };
    if !n.is_master() {
        return Frame::err("ERR The specified node is not a master");
    }
    Frame::Array(c.nodes.values().filter(|r| r.master.as_deref() == Some(id.as_str())).map(|r| Frame::bulk(Bytes::from(c.node_line(r)))).collect())
}

fn node_entry(n: &cluster::Node) -> Frame {
    let ip = if n.ip.is_empty() { "127.0.0.1".to_string() } else { n.ip.clone() };
    Frame::Array(vec![Frame::bulk(Bytes::from(ip)), Frame::Integer(n.port as i64), Frame::bulk(Bytes::from(n.id.clone())), Frame::Map(vec![])])
}

pub fn slots(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let mut out = Vec::new();
    let mut start = 0;
    while start < SLOTS {
        let Some(owner) = c.slots[start].clone() else {
            start += 1;
            continue;
        };
        let mut end = start;
        while end + 1 < SLOTS && c.slots[end + 1].as_deref() == Some(owner.as_str()) {
            end += 1;
        }
        if let Some(m) = c.nodes.get(&owner) {
            let mut e = vec![Frame::Integer(start as i64), Frame::Integer(end as i64), node_entry(m)];
            for r in c.nodes.values().filter(|r| r.master.as_deref() == Some(owner.as_str()) && r.flags & NF::FAIL == 0) {
                e.push(node_entry(r));
            }
            out.push(Frame::Array(e));
        }
        start = end + 1;
    }
    Frame::Array(out)
}

pub fn shards(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let mut out = Vec::new();
    for m in c.nodes.values().filter(|n| n.is_master() && n.flags & NF::HANDSHAKE == 0) {
        let mut slots = Vec::new();
        for (a, b) in cluster::slot_ranges(m) {
            slots.push(Frame::Integer(a as i64));
            slots.push(Frame::Integer(b as i64));
        }
        let mut members: Vec<&cluster::Node> = vec![m];
        members.extend(c.nodes.values().filter(|r| r.master.as_deref() == Some(m.id.as_str())));
        let nodes = members
            .into_iter()
            .map(|n| {
                let ip = if n.ip.is_empty() { "127.0.0.1".to_string() } else { n.ip.clone() };
                let health = if n.flags & NF::FAIL != 0 { "fail" } else if n.repl_offset == 0 && n.flags & NF::SLAVE != 0 { "loading" } else { "online" };
                let off = if n.flags & NF::MYSELF != 0 { ctx_offset(n) } else { n.repl_offset };
                Frame::Map(vec![
                    (Frame::bulk_str("id"), Frame::bulk(Bytes::from(n.id.clone()))),
                    (Frame::bulk_str("port"), Frame::Integer(n.port as i64)),
                    (Frame::bulk_str("ip"), Frame::bulk(Bytes::from(ip.clone()))),
                    (Frame::bulk_str("endpoint"), Frame::bulk(Bytes::from(ip))),
                    (Frame::bulk_str("role"), Frame::bulk_str(if n.is_master() { "master" } else { "replica" })),
                    (Frame::bulk_str("replication-offset"), Frame::Integer(off)),
                    (Frame::bulk_str("health"), Frame::bulk_str(health)),
                ])
            })
            .collect();
        out.push(Frame::Map(vec![(Frame::bulk_str("slots"), Frame::Array(slots)), (Frame::bulk_str("nodes"), Frame::Array(nodes))]));
    }
    Frame::Array(out)
}

fn ctx_offset(n: &cluster::Node) -> i64 {
    n.repl_offset
}

pub fn links(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    Frame::Array(
        c.nodes
            .values()
            .filter(|n| n.link.is_some() && n.flags & NF::MYSELF == 0)
            .map(|n| {
                Frame::Map(vec![
                    (Frame::bulk_str("direction"), Frame::bulk_str(if n.inbound { "from" } else { "to" })),
                    (Frame::bulk_str("node"), Frame::bulk(Bytes::from(n.id.clone()))),
                    (Frame::bulk_str("create-time"), Frame::Integer(n.link_created)),
                    (Frame::bulk_str("events"), Frame::bulk_str("rw")),
                    (Frame::bulk_str("send-buffer-allocated"), Frame::Integer(0)),
                    (Frame::bulk_str("send-buffer-used"), Frame::Integer(0)),
                ])
            })
            .collect(),
    )
}

fn add_slots(ctx: &mut Ctx, slots: Vec<usize>) -> Frame {
    let c = cl!(ctx);
    if c.me().flags & NF::SLAVE != 0 {
        return Frame::err("ERR Please use SETSLOT only with masters.");
    }
    let mut seen = std::collections::HashSet::new();
    for &s in &slots {
        if c.slots[s].is_some() {
            return Frame::err(format!("ERR Slot {} is already busy", s));
        }
        if !seen.insert(s) {
            return Frame::err(format!("ERR Slot {} specified multiple times", s));
        }
    }
    for s in slots {
        cluster::add_slot(c, s);
    }
    let req = ctx.s.cfg.cluster_require_full_coverage;
    let c = ctx.s.cluster.as_mut().unwrap();
    c.update_state(req);
    save(c);
    Frame::ok()
}

fn del_slots(ctx: &mut Ctx, slots: Vec<usize>) -> Frame {
    let c = cl!(ctx);
    let mut seen = std::collections::HashSet::new();
    for &s in &slots {
        if c.slots[s].is_none() {
            return Frame::err(format!("ERR Slot {} is already unassigned", s));
        }
        if !seen.insert(s) {
            return Frame::err(format!("ERR Slot {} specified multiple times", s));
        }
    }
    for s in slots {
        cluster::del_slot(c, s);
    }
    let req = ctx.s.cfg.cluster_require_full_coverage;
    let c = ctx.s.cluster.as_mut().unwrap();
    c.update_state(req);
    save(c);
    Frame::ok()
}

fn slot_list(args: &[Bytes]) -> Result<Vec<usize>, Frame> {
    args.iter().map(|a| parse_slot(a)).collect()
}

fn slot_ranges(args: &[Bytes]) -> Result<Vec<usize>, Frame> {
    if args.len() % 2 != 0 {
        return Err(Frame::err("ERR wrong number of arguments"));
    }
    let mut v = Vec::new();
    for p in args.chunks(2) {
        let a = parse_slot(&p[0])?;
        let b = parse_slot(&p[1])?;
        if a > b {
            return Err(Frame::err(format!("ERR start slot number {} is greater than end slot number {}", a, b)));
        }
        v.extend(a..=b);
    }
    Ok(v)
}

pub fn addslots(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let s = tri!(slot_list(&args[2..]));
    add_slots(ctx, s)
}
pub fn addslotsrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 == 1 {
        return Frame::err("ERR wrong number of arguments for 'cluster|addslotsrange' command");
    }
    let s = tri!(slot_ranges(&args[2..]));
    add_slots(ctx, s)
}
pub fn delslots(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let s = tri!(slot_list(&args[2..]));
    del_slots(ctx, s)
}
pub fn delslotsrange(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() % 2 == 1 {
        return Frame::err("ERR wrong number of arguments for 'cluster|delslotsrange' command");
    }
    let s = tri!(slot_ranges(&args[2..]));
    del_slots(ctx, s)
}

pub fn flushslots(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    if ctx.s.dbs[0].dict.len() != 0 {
        return Frame::err("ERR DB must be empty to perform CLUSTER FLUSHSLOTS.");
    }
    let c = cl!(ctx);
    let me = c.myself.clone();
    for s in 0..SLOTS {
        if c.slots[s].as_deref() == Some(me.as_str()) {
            cluster::del_slot(c, s);
        }
    }
    save(c);
    Frame::ok()
}

pub fn setslot(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    let slot = tri!(parse_slot(&args[2]));
    let action = String::from_utf8_lossy(&args[3]).to_ascii_lowercase();
    let node_arg = args.get(4).map(|a| String::from_utf8_lossy(a).into_owned());
    let keys_in_slot = cluster::count_keys_in_slot(ctx.s, slot as u16);
    let req = ctx.s.cfg.cluster_require_full_coverage;
    let c = ctx.s.cluster.as_mut().unwrap();
    if c.me().flags & NF::SLAVE != 0 {
        return Frame::err("ERR Please use SETSLOT only with masters.");
    }
    let me = c.myself.clone();
    match action.as_str() {
        "migrating" => {
            let Some(id) = node_arg else { return err_syntax() };
            if c.slots[slot].as_deref() != Some(me.as_str()) {
                return Frame::err(format!("ERR I'm not the owner of hash slot {}", slot));
            }
            let Some(n) = c.nodes.get(&id) else { return Frame::err(format!("ERR I don't know about node {}", id)) };
            if !n.is_master() {
                return Frame::err("ERR Target node is not a master");
            }
            c.migrating.insert(slot as u16, id);
        }
        "importing" => {
            let Some(id) = node_arg else { return err_syntax() };
            if c.slots[slot].as_deref() == Some(me.as_str()) {
                return Frame::err(format!("ERR I'm already the owner of hash slot {}", slot));
            }
            let Some(n) = c.nodes.get(&id) else { return Frame::err(format!("ERR I don't know about node {}", id)) };
            if !n.is_master() {
                return Frame::err("ERR Target node is not a master");
            }
            c.importing.insert(slot as u16, id);
        }
        "stable" => {
            c.migrating.remove(&(slot as u16));
            c.importing.remove(&(slot as u16));
        }
        "node" => {
            let Some(id) = node_arg else { return err_syntax() };
            if !c.nodes.contains_key(&id) {
                return Frame::err(format!("ERR Unknown node {}", id));
            }
            if c.nodes[&id].flags & NF::SLAVE != 0 {
                return Frame::err("ERR Target node is not a master");
            }
            if c.slots[slot].as_deref() == Some(me.as_str()) && id != me && keys_in_slot > 0 {
                return Frame::err(format!("ERR Can't assign hashslot {} to a different node while I still hold keys for this hash slot.", slot));
            }
            if keys_in_slot == 0 {
                c.migrating.remove(&(slot as u16));
            }
            // Finishing an import bumps our epoch so the new ownership wins.
            if id == me && c.importing.remove(&(slot as u16)).is_some() {
                c.current_epoch += 1;
                let e = c.current_epoch;
                c.nodes.get_mut(&me).unwrap().config_epoch = e;
            }
            cluster::set_slot_node(c, slot, &id);
        }
        _ => return Frame::err("ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP"),
    }
    c.update_state(req);
    save(c);
    Frame::ok()
}

pub fn meet(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    let ip = String::from_utf8_lossy(&args[2]).into_owned();
    let port = crate::util::parse_i64(&args[3]);
    let cport = match args.get(4) {
        Some(a) => crate::util::parse_i64(a),
        None => port.map(|p| p + 10000),
    };
    let (Some(port), Some(cport)) = (port, cport) else {
        return Frame::err(format!("ERR Invalid base port specified: {}", String::from_utf8_lossy(&args[3])));
    };
    if ip.parse::<std::net::IpAddr>().is_err() || !(0..=65535).contains(&port) || !(0..=65535).contains(&cport) {
        return Frame::err(format!("ERR Invalid node address specified: {}:{}", ip, port));
    }
    cluster::meet(ctx.s, ip, port as u16, cport as u16);
    Frame::ok()
}

pub fn forget(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let id = String::from_utf8_lossy(&args[2]).into_owned();
    if !c.nodes.contains_key(&id) {
        return Frame::err(format!("ERR Unknown node {}", id));
    }
    if id == c.myself {
        return Frame::err("ERR I tried hard but I can't forget myself...");
    }
    if c.me().master.as_deref() == Some(id.as_str()) {
        return Frame::err("ERR Can't forget my master!");
    }
    cluster::forget(c, &id);
    save(c);
    Frame::ok()
}

pub fn replicate(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    let id = String::from_utf8_lossy(&args[2]).into_owned();
    let has_keys = ctx.s.dbs.iter().any(|d| d.dict.len() > 0);
    let c = ctx.s.cluster.as_mut().unwrap();
    let Some(n) = c.nodes.get(&id) else { return Frame::err(format!("ERR Unknown node {}", id)) };
    if id == c.myself {
        return Frame::err("ERR Can't replicate myself");
    }
    if !n.is_master() {
        return Frame::err("ERR I can only replicate a master, not a replica.");
    }
    if c.me().is_master() && (c.me().slot_count() > 0 || has_keys) {
        return Frame::err("ERR To set a master the node must be empty and without assigned slots.");
    }
    let (ip, port) = (n.ip.clone(), n.port);
    {
        let me = c.nodes.get_mut(&c.myself.clone()).unwrap();
        me.flags = (me.flags & !NF::MASTER) | NF::SLAVE;
        me.master = Some(id);
    }
    save(c);
    let Some(shared) = crate::SHARED.get().cloned() else { return Frame::err("ERR server not ready") };
    crate::repl::set_master(ctx.s, shared, ip, port);
    Frame::ok()
}

pub fn failover(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let (mut force, mut takeover) = (false, false);
    if args.len() == 3 {
        if eq(&args[2], "force") {
            force = true;
        } else if eq(&args[2], "takeover") {
            takeover = true;
            force = true;
        } else {
            return err_syntax();
        }
    } else if args.len() > 3 {
        return err_syntax();
    }
    if c.me().is_master() {
        return Frame::err("ERR You should send CLUSTER FAILOVER to a replica");
    }
    let Some(master) = c.me().master.clone().and_then(|m| c.nodes.get(&m).cloned()) else {
        return Frame::err("ERR I'm a replica but my master is unknown to me");
    };
    if !force && (master.flags & NF::FAIL != 0 || master.link.is_none()) {
        return Frame::err("ERR Master is down or failed, please use CLUSTER FAILOVER FORCE");
    }
    if takeover {
        cluster::takeover(ctx.s);
    } else {
        cluster::request_manual_failover(ctx.s, force);
    }
    Frame::ok()
}

pub fn reset(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    let hard = match args.get(2) {
        None => false,
        Some(a) if eq(a, "hard") => true,
        Some(a) if eq(a, "soft") => false,
        _ => return err_syntax(),
    };
    let is_master = ctx.s.cluster.as_ref().unwrap().me().is_master();
    if is_master && ctx.s.dbs[0].dict.len() > 0 {
        return Frame::err("ERR CLUSTER RESET can't be called with master nodes containing keys");
    }
    if !is_master {
        crate::repl::unset_master(ctx.s);
        for d in ctx.s.dbs.iter_mut() {
            d.clear();
        }
    }
    let c = ctx.s.cluster.as_mut().unwrap();
    let me = c.myself.clone();
    for s in 0..SLOTS {
        cluster::del_slot(c, s);
    }
    c.migrating.clear();
    c.importing.clear();
    let others: Vec<String> = c.nodes.keys().filter(|k| **k != me).cloned().collect();
    for id in others {
        c.nodes.remove(&id);
    }
    let mut node = c.nodes.remove(&me).unwrap();
    node.flags = NF::MYSELF | NF::MASTER;
    node.master = None;
    if hard {
        c.current_epoch = 0;
        c.last_vote_epoch = 0;
        node.config_epoch = 0;
        node.id = crate::util::random_hex(40);
        c.myself = node.id.clone();
    }
    c.nodes.insert(node.id.clone(), node);
    save(c);
    Frame::ok()
}

pub fn saveconfig(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    match c.save_config() {
        Ok(()) => Frame::ok(),
        Err(e) => Frame::err(format!("ERR error saving the cluster node config: {}", e)),
    }
}

pub fn bumpepoch(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let me = c.myself.clone();
    let max = c.nodes.values().map(|n| n.config_epoch).max().unwrap_or(0);
    let mine = c.nodes[&me].config_epoch;
    if mine == 0 || mine != max || c.nodes.values().filter(|n| n.config_epoch == mine).count() > 1 {
        c.current_epoch += 1;
        let e = c.current_epoch;
        c.nodes.get_mut(&me).unwrap().config_epoch = e;
        save(c);
        Frame::Simple(format!("BUMPED {}", e))
    } else {
        Frame::Simple(format!("STILL {}", mine))
    }
}

pub fn set_config_epoch(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let e = tri!(int(&args[2]));
    if e < 0 {
        return Frame::err(format!("ERR Invalid config epoch specified: {}", e));
    }
    if c.nodes.len() > 1 {
        return Frame::err("ERR The user can assign a config epoch only when the node does not know any other node.");
    }
    let me = c.myself.clone();
    if c.nodes[&me].config_epoch != 0 {
        return Frame::err("ERR Node config epoch is already non-zero");
    }
    c.nodes.get_mut(&me).unwrap().config_epoch = e as u64;
    if c.current_epoch < e as u64 {
        c.current_epoch = e as u64;
    }
    save(c);
    Frame::ok()
}

pub fn count_failure_reports(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let c = cl!(ctx);
    let id = String::from_utf8_lossy(&args[2]).into_owned();
    match c.nodes.get(&id) {
        Some(n) => Frame::Integer(n.fail_reports.len() as i64),
        None => Frame::err(format!("ERR Unknown node {}", id)),
    }
}

pub fn countkeysinslot(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    match crate::util::parse_i64(&args[2]) {
        Some(v) if (0..SLOTS as i64).contains(&v) => Frame::Integer(cluster::count_keys_in_slot(ctx.s, v as u16) as i64),
        _ => Frame::err("ERR Invalid slot"),
    }
}

pub fn getkeysinslot(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_none() {
        return Frame::err(DISABLED);
    }
    let slot = crate::util::parse_i64(&args[2]);
    let n = crate::util::parse_i64(&args[3]);
    match (slot, n) {
        (Some(s), Some(n)) if (0..SLOTS as i64).contains(&s) && n >= 0 => Frame::Array(cluster::keys_in_slot(ctx.s, s as u16, n as usize).into_iter().map(Frame::Bulk).collect()),
        _ => Frame::err("ERR Invalid slot or number of keys"),
    }
}

pub fn help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "CLUSTER",
        &[
            "ADDSLOTS <slot> [<slot> ...]",
            "    Assign slots to current node.",
            "ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
            "    Assign slots which are between <start-slot> and <end-slot> to current node.",
            "BUMPEPOCH",
            "    Advance the cluster config epoch.",
            "COUNT-FAILURE-REPORTS <node-id>",
            "    Return number of failure reports for <node-id>.",
            "COUNTKEYSINSLOT <slot>",
            "    Return the number of keys in <slot>.",
            "DELSLOTS <slot> [<slot> ...]",
            "    Delete slots information from current node.",
            "DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
            "    Delete slots information which are between <start-slot> and <end-slot> from current node.",
            "FAILOVER [FORCE|TAKEOVER]",
            "    Promote current replica node to being a master.",
            "FORGET <node-id>",
            "    Remove a node from the cluster.",
            "FLUSHSLOTS",
            "    Delete current node own slots information.",
            "GETKEYSINSLOT <slot> <count>",
            "    Return key names stored by current node in a slot.",
            "INFO",
            "    Return information about the cluster.",
            "KEYSLOT <key>",
            "    Return the hash slot for <key>.",
            "LINKS",
            "    Return information about all network links between this node and its peers.",
            "    Output format is an array where each array element is a map containing attributes of a link",
            "MEET <ip> <port> [<bus-port>]",
            "    Connect nodes into a working cluster.",
            "MYID",
            "    Return the node id.",
            "MYSHARDID",
            "    Return the node's shard id.",
            "NODES",
            "    Return cluster configuration seen by node. Output format:",
            "    <id> <ip:port@bus-port[,hostname]> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
            "REPLICATE <node-id>",
            "    Configure current node as replica to <node-id>.",
            "RESET [HARD|SOFT]",
            "    Reset current node (default: soft).",
            "SET-CONFIG-EPOCH <epoch>",
            "    Set config epoch of current node.",
            "SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)",
            "    Set slot state.",
            "REPLICAS <node-id>",
            "    Return <node-id> replicas.",
            "SAVECONFIG",
            "    Force saving cluster configuration on disk.",
            "SLOTS",
            "    Return information about slots range mappings. Each range is made of:",
            "    start, end, master and replicas IP addresses, ports and ids",
            "SHARDS",
            "    Return information about slot range mappings and the nodes associated with them.",
        ],
    )
}
