//! Pub/Sub commands.

use super::util::*;
use crate::exec::Ctx;
use crate::pubsub::{self, Kind, sub_reply};
use crate::resp::Frame;
use bytes::Bytes;

fn deny_in_multi(ctx: &mut Ctx, name: &str) -> Option<Frame> {
    if ctx.flags & crate::exec::CTX_MULTI != 0 && ctx.proto() < 3 {
        let _ = name;
    }
    None
}

pub fn subscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if let Some(e) = deny_in_multi(ctx, "subscribe") {
        return e;
    }
    let mut out = Vec::new();
    for ch in &args[1..] {
        let n = pubsub::subscribe(ctx.s, ctx.cid, ch.clone(), Kind::Channel);
        out.push(sub_reply("subscribe", Some(ch), n));
    }
    seq(out)
}

pub fn psubscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut out = Vec::new();
    for ch in &args[1..] {
        let n = pubsub::subscribe(ctx.s, ctx.cid, ch.clone(), Kind::Pattern);
        out.push(sub_reply("psubscribe", Some(ch), n));
    }
    seq(out)
}

pub fn ssubscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if ctx.s.cluster.is_some() {
        let slot = crate::cluster::key_slot(&args[1]);
        if args[2..].iter().any(|c| crate::cluster::key_slot(c) != slot) {
            return Frame::err("CROSSSLOT Keys in request don't hash to the same slot");
        }
    }
    let mut out = Vec::new();
    for ch in &args[1..] {
        let n = pubsub::subscribe(ctx.s, ctx.cid, ch.clone(), Kind::Shard);
        out.push(sub_reply("ssubscribe", Some(ch), n));
    }
    seq(out)
}

fn unsub_generic(ctx: &mut Ctx, args: &[Bytes], kind: fn() -> Kind, name: &'static str) -> Frame {
    let targets: Vec<Bytes> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        let c = ctx.client_ref().unwrap();
        match kind() {
            Kind::Channel => c.channels.iter().cloned().collect(),
            Kind::Pattern => c.patterns.iter().cloned().collect(),
            Kind::Shard => c.shard_channels.iter().cloned().collect(),
        }
    };
    if targets.is_empty() {
        let n = match kind() {
            Kind::Shard => ctx.client_ref().unwrap().shard_channels.len(),
            _ => ctx.client_ref().unwrap().subscriptions(),
        };
        return sub_reply(name, None, n);
    }
    let mut out = Vec::new();
    for ch in targets {
        let (_, n) = pubsub::unsubscribe(ctx.s, ctx.cid, &ch, kind());
        out.push(sub_reply(name, Some(&ch), n));
    }
    seq(out)
}

pub fn unsubscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    unsub_generic(ctx, args, || Kind::Channel, "unsubscribe")
}
pub fn punsubscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    unsub_generic(ctx, args, || Kind::Pattern, "punsubscribe")
}
pub fn sunsubscribe(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    unsub_generic(ctx, args, || Kind::Shard, "sunsubscribe")
}

/// Multiple push frames emitted as consecutive replies.
fn seq(v: Vec<Frame>) -> Frame {
    if v.len() == 1 {
        return v.into_iter().next().unwrap();
    }
    Frame::Seq(v)
}

pub fn publish(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let n = pubsub::publish(ctx.s, &args[1], &args[2]);
    if ctx.s.cluster.is_some() {
        crate::cluster::broadcast_publish(ctx.s, &args[1], &args[2]);
    }
    ctx.dirty(0);
    Frame::Integer(n as i64)
}

pub fn spublish(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Integer(pubsub::spublish(ctx.s, &args[1], &args[2]) as i64)
}

pub fn channels(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let pat = args.get(2);
    Frame::Array(
        ctx.s
            .pubsub
            .channels
            .keys()
            .filter(|c| pat.is_none_or(|p| crate::util::glob_match(p, c, false)))
            .map(|c| Frame::Bulk(c.clone()))
            .collect(),
    )
}

pub fn shardchannels(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let pat = args.get(2);
    Frame::Array(
        ctx.s
            .pubsub
            .shard
            .keys()
            .filter(|c| pat.is_none_or(|p| crate::util::glob_match(p, c, false)))
            .map(|c| Frame::Bulk(c.clone()))
            .collect(),
    )
}

pub fn numsub(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Map(
        args[2..]
            .iter()
            .map(|c| (Frame::Bulk(c.clone()), Frame::Integer(ctx.s.pubsub.channels.get(c).map(|v| v.len()).unwrap_or(0) as i64)))
            .collect(),
    )
}

pub fn shardnumsub(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Map(
        args[2..]
            .iter()
            .map(|c| (Frame::Bulk(c.clone()), Frame::Integer(ctx.s.pubsub.shard.get(c).map(|v| v.len()).unwrap_or(0) as i64)))
            .collect(),
    )
}

pub fn numpat(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.s.pubsub.patterns.len() as i64)
}

pub fn help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "PUBSUB",
        &[
            "CHANNELS [<pattern>]",
            "    Return the currently active channels matching a <pattern> (default: '*').",
            "NUMPAT",
            "    Return number of subscriptions to patterns.",
            "NUMSUB [<channel> ...]",
            "    Return the number of subscribers for the specified channels, excluding",
            "    pattern subscriptions(default: no channels).",
            "SHARDCHANNELS [<pattern>]",
            "    Return the currently active shard level channels matching a <pattern> (default: '*').",
            "SHARDNUMSUB [<shardchannel> ...]",
            "    Return the number of subscribers for the specified shard level channel(s)",
        ],
    )
}
