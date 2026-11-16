//! ACL subcommands.

use super::util::*;
use super::{cat as C, lookup_full};
use crate::acl::{self, Denied};
use crate::exec::Ctx;
use crate::resp::Frame;
use bytes::Bytes;

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

pub fn setuser(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let name = s(&args[2]);
    if name.bytes().any(|c| c.is_ascii_whitespace() || c == 0) {
        return Frame::err("ERR Usernames can't contain spaces or null characters");
    }
    let ops: Vec<String> = args[3..].iter().map(|a| s(a)).collect();
    let all = ctx.s.cfg.acl_pubsub_default_all;
    match ctx.s.acl.set_user(&name, &ops, all) {
        Ok(()) => {
            acl::enforce_users(ctx.s);
            Frame::ok()
        }
        Err(e) => Frame::err(format!("ERR {}", e)),
    }
}

pub fn deluser(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args[2..].iter().any(|a| a.as_ref() == b"default") {
        return Frame::err("ERR The 'default' user cannot be removed");
    }
    let mut n = 0;
    for a in &args[2..] {
        if ctx.s.acl.users.remove(&s(a)).is_some() {
            n += 1;
        }
    }
    if n > 0 {
        acl::enforce_users(ctx.s);
    }
    Frame::Integer(n)
}

fn selector_fields(sel: &acl::Selector) -> Vec<(Frame, Frame)> {
    let keys = if sel.allkeys { "~*".to_string() } else { sel.patterns.iter().map(|p| match p.flags {
        acl::PERM_RW => format!("~{}", p.pattern),
        acl::PERM_R => format!("%R~{}", p.pattern),
        _ => format!("%W~{}", p.pattern),
    }).collect::<Vec<_>>().join(" ") };
    let chans = if sel.allchannels { "&*".to_string() } else { sel.channels.iter().map(|c| format!("&{}", c)).collect::<Vec<_>>().join(" ") };
    vec![
        (Frame::bulk_str("commands"), Frame::bulk(Bytes::from(sel.describe_commands()))),
        (Frame::bulk_str("keys"), Frame::bulk(Bytes::from(keys))),
        (Frame::bulk_str("channels"), Frame::bulk(Bytes::from(chans))),
    ]
}

pub fn getuser(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let Some(u) = ctx.s.acl.users.get(&s(&args[2])) else { return Frame::Null };
    let mut m = vec![
        (Frame::bulk_str("flags"), Frame::Set(u.flag_names().into_iter().map(Frame::bulk_str).collect())),
        (Frame::bulk_str("passwords"), Frame::Array(u.passwords.iter().map(|p| Frame::bulk(Bytes::from(p.clone()))).collect())),
    ];
    m.extend(selector_fields(&u.selectors[0]));
    m.push((Frame::bulk_str("selectors"), Frame::Array(u.selectors[1..].iter().map(|sel| Frame::Map(selector_fields(sel))).collect())));
    Frame::Map(m)
}

pub fn list(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Array(ctx.s.acl.users.values().map(|u| Frame::bulk(Bytes::from(format!("user {} {}", u.name, u.describe())))).collect())
}

pub fn users(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Array(ctx.s.acl.users.keys().map(|n| Frame::bulk(Bytes::from(n.clone()))).collect())
}

pub fn whoami(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::bulk(Bytes::from(ctx.client().user.clone()))
}

const NO_FILE: &str = "ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.";

pub fn load(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let path = ctx.s.cfg.aclfile.clone();
    if path.is_empty() {
        return Frame::err(NO_FILE);
    }
    let all = ctx.s.cfg.acl_pubsub_default_all;
    match ctx.s.acl.load_file(&path, all) {
        Ok(()) => {
            acl::enforce_users(ctx.s);
            Frame::ok()
        }
        Err(e) => Frame::err(format!("ERR {}", e)),
    }
}

pub fn save(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let path = ctx.s.cfg.aclfile.clone();
    if path.is_empty() {
        return Frame::err(NO_FILE);
    }
    match ctx.s.acl.save_file(&path) {
        Ok(()) => Frame::ok(),
        Err(_) => Frame::err("ERR There was an error trying to save the ACLs. Please check the server logs for more information"),
    }
}

pub fn cat(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() == 2 {
        return Frame::Array(C::NAMES.iter().map(|(n, _)| Frame::bulk_str(n)).collect());
    }
    if args.len() > 3 {
        return err_syntax();
    }
    let name = s(&args[2]).to_ascii_lowercase();
    let Some((_, flag)) = C::NAMES.iter().find(|(n, _)| *n == name) else {
        return Frame::err(format!("ERR Unknown category '{}'", s(&args[2])));
    };
    let mut out = Vec::new();
    for c in super::all_commands() {
        if c.acl_categories() & flag != 0 {
            out.push(Frame::bulk(Bytes::from(c.full_name())));
        }
        for sc in c.subs {
            if sc.acl_categories() & flag != 0 {
                out.push(Frame::bulk(Bytes::from(sc.full_name())));
            }
        }
    }
    Frame::Array(out)
}

pub fn genpass(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut bits: i64 = 256;
    if args.len() == 3 {
        bits = tri!(int(&args[2]));
    } else if args.len() > 3 {
        return err_syntax();
    }
    if bits <= 0 || bits > 4096 {
        return Frame::err("ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096");
    }
    let chars = ((bits + 3) / 4) as usize;
    Frame::bulk(Bytes::from(crate::util::random_hex(chars)))
}

pub fn log(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut count: i64 = 10;
    if args.len() == 3 {
        if eq(&args[2], "reset") {
            ctx.s.acl.log.clear();
            return Frame::ok();
        }
        count = tri!(int(&args[2])).max(0);
    } else if args.len() > 3 {
        return err_syntax();
    }
    let now = crate::util::now_ms();
    Frame::Array(
        ctx.s
            .acl
            .log
            .iter()
            .take(count as usize)
            .map(|e| {
                let reason = match e.reason {
                    Denied::Cmd => "command",
                    Denied::Key => "key",
                    Denied::Channel => "channel",
                    Denied::Auth => "auth",
                };
                Frame::Map(vec![
                    (Frame::bulk_str("count"), Frame::Integer(e.count as i64)),
                    (Frame::bulk_str("reason"), Frame::bulk_str(reason)),
                    (Frame::bulk_str("context"), Frame::bulk_str(e.context)),
                    (Frame::bulk_str("object"), Frame::bulk(Bytes::from(e.object.clone()))),
                    (Frame::bulk_str("username"), Frame::bulk(Bytes::from(e.username.clone()))),
                    (Frame::bulk_str("age-seconds"), Frame::Double((now - e.ctime) as f64 / 1000.0)),
                    (Frame::bulk_str("client-info"), Frame::bulk(Bytes::from(e.cinfo.clone()))),
                    (Frame::bulk_str("entry-id"), Frame::Integer(e.entry_id as i64)),
                    (Frame::bulk_str("timestamp-created"), Frame::Integer(e.created)),
                    (Frame::bulk_str("timestamp-last-updated"), Frame::Integer(e.ctime)),
                ])
            })
            .collect(),
    )
}

pub fn dryrun(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let uname = s(&args[2]);
    let Some(u) = ctx.s.acl.users.get(&uname) else {
        return Frame::err(format!("ERR User '{}' not found", uname));
    };
    let sub = &args[3..];
    let Some(mut c) = super::lookup(&sub[0]) else {
        return Frame::err(format!("ERR Command '{}' not found", s(&sub[0])));
    };
    if !c.subs.is_empty() && sub.len() > 1 {
        match super::lookup_sub(c, &sub[1]) {
            Some(sc) => c = sc,
            None => return Frame::err(format!("ERR Command '{}' not found", s(&sub[0]))),
        }
    }
    let n = sub.len() as i32;
    if (c.arity > 0 && c.arity != n) || n < -c.arity {
        return Frame::err(format!("ERR wrong number of arguments for '{}' command", c.full_name()));
    }
    let _ = lookup_full;
    match u.check_cmd(c, sub) {
        Ok(()) => Frame::ok(),
        Err((r, idx)) => Frame::bulk(Bytes::from(acl::error_message(r, &uname, c, &sub[idx], true))),
    }
}

pub fn help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "ACL",
        &[
            "CAT [<category>]",
            "    List all commands that belong to <category>, or all command categories",
            "    when no category is specified.",
            "DELUSER <username> [<username> ...]",
            "    Delete a list of users.",
            "DRYRUN <username> <command> [<arg> ...]",
            "    Returns whether the user can execute the given command without executing the command.",
            "GETUSER <username>",
            "    Get the user's details.",
            "GENPASS [<bits>]",
            "    Generate a secure 256-bit user password. The optional `bits` argument can",
            "    be used to specify a different size.",
            "LIST",
            "    Show users details in config file format.",
            "LOAD",
            "    Reload users from the ACL file.",
            "LOG [<count> | RESET]",
            "    Show the ACL log entries.",
            "SAVE",
            "    Save the current config to the ACL file.",
            "SETUSER <username> <attribute> [<attribute> ...]",
            "    Create or modify a user with the specified attributes.",
            "USERS",
            "    List all the registered usernames.",
            "WHOAMI",
            "    Return the current connection username.",
        ],
    )
}
