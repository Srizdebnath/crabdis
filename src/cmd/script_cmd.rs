//! EVAL / FCALL / SCRIPT / FUNCTION commands.

use super::util::*;
use crate::exec::Ctx;
use crate::resp::Frame;
use crate::scripting::{self, FLAG_NAMES};
use bytes::Bytes;

fn run_eval(ctx: &mut Ctx, args: &[Bytes], sha: bool, ro: bool) -> Frame {
    ctx.no_prop = true;
    let (cid, db) = (ctx.cid, ctx.db);
    scripting::eval(ctx.s, cid, db, args, sha, ro)
}

pub fn eval(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    run_eval(ctx, args, false, false)
}
pub fn eval_ro(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    run_eval(ctx, args, false, true)
}
pub fn evalsha(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    run_eval(ctx, args, true, false)
}
pub fn evalsha_ro(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    run_eval(ctx, args, true, true)
}

pub fn fcall(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ctx.no_prop = true;
    let (cid, db) = (ctx.cid, ctx.db);
    scripting::fcall(ctx.s, cid, db, args, false)
}
pub fn fcall_ro(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    ctx.no_prop = true;
    let (cid, db) = (ctx.cid, ctx.db);
    scripting::fcall(ctx.s, cid, db, args, true)
}

// ---------- SCRIPT ----------

pub fn script_load(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    match ctx.s.scripting.create_script(&args[2]) {
        Ok(sha) => Frame::bulk(Bytes::from(sha)),
        Err(e) => Frame::err(format!("ERR {}", e)),
    }
}

pub fn script_exists(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    Frame::Array(
        args[2..]
            .iter()
            .map(|a| Frame::Integer(ctx.s.scripting.scripts.contains_key(&String::from_utf8_lossy(a).to_ascii_lowercase()) as i64))
            .collect(),
    )
}

pub fn script_flush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 3 || (args.len() == 3 && !eq(&args[2], "sync") && !eq(&args[2], "async")) {
        return Frame::err("ERR SCRIPT FLUSH only support SYNC|ASYNC option");
    }
    ctx.s.scripting.flush_scripts();
    Frame::ok()
}

pub fn script_kill(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    // Reaching here means no script is running (a running script holds the
    // store; SCRIPT KILL during a busy script is answered by the connection).
    Frame::err("NOTBUSY No scripts in execution right now.")
}

pub fn script_debug(_ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if eq(&args[2], "no") {
        Frame::ok()
    } else if eq(&args[2], "yes") || eq(&args[2], "sync") {
        Frame::err("ERR The Lua debugger (LDB) is not available in crabdis")
    } else {
        Frame::err("ERR Use SCRIPT DEBUG YES/SYNC/NO")
    }
}

pub fn script_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "SCRIPT",
        &[
            "DEBUG (YES|SYNC|NO)",
            "    Set the debug mode for subsequent scripts executed.",
            "EXISTS <sha1> [<sha1> ...]",
            "    Return information about the existence of the scripts in the script cache.",
            "FLUSH [ASYNC|SYNC]",
            "    Flush the Lua scripts cache. Very dangerous on replicas.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the scripts cache.",
            "    * SYNC: Synchronously flush the scripts cache.",
            "KILL",
            "    Kill the currently executing Lua script.",
            "LOAD <script>",
            "    Load a script into the scripts cache without executing it.",
        ],
    )
}

// ---------- FUNCTION ----------

pub fn function_load(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut replace = false;
    let mut i = 2;
    while i < args.len() - 1 {
        if eq(&args[i], "replace") {
            replace = true;
            i += 1;
        } else {
            return Frame::err(format!("ERR Unknown option given: {}", String::from_utf8_lossy(&args[i])));
        }
    }
    if i >= args.len() {
        return Frame::err("ERR Function code is missing");
    }
    let code = String::from_utf8_lossy(&args[i]).into_owned();
    match scripting::load_library(ctx.s, &code, replace) {
        Ok(name) => {
            ctx.dirty(1);
            Frame::bulk(Bytes::from(name))
        }
        Err(e) => Frame::err(format!("ERR {}", e)),
    }
}

pub fn function_delete(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let name = String::from_utf8_lossy(&args[2]).into_owned();
    if scripting::delete_library(ctx.s, &name) {
        ctx.dirty(1);
        Frame::ok()
    } else {
        Frame::err("ERR Library not found")
    }
}

pub fn function_flush(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    if args.len() > 3 || (args.len() == 3 && !eq(&args[2], "sync") && !eq(&args[2], "async")) {
        return Frame::err("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
    }
    scripting::flush_functions(ctx.s);
    ctx.dirty(1);
    Frame::ok()
}

pub fn function_kill(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::err("NOTBUSY No scripts in execution right now.")
}

pub fn function_list(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    let mut with_code = false;
    let mut pattern: Option<Bytes> = None;
    let mut i = 2;
    while i < args.len() {
        if eq(&args[i], "withcode") {
            with_code = true;
        } else if eq(&args[i], "libraryname") {
            if i + 1 >= args.len() {
                return Frame::err("ERR library name argument was not given");
            }
            if pattern.is_some() {
                return Frame::err("ERR library name can be given only once");
            }
            pattern = Some(args[i + 1].clone());
            i += 1;
        } else {
            return Frame::err(format!("ERR Unknown argument {}", String::from_utf8_lossy(&args[i])));
        }
        i += 1;
    }
    let mut out = Vec::new();
    for lib in ctx.s.scripting.libraries.values() {
        if let Some(p) = &pattern {
            if !crate::util::glob_match(p, lib.name.as_bytes(), false) {
                continue;
            }
        }
        let funcs = lib
            .functions
            .values()
            .map(|f| {
                Frame::Map(vec![
                    (Frame::bulk_str("name"), Frame::bulk(Bytes::from(f.name.clone()))),
                    (Frame::bulk_str("description"), f.desc.as_ref().map(|d| Frame::bulk(Bytes::from(d.clone()))).unwrap_or(Frame::Null)),
                    (Frame::bulk_str("flags"), Frame::Set(FLAG_NAMES.iter().filter(|(_, v)| f.flags & v != 0).map(|(n, _)| Frame::Simple(n.to_string())).collect())),
                ])
            })
            .collect();
        let mut m = vec![
            (Frame::bulk_str("library_name"), Frame::bulk(Bytes::from(lib.name.clone()))),
            (Frame::bulk_str("engine"), Frame::bulk_str("LUA")),
            (Frame::bulk_str("functions"), Frame::Array(funcs)),
        ];
        if with_code {
            m.push((Frame::bulk_str("library_code"), Frame::bulk(Bytes::from(lib.code.clone()))));
        }
        out.push(Frame::Map(m));
    }
    Frame::Array(out)
}

pub fn function_stats(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    Frame::Map(vec![
        (Frame::bulk_str("running_script"), scripting::running_script()),
        (
            Frame::bulk_str("engines"),
            Frame::Map(vec![(
                Frame::bulk_str("LUA"),
                Frame::Map(vec![
                    (Frame::bulk_str("libraries_count"), Frame::Integer(ctx.s.scripting.library_count() as i64)),
                    (Frame::bulk_str("functions_count"), Frame::Integer(ctx.s.scripting.function_count() as i64)),
                ]),
            )]),
        ),
    ])
}

pub fn function_dump(ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    let mut out = Vec::new();
    for code in ctx.s.scripting.library_codes() {
        out.push(245u8); // RDB_OPCODE_FUNCTION2
        let mut w = crate::rdb::W::new(false);
        w.raw_str(code.as_bytes());
        out.extend_from_slice(&w.buf);
    }
    out.extend_from_slice(&crate::rdb::RDB_VERSION.to_le_bytes());
    let crc = crate::rdb::crc64(0, &out);
    out.extend_from_slice(&crc.to_le_bytes());
    Frame::bulk(Bytes::from(out))
}

pub fn function_restore(ctx: &mut Ctx, args: &[Bytes]) -> Frame {
    #[derive(PartialEq)]
    enum Policy {
        Flush,
        Append,
        Replace,
    }
    let mut policy = Policy::Append;
    if args.len() == 4 {
        policy = if eq(&args[3], "flush") {
            Policy::Flush
        } else if eq(&args[3], "append") {
            Policy::Append
        } else if eq(&args[3], "replace") {
            Policy::Replace
        } else {
            return Frame::err("ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.");
        };
    } else if args.len() > 4 {
        return err_syntax();
    }
    let p = &args[2];
    if p.len() < 10 {
        return Frame::err("ERR DUMP payload version or checksum are wrong");
    }
    let n = p.len();
    let ver = u16::from_le_bytes([p[n - 10], p[n - 9]]);
    let crc = u64::from_le_bytes(p[n - 8..].try_into().unwrap());
    if ver > crate::rdb::RDB_VERSION || (crc != 0 && crate::rdb::crc64(0, &p[..n - 8]) != crc) {
        return Frame::err("ERR DUMP payload version or checksum are wrong");
    }
    let mut r = crate::rdb::R::new(&p[..n - 10]);
    let mut codes = Vec::new();
    while r.p < r.b.len() {
        let t = r.b[r.p];
        r.p += 1;
        if t == 246 {
            return Frame::err("ERR Pre-GA function format not supported");
        }
        if t != 245 {
            return Frame::err("ERR given type is not a function");
        }
        match r.string() {
            Ok(c) => codes.push(String::from_utf8_lossy(&c).into_owned()),
            Err(_) => return Frame::err("ERR can not read data type"),
        }
    }
    // Stage on a copy so a failure leaves the current libraries untouched.
    let backup: Vec<String> = ctx.s.scripting.library_codes();
    if policy == Policy::Flush {
        scripting::flush_functions(ctx.s);
    }
    for code in &codes {
        if let Err(e) = scripting::load_library(ctx.s, code, policy == Policy::Replace) {
            scripting::flush_functions(ctx.s);
            for b in &backup {
                let _ = scripting::load_library(ctx.s, b, true);
            }
            return Frame::err(format!("ERR {}", e));
        }
    }
    ctx.dirty(1);
    Frame::ok()
}

pub fn function_help(_ctx: &mut Ctx, _args: &[Bytes]) -> Frame {
    help_with_header(
        "FUNCTION",
        &[
            "LOAD [REPLACE] <FUNCTION CODE>",
            "    Create a new library with the given library name and code.",
            "DELETE <LIBRARY NAME>",
            "    Delete the given library.",
            "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
            "    Return general information on all the libraries:",
            "    * Library name",
            "    * The engine used to run the Library",
            "    * Library description",
            "    * Functions list",
            "    * Library code (if WITHCODE is given)",
            "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
            "STATS",
            "    Return information about the current function running:",
            "    * Function name",
            "    * Command used to run the function",
            "    * Duration in MS that the function is running",
            "    If no function is running, return nil",
            "    In addition, returns a list of available engines.",
            "KILL",
            "    Kill the current running function.",
            "FLUSH [ASYNC|SYNC]",
            "    Delete all the libraries.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the libraries.",
            "    * SYNC: Synchronously flush the libraries.",
            "DUMP",
            "    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
            "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
            "    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
            "    control how to handle existing libraries (default APPEND):",
            "    * FLUSH: delete all existing libraries.",
            "    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
            "    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
            "      libraries with the new libraries (notice that even on this option there is a chance of failure",
            "      in case of functions name collision with another library).",
        ],
    )
}
