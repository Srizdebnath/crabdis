//! Lua scripting: EVAL/EVALSHA (with script cache and shebang flags) and the
//! FUNCTION engine (libraries, FCALL), following Redis 7.4 semantics. Two Lua 5.1
//! VMs are used, like Redis: one for EVAL scripts and one for functions.

use crate::exec::{self, CTX_LUA};
use crate::resp::Frame;
use crate::server::{Store, cflags};
use bytes::Bytes;
use mlua::{Function, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table, Value, VmState};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

pub const SF_NO_WRITES: u32 = 1 << 0;
pub const SF_ALLOW_OOM: u32 = 1 << 1;
pub const SF_ALLOW_STALE: u32 = 1 << 2;
pub const SF_NO_CLUSTER: u32 = 1 << 3;
pub const SF_ALLOW_CROSS_SLOT: u32 = 1 << 4;
pub const SF_EVAL_COMPAT: u32 = 1 << 5;

pub const FLAG_NAMES: &[(&str, u32)] = &[
    ("no-writes", SF_NO_WRITES),
    ("allow-oom", SF_ALLOW_OOM),
    ("allow-stale", SF_ALLOW_STALE),
    ("no-cluster", SF_NO_CLUSTER),
    ("allow-cross-slot-keys", SF_ALLOW_CROSS_SLOT),
];

// ---------- busy-script state, readable without the store lock ----------

pub static RUNNING: AtomicBool = AtomicBool::new(false);
pub static BUSY: AtomicBool = AtomicBool::new(false);
pub static KILL: AtomicBool = AtomicBool::new(false);
pub static WRITE_DIRTY: AtomicBool = AtomicBool::new(false);
pub static IS_EVAL: AtomicBool = AtomicBool::new(false);
pub static FROM_MASTER: AtomicBool = AtomicBool::new(false);
pub static RUN_START: AtomicI64 = AtomicI64::new(0);
pub static BUSY_THRESHOLD: AtomicI64 = AtomicI64::new(5000);
pub static RUNNING_INFO: Mutex<Option<(String, Vec<Bytes>)>> = Mutex::new(None);

const SLOW_EVAL: &str = "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";
const SLOW_FUNC: &str = "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.";

fn kill_reply(is_eval_cmd: bool) -> Frame {
    if !RUNNING.load(Ordering::SeqCst) {
        return Frame::err("NOTBUSY No scripts in execution right now.");
    }
    if FROM_MASTER.load(Ordering::SeqCst) {
        return Frame::err("UNKILLABLE The busy script was sent by a master instance in the context of replication and cannot be killed.");
    }
    if WRITE_DIRTY.load(Ordering::SeqCst) {
        return Frame::err("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.");
    }
    let running_eval = IS_EVAL.load(Ordering::SeqCst);
    if is_eval_cmd && !running_eval {
        return Frame::err(SLOW_FUNC);
    }
    if !is_eval_cmd && running_eval {
        return Frame::err(SLOW_EVAL);
    }
    KILL.store(true, Ordering::SeqCst);
    Frame::ok()
}

/// Called by connections before taking the store lock. When a script has been
/// running past busy-reply-threshold, answer without the lock.
pub fn busy_reply(args: &[Bytes]) -> Option<Frame> {
    if !BUSY.load(Ordering::SeqCst) {
        return None;
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let sub = args.get(1).map(|a| String::from_utf8_lossy(a).to_ascii_lowercase()).unwrap_or_default();
    match (name.as_str(), sub.as_str()) {
        ("script", "kill") => Some(kill_reply(true)),
        ("function", "kill") => Some(kill_reply(false)),
        ("function", "stats") => Some(stats_running_only()),
        ("shutdown", _) if args.iter().skip(1).any(|a| crate::util::eq_ic(a, "nosave")) => {
            eprintln!("User requested shutdown while a script was busy (SHUTDOWN NOSAVE)");
            std::process::exit(0);
        }
        _ => Some(Frame::err(if IS_EVAL.load(Ordering::SeqCst) { SLOW_EVAL } else { SLOW_FUNC })),
    }
}

fn running_script_frame() -> Frame {
    let info = RUNNING_INFO.lock();
    match info.as_ref() {
        Some((name, cmd)) if RUNNING.load(Ordering::SeqCst) => Frame::Map(vec![
            (Frame::bulk_str("name"), Frame::bulk(Bytes::from(name.clone()))),
            (Frame::bulk_str("command"), Frame::Array(cmd.iter().map(|a| Frame::Bulk(a.clone())).collect())),
            (Frame::bulk_str("duration_ms"), Frame::Integer(crate::util::now_ms() - RUN_START.load(Ordering::SeqCst))),
        ]),
        _ => Frame::Null,
    }
}

fn stats_running_only() -> Frame {
    Frame::Map(vec![(Frame::bulk_str("running_script"), running_script_frame()), (Frame::bulk_str("engines"), Frame::Map(vec![]))])
}

// ---------- data ----------

pub struct ScriptEntry {
    pub body: Bytes,
    pub flags: u32,
}

#[derive(Clone)]
pub struct FuncInfo {
    pub name: String,
    pub desc: Option<String>,
    pub flags: u32,
}

#[derive(Clone)]
pub struct Library {
    pub name: String,
    pub code: String,
    pub functions: BTreeMap<String, FuncInfo>,
}

pub struct Scripting {
    pub eval_lua: Lua,
    pub func_lua: Lua,
    pub scripts: HashMap<String, ScriptEntry>,
    pub libraries: BTreeMap<String, Library>,
    /// function name -> library name
    pub functions: HashMap<String, String>,
}

impl Default for Scripting {
    fn default() -> Self {
        Self::new()
    }
}

/// Lua prelude shared by both VMs. Compiled with chunk name "=[crabdis]" so the
/// error handler can skip internal frames when locating the user's line.
const PRELUDE: &str = r#"
local rawget, rawset, setmetatable, error, tostring, type, select, unpack = rawget, rawset, setmetatable, error, tostring, type, select, unpack
local function readonly(t, what)
  local p = {}
  setmetatable(p, {
    __index = function(_, k)
      local v = t[k]
      if v == nil then error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'", 2) end
      return v
    end,
    __newindex = function() error("Attempt to modify a readonly table", 2) end,
    __metatable = false,
  })
  return p
end
__crabdis_readonly = readonly
-- Turn (ok, a, b) results of raw Rust functions into Redis style errors.
function __crabdis_wrap(f)
  return function(...)
    local ok, a, b = f(...)
    if ok == false then error({err = a}) end
    return a, b
  end
end
local dbg = debug
local killed = __crabdis_killed
function __redis__err__handler(err)
  local lvl = 2
  local i = dbg.getinfo(lvl, 'nSl')
  while i and (i.what == 'C' or i.source == '=[crabdis]') do
    lvl = lvl + 1
    i = dbg.getinfo(lvl, 'nSl')
  end
  if killed() then
    err = {err = 'ERR Script killed by user with SCRIPT KILL...'}
  elseif type(err) ~= 'table' then
    err = {err = 'ERR ' .. tostring(err)}
  end
  if i then
    err['source'] = i.source
    err['line'] = i.currentline
  end
  return err
end
function __crabdis_run(f, ...)
  return xpcall(f, __redis__err__handler)
end
"#;

fn new_vm() -> Lua {
    let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::DEBUG | StdLib::OS;
    // SAFETY: the debug library is needed for the error handler (as in Redis);
    // user code cannot reach it once globals are replaced by readonly proxies.
    let lua = unsafe { Lua::unsafe_new_with(libs, LuaOptions::new()) };
    {
        let g = lua.globals();
        for name in ["loadfile", "dofile", "print"] {
            let _ = g.raw_set(name, Value::Nil);
        }
        // Redis 7.4 only exposes os.clock.
        if let Ok(os) = g.get::<Table>("os") {
            let t = lua.create_table().unwrap();
            let _ = t.set("clock", os.get::<Value>("clock").unwrap_or(Value::Nil));
            let _ = g.raw_set("os", t);
        }
        let killed = lua.create_function(|_, ()| Ok(KILL.load(Ordering::SeqCst))).unwrap();
        let _ = g.raw_set("__crabdis_killed", killed);
        crate::luaext::register(&lua).expect("lua extensions");
        install_rand(&lua);
    }
    lua.load(PRELUDE).set_name("=[crabdis]").exec().expect("lua prelude");
    lua
}

// Redis' deterministic PRNG (rand48) for math.random.
static RAND48: Mutex<u64> = Mutex::new(0x1234abcd330e);

fn rand48_seed(seed: u32) {
    *RAND48.lock() = ((seed as u64) << 16) | 0x330e;
}

fn rand48() -> i32 {
    let mut x = RAND48.lock();
    *x = (x.wrapping_mul(0x5deece66d).wrapping_add(0xb)) & ((1u64 << 48) - 1);
    (*x >> 17) as i32
}

fn install_rand(lua: &Lua) {
    let g = lua.globals();
    let math: Table = g.get("math").unwrap();
    let random = lua
        .create_function(|_, args: MultiValue| {
            let r = (rand48() % 2147483647) as f64 / 2147483647.0;
            let nums: Vec<f64> = args.iter().filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))).collect();
            match nums.len() {
                0 => Ok(Value::Number(r)),
                1 => {
                    let u = nums[0];
                    if u < 1.0 {
                        return Err(mlua::Error::RuntimeError("bad argument #1 to 'random' (interval is empty)".into()));
                    }
                    Ok(Value::Number((r * u).floor() + 1.0))
                }
                _ => {
                    let (l, u) = (nums[0], nums[1]);
                    if l > u {
                        return Err(mlua::Error::RuntimeError("bad argument #2 to 'random' (interval is empty)".into()));
                    }
                    Ok(Value::Number((r * (u - l + 1.0)).floor() + l))
                }
            }
        })
        .unwrap();
    let seed = lua
        .create_function(|_, s: f64| {
            rand48_seed(s as i64 as u32);
            Ok(())
        })
        .unwrap();
    let _ = math.set("random", random);
    let _ = math.set("randomseed", seed);
}

pub fn sha1hex(b: &[u8]) -> String {
    let mut h = sha1_smol::Sha1::new();
    h.update(b);
    h.digest().to_string()
}

fn parse_shebang(body: &[u8]) -> Result<(u32, usize), String> {
    if !body.starts_with(b"#!") {
        return Ok((SF_EVAL_COMPAT, 0));
    }
    let Some(end) = body.iter().position(|&c| c == b'\n') else { return Err("Invalid script shebang".into()) };
    let parts = crate::resp::split_args(&body[..end]).ok_or("Invalid engine in script shebang")?;
    if parts.is_empty() {
        return Err("Invalid engine in script shebang".into());
    }
    if parts[0] != b"#!lua" {
        return Err(format!("Unexpected engine in script shebang: {}", String::from_utf8_lossy(&parts[0])));
    }
    let mut flags = 0;
    for p in &parts[1..] {
        if let Some(fl) = p.strip_prefix(b"flags=") {
            for f in fl.split(|&c| c == b',') {
                if f.is_empty() {
                    continue;
                }
                match FLAG_NAMES.iter().find(|(n, _)| n.as_bytes() == f) {
                    Some((_, v)) => flags |= v,
                    None => return Err(format!("Unexpected flag in script shebang: {}", String::from_utf8_lossy(f))),
                }
            }
        } else {
            return Err(format!("Unknown lua shebang option: {}", String::from_utf8_lossy(p)));
        }
    }
    Ok((flags, end))
}

impl Scripting {
    pub fn new() -> Self {
        let s = Scripting { eval_lua: new_vm(), func_lua: new_vm(), scripts: HashMap::new(), libraries: BTreeMap::new(), functions: HashMap::new() };
        init_eval_vm(&s.eval_lua);
        init_func_vm(&s.func_lua);
        s
    }

    pub fn memory(&self) -> usize {
        self.eval_lua.used_memory()
    }
    pub fn functions_memory(&self) -> usize {
        self.func_lua.used_memory()
    }
    pub fn script_count(&self) -> usize {
        self.scripts.len()
    }
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }
    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }
    pub fn library_codes(&self) -> Vec<String> {
        self.libraries.values().map(|l| l.code.clone()).collect()
    }

    /// SCRIPT FLUSH: drop the cache and recreate the eval VM.
    pub fn flush_scripts(&mut self) {
        self.scripts.clear();
        self.eval_lua = new_vm();
        init_eval_vm(&self.eval_lua);
    }

    /// Compile and cache an EVAL script. Returns its sha.
    pub fn create_script(&mut self, body: &Bytes) -> Result<String, String> {
        let sha = sha1hex(body);
        if self.scripts.contains_key(&sha) {
            return Ok(sha);
        }
        let (flags, shebang_len) = parse_shebang(body)?;
        let src = &body[shebang_len..];
        let f = self
            .eval_lua
            .load(src)
            .set_name("@user_script")
            .into_function()
            .map_err(|e| format!("Error compiling script (new function): {}", lua_err_msg(&e)))?;
        let env: Table = self.eval_lua.named_registry_value("crabdis_env").unwrap();
        let _ = f.set_environment(env);
        let reg: Table = self.eval_lua.named_registry_value("crabdis_scripts").unwrap();
        reg.raw_set(sha.clone(), f).map_err(|e| e.to_string())?;
        self.scripts.insert(sha.clone(), ScriptEntry { body: body.clone(), flags });
        Ok(sha)
    }
}

fn lua_err_msg(e: &mlua::Error) -> String {
    match e {
        mlua::Error::SyntaxError { message, .. } => message.clone(),
        mlua::Error::RuntimeError(m) => m.clone(),
        mlua::Error::CallbackError { cause, .. } => lua_err_msg(cause),
        other => other.to_string(),
    }
}

/// The eval VM's runtime environment: a readonly proxy over the globals with
/// the full `redis` API.
fn init_eval_vm(lua: &Lua) {
    let g = lua.globals();
    let api = build_runtime_api(lua);
    g.raw_set("redis", api).unwrap();
    let readonly: Function = g.get("__crabdis_readonly").unwrap();
    let env: Table = readonly.call(g.clone()).unwrap();
    g.raw_set("_G", env.clone()).unwrap();
    lua.set_named_registry_value("crabdis_env", env).unwrap();
    lua.set_named_registry_value("crabdis_scripts", lua.create_table().unwrap()).unwrap();
}

/// The functions VM: runtime env (full API) for callbacks, and a load env that
/// exposes only the library registration API.
fn init_func_vm(lua: &Lua) {
    let g = lua.globals();
    let api = build_runtime_api(lua);
    g.raw_set("redis", api).unwrap();
    let readonly: Function = g.get("__crabdis_readonly").unwrap();
    let env: Table = readonly.call(g.clone()).unwrap();
    g.raw_set("_G", env.clone()).unwrap();
    lua.set_named_registry_value("crabdis_env", env).unwrap();
    lua.set_named_registry_value("crabdis_funcs", lua.create_table().unwrap()).unwrap();

    // Load-time API.
    let load_api = lua.create_table().unwrap();
    let wrap: Function = g.get("__crabdis_wrap").unwrap();
    let reg = lua
        .create_function(|lua, args: MultiValue| -> mlua::Result<(bool, Value)> {
            match register_function(lua, args) {
                Ok(()) => Ok((true, Value::Nil)),
                Err(e) => Ok((false, Value::String(lua.create_string(format!("ERR {}", e))?))),
            }
        })
        .unwrap();
    load_api.set("register_function", wrap.call::<Function>(reg).unwrap()).unwrap();
    add_log_api(lua, &load_api);
    add_constants(lua, &load_api);
    let load_g = lua.create_table().unwrap();
    load_g.set("redis", readonly.call::<Table>(load_api).unwrap()).unwrap();
    let load_env: Table = readonly.call(load_g).unwrap();
    lua.set_named_registry_value("crabdis_load_env", load_env).unwrap();
    lua.set_named_registry_value("crabdis_loading", false).unwrap();
}

fn register_function(lua: &Lua, args: MultiValue) -> Result<(), String> {
    if !lua.named_registry_value::<bool>("crabdis_loading").unwrap_or(false) {
        return Err("redis.register_function can only be called on FUNCTION LOAD command".into());
    }
    let args: Vec<Value> = args.into_iter().collect();
    let (name, cb, desc, flags) = match args.len() {
        1 => {
            let Value::Table(t) = &args[0] else { return Err("calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).".into()) };
            let (mut name, mut cb, mut desc, mut flags) = (None, None, None, 0u32);
            for pair in t.pairs::<Value, Value>() {
                let (k, v) = pair.map_err(|e| e.to_string())?;
                let Value::String(k) = k else { return Err("named argument key given to redis.register_function is not a string".into()) };
                let k = k.to_string_lossy().to_ascii_lowercase();
                match k.as_str() {
                    "function_name" => match v {
                        Value::String(s) => name = Some(s.to_string_lossy().to_string()),
                        _ => return Err("function_name argument given to redis.register_function must be a string".into()),
                    },
                    "description" => match v {
                        Value::String(s) => desc = Some(s.to_string_lossy().to_string()),
                        _ => return Err("description argument given to redis.register_function must be a string".into()),
                    },
                    "callback" => match v {
                        Value::Function(f) => cb = Some(f),
                        _ => return Err("callback argument given to redis.register_function must be a function".into()),
                    },
                    "flags" => {
                        let Value::Table(ft) = v else { return Err("flags argument to redis.register_function must be a table representing function flags".into()) };
                        for f in ft.sequence_values::<Value>() {
                            let Ok(Value::String(f)) = f else { return Err("unknown flag given".into()) };
                            let f = f.to_string_lossy();
                            match FLAG_NAMES.iter().find(|(n, _)| *n == f.as_str()) {
                                Some((_, v)) => flags |= v,
                                None => return Err("unknown flag given".into()),
                            }
                        }
                    }
                    _ => return Err("unknown argument given to redis.register_function".into()),
                }
            }
            let Some(name) = name else { return Err("redis.register_function must get a function name argument".into()) };
            let Some(cb) = cb else { return Err("redis.register_function must get a callback argument".into()) };
            (name, cb, desc, flags)
        }
        2 => {
            let Value::String(n) = &args[0] else { return Err("first argument to redis.register_function must be a string".into()) };
            let Value::Function(f) = &args[1] else { return Err("second argument to redis.register_function must be a function".into()) };
            (n.to_string_lossy().to_string(), f.clone(), None, 0)
        }
        _ => return Err("wrong number of arguments to redis.register_function".into()),
    };
    if name.is_empty() || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err("Function names can only contain letters, numbers, or underscores(_) and must be at least one character long".into());
    }
    let pending: Table = lua.named_registry_value("crabdis_pending").map_err(|e| e.to_string())?;
    if pending.contains_key(name.as_str()).unwrap_or(false) {
        return Err("Function already exists in the library".into());
    }
    let env: Table = lua.named_registry_value("crabdis_env").map_err(|e| e.to_string())?;
    let _ = cb.set_environment(env);
    let entry = lua.create_table().map_err(|e| e.to_string())?;
    entry.set("callback", cb).map_err(|e| e.to_string())?;
    entry.set("flags", flags).map_err(|e| e.to_string())?;
    if let Some(d) = desc {
        entry.set("description", d).map_err(|e| e.to_string())?;
    }
    pending.set(name, entry).map_err(|e| e.to_string())?;
    Ok(())
}

fn add_constants(lua: &Lua, t: &Table) {
    let _ = t.set("REDIS_VERSION", crate::server::VERSION);
    let parts: Vec<i64> = crate::server::VERSION.split('.').map(|p| p.parse().unwrap_or(0)).collect();
    let num = (parts.first().copied().unwrap_or(0) << 16) | (parts.get(1).copied().unwrap_or(0) << 8) | parts.get(2).copied().unwrap_or(0);
    let _ = t.set("REDIS_VERSION_NUM", num);
    let _ = lua;
}

fn add_log_api(lua: &Lua, t: &Table) {
    let _ = t.set("LOG_DEBUG", 0);
    let _ = t.set("LOG_VERBOSE", 1);
    let _ = t.set("LOG_NOTICE", 2);
    let _ = t.set("LOG_WARNING", 3);
    let log = lua
        .create_function(|_, args: MultiValue| -> mlua::Result<()> {
            let args: Vec<Value> = args.into_iter().collect();
            if args.len() < 2 {
                return Err(mlua::Error::RuntimeError("redis.log() requires two arguments or more.".into()));
            }
            let level = args[0].as_i64().or_else(|| args[0].as_f64().map(|f| f as i64)).ok_or_else(|| mlua::Error::RuntimeError("First argument must be a number (log level).".into()))?;
            if !(0..=3).contains(&level) {
                return Err(mlua::Error::RuntimeError("Invalid debug level.".into()));
            }
            let msg: Vec<String> = args[1..].iter().map(|v| v.to_string().unwrap_or_default()).collect();
            let ch = ['.', '-', '*', '#'][level as usize];
            eprintln!("{}:M {} {} {}", std::process::id(), crate::util::log_time(), ch, msg.join(" "));
            Ok(())
        })
        .unwrap();
    let _ = t.set("log", log);
}

/// Functions that need the running script context are thin Lua wrappers
/// around raw `__crabdis_*` functions installed per run inside a scope.
const RUNTIME_API: &str = r#"
local api = ...
local wrap = __crabdis_wrap
local G = __crabdis_ctx
api.call = function(...) local ok, r = G.call(...) if not ok then error(r) end return r end
api.pcall = function(...) local ok, r = G.call(...) return r end
api.setresp = wrap(function(...) return G.setresp(...) end)
api.set_repl = wrap(function(...) return G.set_repl(...) end)
api.acl_check_cmd = wrap(function(...) return G.acl_check_cmd(...) end)
api.replicate_commands = function() return true end
api.breakpoint = function() return false end
api.debug = function() end
api.error_reply = function(e)
  if type(e) ~= 'string' then error({err='ERR wrong number or type of arguments'}) end
  if string.sub(e, 1, 1) == '-' then e = string.sub(e, 2) end
  if not string.find(e, ' ') then e = e end
  return {err = e}
end
api.status_reply = function(s)
  if type(s) ~= 'string' then error({err='ERR wrong number or type of arguments'}) end
  return {ok = s}
end
api.sha1hex = function(s)
  if s == nil or select('#', s) ~= 1 then error({err='ERR wrong number of arguments'}) end
  return G.sha1hex(s)
end
api.REPL_NONE = 0
api.REPL_AOF = 1
api.REPL_SLAVE = 2
api.REPL_REPLICA = 2
api.REPL_ALL = 3
"#;

fn build_runtime_api(lua: &Lua) -> Table {
    let api = lua.create_table().unwrap();
    // __crabdis_ctx holds the per-run raw functions (replaced on every run).
    let ctx = lua.create_table().unwrap();
    let not_running = lua.create_function(|_, _: MultiValue| -> mlua::Result<Value> { Err(mlua::Error::RuntimeError("redis API used outside a script".into())) }).unwrap();
    for k in ["call", "setresp", "set_repl", "acl_check_cmd"] {
        ctx.set(k, not_running.clone()).unwrap();
    }
    let sha = lua
        .create_function(|lua, v: Value| -> mlua::Result<Value> {
            let b = match &v {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::Integer(i) => i.to_string().into_bytes(),
                Value::Number(n) => crate::util::fmt_lua_number(*n).into_bytes(),
                _ => return Err(mlua::Error::RuntimeError("wrong number or type of arguments".into())),
            };
            Ok(Value::String(lua.create_string(sha1hex(&b))?))
        })
        .unwrap();
    ctx.set("sha1hex", sha).unwrap();
    lua.globals().raw_set("__crabdis_ctx", ctx).unwrap();
    lua.load(RUNTIME_API).set_name("=[crabdis]").call::<()>(api.clone()).unwrap();
    add_log_api(lua, &api);
    add_constants(lua, &api);
    let readonly: Function = lua.globals().get("__crabdis_readonly").unwrap();
    readonly.call(api).unwrap()
}

// ---------- reply conversion ----------

fn err_table(lua: &Lua, msg: &str) -> mlua::Result<Value> {
    let t = lua.create_table()?;
    t.set("err", msg)?;
    Ok(Value::Table(t))
}

pub fn frame_to_lua(lua: &Lua, f: Frame, resp: u8) -> mlua::Result<Value> {
    Ok(match f {
        Frame::Simple(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            Value::Table(t)
        }
        Frame::Error(e) => err_table(lua, &e)?,
        Frame::Integer(i) => Value::Number(i as f64),
        Frame::Bulk(b) => Value::String(lua.create_string(&b)?),
        Frame::Null | Frame::NullArray => {
            if resp == 2 {
                Value::Boolean(false)
            } else {
                Value::Nil
            }
        }
        Frame::Array(v) | Frame::Push(v) => {
            let t = lua.create_table()?;
            for (i, e) in v.into_iter().enumerate() {
                t.raw_set(i + 1, frame_to_lua(lua, e, resp)?)?;
            }
            Value::Table(t)
        }
        Frame::Set(v) => {
            let t = lua.create_table()?;
            if resp == 2 {
                for (i, e) in v.into_iter().enumerate() {
                    t.raw_set(i + 1, frame_to_lua(lua, e, resp)?)?;
                }
            } else {
                let inner = lua.create_table()?;
                for e in v {
                    inner.raw_set(frame_to_lua(lua, e, resp)?, true)?;
                }
                t.set("set", inner)?;
            }
            Value::Table(t)
        }
        Frame::Map(v) => {
            let t = lua.create_table()?;
            if resp == 2 {
                let mut i = 1;
                for (k, val) in v {
                    t.raw_set(i, frame_to_lua(lua, k, resp)?)?;
                    t.raw_set(i + 1, frame_to_lua(lua, val, resp)?)?;
                    i += 2;
                }
            } else {
                let inner = lua.create_table()?;
                for (k, val) in v {
                    inner.raw_set(frame_to_lua(lua, k, resp)?, frame_to_lua(lua, val, resp)?)?;
                }
                t.set("map", inner)?;
            }
            Value::Table(t)
        }
        Frame::Double(d) => {
            if resp == 2 {
                Value::String(lua.create_string(crate::util::fmt_double_resp(d))?)
            } else {
                let t = lua.create_table()?;
                t.set("double", d)?;
                Value::Table(t)
            }
        }
        Frame::Bool(b) => {
            if resp == 2 {
                if b { Value::Number(1.0) } else { Value::Boolean(false) }
            } else {
                Value::Boolean(b)
            }
        }
        Frame::Verbatim(fmt, b) => {
            if resp == 2 {
                Value::String(lua.create_string(&b)?)
            } else {
                let t = lua.create_table()?;
                let inner = lua.create_table()?;
                inner.set("format", fmt)?;
                inner.set("string", lua.create_string(&b)?)?;
                t.set("verbatim_string", inner)?;
                Value::Table(t)
            }
        }
        Frame::BigNumber(n) => {
            if resp == 2 {
                Value::String(lua.create_string(&n)?)
            } else {
                let t = lua.create_table()?;
                t.set("big_number", n)?;
                Value::Table(t)
            }
        }
        Frame::Seq(mut v) => {
            if v.is_empty() {
                Value::Nil
            } else {
                frame_to_lua(lua, v.remove(0), resp)?
            }
        }
        Frame::Raw(_) | Frame::Block(_) | Frame::NoReply => Value::Nil,
    })
}

fn sanitize_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

/// Convert the script's return value to a reply (luaReplyToRedisReply).
pub fn lua_to_frame(v: &Value, resp: u8) -> Frame {
    match v {
        Value::String(s) => Frame::Bulk(Bytes::copy_from_slice(&s.as_bytes())),
        Value::Boolean(b) => {
            if resp == 2 {
                if *b { Frame::Integer(1) } else { Frame::Null }
            } else {
                Frame::Bool(*b)
            }
        }
        Value::Integer(i) => Frame::Integer(*i as i64),
        Value::Number(n) => Frame::Integer(*n as i64),
        Value::Table(t) => {
            if let Ok(Value::String(e)) = t.raw_get::<Value>("err") {
                return Frame::Error(sanitize_line(&e.to_string_lossy()));
            }
            if let Ok(Value::String(s)) = t.raw_get::<Value>("ok") {
                return Frame::Simple(sanitize_line(&s.to_string_lossy()));
            }
            if let Ok(d) = t.raw_get::<Value>("double") {
                if let Some(n) = d.as_f64().or_else(|| d.as_i64().map(|i| i as f64)) {
                    return Frame::Double(n);
                }
            }
            if let Ok(Value::String(s)) = t.raw_get::<Value>("big_number") {
                return Frame::BigNumber(sanitize_line(&s.to_string_lossy()));
            }
            if let Ok(Value::Table(m)) = t.raw_get::<Value>("map") {
                let mut out = Vec::new();
                for (k, val) in m.pairs::<Value, Value>().flatten() {
                    out.push((lua_to_frame(&k, resp), lua_to_frame(&val, resp)));
                }
                return Frame::Map(out);
            }
            if let Ok(Value::Table(m)) = t.raw_get::<Value>("set") {
                let mut out = Vec::new();
                for (k, _) in m.pairs::<Value, Value>().flatten() {
                    out.push(lua_to_frame(&k, resp));
                }
                return Frame::Set(out);
            }
            if let Ok(Value::Table(vt)) = t.raw_get::<Value>("verbatim_string") {
                if let (Ok(Value::String(fmt)), Ok(Value::String(s))) = (vt.raw_get::<Value>("format"), vt.raw_get::<Value>("string")) {
                    let f: &'static str = match fmt.to_string_lossy().as_str() {
                        "mkd" => "mkd",
                        _ => "txt",
                    };
                    return Frame::Verbatim(f, Bytes::copy_from_slice(&s.as_bytes()));
                }
            }
            let mut out = Vec::new();
            let mut i = 1;
            loop {
                match t.raw_get::<Value>(i) {
                    Ok(Value::Nil) | Err(_) => break,
                    Ok(v) => out.push(lua_to_frame(&v, resp)),
                }
                i += 1;
            }
            Frame::Array(out)
        }
        _ => Frame::Null,
    }
}

// ---------- running ----------

struct Run<'a> {
    s: &'a mut Store,
    caller: u64,
    lua_cid: u64,
    resp: u8,
    ro: bool,
    flags: u32,
    repl: u8,
    wrote: bool,
}

fn args_from_lua(args: &MultiValue) -> Result<Vec<Bytes>, &'static str> {
    if args.is_empty() {
        return Err("Please specify at least one argument for this redis lib call");
    }
    let mut out = Vec::with_capacity(args.len());
    for a in args.iter() {
        match a {
            Value::String(s) => out.push(Bytes::copy_from_slice(&s.as_bytes())),
            Value::Integer(i) => out.push(Bytes::from(i.to_string())),
            Value::Number(n) => out.push(Bytes::from(crate::util::fmt_lua_number(*n))),
            _ => return Err("Lua redis lib command arguments must be strings or integers"),
        }
    }
    Ok(out)
}

fn script_call(run: &mut Run, args: Vec<Bytes>) -> Result<Frame, String> {
    let cmd = match exec::resolve(&args) {
        Ok(c) => c,
        Err(Frame::Error(e)) if e.starts_with("ERR wrong number") => return Err(concat!("ERR ", "Wrong number of args calling Redis command from script").into()),
        Err(_) => return Err(concat!("ERR ", "Unknown Redis command called from script").into()),
    };
    use crate::cmd::flags as F;
    if cmd.flags & F::NOSCRIPT != 0 {
        return Err(concat!("ERR ", "This Redis command is not allowed from script").into());
    }
    // ACL check as the caller's user.
    let uname = run.s.clients.get(&run.caller).map(|c| c.user.clone()).unwrap_or_else(|| "default".into());
    let from_master = run.s.clients.get(&run.caller).map(|c| c.has(cflags::MASTER)).unwrap_or(true);
    if !from_master {
        if let Some(u) = run.s.acl.users.get(&uname) {
            if let Err((reason, idx)) = u.check_cmd(cmd, &args) {
                let msg = crate::acl::error_message(reason, &uname, cmd, args.get(idx).map(|b| b.as_ref()).unwrap_or(b""), false);
                let object = match reason {
                    crate::acl::Denied::Cmd | crate::acl::Denied::Auth => cmd.full_name(),
                    _ => String::from_utf8_lossy(&args[idx]).into_owned(),
                };
                let max = run.s.cfg.acllog_max_len;
                let info = run.s.clients.get(&run.caller).map(|c| crate::cmd::client::client_line(c, run.s)).unwrap_or_default();
                run.s.acl.add_log(reason, "lua", object, uname, info, max);
                return Err(format!("ERR ACL failure in script: {}", msg));
            }
        }
    }
    let is_write = cmd.is_write_for(&args);
    if is_write {
        if run.ro || run.flags & SF_NO_WRITES != 0 {
            return Err(concat!("ERR ", "Write commands are not allowed from read-only scripts.").into());
        }
        if run.s.repl.is_replica() && run.s.cfg.replica_read_only && !from_master {
            return Err("READONLY You can't write against a read only replica.".into());
        }
        if !from_master && !crate::repl::good_replicas_ok(run.s) {
            return Err("NOREPLICAS Not enough good replicas to write.".into());
        }
    }
    if cmd.flags & F::DENYOOM != 0 && run.s.cfg.maxmemory > 0 && !from_master {
        let allow = run.flags & SF_ALLOW_OOM != 0 || (run.flags & SF_EVAL_COMPAT != 0 && run.wrote);
        if !allow && crate::evict::over_maxmemory(run.s) {
            return Err("OOM command not allowed when used memory > 'maxmemory'.".into());
        }
    }
    if run.s.cluster.is_some() && !from_master {
        if let Err(e) = crate::cluster::script_check(run.s, run.caller, cmd, &args, run.flags & SF_ALLOW_CROSS_SLOT != 0) {
            return Err(format!("ERR {}", e));
        }
    }
    let db = run.s.clients.get(&run.lua_cid).map(|c| c.db).unwrap_or(0);
    let pending_before = run.s.repl.pending.len();
    let dirty_before = run.s.dirty;
    let prev = run.s.current_client;
    run.s.current_client = run.caller;
    let reply = exec::exec_internal(run.s, run.lua_cid, db, &args, CTX_LUA);
    run.s.current_client = prev;
    if run.s.dirty != dirty_before {
        run.wrote = true;
        WRITE_DIRTY.store(true, Ordering::SeqCst);
    }
    if run.repl == 0 {
        run.s.repl.pending.truncate(pending_before);
    }
    Ok(reply)
}

/// Execute a compiled Lua function (an eval script or a library callback).
#[allow(clippy::too_many_arguments)]
fn run_function(
    s: &mut Store,
    lua: &Lua,
    caller: u64,
    db: usize,
    func: Function,
    keys: &[Bytes],
    argv: &[Bytes],
    eval_mode: bool,
    name: &str,
    flags: u32,
    ro: bool,
    cmdline: &[Bytes],
) -> Frame {
    // Pseudo client that executes the script's commands.
    let lua_cid = crate::aof::pseudo_client(s, cflags::LUA);
    let (caller_flags, caller_proto) = s.clients.get(&caller).map(|c| (c.flags, c.proto)).unwrap_or((cflags::MASTER, 2));
    if let Some(c) = s.clients.get_mut(&lua_cid) {
        c.db = db;
        c.flags |= caller_flags & (cflags::READONLY | cflags::ASKING | cflags::MASTER);
    }
    let from_master = caller_flags & cflags::MASTER != 0;
    let prev_ctx = s.acl.context;
    s.acl.context = "lua";

    RUNNING.store(true, Ordering::SeqCst);
    BUSY.store(false, Ordering::SeqCst);
    KILL.store(false, Ordering::SeqCst);
    WRITE_DIRTY.store(false, Ordering::SeqCst);
    IS_EVAL.store(eval_mode, Ordering::SeqCst);
    FROM_MASTER.store(from_master, Ordering::SeqCst);
    RUN_START.store(crate::util::now_ms(), Ordering::SeqCst);
    BUSY_THRESHOLD.store(s.cfg.busy_reply_threshold as i64, Ordering::SeqCst);
    *RUNNING_INFO.lock() = Some((name.to_string(), cmdline.to_vec()));
    rand48_seed(0);

    let run = RefCell::new(Run { s, caller, lua_cid, resp: 2, ro, flags, repl: 3, wrote: false });
    let threshold = BUSY_THRESHOLD.load(Ordering::SeqCst);
    if threshold > 0 {
        let _ = lua.set_hook(HookTriggers { every_nth_instruction: Some(100_000), ..Default::default() }, |_, _| {
            if KILL.load(Ordering::SeqCst) {
                return Err(mlua::Error::RuntimeError("Script killed by user with SCRIPT KILL...".into()));
            }
            let elapsed = crate::util::now_ms() - RUN_START.load(Ordering::SeqCst);
            if elapsed >= BUSY_THRESHOLD.load(Ordering::SeqCst) && !BUSY.load(Ordering::SeqCst) {
                eprintln!("Slow script detected: still in execution after {} milliseconds. You can try killing the script using the SCRIPT KILL command.", elapsed);
                BUSY.store(true, Ordering::SeqCst);
            }
            Ok(VmState::Continue)
        });
    }

    let result: mlua::Result<Frame> = lua.scope(|scope| {
        let call = scope.create_function(|lua, args: MultiValue| -> mlua::Result<(bool, Value)> {
            let argv = match args_from_lua(&args) {
                Ok(a) => a,
                Err(e) => return Ok((false, err_table(lua, &format!("ERR {}", e))?)),
            };
            let mut r = run.borrow_mut();
            let resp = r.resp;
            match script_call(&mut r, argv) {
                Ok(Frame::Error(e)) => Ok((false, err_table(lua, &e)?)),
                Ok(f) => Ok((true, frame_to_lua(lua, f, resp)?)),
                Err(e) => Ok((false, err_table(lua, &e)?)),
            }
        })?;
        let setresp = scope.create_function(|lua, args: MultiValue| -> mlua::Result<(bool, Value)> {
            let v: Vec<Value> = args.into_iter().collect();
            if v.len() != 1 {
                return Ok((false, Value::String(lua.create_string("ERR redis.setresp() requires one argument.")?)));
            }
            let n = v[0].as_i64().or_else(|| v[0].as_f64().map(|f| f as i64)).unwrap_or(0);
            if n != 2 && n != 3 {
                return Ok((false, Value::String(lua.create_string("ERR RESP version must be 2 or 3.")?)));
            }
            run.borrow_mut().resp = n as u8;
            if let Some(c) = run.borrow_mut().s.clients.get_mut(&lua_cid) {
                c.proto = n as u8;
            }
            Ok((true, Value::Nil))
        })?;
        let set_repl = scope.create_function(|lua, args: MultiValue| -> mlua::Result<(bool, Value)> {
            let v: Vec<Value> = args.into_iter().collect();
            if v.len() != 1 {
                return Ok((false, Value::String(lua.create_string("ERR redis.set_repl() requires one argument.")?)));
            }
            let n = v[0].as_i64().or_else(|| v[0].as_f64().map(|f| f as i64)).unwrap_or(-1);
            if !(0..=3).contains(&n) {
                return Ok((false, Value::String(lua.create_string("ERR Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.")?)));
            }
            run.borrow_mut().repl = n as u8;
            Ok((true, Value::Nil))
        })?;
        let acl_check = scope.create_function(|lua, args: MultiValue| -> mlua::Result<(bool, Value)> {
            let argv = match args_from_lua(&args) {
                Ok(a) => a,
                Err(e) => return Ok((false, Value::String(lua.create_string(format!("ERR {}", e))?))),
            };
            let r = run.borrow();
            let Some(cmd) = crate::cmd::lookup(&argv[0]) else {
                return Ok((false, Value::String(lua.create_string("ERR Invalid command passed to redis.acl_check_cmd()")?)));
            };
            let cmd = if !cmd.subs.is_empty() && argv.len() > 1 { crate::cmd::lookup_sub(cmd, &argv[1]).unwrap_or(cmd) } else { cmd };
            let uname = r.s.clients.get(&r.caller).map(|c| c.user.clone()).unwrap_or_default();
            let ok = r.s.acl.users.get(&uname).map(|u| u.check_cmd(cmd, &argv).is_ok()).unwrap_or(true);
            Ok((true, Value::Boolean(ok)))
        })?;
        let ctx: Table = lua.globals().raw_get("__crabdis_ctx")?;
        ctx.set("call", call)?;
        ctx.set("setresp", setresp)?;
        ctx.set("set_repl", set_repl)?;
        ctx.set("acl_check_cmd", acl_check)?;

        let mk = |v: &[Bytes]| -> mlua::Result<Table> {
            let t = lua.create_table_with_capacity(v.len(), 0)?;
            for (i, b) in v.iter().enumerate() {
                t.raw_set(i + 1, lua.create_string(b)?)?;
            }
            Ok(t)
        };
        let keys_t = mk(keys)?;
        let argv_t = mk(argv)?;
        let runner: Function = lua.globals().raw_get("__crabdis_run")?;
        let (ok, val): (bool, Value) = if eval_mode {
            let g = lua.globals();
            g.raw_set("KEYS", keys_t)?;
            g.raw_set("ARGV", argv_t)?;
            runner.call((func,))?
        } else {
            let bound = lua.load("local f, k, a = ... return function() return f(k, a) end").set_name("=[crabdis]").call::<Function>((func, keys_t, argv_t))?;
            runner.call((bound,))?
        };
        // Reset the context functions before the scope invalidates them.
        let nr = lua.create_function(|_, _: MultiValue| -> mlua::Result<Value> { Err(mlua::Error::RuntimeError("redis API used outside a script".into())) })?;
        for k in ["call", "setresp", "set_repl", "acl_check_cmd"] {
            ctx.set(k, nr.clone())?;
        }
        if ok {
            let resp = run.borrow().resp;
            Ok(lua_to_frame(&val, resp))
        } else {
            let (msg, source, line) = match &val {
                Value::Table(t) => (
                    t.raw_get::<Value>("err").ok().and_then(|v| v.to_string().ok()).unwrap_or_else(|| "ERR unknown error".into()),
                    t.raw_get::<Value>("source").ok().and_then(|v| v.to_string().ok()),
                    t.raw_get::<Value>("line").ok().and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))),
                ),
                other => (format!("ERR {}", other.to_string().unwrap_or_default()), None, None),
            };
            let mut m = sanitize_line(&msg);
            if let (Some(src), Some(l)) = (source, line) {
                m.push_str(&format!(" script: {}, on {}:{}.", name, src, l));
            }
            Ok(Frame::Error(m))
        }
    });
    lua.remove_hook();
    let Run { s, .. } = run.into_inner();
    s.clients.remove(&lua_cid);
    s.acl.context = prev_ctx;
    RUNNING.store(false, Ordering::SeqCst);
    BUSY.store(false, Ordering::SeqCst);
    KILL.store(false, Ordering::SeqCst);
    *RUNNING_INFO.lock() = None;
    lua.gc_collect().ok();
    let _ = caller_proto;
    match result {
        Ok(f) => f,
        Err(e) => Frame::err(format!("ERR Error running script {}, {:.100}", name, lua_err_msg(&e))),
    }
}

/// Checks done before running a script (scriptPrepareForRun).
fn prepare(s: &Store, caller: u64, flags: u32, ro: bool) -> Result<(), Frame> {
    let from_master = s.clients.get(&caller).map(|c| c.has(cflags::MASTER)).unwrap_or(true);
    let stale = s.repl.is_replica() && !s.repl.link_up() && !s.cfg.replica_serve_stale_data;
    if flags & SF_EVAL_COMPAT == 0 {
        if flags & SF_NO_CLUSTER != 0 && s.cluster.is_some() {
            return Err(Frame::err("ERR Can not run script on cluster, 'no-cluster' flag is set."));
        }
        if stale && flags & SF_ALLOW_STALE == 0 {
            return Err(Frame::err("MASTERDOWN Link with MASTER is down, replica-serve-stale-data is set to 'no' and 'allow-stale' flag is not set on the script."));
        }
        if flags & SF_NO_WRITES == 0 {
            if s.repl.is_replica() && s.cfg.replica_read_only && !from_master {
                return Err(Frame::err("READONLY Can not run script with write flag on readonly replica"));
            }
            if !from_master {
                if let Some(e) = crate::server::disk_error(s) {
                    return Err(Frame::err(match e {
                        crate::server::DiskErr::Rdb => "MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Writable scripts are blocked. Use 'no-writes' flag for read only scripts.".to_string(),
                        crate::server::DiskErr::Aof => "MISCONF Redis is configured to persist data to AOF, but it's currently unable to persist to disk. Writable scripts are blocked. Use 'no-writes' flag for read only scripts. AOF error: Input/output error".to_string(),
                    }));
                }
            }
            if ro {
                return Err(Frame::err("ERR Can not execute a script with write flag using *_ro command."));
            }
            if !from_master && !crate::repl::good_replicas_ok(s) {
                return Err(Frame::err("NOREPLICAS Not enough good replicas to write."));
            }
        }
        let allow_oom = from_master;
        if !allow_oom && s.cfg.maxmemory > 0 && flags & (SF_ALLOW_OOM | SF_NO_WRITES) == 0 && crate::evict::over_maxmemory(s) && !from_master {
            return Err(Frame::err("OOM allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'"));
        }
    } else if stale {
        return Err(Frame::err("MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."));
    }
    Ok(())
}

fn parse_numkeys(args: &[Bytes]) -> Result<usize, Frame> {
    let n = crate::util::parse_i64(&args[2]).ok_or_else(|| Frame::err("ERR value is not an integer or out of range"))?;
    if n < 0 {
        return Err(Frame::err("ERR Number of keys can't be negative"));
    }
    if n as usize > args.len() - 3 {
        return Err(Frame::err("ERR Number of keys can't be greater than number of args"));
    }
    Ok(n as usize)
}

/// EVAL / EVALSHA / *_RO entry point.
pub fn eval(s: &mut Store, caller: u64, db: usize, args: &[Bytes], evalsha: bool, ro: bool) -> Frame {
    let nk = match parse_numkeys(args) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let sha = if evalsha {
        let sha = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
        if !s.scripting.scripts.contains_key(&sha) {
            return Frame::err("NOSCRIPT No matching script. Please use EVAL.");
        }
        sha
    } else {
        match s.scripting.create_script(&args[1]) {
            Ok(sha) => sha,
            Err(e) => return Frame::err(format!("ERR {}", e)),
        }
    };
    let flags = s.scripting.scripts[&sha].flags;
    if let Err(e) = prepare(s, caller, flags, ro) {
        return e;
    }
    let lua = s.scripting.eval_lua.clone();
    let reg: Table = match lua.named_registry_value("crabdis_scripts") {
        Ok(t) => t,
        Err(e) => return Frame::err(format!("ERR {}", e)),
    };
    let func: Function = match reg.raw_get(sha.as_str()) {
        Ok(f) => f,
        Err(_) => return Frame::err("NOSCRIPT No matching script. Please use EVAL."),
    };
    let keys = &args[3..3 + nk];
    let argv = &args[3 + nk..];
    run_function(s, &lua, caller, db, func, keys, argv, true, &sha, flags, ro, args)
}

pub fn fcall(s: &mut Store, caller: u64, db: usize, args: &[Bytes], ro: bool) -> Frame {
    let fname = String::from_utf8_lossy(&args[1]).into_owned();
    let Some(libname) = s.scripting.functions.get(&fname).cloned() else {
        return Frame::err("ERR Function not found");
    };
    let n = match crate::util::parse_i64(&args[2]) {
        Some(n) => n,
        None => return Frame::err("ERR Bad number of keys provided"),
    };
    if n < 0 {
        return Frame::err("ERR Number of keys can't be negative");
    }
    if n as usize > args.len() - 3 {
        return Frame::err("ERR Number of keys can't be greater than number of args");
    }
    let nk = n as usize;
    let flags = s.scripting.libraries[&libname].functions[&fname].flags;
    if let Err(e) = prepare(s, caller, flags, ro) {
        return e;
    }
    let lua = s.scripting.func_lua.clone();
    let reg: Table = lua.named_registry_value("crabdis_funcs").unwrap();
    let func: Function = match reg.raw_get(fname.as_str()) {
        Ok(f) => f,
        Err(_) => return Frame::err("ERR Function not found"),
    };
    run_function(s, &lua, caller, db, func, &args[3..3 + nk], &args[3 + nk..], false, &fname, flags, ro, args)
}

// ---------- libraries ----------

fn parse_lib_metadata(code: &str) -> Result<(String, String), String> {
    if !code.starts_with("#!") {
        return Err("Missing library metadata".into());
    }
    let Some(end) = code.find('\n') else { return Err("Invalid library metadata".into()) };
    let parts = crate::resp::split_args(&code.as_bytes()[..end]).ok_or("Invalid library metadata")?;
    if parts.is_empty() {
        return Err("Invalid library metadata".into());
    }
    let engine = String::from_utf8_lossy(&parts[0][2..]).into_owned();
    let mut name = None;
    for p in &parts[1..] {
        let p = String::from_utf8_lossy(p);
        if let Some(n) = p.strip_prefix("name=") {
            if name.is_some() {
                return Err("Invalid metadata value, name argument was given multiple times".into());
            }
            name = Some(n.to_string());
        } else {
            return Err(format!("Invalid metadata value given: {}", p));
        }
    }
    let name = name.ok_or("Library name was not given")?;
    Ok((engine, name))
}

/// FUNCTION LOAD: compile a library and register its functions.
pub fn load_library(s: &mut Store, code: &str, replace: bool) -> Result<String, String> {
    let (engine, name) = parse_lib_metadata(code)?;
    if name.is_empty() || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err("Library names can only contain letters, numbers, or underscores(_) and must be at least one character long".into());
    }
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("Engine '{}' not found", engine));
    }
    if s.scripting.libraries.contains_key(&name) && !replace {
        return Err(format!("Library '{}' already exists", name));
    }
    let lua = s.scripting.func_lua.clone();
    let body = &code[code.find('\n').unwrap_or(code.len())..];
    let chunk = lua
        .load(body)
        .set_name("@user_function")
        .into_function()
        .map_err(|e| format!("Error compiling function: {}", lua_err_msg(&e)))?;
    let load_env: Table = lua.named_registry_value("crabdis_load_env").map_err(|e| e.to_string())?;
    let _ = chunk.set_environment(load_env);
    let pending = lua.create_table().map_err(|e| e.to_string())?;
    lua.set_named_registry_value("crabdis_pending", pending.clone()).map_err(|e| e.to_string())?;
    lua.set_named_registry_value("crabdis_loading", true).map_err(|e| e.to_string())?;
    let runner: Function = lua.globals().raw_get("__crabdis_run").map_err(|e| e.to_string())?;
    // FUNCTION LOAD has a 500ms timeout.
    let start = std::time::Instant::now();
    let _ = lua.set_hook(HookTriggers { every_nth_instruction: Some(100_000), ..Default::default() }, move |_, _| {
        if start.elapsed().as_millis() > 500 {
            return Err(mlua::Error::RuntimeError("FUNCTION LOAD timeout".into()));
        }
        Ok(VmState::Continue)
    });
    let res: mlua::Result<(bool, Value)> = runner.call((chunk,));
    lua.remove_hook();
    lua.set_named_registry_value("crabdis_loading", false).ok();
    let (ok, val) = res.map_err(|e| format!("Error registering functions: {}", lua_err_msg(&e)))?;
    if !ok {
        let msg = match &val {
            Value::Table(t) => t.raw_get::<Value>("err").ok().and_then(|v| v.to_string().ok()).unwrap_or_default(),
            other => other.to_string().unwrap_or_default(),
        };
        return Err(format!("Error registering functions: {}", msg));
    }
    let mut funcs = BTreeMap::new();
    let mut lua_funcs = Vec::new();
    for pair in pending.pairs::<String, Table>() {
        let (fname, entry) = pair.map_err(|e| e.to_string())?;
        let flags: u32 = entry.get("flags").unwrap_or(0);
        let desc: Option<String> = entry.get("description").ok();
        let cb: Function = entry.get("callback").map_err(|e| e.to_string())?;
        funcs.insert(fname.clone(), FuncInfo { name: fname.clone(), desc, flags });
        lua_funcs.push((fname, cb));
    }
    if funcs.is_empty() {
        return Err("No functions registered".into());
    }
    for f in funcs.keys() {
        if let Some(owner) = s.scripting.functions.get(f) {
            if *owner != name {
                return Err(format!("Function {} already exists", f));
            }
        }
    }
    // Commit: remove the old library (if replacing), then add the new one.
    if let Some(old) = s.scripting.libraries.remove(&name) {
        let reg: Table = lua.named_registry_value("crabdis_funcs").map_err(|e| e.to_string())?;
        for f in old.functions.keys() {
            s.scripting.functions.remove(f);
            let _ = reg.raw_set(f.as_str(), Value::Nil);
        }
    }
    let reg: Table = lua.named_registry_value("crabdis_funcs").map_err(|e| e.to_string())?;
    for (fname, cb) in lua_funcs {
        reg.raw_set(fname.as_str(), cb).map_err(|e| e.to_string())?;
        s.scripting.functions.insert(fname, name.clone());
    }
    s.scripting.libraries.insert(name.clone(), Library { name: name.clone(), code: code.to_string(), functions: funcs });
    Ok(name)
}

pub fn delete_library(s: &mut Store, name: &str) -> bool {
    let Some(old) = s.scripting.libraries.remove(name) else { return false };
    let lua = s.scripting.func_lua.clone();
    if let Ok(reg) = lua.named_registry_value::<Table>("crabdis_funcs") {
        for f in old.functions.keys() {
            s.scripting.functions.remove(f);
            let _ = reg.raw_set(f.as_str(), Value::Nil);
        }
    }
    true
}

pub fn flush_functions(s: &mut Store) {
    s.scripting.libraries.clear();
    s.scripting.functions.clear();
    s.scripting.func_lua = new_vm();
    init_func_vm(&s.scripting.func_lua);
}

pub fn running_script() -> Frame {
    running_script_frame()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shebang() {
        assert_eq!(parse_shebang(b"return 1").unwrap(), (SF_EVAL_COMPAT, 0));
        assert_eq!(parse_shebang(b"#!lua flags=no-writes\nreturn 1").unwrap().0, SF_NO_WRITES);
        assert!(parse_shebang(b"#!lua flags=bad\nreturn 1").is_err());
    }
    #[test]
    fn sha() {
        assert_eq!(sha1hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }
}
