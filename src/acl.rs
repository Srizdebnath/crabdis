//! Access Control Lists, modelled on Redis' acl.c: users with flags and SHA-256
//! password hashes, a root selector plus optional extra selectors, each holding a
//! command bitmap (with the ordered textual rule list used by ACL LIST/GETUSER),
//! key patterns with read/write permissions and channel patterns.

use crate::cmd::{self, Cmd, cat};
use crate::resp::Frame;
use crate::server::Store;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::OnceLock;

pub const PERM_R: u8 = 1;
pub const PERM_W: u8 = 2;
pub const PERM_RW: u8 = 3;

// ---------- command ids ----------

struct CmdIds {
    by_name: HashMap<String, usize>,
    list: Vec<&'static Cmd>,
}

fn ids() -> &'static CmdIds {
    static IDS: OnceLock<CmdIds> = OnceLock::new();
    IDS.get_or_init(|| {
        let mut by_name = HashMap::new();
        let mut list = Vec::new();
        for c in cmd::all_commands() {
            by_name.insert(c.full_name(), list.len());
            list.push(c);
            for s in c.subs {
                by_name.insert(s.full_name(), list.len());
                list.push(s);
            }
        }
        CmdIds { by_name, list }
    })
}

fn cmd_id(c: &Cmd) -> usize {
    ids().by_name[&c.full_name()]
}

fn nbits() -> usize {
    ids().list.len() + 1
}

fn future_bit() -> usize {
    ids().list.len()
}

// ---------- selectors ----------

#[derive(Clone, Debug, PartialEq)]
pub struct KeyPattern {
    pub flags: u8,
    pub pattern: String,
}

impl KeyPattern {
    fn describe(&self) -> String {
        match self.flags {
            PERM_RW => format!("~{}", self.pattern),
            PERM_R => format!("%R~{}", self.pattern),
            _ => format!("%W~{}", self.pattern),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Selector {
    pub root: bool,
    pub allkeys: bool,
    pub allchannels: bool,
    pub allcommands: bool,
    pub allowed: Vec<u64>,
    pub firstargs: HashMap<usize, Vec<String>>,
    pub patterns: Vec<KeyPattern>,
    pub channels: Vec<String>,
    pub command_rules: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetErr {
    NoEnt,
    Inval,
    Exist,
    IsDir,
    NoDev,
    BadMsg,
    Already,
    Child,
}

impl SetErr {
    pub fn msg(&self) -> &'static str {
        match self {
            SetErr::NoEnt => "Unknown command or category name in ACL",
            SetErr::Inval => "Syntax error",
            SetErr::Exist => "Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start with an empty list of patterns",
            SetErr::IsDir => "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels",
            SetErr::NoDev => "The password you are trying to remove from the user does not exist",
            SetErr::BadMsg => "The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters",
            SetErr::Already => "Duplicate user found. A user can only be defined once in config files",
            SetErr::Child => "Allowing first-arg of a subcommand is not supported",
        }
    }
}

fn has_spaces(s: &str) -> bool {
    s.bytes().any(|c| c.is_ascii_whitespace() || c == 0)
}

impl Selector {
    pub fn new(root: bool, allchannels: bool) -> Self {
        Selector {
            root,
            allkeys: false,
            allchannels,
            allcommands: false,
            allowed: vec![0; nbits().div_ceil(64)],
            firstargs: HashMap::new(),
            patterns: Vec::new(),
            channels: Vec::new(),
            command_rules: String::new(),
        }
    }

    fn get_bit(&self, id: usize) -> bool {
        self.allowed[id / 64] & (1 << (id % 64)) != 0
    }

    fn set_bit(&mut self, id: usize, v: bool) {
        if v {
            self.allowed[id / 64] |= 1 << (id % 64);
        } else {
            self.allowed[id / 64] &= !(1 << (id % 64));
            self.allcommands = false;
        }
    }

    pub fn can_future(&self) -> bool {
        self.get_bit(future_bit())
    }

    fn change_perm(&mut self, c: &Cmd, allow: bool) {
        let id = cmd_id(c);
        self.set_bit(id, allow);
        self.firstargs.remove(&id);
        for s in c.subs {
            self.set_bit(cmd_id(s), allow);
        }
    }

    fn remove_rule(&mut self, new_rule: &str) {
        let rules: Vec<&str> = self.command_rules.split(' ').filter(|r| !r.is_empty()).collect();
        let kept: Vec<&str> = rules
            .into_iter()
            .filter(|r| {
                let body = &r[1..];
                !(body == new_rule || (body.len() > new_rule.len() && body.starts_with(new_rule) && body.as_bytes()[new_rule.len()] == b'|'))
            })
            .collect();
        self.command_rules = kept.join(" ");
    }

    fn update_rules(&mut self, rule: &str, allow: bool) {
        let r = rule.to_ascii_lowercase();
        self.remove_rule(&r);
        if !self.command_rules.is_empty() {
            self.command_rules.push(' ');
        }
        self.command_rules.push(if allow { '+' } else { '-' });
        self.command_rules.push_str(&r);
    }

    fn set_category(&mut self, catname: &str, allow: bool) -> Result<(), SetErr> {
        let Some((_, flag)) = cat::NAMES.iter().find(|(n, _)| n.eq_ignore_ascii_case(&catname[1..])) else { return Err(SetErr::NoEnt) };
        self.update_rules(catname, allow);
        for c in cmd::all_commands() {
            if c.acl_categories() & flag != 0 {
                self.change_perm(c, allow);
            }
            for s in c.subs {
                if s.acl_categories() & flag != 0 {
                    self.change_perm(s, allow);
                }
            }
        }
        Ok(())
    }

    pub fn set(&mut self, op: &str) -> Result<(), SetErr> {
        let lower = op.to_ascii_lowercase();
        let b = op.as_bytes();
        if lower == "allkeys" || op == "~*" {
            self.allkeys = true;
            self.patterns.clear();
        } else if lower == "resetkeys" {
            self.allkeys = false;
            self.patterns.clear();
        } else if lower == "allchannels" || op == "&*" {
            self.allchannels = true;
            self.channels.clear();
        } else if lower == "resetchannels" {
            self.allchannels = false;
            self.channels.clear();
        } else if lower == "allcommands" || lower == "+@all" {
            for w in self.allowed.iter_mut() {
                *w = u64::MAX;
            }
            self.allcommands = true;
            self.command_rules.clear();
            self.firstargs.clear();
        } else if lower == "nocommands" || lower == "-@all" {
            for w in self.allowed.iter_mut() {
                *w = 0;
            }
            self.allcommands = false;
            self.command_rules.clear();
            self.firstargs.clear();
        } else if b[0] == b'~' || b[0] == b'%' {
            if self.allkeys {
                return Err(SetErr::Exist);
            }
            let mut flags = 0u8;
            let mut offset = 1;
            if b[0] == b'%' {
                loop {
                    if offset >= b.len() {
                        return Err(SetErr::Inval);
                    }
                    let c = b[offset].to_ascii_uppercase();
                    if c == b'R' && flags & PERM_R == 0 {
                        flags |= PERM_R;
                    } else if c == b'W' && flags & PERM_W == 0 {
                        flags |= PERM_W;
                    } else if b[offset] == b'~' && flags != 0 {
                        offset += 1;
                        break;
                    } else {
                        return Err(SetErr::Inval);
                    }
                    offset += 1;
                }
            } else {
                flags = PERM_RW;
            }
            let pat = &op[offset..];
            if has_spaces(pat) {
                return Err(SetErr::Inval);
            }
            match self.patterns.iter_mut().find(|p| p.pattern == pat) {
                Some(p) => p.flags |= flags,
                None => self.patterns.push(KeyPattern { flags, pattern: pat.to_string() }),
            }
            self.allkeys = false;
        } else if b[0] == b'&' {
            if self.allchannels {
                return Err(SetErr::IsDir);
            }
            let pat = &op[1..];
            if has_spaces(pat) {
                return Err(SetErr::Inval);
            }
            if !self.channels.iter().any(|c| c == pat) {
                self.channels.push(pat.to_string());
            }
            self.allchannels = false;
        } else if b[0] == b'+' && b.get(1) != Some(&b'@') {
            let name = &op[1..];
            if !name.contains('|') {
                let c = cmd::lookup_full(&name.to_ascii_lowercase()).ok_or(SetErr::NoEnt)?;
                self.change_perm(c, true);
                self.update_rules(&c.full_name(), true);
            } else {
                let (main, sub) = name.rsplit_once('|').unwrap();
                let c = cmd::lookup_full(&main.to_ascii_lowercase()).ok_or(SetErr::NoEnt)?;
                if c.parent.is_some() {
                    return Err(SetErr::Child);
                }
                if sub.is_empty() {
                    return Err(SetErr::Inval);
                }
                if !c.subs.is_empty() {
                    let sc = cmd::lookup_full(&name.to_ascii_lowercase()).ok_or(SetErr::NoEnt)?;
                    self.change_perm(sc, true);
                } else {
                    let id = cmd_id(c);
                    let e = self.firstargs.entry(id).or_default();
                    if !e.iter().any(|x| x.eq_ignore_ascii_case(sub)) {
                        e.push(sub.to_string());
                    }
                }
                self.update_rules(name, true);
            }
        } else if b[0] == b'-' && b.get(1) != Some(&b'@') {
            let c = cmd::lookup_full(&op[1..].to_ascii_lowercase()).ok_or(SetErr::NoEnt)?;
            self.change_perm(c, false);
            self.update_rules(&c.full_name(), false);
        } else if (b[0] == b'+' || b[0] == b'-') && b.get(1) == Some(&b'@') {
            self.set_category(&op[1..], b[0] == b'+')?;
        } else {
            return Err(SetErr::Inval);
        }
        Ok(())
    }

    pub fn describe_commands(&self) -> String {
        let mut s = String::from(if self.can_future() { "+@all" } else { "-@all" });
        if !self.command_rules.is_empty() {
            s.push(' ');
            s.push_str(&self.command_rules);
        }
        s
    }

    pub fn describe(&self) -> String {
        let mut r = String::new();
        if self.allkeys {
            r.push_str("~* ");
        } else {
            for p in &self.patterns {
                r.push_str(&p.describe());
                r.push(' ');
            }
        }
        if self.allchannels {
            r.push_str("&* ");
        } else {
            r.push_str("resetchannels ");
            for c in &self.channels {
                r.push('&');
                r.push_str(c);
                r.push(' ');
            }
        }
        r.push_str(&self.describe_commands());
        r
    }

    fn keys_desc(&self) -> String {
        if self.allkeys {
            "~*".into()
        } else {
            self.patterns.iter().map(|p| p.describe()).collect::<Vec<_>>().join(" ")
        }
    }

    fn channels_desc(&self) -> String {
        if self.allchannels {
            "&*".into()
        } else {
            self.channels.iter().map(|c| format!("&{}", c)).collect::<Vec<_>>().join(" ")
        }
    }

    fn check_key(&self, key: &[u8], need: u8) -> bool {
        if self.allkeys {
            return true;
        }
        self.patterns.iter().any(|p| p.flags & need == need && crate::util::glob_match(p.pattern.as_bytes(), key, false))
    }

    fn check_channel(&self, ch: &[u8], is_pattern: bool) -> bool {
        if self.allchannels {
            return true;
        }
        self.channels.iter().any(|p| if is_pattern { p.as_bytes() == ch } else { crate::util::glob_match(p.as_bytes(), ch, false) })
    }

    /// Returns Ok or (reason, argument index).
    fn check_cmd(&self, c: &Cmd, args: &[Bytes]) -> Result<(), (Denied, usize)> {
        if !self.allcommands && c.flags & cmd::flags::NO_AUTH == 0 {
            let id = cmd_id(c);
            if !self.get_bit(id) {
                let idx = if c.parent.is_some() { 2 } else { 1 };
                let ok = match (self.firstargs.get(&id), args.get(idx)) {
                    (Some(list), Some(a)) => list.iter().any(|x| x.as_bytes().eq_ignore_ascii_case(a)),
                    _ => false,
                };
                if !ok {
                    return Err((Denied::Cmd, 0));
                }
            }
        }
        if !self.allkeys {
            for (i, need) in key_perms(c, args) {
                if !self.check_key(&args[i], need) {
                    return Err((Denied::Key, i));
                }
            }
        }
        if !self.allchannels {
            for (i, is_pat) in channels_of(c, args) {
                if !self.check_channel(&args[i], is_pat) {
                    return Err((Denied::Channel, i));
                }
            }
        }
        Ok(())
    }
}

/// Keys and the permission each needs (approximating Redis key-spec flags).
pub fn key_perms(c: &Cmd, args: &[Bytes]) -> Vec<(usize, u8)> {
    let idx = c.key_indexes(args);
    if idx.is_empty() {
        return vec![];
    }
    let name = c.full_name();
    let write = c.is_write_for(args);
    // Commands that write a destination and only read their sources.
    let dest_first = matches!(
        name.as_str(),
        "sinterstore" | "sunionstore" | "sdiffstore" | "zunionstore" | "zinterstore" | "zdiffstore" | "zrangestore" | "geosearchstore" | "bitop" | "pfmerge"
    );
    let src_dst = matches!(name.as_str(), "copy" | "lmove" | "blmove" | "rpoplpush" | "brpoplpush" | "smove");
    // Write commands that also return (access) the existing value.
    let access_write = matches!(
        name.as_str(),
        "getset" | "getdel" | "getex" | "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "append" | "setrange" | "lpop" | "rpop" | "blpop" | "brpop"
            | "lmpop" | "blmpop" | "spop" | "zpopmin" | "zpopmax" | "bzpopmin" | "bzpopmax" | "zmpop" | "bzmpop" | "hincrby" | "hincrbyfloat" | "zincrby"
            | "rename" | "renamenx" | "move" | "restore" | "xreadgroup" | "xclaim" | "xautoclaim" | "bitfield" | "linsert" | "lset" | "lrem" | "ltrim"
            | "setbit" | "pfadd" | "pfcount" | "sort" | "georadius" | "georadiusbymember" | "migrate" | "xack" | "xdel" | "xtrim" | "xsetid"
    ) || (name == "set" && args.iter().skip(3).any(|a| crate::util::eq_ic(a, "get")));
    let mut out = Vec::with_capacity(idx.len());
    for (n, i) in idx.into_iter().enumerate() {
        let need = if !write {
            PERM_R
        } else if dest_first {
            if n == 0 { if name == "pfmerge" { PERM_RW } else { PERM_W } } else { PERM_R }
        } else if src_dst {
            if n == 0 { PERM_RW } else { PERM_W }
        } else if name == "sort" || name == "georadius" || name == "georadiusbymember" {
            if n == 0 { PERM_R } else { PERM_W }
        } else if access_write {
            PERM_RW
        } else {
            PERM_W
        };
        out.push((i, need));
    }
    out
}

/// Channel arguments checked by ACLs: (index, is_pattern). Only publish/subscribe.
pub fn channels_of(c: &Cmd, args: &[Bytes]) -> Vec<(usize, bool)> {
    match c.full_name().as_str() {
        "publish" | "spublish" => vec![(1, false)],
        "subscribe" | "ssubscribe" => (1..args.len()).map(|i| (i, false)).collect(),
        "psubscribe" => (1..args.len()).map(|i| (i, true)).collect(),
        _ => vec![],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Denied {
    Cmd = 1,
    Key = 2,
    Auth = 3,
    Channel = 4,
}

// ---------- users ----------

#[derive(Clone, Debug)]
pub struct User {
    pub name: String,
    pub enabled: bool,
    pub nopass: bool,
    pub skip_sanitize: bool,
    pub passwords: Vec<String>,
    pub selectors: Vec<Selector>,
}

pub fn hash_password(p: &[u8]) -> String {
    let d = Sha256::digest(p);
    d.iter().map(|b| format!("{:02x}", b)).collect()
}

fn valid_hash(h: &str) -> bool {
    h.len() == 64 && h.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

impl User {
    pub fn new(name: &str, allchannels: bool) -> Self {
        User {
            name: name.to_string(),
            enabled: false,
            nopass: false,
            skip_sanitize: false,
            passwords: Vec::new(),
            selectors: vec![Selector::new(true, allchannels)],
        }
    }

    pub fn root(&mut self) -> &mut Selector {
        &mut self.selectors[0]
    }

    pub fn set(&mut self, op: &str, pubsub_all: bool) -> Result<(), SetErr> {
        if op.is_empty() {
            return Ok(());
        }
        let lower = op.to_ascii_lowercase();
        let b = op.as_bytes();
        match lower.as_str() {
            "on" => self.enabled = true,
            "off" => self.enabled = false,
            "skip-sanitize-payload" => self.skip_sanitize = true,
            "sanitize-payload" => self.skip_sanitize = false,
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            "resetpass" => {
                self.nopass = false;
                self.passwords.clear();
            }
            "clearselectors" => self.selectors.truncate(1),
            "reset" => {
                self.set("resetpass", pubsub_all)?;
                self.set("resetkeys", pubsub_all)?;
                self.set("resetchannels", pubsub_all)?;
                if pubsub_all {
                    self.set("allchannels", pubsub_all)?;
                }
                self.set("off", pubsub_all)?;
                self.set("sanitize-payload", pubsub_all)?;
                self.set("clearselectors", pubsub_all)?;
                self.set("-@all", pubsub_all)?;
            }
            _ => {
                if b[0] == b'>' || b[0] == b'#' {
                    let h = if b[0] == b'>' {
                        hash_password(&b[1..])
                    } else {
                        if !valid_hash(&op[1..]) {
                            return Err(SetErr::BadMsg);
                        }
                        op[1..].to_string()
                    };
                    if !self.passwords.contains(&h) {
                        self.passwords.push(h);
                    }
                    self.nopass = false;
                } else if b[0] == b'<' || b[0] == b'!' {
                    let h = if b[0] == b'<' {
                        hash_password(&b[1..])
                    } else {
                        if !valid_hash(&op[1..]) {
                            return Err(SetErr::BadMsg);
                        }
                        op[1..].to_string()
                    };
                    match self.passwords.iter().position(|p| *p == h) {
                        Some(i) => {
                            self.passwords.remove(i);
                        }
                        None => return Err(SetErr::NoDev),
                    }
                } else if b[0] == b'(' && b[b.len() - 1] == b')' {
                    let mut sel = Selector::new(false, pubsub_all);
                    let inner = &op[1..op.len() - 1];
                    let parts = crate::resp::split_args(inner.as_bytes()).ok_or(SetErr::Inval)?;
                    for p in parts {
                        sel.set(&String::from_utf8_lossy(&p))?;
                    }
                    self.selectors.push(sel);
                } else {
                    self.root().set(op)?;
                }
            }
        }
        Ok(())
    }

    pub fn flag_names(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        v.push(if self.enabled { "on" } else { "off" });
        if self.nopass {
            v.push("nopass");
        }
        v.push(if self.skip_sanitize { "skip-sanitize-payload" } else { "sanitize-payload" });
        v
    }

    pub fn describe(&self) -> String {
        let mut r = String::new();
        for f in self.flag_names() {
            r.push_str(f);
            r.push(' ');
        }
        for p in &self.passwords {
            r.push('#');
            r.push_str(p);
            r.push(' ');
        }
        for s in &self.selectors {
            if s.root {
                r.push_str(&s.describe());
            } else {
                r.push_str(&format!(" ({})", s.describe()));
            }
        }
        r
    }

    pub fn check_cmd(&self, c: &Cmd, args: &[Bytes]) -> Result<(), (Denied, usize)> {
        let mut relevant = (Denied::Cmd, 0usize);
        for s in &self.selectors {
            match s.check_cmd(c, args) {
                Ok(()) => return Ok(()),
                Err((d, i)) => {
                    if d > relevant.0 || (d == relevant.0 && i > relevant.1) {
                        relevant = (d, i);
                    }
                }
            }
        }
        Err(relevant)
    }

    pub fn check_channel(&self, ch: &[u8], is_pattern: bool) -> bool {
        self.selectors.iter().any(|s| s.check_channel(ch, is_pattern))
    }

    pub fn check_key(&self, key: &[u8], need: u8) -> bool {
        self.selectors.iter().any(|s| s.check_key(key, need))
    }
}

/// Merge "(a b" "c)" style selector arguments.
fn merge_selector_args(args: &[String]) -> Result<Vec<String>, usize> {
    let mut out = Vec::new();
    let mut open: Option<(usize, String)> = None;
    for (j, op) in args.iter().enumerate() {
        if open.is_none() && op.starts_with('(') && !op.ends_with(')') {
            open = Some((j, op.clone()));
            continue;
        }
        if let Some((start, mut s)) = open.take() {
            s.push(' ');
            s.push_str(op);
            if op.ends_with(')') {
                out.push(s);
            } else {
                open = Some((start, s));
            }
            continue;
        }
        out.push(op.clone());
    }
    if let Some((start, _)) = open {
        return Err(start);
    }
    Ok(out)
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub count: u64,
    pub reason: Denied,
    pub context: &'static str,
    pub object: String,
    pub username: String,
    pub ctime: i64,
    pub cinfo: String,
    pub entry_id: u64,
    pub created: i64,
}

pub struct Acl {
    pub users: BTreeMap<String, User>,
    pub log: VecDeque<LogEntry>,
    pub log_count: u64,
    pub denied_auth: u64,
    pub denied_cmd: u64,
    pub denied_key: u64,
    pub denied_channel: u64,
    /// "toplevel" | "multi" | "lua": set by callers that run nested commands.
    pub context: &'static str,
}

impl Acl {
    pub fn new(cfg: &crate::config::Config) -> Self {
        let mut users = BTreeMap::new();
        let mut d = User::new("default", true);
        for op in ["+@all", "~*", "&*", "on", "nopass"] {
            d.set(op, true).unwrap();
        }
        users.insert("default".to_string(), d);
        let mut acl = Acl { users, log: VecDeque::new(), log_count: 0, denied_auth: 0, denied_cmd: 0, denied_key: 0, denied_channel: 0, context: "toplevel" };
        if cfg.requirepass.is_some() {
            acl.set_default_password(cfg.requirepass.clone());
        }
        // Users defined in redis.conf ("user ..." lines).
        for line in &cfg.users {
            if line.is_empty() {
                continue;
            }
            let name = line[0].clone();
            let mut u = User::new(&name, cfg.acl_pubsub_default_all);
            if name == "default" {
                u = acl.users["default"].clone();
            }
            let merged = match merge_selector_args(&line[1..]) {
                Ok(m) => m,
                Err(_) => {
                    eprintln!("Unmatched parenthesis in ACL user '{}'", name);
                    continue;
                }
            };
            let mut ok = true;
            for op in merged {
                if let Err(e) = u.set(&op, cfg.acl_pubsub_default_all) {
                    eprintln!("Error in user declaration '{}': {}", op, e.msg());
                    ok = false;
                    break;
                }
            }
            if ok {
                acl.users.insert(name, u);
            }
        }
        if !cfg.aclfile.is_empty() {
            if let Err(e) = acl.load_file(&cfg.aclfile, cfg.acl_pubsub_default_all) {
                eprintln!("Aborting startup because of ACL errors: {}", e);
            }
        }
        acl
    }

    /// requirepass maps to the default user's password (or nopass when unset).
    pub fn set_default_password(&mut self, p: Option<Vec<u8>>) {
        let d = self.users.get_mut("default").unwrap();
        let _ = d.set("resetpass", false);
        match p {
            Some(p) if !p.is_empty() => {
                d.passwords.push(hash_password(&p));
                d.nopass = false;
            }
            _ => {
                let _ = d.set("nopass", false);
            }
        }
    }

    /// ACL SETUSER semantics, staged on a copy. Returns an error message.
    pub fn set_user(&mut self, name: &str, ops: &[String], pubsub_all: bool) -> Result<(), String> {
        let merged = merge_selector_args(ops).map_err(|i| format!("Unmatched parenthesis in acl selector starting at '{}'.", ops[i]))?;
        let mut u = self.users.get(name).cloned().unwrap_or_else(|| User::new(name, pubsub_all));
        for op in &merged {
            u.set(op, pubsub_all).map_err(|e| format!("Error in ACL SETUSER modifier '{}': {}", op, e.msg()))?;
        }
        self.users.insert(name.to_string(), u);
        Ok(())
    }

    pub fn check_credentials(&self, user: &str, pass: &[u8]) -> bool {
        let Some(u) = self.users.get(user) else { return false };
        if !u.enabled {
            return false;
        }
        if u.nopass {
            return true;
        }
        let h = hash_password(pass);
        u.passwords.iter().any(|p| constant_eq(p.as_bytes(), h.as_bytes()))
    }

    pub fn load_file(&mut self, path: &str, pubsub_all: bool) -> Result<(), String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("Error loading ACLs, opening file '{}': {}", path, e))?;
        let mut staged: BTreeMap<String, User> = BTreeMap::new();
        let mut errors = String::new();
        for (ln, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some(parts) = crate::resp::split_args(line.as_bytes()) else {
                errors.push_str(&format!("{}:{}: unbalanced quotes in acl line. ", path, ln + 1));
                continue;
            };
            let parts: Vec<String> = parts.into_iter().map(|p| String::from_utf8_lossy(&p).into_owned()).collect();
            if parts.is_empty() {
                continue;
            }
            if parts[0] != "user" || parts.len() < 2 {
                errors.push_str(&format!("{}:{}: line should start with user keyword. ", path, ln + 1));
                continue;
            }
            let name = &parts[1];
            if has_spaces(name) {
                errors.push_str(&format!("{}:{}: username '{}' contains invalid characters. ", path, ln + 1, name));
                continue;
            }
            if staged.contains_key(name) {
                errors.push_str(&format!("{}:{}: Duplicate user '{}' found. ", path, ln + 1, name));
                continue;
            }
            let merged = match merge_selector_args(&parts[2..]) {
                Ok(m) => m,
                Err(_) => {
                    errors.push_str(&format!("{}:{}: Unmatched parenthesis in selector definition. ", path, ln + 1));
                    continue;
                }
            };
            let mut u = User::new(name, pubsub_all);
            let mut bad = false;
            for op in &merged {
                if let Err(e) = u.set(op, pubsub_all) {
                    errors.push_str(&format!("{}:{}: {}. ", path, ln + 1, e.msg()));
                    bad = true;
                    break;
                }
            }
            if !bad {
                staged.insert(name.clone(), u);
            }
        }
        if !errors.is_empty() {
            return Err(errors.trim_end().to_string());
        }
        if !staged.contains_key("default") {
            let mut d = User::new("default", true);
            for op in ["+@all", "~*", "&*", "on", "nopass"] {
                d.set(op, true).unwrap();
            }
            staged.insert("default".into(), d);
        }
        self.users = staged;
        Ok(())
    }

    pub fn save_file(&self, path: &str) -> std::io::Result<()> {
        let mut out = String::new();
        for u in self.users.values() {
            out.push_str(&format!("user {} {}\n", u.name, u.describe()));
        }
        let tmp = format!("{}.tmp-{}", path, std::process::id());
        std::fs::write(&tmp, out)?;
        std::fs::rename(&tmp, path)
    }

    pub fn add_log(&mut self, reason: Denied, context: &'static str, object: String, username: String, cinfo: String, max_len: usize) {
        match reason {
            Denied::Auth => self.denied_auth += 1,
            Denied::Cmd => self.denied_cmd += 1,
            Denied::Key => self.denied_key += 1,
            Denied::Channel => self.denied_channel += 1,
        }
        if max_len == 0 {
            self.log.clear();
            return;
        }
        let now = crate::util::now_ms();
        let pos = self
            .log
            .iter()
            .take(10)
            .position(|e| e.reason == reason && e.context == context && e.object == object && e.username == username && now - e.ctime <= 60_000);
        if let Some(i) = pos {
            let mut e = self.log.remove(i).unwrap();
            e.cinfo = cinfo;
            e.ctime = now;
            e.count += 1;
            self.log.push_front(e);
        } else {
            let e = LogEntry { count: 1, reason, context, object, username, ctime: now, cinfo, entry_id: self.log_count, created: now };
            self.log_count += 1;
            self.log.push_front(e);
            while self.log.len() > max_len {
                self.log.pop_back();
            }
        }
    }
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ---------- integration with the server ----------

pub fn default_user_nopass(s: &Store) -> bool {
    s.acl.users.get("default").map(|u| u.nopass).unwrap_or(true)
}

/// Whether new connections start authenticated as "default".
pub fn default_user_authenticated(s: &Store) -> bool {
    s.acl.users.get("default").map(|u| u.nopass && u.enabled).unwrap_or(false)
}

fn client_info(s: &Store, cid: u64) -> String {
    let real = if s.current_client != 0 { s.current_client } else { cid };
    match s.clients.get(&real).or_else(|| s.clients.get(&cid)) {
        Some(c) => crate::cmd::client::client_line(c, s),
        None => String::new(),
    }
}

pub fn authenticate(s: &mut Store, cid: u64, user: &[u8], pass: &[u8]) -> Result<(), Frame> {
    let uname = String::from_utf8_lossy(user).into_owned();
    if s.acl.check_credentials(&uname, pass) {
        if let Some(c) = s.clients.get_mut(&cid) {
            c.authenticated = true;
            c.user = uname;
        }
        return Ok(());
    }
    let info = client_info(s, cid);
    let ctx = s.acl.context;
    let max = s.cfg.acllog_max_len;
    s.acl.add_log(Denied::Auth, ctx, "AUTH".into(), uname, info, max);
    Err(Frame::err("WRONGPASS invalid username-password pair or user is disabled."))
}

pub fn error_message(r: Denied, user: &str, c: &Cmd, val: &[u8], verbose: bool) -> String {
    match r {
        Denied::Cmd | Denied::Auth => format!("User {} has no permissions to run the '{}' command", user, c.full_name()),
        Denied::Key => {
            if verbose {
                format!("User {} has no permissions to access the '{}' key", user, String::from_utf8_lossy(val))
            } else {
                "No permissions to access a key".into()
            }
        }
        Denied::Channel => {
            if verbose {
                format!("User {} has no permissions to access the '{}' channel", user, String::from_utf8_lossy(val))
            } else {
                "No permissions to access a channel".into()
            }
        }
    }
}

/// Permission check for a command about to run.
pub fn check_command(s: &mut Store, cid: u64, c: &'static Cmd, args: &[Bytes]) -> Result<(), Frame> {
    let Some(cl) = s.clients.get(&cid) else { return Ok(()) };
    if cl.has(crate::server::cflags::MASTER) {
        return Ok(());
    }
    let uname = cl.user.clone();
    let Some(u) = s.acl.users.get(&uname) else { return Ok(()) };
    match u.check_cmd(c, args) {
        Ok(()) => Ok(()),
        Err((reason, idx)) => {
            let object = match reason {
                Denied::Cmd | Denied::Auth => c.full_name(),
                _ => String::from_utf8_lossy(&args[idx]).into_owned(),
            };
            let msg = error_message(reason, &uname, c, args.get(idx).map(|b| b.as_ref()).unwrap_or(b""), false);
            let info = client_info(s, cid);
            let ctx = s.acl.context;
            let max = s.cfg.acllog_max_len;
            s.acl.add_log(reason, ctx, object, uname, info, max);
            Err(Frame::err(format!("NOPERM {}", msg)))
        }
    }
}

/// Kill clients authenticated as users that were deleted, and pub/sub clients that
/// lost access to channels they are subscribed to.
pub fn enforce_users(s: &mut Store) {
    let mut kill = Vec::new();
    for c in s.clients.values() {
        match s.acl.users.get(&c.user) {
            None => kill.push(c.id),
            Some(u) => {
                if c.in_pubsub() {
                    let lost = c.channels.iter().any(|ch| !u.check_channel(ch, false))
                        || c.shard_channels.iter().any(|ch| !u.check_channel(ch, false))
                        || c.patterns.iter().any(|p| !u.check_channel(p, true));
                    if lost {
                        kill.push(c.id);
                    }
                }
            }
        }
    }
    for id in kill {
        if let Some(c) = s.clients.get_mut(&id) {
            if s.current_client == id {
                c.flags |= crate::server::cflags::CLOSE_AFTER_REPLY;
            } else {
                c.kill_now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn describe_default() {
        let cfg = crate::config::Config::default();
        let acl = Acl::new(&cfg);
        assert_eq!(acl.users["default"].describe(), "on nopass sanitize-payload ~* &* +@all");
    }
    #[test]
    fn rules() {
        let mut u = User::new("x", false);
        for op in ["on", ">pw", "~foo:*", "+@read", "-get", "+get", "+config|get"] {
            u.set(op, false).unwrap();
        }
        assert!(u.describe().ends_with("~foo:* resetchannels -@all +@read +get +config|get"), "{}", u.describe());
        let get = cmd::lookup(b"get").unwrap();
        assert!(u.check_cmd(get, &[Bytes::from_static(b"get"), Bytes::from_static(b"foo:1")]).is_ok());
        assert_eq!(u.check_cmd(get, &[Bytes::from_static(b"get"), Bytes::from_static(b"bar")]).unwrap_err().0, Denied::Key);
    }
}
