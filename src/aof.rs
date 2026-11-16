//! Append-only file persistence using the Redis 7 multi-part layout: an
//! `appenddirname` directory holding a base file (RDB preamble or AOF commands),
//! incremental AOF files, and a manifest describing them.

use crate::server::{Shared, Store};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, PartialEq)]
pub enum FileType {
    Base,
    Incr,
    History,
}

#[derive(Clone, Debug)]
pub struct ManifestEntry {
    pub name: String,
    pub seq: u64,
    pub ftype: FileType,
}

#[derive(Clone, Debug, Default)]
pub struct Manifest {
    pub base: Option<ManifestEntry>,
    pub incrs: Vec<ManifestEntry>,
    pub history: Vec<ManifestEntry>,
    pub cur_base_seq: u64,
    pub cur_incr_seq: u64,
}

impl Manifest {
    pub fn parse(text: &str) -> Result<Manifest, String> {
        let mut m = Manifest::default();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = crate::resp::split_args(line.as_bytes()).ok_or("Invalid AOF manifest file format")?;
            let parts: Vec<String> = parts.into_iter().map(|p| String::from_utf8_lossy(&p).into_owned()).collect();
            if parts.len() % 2 != 0 {
                return Err("Invalid AOF manifest file format".into());
            }
            let (mut name, mut seq, mut t) = (None, None, None);
            for kv in parts.chunks(2) {
                match kv[0].as_str() {
                    "file" => name = Some(kv[1].clone()),
                    "seq" => seq = kv[1].parse::<u64>().ok(),
                    "type" => t = Some(kv[1].clone()),
                    _ => {}
                }
            }
            let (Some(name), Some(seq), Some(t)) = (name, seq, t) else { return Err("Invalid AOF manifest file format".into()) };
            let ftype = match t.as_str() {
                "b" => FileType::Base,
                "i" => FileType::Incr,
                "h" => FileType::History,
                _ => return Err("Unknown AOF file type".into()),
            };
            let e = ManifestEntry { name, seq, ftype: ftype.clone() };
            match ftype {
                FileType::Base => {
                    if m.base.is_some() {
                        return Err("Found duplicate base file information".into());
                    }
                    m.cur_base_seq = seq;
                    m.base = Some(e);
                }
                FileType::Incr => {
                    if seq <= m.cur_incr_seq {
                        return Err("Found a non-monotonic sequence number".into());
                    }
                    m.cur_incr_seq = seq;
                    m.incrs.push(e);
                }
                FileType::History => m.history.push(e),
            }
        }
        Ok(m)
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        let mut line = |e: &ManifestEntry| {
            let t = match e.ftype {
                FileType::Base => "b",
                FileType::Incr => "i",
                FileType::History => "h",
            };
            out.push_str(&format!("file {} seq {} type {}\n", e.name, e.seq, t));
        };
        if let Some(b) = &self.base {
            line(b);
        }
        for h in &self.history {
            line(h);
        }
        for i in &self.incrs {
            line(i);
        }
        out
    }
}

pub struct AofHandle {
    pub dir: PathBuf,
    pub manifest: Manifest,
    file: File,
    pub selected_db: Option<usize>,
    pub fsync_always: bool,
    pub fsync_everysec: Arc<AtomicBool>,
    pub dirty_since_fsync: Arc<AtomicBool>,
    pub stop: Arc<AtomicBool>,
    pub incr_size: u64,
    pub timestamps: bool,
    pub last_ts: i64,
}

fn dir_of(s: &Store) -> PathBuf {
    Path::new(&s.cfg.dir).join(&s.cfg.appenddirname)
}

fn manifest_name(s: &Store) -> String {
    format!("{}.manifest", s.cfg.appendfilename)
}

fn write_manifest(dir: &Path, name: &str, m: &Manifest) -> std::io::Result<()> {
    let tmp = dir.join(format!("temp-{}", name));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(m.render().as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

pub fn encode_command(argv: &[Bytes]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + argv.iter().map(|a| a.len() + 16).sum::<usize>());
    out.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
    for a in argv {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

impl AofHandle {
    fn open_incr(dir: &Path, name: &str) -> std::io::Result<File> {
        OpenOptions::new().create(true).append(true).open(dir.join(name))
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.file.write_all(data)?;
        self.incr_size += data.len() as u64;
        if self.fsync_always {
            self.file.sync_data()?;
        } else {
            self.dirty_since_fsync.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn spawn_fsync_thread(&self) {
        let f = match self.file.try_clone() {
            Ok(f) => f,
            Err(_) => return,
        };
        let everysec = self.fsync_everysec.clone();
        let dirty = self.dirty_since_fsync.clone();
        let stop = self.stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(1000));
                if everysec.load(Ordering::Relaxed) && dirty.swap(false, Ordering::Relaxed) {
                    let _ = f.sync_data();
                }
            }
        });
    }
}

impl Drop for AofHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.file.sync_data();
    }
}

/// Append a propagated command to the AOF.
pub fn feed(s: &mut Store, db: usize, argv: &[Bytes]) {
    let Some(h) = s.aof.as_mut() else { return };
    let mut data = Vec::new();
    if h.timestamps {
        let now = crate::util::now_ms() / 1000;
        if now != h.last_ts {
            h.last_ts = now;
            data.extend_from_slice(format!("#TS:{}\r\n", now).as_bytes());
        }
    }
    if h.selected_db != Some(db) {
        data.extend_from_slice(&encode_command(&[Bytes::from_static(b"SELECT"), Bytes::from(db.to_string())]));
        h.selected_db = Some(db);
    }
    data.extend_from_slice(&encode_command(argv));
    let res = h.write(&data);
    s.persist.aof_last_write_ok = res.is_ok();
    if res.is_ok() {
        s.persist.aof_current_size += data.len() as u64;
    } else if let Err(e) = res {
        eprintln!("Error writing to the AOF file: {}", e);
    }
}

pub fn flush_now(s: &mut Store) {
    if let Some(h) = s.aof.as_mut() {
        let _ = h.file.sync_data();
        h.dirty_since_fsync.store(false, Ordering::Relaxed);
    }
}

pub fn set_fsync(s: &mut Store) {
    let pol = s.cfg.appendfsync.clone();
    if let Some(h) = s.aof.as_mut() {
        h.fsync_always = pol == "always";
        h.fsync_everysec.store(pol == "everysec", Ordering::Relaxed);
    }
}

/// Write the dataset as the content of a base file.
fn base_content(snap: &crate::rdb::Snapshot, preamble: bool) -> Vec<u8> {
    if preamble {
        return crate::rdb::serialize(snap);
    }
    let mut out = Vec::new();
    for f in &snap.functions {
        out.extend_from_slice(&encode_command(&[Bytes::from_static(b"FUNCTION"), Bytes::from_static(b"LOAD"), Bytes::from(f.clone())]));
    }
    for (i, db) in snap.dbs.iter().enumerate() {
        if db.is_empty() {
            continue;
        }
        out.extend_from_slice(&encode_command(&[Bytes::from_static(b"SELECT"), Bytes::from(i.to_string())]));
        for (k, v, exp) in db {
            for cmd in crate::value::rewrite_commands(k, v) {
                out.extend_from_slice(&encode_command(&cmd));
            }
            if let Some(e) = exp {
                out.extend_from_slice(&encode_command(&[Bytes::from_static(b"PEXPIREAT"), k.clone(), Bytes::from(e.to_string())]));
            }
        }
    }
    out
}

fn write_file_sync(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut f = File::create(path)?;
    f.write_all(data)?;
    f.sync_all()
}

/// Open the AOF for writing after load (or create an initial base when absent).
pub fn open(s: &mut Store) -> Result<(), String> {
    let dir = dir_of(s);
    std::fs::create_dir_all(&dir).map_err(|e| format!("Can't create AOF directory {}: {}", dir.display(), e))?;
    let mname = manifest_name(s);
    let mut m = match std::fs::read_to_string(dir.join(&mname)) {
        Ok(t) => Manifest::parse(&t)?,
        Err(_) => Manifest::default(),
    };
    let fresh = m.base.is_none() && m.incrs.is_empty();
    if fresh {
        // Write an initial base containing the current dataset.
        let snap = crate::rdb::snapshot(s);
        let preamble = s.cfg.aof_use_rdb_preamble;
        let seq = m.cur_base_seq + 1;
        let name = format!("{}.{}.base.{}", s.cfg.appendfilename, seq, if preamble { "rdb" } else { "aof" });
        let content = base_content(&snap, preamble);
        write_file_sync(&dir.join(&name), &content).map_err(|e| e.to_string())?;
        s.persist.aof_base_size = content.len() as u64;
        m.base = Some(ManifestEntry { name, seq, ftype: FileType::Base });
        m.cur_base_seq = seq;
    }
    // Always continue appending to the last incr file, or create a new one.
    let incr_name = match m.incrs.last() {
        Some(e) if !fresh => e.name.clone(),
        _ => {
            let seq = m.cur_incr_seq + 1;
            let name = format!("{}.{}.incr.aof", s.cfg.appendfilename, seq);
            m.incrs.push(ManifestEntry { name: name.clone(), seq, ftype: FileType::Incr });
            m.cur_incr_seq = seq;
            name
        }
    };
    write_manifest(&dir, &mname, &m).map_err(|e| e.to_string())?;
    let file = AofHandle::open_incr(&dir, &incr_name).map_err(|e| e.to_string())?;
    let incr_size = file.metadata().map(|md| md.len()).unwrap_or(0);
    let h = AofHandle {
        dir,
        manifest: m,
        file,
        selected_db: None,
        fsync_always: s.cfg.appendfsync == "always",
        fsync_everysec: Arc::new(AtomicBool::new(s.cfg.appendfsync == "everysec")),
        dirty_since_fsync: Arc::new(AtomicBool::new(false)),
        stop: Arc::new(AtomicBool::new(false)),
        incr_size,
        timestamps: s.cfg.aof_timestamp_enabled,
        last_ts: 0,
    };
    h.spawn_fsync_thread();
    if s.persist.aof_base_size == 0 {
        if let Some(b) = &h.manifest.base {
            s.persist.aof_base_size = std::fs::metadata(h.dir.join(&b.name)).map(|m| m.len()).unwrap_or(0);
        }
    }
    s.persist.aof_current_size = s.persist.aof_base_size + incr_size;
    s.aof = Some(h);
    Ok(())
}

/// CONFIG SET appendonly yes: Redis rewrites the AOF so it reflects the dataset.
pub fn start_aof(s: &mut Store) {
    // Discard any stale AOF set and create a fresh one from the live dataset.
    let dir = dir_of(s);
    let mname = manifest_name(s);
    if let Ok(t) = std::fs::read_to_string(dir.join(&mname)) {
        if let Ok(m) = Manifest::parse(&t) {
            let _ = std::fs::remove_file(dir.join(&mname));
            for e in m.base.iter().chain(m.incrs.iter()).chain(m.history.iter()) {
                let _ = std::fs::remove_file(dir.join(&e.name));
            }
        }
    }
    s.persist.aof_base_size = 0;
    if let Err(e) = open(s) {
        eprintln!("Redis needs to enable the AOF but can't trigger a background AOF rewrite operation: {}", e);
        s.persist.aof_last_write_ok = false;
    } else {
        s.persist.aof_rewrites += 1;
    }
}

pub fn stop_aof(s: &mut Store) {
    flush_now(s);
    s.aof = None;
}

/// BGREWRITEAOF: switch writes to a new incr file immediately, then build a new
/// base from a snapshot on a background thread and retire the old files.
pub fn bgrewrite(s: &mut Store, shared: Arc<Shared>) {
    if s.persist.aof_rewrite_in_progress {
        return;
    }
    if s.aof.is_none() {
        // Without AOF enabled, BGREWRITEAOF just produces a fresh AOF set.
        let snap = crate::rdb::snapshot(s);
        let dir = dir_of(s);
        let mname = manifest_name(s);
        let preamble = s.cfg.aof_use_rdb_preamble;
        let fname = s.cfg.appendfilename.clone();
        s.persist.aof_rewrite_in_progress = true;
        std::thread::spawn(move || {
            let res = (|| -> std::io::Result<u64> {
                std::fs::create_dir_all(&dir)?;
                let old = std::fs::read_to_string(dir.join(&mname)).ok().and_then(|t| Manifest::parse(&t).ok()).unwrap_or_default();
                let seq = old.cur_base_seq + 1;
                let name = format!("{}.{}.base.{}", fname, seq, if preamble { "rdb" } else { "aof" });
                let content = base_content(&snap, preamble);
                write_file_sync(&dir.join(&name), &content)?;
                let iseq = old.cur_incr_seq + 1;
                let iname = format!("{}.{}.incr.aof", fname, iseq);
                File::create(dir.join(&iname))?;
                let m = Manifest {
                    base: Some(ManifestEntry { name, seq, ftype: FileType::Base }),
                    incrs: vec![ManifestEntry { name: iname, seq: iseq, ftype: FileType::Incr }],
                    history: vec![],
                    cur_base_seq: seq,
                    cur_incr_seq: iseq,
                };
                write_manifest(&dir, &mname, &m)?;
                for e in old.base.iter().chain(old.incrs.iter()).chain(old.history.iter()) {
                    let _ = std::fs::remove_file(dir.join(&e.name));
                }
                Ok(content.len() as u64)
            })();
            let mut s = shared.store.lock();
            s.persist.aof_rewrite_in_progress = false;
            s.persist.aof_last_rewrite_ok = res.is_ok();
            if res.is_ok() {
                s.persist.aof_rewrites += 1;
            }
        });
        return;
    }
    let snap = crate::rdb::snapshot(s);
    let preamble = s.cfg.aof_use_rdb_preamble;
    let fname = s.cfg.appendfilename.clone();
    let mname = manifest_name(s);
    let h = s.aof.as_mut().unwrap();
    let dir = h.dir.clone();
    // Open the new incr file and record it in the manifest right away.
    let iseq = h.manifest.cur_incr_seq + 1;
    let iname = format!("{}.{}.incr.aof", fname, iseq);
    let file = match AofHandle::open_incr(&dir, &iname) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Can't open the append-only file {}: {}", iname, e);
            s.persist.aof_last_rewrite_ok = false;
            return;
        }
    };
    let _ = h.file.sync_data();
    let old_incrs = std::mem::take(&mut h.manifest.incrs);
    // Old incrs become history until the new base is in place.
    for mut e in old_incrs {
        e.ftype = FileType::History;
        h.manifest.history.push(e);
    }
    h.manifest.incrs.push(ManifestEntry { name: iname, seq: iseq, ftype: FileType::Incr });
    h.manifest.cur_incr_seq = iseq;
    // History files must still be replayed if we crash before the rewrite ends,
    // so keep them listed as incr in the on-disk manifest for now.
    let mut on_disk = h.manifest.clone();
    let mut hist: Vec<ManifestEntry> = std::mem::take(&mut on_disk.history).into_iter().map(|mut e| {
        e.ftype = FileType::Incr;
        e
    }).collect();
    hist.extend(on_disk.incrs);
    on_disk.incrs = hist;
    if let Err(e) = write_manifest(&dir, &mname, &on_disk) {
        eprintln!("Can't persist the AOF manifest file: {}", e);
    }
    h.stop.store(true, Ordering::Relaxed);
    h.file = file;
    h.stop = Arc::new(AtomicBool::new(false));
    h.spawn_fsync_thread();
    h.selected_db = None;
    h.incr_size = 0;
    s.persist.aof_rewrite_in_progress = true;
    s.stats.total_forks += 1;
    let bseq = s.aof.as_ref().unwrap().manifest.cur_base_seq + 1;
    std::thread::spawn(move || {
        let name = format!("{}.{}.base.{}", fname, bseq, if preamble { "rdb" } else { "aof" });
        let content = base_content(&snap, preamble);
        let res = write_file_sync(&dir.join(&name), &content);
        let mut s = shared.store.lock();
        s.persist.aof_rewrite_in_progress = false;
        match (res, s.aof.as_mut()) {
            (Ok(()), Some(h)) => {
                let old_base = h.manifest.base.take();
                let history = std::mem::take(&mut h.manifest.history);
                h.manifest.base = Some(ManifestEntry { name, seq: bseq, ftype: FileType::Base });
                h.manifest.cur_base_seq = bseq;
                if write_manifest(&h.dir, &mname, &h.manifest).is_ok() {
                    for e in old_base.iter().chain(history.iter()) {
                        let _ = std::fs::remove_file(h.dir.join(&e.name));
                    }
                    let incr = h.incr_size;
                    s.persist.aof_base_size = content.len() as u64;
                    s.persist.aof_current_size = content.len() as u64 + incr;
                    s.persist.aof_last_rewrite_ok = true;
                    s.persist.aof_rewrites += 1;
                    eprintln!("Background AOF rewrite finished successfully");
                } else {
                    s.persist.aof_last_rewrite_ok = false;
                }
            }
            (Ok(()), None) => {
                let _ = std::fs::remove_file(dir.join(&name));
            }
            (Err(e), _) => {
                eprintln!("Background AOF rewrite failed: {}", e);
                s.persist.aof_last_rewrite_ok = false;
            }
        }
    });
}

// ---------------- loading ----------------

/// Create an internal client used to replay commands.
pub fn pseudo_client(s: &mut Store, flags: u64) -> u64 {
    let id = s.next_client_id;
    s.next_client_id += 1;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut c = crate::server::Client::new(id, String::new(), String::new(), tx, true);
    c.flags |= flags;
    c.user = "default".into();
    s.clients.insert(id, c);
    id
}

/// Parse a buffer of RESP commands (as written to the AOF). Returns the parsed
/// commands and whether the tail was truncated.
fn parse_commands(data: &[u8]) -> Result<(Vec<Vec<Bytes>>, usize, bool), String> {
    let mut out = Vec::new();
    let mut p = 0;
    let line = |p: &mut usize| -> Option<&[u8]> {
        let start = *p;
        let rel = data[start..].windows(2).position(|w| w == b"\r\n")?;
        *p = start + rel + 2;
        Some(&data[start..start + rel])
    };
    while p < data.len() {
        let start = p;
        if data[p] == b'#' {
            // annotation line (e.g. #TS:...)
            match line(&mut p) {
                Some(_) => continue,
                None => return Ok((out, start, true)),
            }
        }
        if data[p] != b'*' {
            return Err(format!("Bad file format reading the append only file at offset {}", p));
        }
        let Some(l) = line(&mut p) else { return Ok((out, start, true)) };
        let n = crate::util::parse_i64(&l[1..]).ok_or("Bad file format reading the append only file")?;
        if n < 1 {
            return Err("Bad file format reading the append only file".into());
        }
        let mut argv = Vec::with_capacity(n as usize);
        let mut truncated = false;
        for _ in 0..n {
            let Some(l) = line(&mut p) else {
                truncated = true;
                break;
            };
            if l.first() != Some(&b'$') {
                return Err("Bad file format reading the append only file".into());
            }
            let len = crate::util::parse_i64(&l[1..]).ok_or("Bad file format reading the append only file")? as usize;
            if p + len + 2 > data.len() {
                truncated = true;
                break;
            }
            argv.push(Bytes::copy_from_slice(&data[p..p + len]));
            p += len + 2;
        }
        if truncated {
            return Ok((out, start, true));
        }
        out.push(argv);
    }
    Ok((out, p, false))
}

fn replay(s: &mut Store, cid: u64, cmds: Vec<Vec<Bytes>>) -> Result<(), String> {
    let mut db = 0usize;
    let mut multi: Option<Vec<Vec<Bytes>>> = None;
    for argv in cmds {
        let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
        if name == "multi" {
            multi = Some(Vec::new());
            continue;
        }
        if name == "exec" {
            if let Some(q) = multi.take() {
                for a in q {
                    exec_one(s, cid, &mut db, &a)?;
                }
            }
            continue;
        }
        if let Some(q) = multi.as_mut() {
            q.push(argv);
            continue;
        }
        exec_one(s, cid, &mut db, &argv)?;
    }
    // An unterminated MULTI at the end of the file is discarded, like Redis does.
    Ok(())
}

fn exec_one(s: &mut Store, cid: u64, db: &mut usize, argv: &[Bytes]) -> Result<(), String> {
    if crate::util::eq_ic(&argv[0], "select") {
        let n = argv.get(1).and_then(|a| crate::util::parse_i64(a)).ok_or("Invalid SELECT in AOF")?;
        if n < 0 || n as usize >= s.dbs.len() {
            return Err("Invalid SELECT in AOF".into());
        }
        *db = n as usize;
        if let Some(c) = s.clients.get_mut(&cid) {
            c.db = *db;
        }
        return Ok(());
    }
    if crate::cmd::lookup(&argv[0]).is_none() {
        return Err(format!("Unknown command '{}' reading the append only file", String::from_utf8_lossy(&argv[0])));
    }
    crate::exec::exec_internal(s, cid, *db, argv, crate::exec::CTX_LOADING);
    s.repl.pending.clear();
    Ok(())
}

fn load_one(s: &mut Store, cid: u64, path: &Path, last: bool) -> Result<u64, String> {
    let data = std::fs::read(path).map_err(|e| format!("Fatal error: can't open the append log file {} for reading: {}", path.display(), e))?;
    if data.starts_with(b"REDIS") {
        let l = crate::rdb::parse(&data, s.dbs.len())?;
        crate::rdb::install(s, l);
        return Ok(data.len() as u64);
    }
    let (cmds, valid, truncated) = parse_commands(&data)?;
    if truncated {
        if last && s.cfg.aof_load_truncated {
            eprintln!("!!! Warning: short read while loading the AOF file {}!!! AOF loaded anyway because aof-load-truncated is enabled", path.display());
            // Truncate to the last valid command so appends continue cleanly.
            if let Ok(f) = OpenOptions::new().write(true).open(path) {
                let _ = f.set_len(valid as u64);
            }
        } else {
            return Err(format!("Unexpected end of file reading the append only file {}. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.", path.display()));
        }
    }
    replay(s, cid, cmds)?;
    Ok(valid as u64)
}

/// Load the AOF set. Returns the number of keys loaded, or 0 when no AOF exists.
pub fn load(s: &mut Store) -> Result<usize, String> {
    let dir = dir_of(s);
    let mname = manifest_name(s);
    let saved = s.aof.take();
    let m = match std::fs::read_to_string(dir.join(&mname)) {
        Ok(t) => Manifest::parse(&t)?,
        Err(_) => {
            // Legacy single-file AOF in `dir`.
            let legacy = Path::new(&s.cfg.dir).join(&s.cfg.appendfilename);
            if legacy.exists() {
                let cid = pseudo_client(s, crate::server::cflags::MASTER);
                s.persist.loading = true;
                let r = load_one(s, cid, &legacy, true);
                s.persist.loading = false;
                s.clients.remove(&cid);
                s.aof = saved;
                r?;
                return Ok(s.dbs.iter().map(|d| d.dict.len()).sum());
            }
            s.aof = saved;
            return Ok(0);
        }
    };
    for d in s.dbs.iter_mut() {
        d.clear();
    }
    let cid = pseudo_client(s, crate::server::cflags::MASTER);
    s.persist.loading = true;
    let files: Vec<&ManifestEntry> = m.base.iter().chain(m.incrs.iter()).collect();
    let n = files.len();
    let mut res = Ok(());
    let mut base_size = 0;
    let mut total = 0;
    for (i, e) in files.into_iter().enumerate() {
        let p = dir.join(&e.name);
        if !p.exists() {
            res = Err(format!("The AOF file {} doesn't exist", e.name));
            break;
        }
        match load_one(s, cid, &p, i + 1 == n) {
            Ok(sz) => {
                if e.ftype == FileType::Base {
                    base_size = sz;
                }
                total += sz;
            }
            Err(er) => {
                res = Err(er);
                break;
            }
        }
    }
    s.persist.loading = false;
    s.clients.remove(&cid);
    s.persist.aof_base_size = base_size;
    s.persist.aof_current_size = total;
    s.aof = saved;
    res?;
    Ok(s.dbs.iter().map(|d| d.dict.len()).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn manifest_roundtrip() {
        let t = "file appendonly.aof.1.base.rdb seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\nfile appendonly.aof.2.incr.aof seq 2 type i\n";
        let m = Manifest::parse(t).unwrap();
        assert_eq!(m.render(), t);
    }
    #[test]
    fn parse_truncated() {
        let mut d = encode_command(&[Bytes::from_static(b"SET"), Bytes::from_static(b"a"), Bytes::from_static(b"1")]);
        d.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb");
        let (cmds, valid, trunc) = parse_commands(&d).unwrap();
        assert_eq!(cmds.len(), 1);
        assert!(trunc);
        assert_eq!(valid, 27);
    }
}
