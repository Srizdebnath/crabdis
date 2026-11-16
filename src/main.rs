//! crabdis: a Redis-compatible in-memory data store written in Rust.

mod acl;
mod aof;
mod blocking;
mod cluster;
mod cmd;
mod config;
mod conn;
mod db;
mod dict;
mod evict;
mod exec;
mod expire;
mod latency;
mod lp;
mod luaext;
mod mem;
mod pubsub;
mod rdb;
mod repl;
mod resp;
mod scripting;
mod server;
mod stream;
mod tracking;
mod util;
mod value;
mod zset;

use server::{Shared, Store};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

#[global_allocator]
static GLOBAL: mem::Counting = mem::Counting;

pub static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();

/// Orderly shutdown (SHUTDOWN, SIGTERM): flush AOF, save RDB when configured.
pub fn shutdown(s: &mut Store, nosave: bool, save: bool) -> Result<(), String> {
    eprintln!("User requested shutdown...");
    if s.aof.is_some() {
        eprintln!("Calling fsync() on the AOF file.");
        aof::flush_now(s);
    }
    if save || (!nosave && !s.cfg.save_params.is_empty()) {
        eprintln!("Saving the final RDB snapshot before exiting.");
        if let Err(e) = rdb::save(s) {
            eprintln!("Error trying to save the DB, can't exit.");
            return Err(e);
        }
        eprintln!("DB saved on disk");
    }
    if let Some(c) = s.cluster.as_ref() {
        let _ = c.save_config();
    }
    if !s.cfg.pidfile.is_empty() {
        let _ = std::fs::remove_file(&s.cfg.pidfile);
    }
    if !s.cfg.unixsocket.is_empty() {
        let _ = std::fs::remove_file(&s.cfg.unixsocket);
    }
    eprintln!("crabdis is now ready to exit, bye bye...");
    Ok(())
}

fn usage() -> ! {
    eprintln!("Usage: ./crabdis [/path/to/redis.conf] [options] [-]");
    eprintln!("       ./crabdis - (read config from stdin)");
    eprintln!("       ./crabdis -v or --version");
    eprintln!("       ./crabdis -h or --help");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("       ./crabdis (run the server with default config)");
    eprintln!("       ./crabdis /etc/redis/6379.conf");
    eprintln!("       ./crabdis --port 7777");
    eprintln!("       ./crabdis --port 7777 --replicaof 127.0.0.1 8888");
    eprintln!("       ./crabdis /etc/myredis.conf --loglevel verbose -");
    std::process::exit(1);
}

/// Parse argv like redis-server: optional config file, then `--name value...`.
fn build_config() -> config::Config {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cfg = config::Config::new();
    let mut i = 0;
    if let Some(a) = args.first() {
        match a.as_str() {
            "-v" | "--version" => {
                println!("crabdis server v={} (Redis {} compatible) bits=64 rust", env!("CARGO_PKG_VERSION"), server::VERSION);
                std::process::exit(0);
            }
            "-h" | "--help" => usage(),
            _ => {}
        }
        if !a.starts_with("--") && a != "-" {
            if let Err(e) = cfg.load_file(a) {
                eprintln!("{}", e);
                std::process::exit(1);
            }
            i = 1;
        }
    }
    let mut text = String::new();
    let mut read_stdin = false;
    while i < args.len() {
        let a = &args[i];
        if a == "-" {
            read_stdin = true;
            i += 1;
            continue;
        }
        if let Some(name) = a.strip_prefix("--") {
            let mut vals = Vec::new();
            i += 1;
            while i < args.len() && !args[i].starts_with("--") && args[i] != "-" {
                vals.push(args[i].clone());
                i += 1;
            }
            let quoted: Vec<String> = vals.iter().map(|v| if v.is_empty() || v.contains([' ', '"']) { format!("\"{}\"", v.replace('"', "\\\"")) } else { v.clone() }).collect();
            text.push_str(&format!("{} {}\n", name, quoted.join(" ")));
        } else {
            eprintln!("Invalid argument '{}'", a);
            usage();
        }
    }
    if read_stdin {
        let mut s = String::new();
        let _ = std::io::Read::read_to_string(&mut std::io::stdin(), &mut s);
        text.push_str(&s);
    }
    if !text.is_empty() {
        if let Err(e) = cfg.load_string(&text) {
            eprintln!("\n*** FATAL CONFIG FILE ERROR (crabdis {}) ***\n{}", env!("CARGO_PKG_VERSION"), e);
            std::process::exit(1);
        }
    }
    cfg
}

fn load_data(s: &mut Store) {
    let start = std::time::Instant::now();
    s.persist.loading = true;
    let res = if s.cfg.appendonly { aof::load(s).map(|n| ("append only file", n)) } else { rdb::load_file(s).map(|n| ("disk", n)) };
    s.persist.loading = false;
    match res {
        Ok((what, n)) => {
            if n > 0 || what == "append only file" {
                eprintln!("DB loaded from {}: {:.3} seconds ({} keys)", what, start.elapsed().as_secs_f64(), n);
            }
        }
        Err(e) => {
            eprintln!("Fatal error loading the DB: {}. Exiting.", e);
            std::process::exit(1);
        }
    }
    if s.cfg.appendonly {
        if let Err(e) = aof::open(s) {
            eprintln!("Can't open the append-only file: {}", e);
            std::process::exit(1);
        }
    }
    s.persist.lastsave = util::now_ms() / 1000;
}

async fn serve_tcp(shared: Arc<Shared>, listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let _ = stream.set_nodelay(true);
                let laddr = stream.local_addr().map(|a| a.to_string()).unwrap_or_default();
                tokio::spawn(conn::handle(shared.clone(), stream, addr.to_string(), laddr, false));
            }
            Err(e) => {
                eprintln!("Accepting client connection: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

#[cfg(unix)]
async fn serve_unix(shared: Arc<Shared>, listener: tokio::net::UnixListener, path: String) {
    loop {
        if let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(conn::handle(shared.clone(), stream, format!("{}:0", path), path.clone(), true));
        }
    }
}

fn main() {
    let _ = server::START_INSTANT.set(std::time::Instant::now());
    let cfg = build_config();
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).clamp(2, 16);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().expect("tokio runtime");
    rt.block_on(async_main(cfg));
}

async fn async_main(cfg: config::Config) {
    if !cfg.dir.is_empty() && cfg.dir != "." {
        if let Err(e) = std::env::set_current_dir(&cfg.dir) {
            eprintln!("Can't chdir to '{}': {}", cfg.dir, e);
            std::process::exit(1);
        }
    }
    let pid = std::process::id();
    eprintln!("{}:C {} # oO0OoO0OoO0Oo crabdis is starting oO0OoO0OoO0Oo", pid, util::log_time());
    eprintln!(
        "{}:C {} # crabdis version={}, Redis compatibility={}, bits=64, pid={}, just started",
        pid,
        util::log_time(),
        env!("CARGO_PKG_VERSION"),
        server::VERSION,
        pid
    );
    let mut store = Store::new(cfg);
    store.executable = std::env::current_exe().map(|p| p.display().to_string()).unwrap_or_default();
    if store.cfg.cluster_enabled {
        if let Err(e) = cluster::init(&mut store) {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    }
    load_data(&mut store);
    let shared = Arc::new(Shared {
        store: parking_lot::Mutex::new(store),
        shutdown: tokio::sync::Notify::new(),
        client_count: AtomicU64::new(0),
        wakeup_aof: tokio::sync::Notify::new(),
    });
    let _ = SHARED.set(shared.clone());

    let (port, binds, unixsock, pidfile, cluster_bus) = {
        let s = shared.store.lock();
        (s.cfg.port, s.cfg.bind.clone(), s.cfg.unixsocket.clone(), s.cfg.pidfile.clone(), s.cluster.as_ref().map(|c| c.bus_port))
    };
    if !pidfile.is_empty() {
        let _ = std::fs::write(&pidfile, format!("{}\n", pid));
    }
    let mut bound = 0;
    if port != 0 {
        for b in &binds {
            let (addr, optional) = match b.strip_prefix('-') {
                Some(a) => (a, true),
                None => (b.as_str(), false),
            };
            let host = match addr {
                "*" => "0.0.0.0",
                "::*" => "::",
                a => a,
            };
            let res = if host.contains(':') {
                // IPv6: bind with v6only so it can coexist with the IPv4 wildcard.
                let sock = tokio::net::TcpSocket::new_v6();
                sock.and_then(|s| {
                    let _ = s.set_reuseaddr(true);
                    let sa: std::net::SocketAddr = format!("[{}]:{}", host, port).parse().map_err(|_| std::io::Error::other("bad address"))?;
                    let raw = socket2_v6only(&s);
                    let _ = raw;
                    s.bind(sa)?;
                    s.listen(1024)
                })
            } else {
                let sock = tokio::net::TcpSocket::new_v4();
                sock.and_then(|s| {
                    let _ = s.set_reuseaddr(true);
                    let sa: std::net::SocketAddr = format!("{}:{}", host, port).parse().map_err(|_| std::io::Error::other("bad address"))?;
                    s.bind(sa)?;
                    s.listen(1024)
                })
            };
            match res {
                Ok(l) => {
                    bound += 1;
                    tokio::spawn(serve_tcp(shared.clone(), l));
                }
                Err(e) => {
                    if !optional {
                        eprintln!("Warning: Could not create server TCP listening socket {}:{}: {}", host, port, e);
                        eprintln!("Failed listening on port {} (tcp), aborting.", port);
                        std::process::exit(1);
                    }
                }
            }
        }
        if bound == 0 {
            eprintln!("Failed listening on port {} (tcp), aborting.", port);
            std::process::exit(1);
        }
    }
    #[cfg(unix)]
    if !unixsock.is_empty() {
        let _ = std::fs::remove_file(&unixsock);
        match tokio::net::UnixListener::bind(&unixsock) {
            Ok(l) => {
                tokio::spawn(serve_unix(shared.clone(), l, unixsock.clone()));
            }
            Err(e) => {
                eprintln!("Failed opening Unix socket: {}", e);
                std::process::exit(1);
            }
        }
    }
    if let Some(bus) = cluster_bus {
        tokio::spawn(cluster::listen(shared.clone(), "0.0.0.0".into(), bus));
    }
    // Replication configured in the config file.
    {
        let mut s = shared.store.lock();
        if let Some((h, p)) = s.cfg.replicaof.clone() {
            repl::set_master(&mut s, shared.clone(), h, p);
        }
    }
    tokio::spawn(server::cron(shared.clone()));
    eprintln!("{}:M {} * Ready to accept connections tcp (port {})", pid, util::log_time(), port);

    // Signals: SIGTERM/SIGINT perform a graceful shutdown.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("signal handler");
        let mut int = signal(SignalKind::interrupt()).expect("signal handler");
        tokio::select! {
            _ = term.recv() => eprintln!("Received SIGTERM scheduling shutdown..."),
            _ = int.recv() => eprintln!("Received SIGINT scheduling shutdown..."),
            _ = shared.shutdown.notified() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    let mut s = shared.store.lock();
    let (nosave, save) = s.shutdown_flags;
    if shutdown(&mut s, nosave, save).is_err() {
        eprintln!("SIGTERM received but errors trying to shut down the server, check the logs for more information");
    }
    std::process::exit(0);
}

/// IPv6 listeners are made v6-only so `bind * -::*` works like Redis.
fn socket2_v6only(_s: &tokio::net::TcpSocket) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let fd = _s.as_raw_fd();
        let on: i32 = 1;
        // SAFETY: setting a socket option on a valid, owned file descriptor.
        unsafe {
            libc_setsockopt(fd, 41 /* IPPROTO_IPV6 */, 26 /* IPV6_V6ONLY */, &on as *const i32 as *const std::ffi::c_void, 4);
        }
    }
}

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "setsockopt"]
    fn libc_setsockopt(fd: i32, level: i32, name: i32, val: *const std::ffi::c_void, len: u32) -> i32;
}
