//! Per-connection task: socket I/O, request parsing, pipelining, replies,
//! out-of-band pushes, blocking waits.

use crate::exec::{self, Outcome};
use crate::resp::{self, Frame};
use crate::server::{Client, Shared, cflags};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

pub async fn handle<S>(shared: Arc<Shared>, stream: S, addr: String, laddr: String, unix: bool)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<Frame>();
    let full = {
        let mut s = shared.store.lock();
        s.stats.total_connections += 1;
        let full = s.clients.len() >= s.cfg.maxclients;
        if full {
            s.stats.rejected_connections += 1;
        }
        full
    };
    if full {
        let _ = wr.write_all(b"-ERR max number of clients reached\r\n").await;
        return;
    }
    let (cid, kill, killed) = {
        let mut s = shared.store.lock();
        let id = s.next_client_id;
        s.next_client_id += 1;
        let authed = crate::acl::default_user_authenticated(&s);
        let mut c = Client::new(id, addr, laddr, push_tx, authed);
        if unix {
            c.flags |= cflags::UNIX;
        }
        let (k, kd) = (c.kill.clone(), c.killed.clone());
        s.clients.insert(id, c);
        (id, k, kd)
    };
    shared.client_count.fetch_add(1, Ordering::Relaxed);

    let mut inbuf = BytesMut::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut pending: VecDeque<Vec<Bytes>> = VecDeque::new();
    let mut proto: u8 = 2;
    let mut close = false;
    let mut max_bulk = 512usize << 20;
    let mut qbuf_limit = 1usize << 30;

    'main: loop {
        if killed.load(Ordering::SeqCst) {
            break;
        }
        // Parse as many complete commands as possible.
        loop {
            match resp::parse_command(&mut inbuf, max_bulk) {
                Ok(Some(args)) => pending.push_back(args),
                Ok(None) => break,
                Err(resp::ProtoError::Msg(m)) => {
                    // Execute what was parsed before the error, then reply with the error.
                    close = true;
                    pending.push_back(vec![Bytes::from_static(b"\x00__protoerr__"), Bytes::from(m)]);
                    break;
                }
            }
        }
        if inbuf.len() > qbuf_limit {
            let mut s = shared.store.lock();
            s.stats.client_qbuf_limit_disconnections += 1;
            break;
        }

        // Execute pending commands under one lock acquisition.
        while !pending.is_empty() {
            let mut blocked_rx = None;
            let mut become_replica = false;
            // A busy script holds the store: answer BUSY / SCRIPT KILL without the lock.
            let mut answered_busy = false;
            while let Some(f) = pending.front().and_then(|a| crate::scripting::busy_reply(a)) {
                pending.pop_front();
                f.encode(proto, &mut out);
                answered_busy = true;
            }
            if answered_busy {
                if wr.write_all(&out).await.is_err() {
                    break 'main;
                }
                out.clear();
                continue;
            }
            {
                let mut s = shared.store.lock();
                while let Some(args) = pending.pop_front() {
                    if args.len() == 2 && args[0].as_ref() == b"\x00__protoerr__" {
                        Frame::err(String::from_utf8_lossy(&args[1]).into_owned()).encode(proto, &mut out);
                        pending.clear();
                        break;
                    }
                    let skip_this = match s.clients.get_mut(&cid) {
                        Some(c) => {
                            c.qbuf = inbuf.len();
                            if c.has(cflags::REPLY_SKIP_NEXT) {
                                c.flags &= !cflags::REPLY_SKIP_NEXT;
                                c.flags |= cflags::REPLY_SKIP;
                            }
                            false
                        }
                        None => {
                            close = true;
                            break;
                        }
                    };
                    let _ = skip_this;
                    let outcome = exec::process(&mut s, cid, args);
                    // Drain pushes generated during execution so ordering matches Redis.
                    let (reply_off, reply_skip, newproto, is_replica) = match s.clients.get_mut(&cid) {
                        Some(c) => {
                            let r = (
                                c.has(cflags::REPLY_OFF),
                                c.has(cflags::REPLY_SKIP),
                                c.proto,
                                c.has(cflags::REPLICA),
                            );
                            c.flags &= !cflags::REPLY_SKIP;
                            r
                        }
                        None => (false, false, proto, false),
                    };
                    while let Ok(p) = push_rx.try_recv() {
                        p.encode(newproto, &mut out);
                    }
                    let suppress = reply_off || reply_skip;
                    match outcome {
                        Outcome::Reply(f) => {
                            if !suppress {
                                f.encode(proto, &mut out);
                            }
                        }
                        Outcome::Close(f) => {
                            if !suppress {
                                f.encode(proto, &mut out);
                            }
                            close = true;
                            pending.clear();
                            break;
                        }
                        Outcome::Block(rx) => {
                            blocked_rx = Some(rx);
                        }
                    }
                    proto = newproto;
                    max_bulk = s.cfg.proto_max_bulk_len;
                    qbuf_limit = s.cfg.client_query_buffer_limit;
                    if is_replica {
                        // Connection becomes a replication stream to a replica.
                        become_replica = true;
                        break;
                    }
                    if blocked_rx.is_some() {
                        break;
                    }
                }
                if !out.is_empty() {
                    s.stats.net_output += out.len() as u64;
                    if let Some(c) = s.clients.get_mut(&cid) {
                        c.tot_net_out += out.len() as u64;
                    }
                }
            }
            if become_replica {
                if !out.is_empty() && wr.write_all(&out).await.is_err() {
                    break 'main;
                }
                out.clear();
                crate::repl::serve_replica(shared.clone(), cid, rd, wr, push_rx, inbuf).await;
                cleanup(&shared, cid);
                return;
            }
            if let Some(rx) = blocked_rx {
                if !out.is_empty() {
                    if wr.write_all(&out).await.is_err() {
                        break 'main;
                    }
                    out.clear();
                }
                match wait_blocked(&shared, cid, rx, &mut rd, &mut inbuf, &kill, &killed, &mut push_rx, &mut wr, proto).await {
                    Some(f) => {
                        let suppress = {
                            let mut s = shared.store.lock();
                            match s.clients.get_mut(&cid) {
                                Some(c) => {
                                    let r = c.has(cflags::REPLY_OFF) || c.has(cflags::REPLY_SKIP);
                                    c.flags &= !cflags::REPLY_SKIP;
                                    r
                                }
                                None => true,
                            }
                        };
                        while let Ok(p) = push_rx.try_recv() {
                            p.encode(proto, &mut out);
                        }
                        if !suppress {
                            f.encode(proto, &mut out);
                        }
                        // Re-parse anything that arrived while blocked.
                        loop {
                            match resp::parse_command(&mut inbuf, max_bulk) {
                                Ok(Some(args)) => pending.push_back(args),
                                Ok(None) => break,
                                Err(resp::ProtoError::Msg(m)) => {
                                    close = true;
                                    pending.push_back(vec![Bytes::from_static(b"\x00__protoerr__"), Bytes::from(m)]);
                                    break;
                                }
                            }
                        }
                    }
                    None => break 'main,
                }
            }
        }

        if !out.is_empty() {
            if wr.write_all(&out).await.is_err() {
                break;
            }
            if out.capacity() > 1 << 20 {
                out = Vec::with_capacity(16 * 1024);
            } else {
                out.clear();
            }
        }
        if close {
            let _ = wr.flush().await;
            break;
        }

        // Wait for input, a push message, or a kill.
        tokio::select! {
            biased;
            _ = kill.notified() => break,
            msg = push_rx.recv() => {
                match msg {
                    Some(m) => {
                        m.encode(proto, &mut out);
                        let mut n = 0;
                        while let Ok(m) = push_rx.try_recv() {
                            m.encode(proto, &mut out);
                            n += 1;
                            if n > 1024 { break; }
                        }
                        if wr.write_all(&out).await.is_err() { break; }
                        {
                            let mut s = shared.store.lock();
                            s.stats.net_output += out.len() as u64;
                        }
                        out.clear();
                    }
                    None => break,
                }
            }
            r = rd.read_buf(&mut inbuf) => {
                match r {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut s = shared.store.lock();
                        s.stats.net_input += n as u64;
                        s.stats.total_reads_processed += 1;
                        if let Some(c) = s.clients.get_mut(&cid) {
                            c.tot_net_in += n as u64;
                            c.last_interaction_ms = crate::util::now_ms();
                        }
                    }
                }
            }
        }
    }
    cleanup(&shared, cid);
}

#[allow(clippy::too_many_arguments)]
async fn wait_blocked<R, W>(
    shared: &Arc<Shared>,
    cid: u64,
    mut rx: tokio::sync::oneshot::Receiver<Frame>,
    rd: &mut R,
    inbuf: &mut BytesMut,
    kill: &Arc<tokio::sync::Notify>,
    killed: &Arc<std::sync::atomic::AtomicBool>,
    push_rx: &mut mpsc::UnboundedReceiver<Frame>,
    wr: &mut W,
    proto: u8,
) -> Option<Frame>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let deadline = {
        let s = shared.store.lock();
        s.clients.get(&cid).and_then(|c| c.blocked.as_ref()).and_then(|b| b.deadline)
    };
    let mut eof = false;
    loop {
        if killed.load(Ordering::SeqCst) {
            let mut s = shared.store.lock();
            s.unblock_client(cid, Some(Frame::NoReply));
            return None;
        }
        let sleep = async {
            match deadline {
                Some(d) => {
                    let now = crate::util::now_ms();
                    let ms = (d - now).max(0) as u64;
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            r = &mut rx => {
                return r.ok();
            }
            _ = kill.notified() => {
                let mut s = shared.store.lock();
                s.unblock_client(cid, Some(Frame::NoReply));
                return None;
            }
            m = push_rx.recv() => {
                // RESP3 clients may receive pushes while blocked.
                if let Some(m) = m {
                    let mut out = Vec::new();
                    m.encode(proto, &mut out);
                    if wr.write_all(&out).await.is_err() {
                        let mut s = shared.store.lock();
                        s.unblock_client(cid, Some(Frame::NoReply));
                        return None;
                    }
                }
            }
            _ = sleep => {
                {
                    let mut s = shared.store.lock();
                    let still = s.clients.get(&cid).map(|c| c.blocked.is_some()).unwrap_or(false);
                    if still {
                        let wait_reply = crate::repl::wait_timeout_reply(&mut s, cid);
                        s.unblock_client(cid, wait_reply);
                    }
                }
                return rx.await.ok();
            }
            r = rd.read_buf(inbuf), if !eof => {
                match r {
                    Ok(0) | Err(_) => {
                        eof = true;
                        let mut s = shared.store.lock();
                        s.unblock_client(cid, Some(Frame::NoReply));
                        return None;
                    }
                    Ok(n) => {
                        let mut s = shared.store.lock();
                        s.stats.net_input += n as u64;
                    }
                }
            }
        }
    }
}

pub fn cleanup(shared: &Arc<Shared>, cid: u64) {
    let mut s = shared.store.lock();
    free_client(&mut s, cid);
    shared.client_count.fetch_sub(1, Ordering::Relaxed);
}

/// Remove every trace of a client from the store.
pub fn free_client(s: &mut crate::server::Store, cid: u64) {
    s.unblock_client(cid, Some(Frame::NoReply));
    crate::cmd::multi::unwatch_all(s, cid);
    crate::pubsub::unsubscribe_all(s, cid);
    crate::tracking::disable(s, cid);
    s.monitors.retain(|&id| id != cid);
    crate::repl::on_client_free(s, cid);
    s.clients.remove(&cid);
}
