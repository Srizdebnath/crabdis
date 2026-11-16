# crabdis

A Redis 7.4-compatible in-memory data store, written from scratch in Rust.

crabdis speaks the Redis protocol (RESP2 and RESP3), so existing Redis clients and tools —
`redis-cli`, `redis-benchmark`, `redis-cli --cluster`, and client libraries — work against it
unchanged. It reads and writes the real Redis RDB format, so snapshots move between crabdis
and Redis in both directions.

> crabdis is an independent project. It is not affiliated with or endorsed by Redis Ltd.
> No Redis source code is included; Redis was used only as a behavioural reference.

## Quick start

```sh
cargo build --release
./target/release/crabdis                       # listens on 6379
./target/release/crabdis --port 7777 --appendonly yes
./target/release/crabdis /path/to/redis.conf --loglevel verbose
```

Then use any Redis client:

```sh
redis-cli -p 6379 set hello world
redis-cli -p 6379 get hello
```

Configuration uses the same directive names as `redis.conf`, either from a file or as
`--name value` arguments. `CONFIG GET` / `CONFIG SET` / `CONFIG REWRITE` are supported.

## What's implemented

251 commands (`COMMAND COUNT`), covering:

| Area | Details |
|---|---|
| Protocol | RESP2 and RESP3 (`HELLO`), inline commands, pipelining, RESP3 push |
| Data types | strings, bitmaps and `BITFIELD`, lists, hashes (including 7.4 per-field TTLs: `HEXPIRE`, `HTTL`, …), sets, sorted sets (skiplist), HyperLogLog (Redis-compatible dense/sparse encoding), geo, streams with consumer groups |
| Keyspace | 16 databases, lazy and active expiry, `SCAN` family, `SORT`, `OBJECT`, `DUMP`/`RESTORE` |
| Blocking | `BLPOP`, `BLMOVE`, `BZPOPMIN`, `BZMPOP`, `XREAD`/`XREADGROUP BLOCK`, `WAIT`, `CLIENT PAUSE`/`UNBLOCK` |
| Server | `MULTI`/`EXEC`/`WATCH`, pub/sub (channels, patterns, shard channels), keyspace notifications, client-side caching (`CLIENT TRACKING`), `INFO`, `SLOWLOG`, `LATENCY`, `MONITOR`, `MEMORY USAGE`, maxmemory with LRU/LFU/random/TTL eviction |
| Security | ACLs: users, SHA-256 passwords, command/category/key/channel permissions, selectors, `ACL LOG`, ACL files |
| Scripting | `EVAL`/`EVALSHA`/`SCRIPT` and `FUNCTION`/`FCALL` on Lua 5.1, with `cjson`, `cmsgpack`, `bit` and `struct`; `SCRIPT KILL`/`FUNCTION KILL` and `BUSY` replies |
| Persistence | RDB v12 (LZF, CRC64; loads ziplist/intset/listpack encodings), `SAVE`/`BGSAVE`/save points; multi-part AOF (manifest, base + incr files, RDB preamble, `appendfsync` policies, `BGREWRITEAOF`) |
| Replication | `REPLICAOF`, `PSYNC` full and partial resync with backlog, diskless loading, `WAIT`, `WAITAOF`, `FAILOVER`, `min-replicas-to-write` |
| Cluster | 16384 hash slots, hash tags, `MOVED`/`ASK`/`TRYAGAIN`/`CROSSSLOT`, all `CLUSTER` subcommands, gossip and failure detection, automatic and manual replica failover, `nodes.conf` |

## Verified against real Redis tooling

These were run with the official Redis 7.4 `redis-cli` and `redis-benchmark`:

- **Replication:** full sync, live streaming, read-only replicas, `WAIT`, partial resync after
  a dropped link, and a full resync after a promoted replica has diverged.
- **Cluster:** a 6-node cluster (3 masters, 3 replicas) created with `redis-cli --cluster create`;
  redirects and `-c` routing; after `kill -9` of a master its replica is promoted, and the
  restarted old master rejoins as a replica; `redis-cli --cluster check` reports no errors.
- **AOF recovery:** every data type, TTLs, hash-field TTLs, stream groups and function libraries
  survive `kill -9`, both before and after `BGREWRITEAOF`.
- **RDB compatibility:** Redis 7.4 loads crabdis snapshots, and crabdis loads Redis 7.4 snapshots.

## Performance

`redis-benchmark -n 200000 -c 50` on a 4-core machine, with the server and benchmark on the same host,
in requests per second:

| Command | crabdis | Redis 7.4 | crabdis `-P 16` | Redis 7.4 `-P 16` |
|---|---:|---:|---:|---:|
| SET | 53,490 | 66,600 | 327,869 | 527,705 |
| GET | 58,565 | 67,682 | 366,972 | 699,301 |
| INCR | 58,928 | 61,595 | 309,598 | 613,497 |
| LPUSH | 59,067 | 68,190 | 273,224 | 503,778 |
| HSET | 56,657 | 62,933 | 310,559 | 550,964 |
| ZADD | 57,787 | 64,495 | 308,642 | 475,059 |
| LRANGE_100 | 28,470 | 34,947 | 62,364 | 73,206 |
| MSET (10 keys) | 56,689 | 55,142 | 193,798 | 194,553 |

Without pipelining crabdis is within roughly 10–20% of Redis. With deep pipelining it is
slower, because it takes the store lock once per command; batching a pipeline under a single lock
is on the roadmap.

## Known differences from Redis

- The Lua debugger (`SCRIPT DEBUG YES|SYNC`) is not available.
- `redis.set_repl()` with AOF-only or replica-only targets propagates to both.
- The cluster bus uses crabdis's own binary format, so crabdis nodes cannot join a Redis cluster.
  Clients and `redis-cli --cluster` work normally.
- Sentinel and the Modules API are not implemented.

## Project layout

```
src/main.rs          startup, CLI/config parsing, listeners, signals
src/conn.rs          per-client connection loop
src/exec.rs          command dispatch: ACL, cluster redirects, MULTI, propagation
src/cmd/             command implementations, one file per area
src/resp.rs          RESP2/RESP3 parser and encoder
src/db.rs value.rs   keyspace and value types
src/dict.rs zset.rs stream.rs lp.rs   data structures and encodings
src/rdb.rs aof.rs    persistence
src/repl.rs          replication
src/cluster.rs       cluster state and bus
src/scripting.rs luaext.rs   Lua engine and libraries
src/acl.rs           access control
```

## Contributing

Open items are listed in [TODO.md](TODO.md). The section *M8 — Hardening* is a good place to start.
Run `cargo test` and `cargo build --release` before opening a pull request. For behaviour changes,
check them against a real Redis 7.4 using `redis-cli`.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
