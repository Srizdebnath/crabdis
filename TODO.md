# crabdis roadmap

A from-scratch Rust implementation of a Redis-compatible server. Each box is
checked only when it works against real clients (`redis-cli`, `redis-benchmark`,
the upstream Tcl test suite run in external-server mode).

## M1 — Core server
- [x] RESP2 + RESP3 parser/encoder, inline commands, pipelining
- [x] HELLO / AUTH / SELECT / PING / ECHO / QUIT / RESET
- [x] 16 databases, lazy + active expiry
- [x] Command table with arity/flags/key specs (COMMAND, COMMAND INFO/COUNT/LIST/GETKEYS)
- [x] Generic keyspace: DEL UNLINK EXISTS TYPE RENAME RENAMENX MOVE COPY KEYS SCAN RANDOMKEY TOUCH
      EXPIRE* PEXPIRE* TTL PTTL PERSIST EXPIRETIME OBJECT DBSIZE FLUSHDB FLUSHALL SWAPDB SORT

## M2 — Data types
- [x] Strings (incl. INCR family, GETEX, GETDEL, SETRANGE, GETRANGE, LCS, MSETNX)
- [x] Bitmaps (SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD BITFIELD_RO)
- [x] Lists (incl. LPOS LMPOP LMOVE, blocking BLPOP BRPOP BLMOVE BLMPOP BRPOPLPUSH)
- [x] Hashes (incl. HRANDFIELD HSCAN, 7.4 field expiry HEXPIRE …)
- [x] Sets (incl. SINTERCARD SMISMEMBER SSCAN)
- [x] Sorted sets (skiplist; ZRANGE family, ZRANGESTORE, ZUNION/ZINTER/ZDIFF, BZPOPMIN/MAX, BZMPOP, ZRANDMEMBER)
- [x] HyperLogLog (PFADD PFCOUNT PFMERGE, Redis-compatible dense/sparse encoding)
- [x] Geo (GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE GEORADIUS*)
- [x] Streams (XADD XRANGE XREAD XTRIM XDEL, consumer groups XGROUP XREADGROUP XACK XPENDING XCLAIM XAUTOCLAIM XINFO)

## M3 — Server semantics
- [x] MULTI / EXEC / DISCARD / WATCH / UNWATCH
- [x] Pub/Sub (channels, patterns, shard channels), RESP3 push
- [x] Keyspace notifications
- [x] Client tracking (CLIENT TRACKING, invalidation)
- [x] CLIENT LIST/KILL/INFO/ID/SETNAME/GETNAME/PAUSE/UNPAUSE/UNBLOCK/NO-EVICT/REPLY
- [x] INFO, CONFIG GET/SET/RESETSTAT/REWRITE, redis.conf parsing
- [x] SLOWLOG, LATENCY, MONITOR, MEMORY USAGE, DEBUG subset
- [x] maxmemory + eviction policies (LRU/LFU/random/volatile-ttl)
- [x] ACL (users, passwords, command/key/channel permissions)

## M4 — Persistence
- [x] RDB writer/loader (real format, LZF, CRC64), SAVE/BGSAVE, save points
- [x] DUMP / RESTORE
- [x] AOF (multi-part, manifest, fsync policies), BGREWRITEAOF
- [ ] crabdis-check-rdb / crabdis-check-aof

## M5 — Scripting
- [x] EVAL / EVALSHA / SCRIPT (Lua 5.1, redis.call/pcall, cjson, cmsgpack, bit)
- [x] FUNCTION / FCALL

## M6 — Distribution
- [x] Replication (REPLICAOF, PSYNC, full + partial resync, WAIT)
- [ ] Sentinel
- [x] Cluster (slots, MOVED/ASK, gossip bus)

## M7 — Tools
- [ ] crabdis-cli
- [ ] crabdis-benchmark

## M8 — Hardening (good first contributions)
- [ ] Run the upstream Tcl test suite against crabdis and fix failures
- [ ] Pipelined throughput: batch a whole pipeline under one store lock (currently ~50-65% of Redis at -P 16)
- [ ] Lua debugger (SCRIPT DEBUG / LDB)
- [ ] redis.set_repl(): honour AOF-only / replica-only targets (currently treated as ALL)
- [ ] Interoperate with the real Redis cluster bus protocol (crabdis nodes only talk to crabdis nodes)

## Out of scope (for now)
- Redis Modules C API
