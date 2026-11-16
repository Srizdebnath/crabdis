# crabdis roadmap

A from-scratch Rust implementation of a Redis-compatible server. Each box is
checked only when it works against real clients (`redis-cli`, `redis-benchmark`,
the upstream Tcl test suite run in external-server mode).

## M1 — Core server
- [ ] RESP2 + RESP3 parser/encoder, inline commands, pipelining
- [ ] HELLO / AUTH / SELECT / PING / ECHO / QUIT / RESET
- [ ] 16 databases, lazy + active expiry
- [ ] Command table with arity/flags/key specs (COMMAND, COMMAND INFO/COUNT/LIST/GETKEYS)
- [ ] Generic keyspace: DEL UNLINK EXISTS TYPE RENAME RENAMENX MOVE COPY KEYS SCAN RANDOMKEY TOUCH
      EXPIRE* PEXPIRE* TTL PTTL PERSIST EXPIRETIME OBJECT DBSIZE FLUSHDB FLUSHALL SWAPDB SORT

## M2 — Data types
- [ ] Strings (incl. INCR family, GETEX, GETDEL, SETRANGE, GETRANGE, LCS, MSETNX)
- [ ] Bitmaps (SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD BITFIELD_RO)
- [ ] Lists (incl. LPOS LMPOP LMOVE, blocking BLPOP BRPOP BLMOVE BLMPOP BRPOPLPUSH)
- [ ] Hashes (incl. HRANDFIELD HSCAN, 7.4 field expiry HEXPIRE …)
- [ ] Sets (incl. SINTERCARD SMISMEMBER SSCAN)
- [ ] Sorted sets (skiplist; ZRANGE family, ZRANGESTORE, ZUNION/ZINTER/ZDIFF, BZPOPMIN/MAX, BZMPOP, ZRANDMEMBER)
- [ ] HyperLogLog (PFADD PFCOUNT PFMERGE, Redis-compatible dense/sparse encoding)
- [ ] Geo (GEOADD GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE GEORADIUS*)
- [ ] Streams (XADD XRANGE XREAD XTRIM XDEL, consumer groups XGROUP XREADGROUP XACK XPENDING XCLAIM XAUTOCLAIM XINFO)

## M3 — Server semantics
- [ ] MULTI / EXEC / DISCARD / WATCH / UNWATCH
- [ ] Pub/Sub (channels, patterns, shard channels), RESP3 push
- [ ] Keyspace notifications
- [ ] Client tracking (CLIENT TRACKING, invalidation)
- [ ] CLIENT LIST/KILL/INFO/ID/SETNAME/GETNAME/PAUSE/UNPAUSE/UNBLOCK/NO-EVICT/REPLY
- [ ] INFO, CONFIG GET/SET/RESETSTAT/REWRITE, redis.conf parsing
- [ ] SLOWLOG, LATENCY, MONITOR, MEMORY USAGE, DEBUG subset
- [ ] maxmemory + eviction policies (LRU/LFU/random/volatile-ttl)
- [ ] ACL (users, passwords, command/key/channel permissions)

## M4 — Persistence
- [ ] RDB writer/loader (real format, LZF, CRC64), SAVE/BGSAVE, save points
- [ ] DUMP / RESTORE
- [ ] AOF (multi-part, manifest, fsync policies), BGREWRITEAOF
- [ ] crabdis-check-rdb / crabdis-check-aof

## M5 — Scripting
- [ ] EVAL / EVALSHA / SCRIPT (Lua 5.1, redis.call/pcall, cjson, cmsgpack, bit)
- [ ] FUNCTION / FCALL

## M6 — Distribution
- [ ] Replication (REPLICAOF, PSYNC, full + partial resync, WAIT)
- [ ] Sentinel
- [ ] Cluster (slots, MOVED/ASK, gossip bus)

## M7 — Tools
- [ ] crabdis-cli
- [ ] crabdis-benchmark

## Out of scope (for now)
- Redis Modules C API
