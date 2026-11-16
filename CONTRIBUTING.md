# Contributing to crabdis

Thanks for helping. crabdis aims to behave exactly like Redis 7.4, so the guiding rule is:
**when in doubt, do what Redis does**, including error messages and reply types.

## Getting started

```sh
git clone https://github.com/Srizdebnath/crabdis
cd crabdis
cargo build --release
cargo test
./target/release/crabdis --port 6379
```

The official Redis tools are the easiest way to check behaviour. Docker is enough, so you don't need Redis installed:

```sh
docker run --rm --network host redis:7.4 redis-cli -p 6379 ping
docker run --rm --network host redis:7.4 redis-benchmark -p 6379 -q -t set,get
```

## Finding something to work on

- Browse the [open issues](https://github.com/Srizdebnath/crabdis/issues). Issues labelled
  `good first issue` are small and self-contained.
- Comment on an issue before starting, so two people don't do the same work.
- Found a behaviour difference from Redis? Open an issue that shows the command, what Redis returns and what crabdis returns.

## Where things live

The README's **Architecture** section has diagrams of every subsystem, and each one names its source file.
New commands go in `src/cmd/<area>.rs` and are registered in the command table in `src/cmd/mod.rs`.

## Pull requests

1. Keep each PR focused on one change.
2. Run `cargo build --release` and `cargo test`. Add unit tests for new logic.
3. For behaviour changes, compare against a real Redis 7.4 with `redis-cli`, and mention in the PR what you checked.
4. Do not copy code or tests from the Redis repository. Redis 7.4+ is not under a license compatible
   with BSD-3-Clause. Use it as a behavioural reference only.

By contributing, you agree that your contributions are licensed under the BSD 3-Clause License.
