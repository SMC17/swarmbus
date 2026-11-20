# swarmbus

**[Status](STATUS.md) · [Roadmap](ROADMAP.md) · [Contributing](CONTRIBUTING.md) · [Issues](https://github.com/SMC17/swarmbus/issues)**

A shared-memory table that lets separate processes take leases on named resources and publish
small key/value claims, plus an in-process blackboard with a compute-budget auction. Zig 0.16.
A read-only Python client and a read-only Rust client use the same memory layout.

Status: **unit-tested** (8 Zig tests, a Python self-test and 2 Rust tests, including a
4-thread lease race and a 3-writer/2-reader torn-read check). This is a prototype.

## What is in it

| Part | What it does |
| --- | --- |
| `src/shm_swarm.zig` (`ShmSwarmBus`) | A file `mmap`ed `MAP_SHARED` with 64 slots (layout v2). A key's slot is found by hash with linear probing, and slots only move forward (empty → initialising → ready), so two processes creating the same key always meet at the same slot. Writes hold a per-slot seqlock, and readers retry instead of seeing a half-written value. Leases and claims are separate slot kinds. Fencing tokens and claim order come from one 64-bit counter. |
| `src/blackboard.zig` (`Blackboard`) | The same ideas in one process, on the heap: LWW claims with merge, leases with TTL, and a greedy compute-budget auction (bids ranked by utility per token). |
| `src/main.zig` | CLI: `lease`, `release`, `claim`, `read`, `auction`, `bench`. |
| `python/swarmbus.py` | Reads claims and leases directly with the seqlock protocol. Writes go through the `swarmbus` binary, because Python has no atomic compare-and-swap. |
| `rust/` | Read-only client with a bit-exact Wyhash port. Its test reads a file written by the Zig binary. |

## Limits

- 64 slots, never freed. Releasing a lease clears its owner but keeps its key.
- Keys are 1–64 bytes and values at most 192 bytes. Longer input is an error, not truncated.
- A process that dies *inside* a write (tens of nanoseconds) leaves that slot locked until the
  file is recreated.
- Lease expiry uses the wall-clock time the caller passes in, so all participants need
  roughly synchronised clocks.
- v1 files (before 2026-11-19) are refused (`error.WrongLayout`). Delete them and start again.

Fixed on 2026-11-19: a lease could overwrite an unrelated claim
([#2](https://github.com/SMC17/swarmbus/issues/2)); a race could grant one lease to two agents,
claims could be torn, fencing tokens were cut to 32 bits, keys were matched by prefix in Rust,
and the bench did not measure shared memory ([#3](https://github.com/SMC17/swarmbus/issues/3)).
The regression tests fail on the old protocol: removing the claim lock makes the torn-read
test report 2,476 torn reads.

## Build

```sh
# Zig 0.16.0
zig build test --summary all       # 8/8
zig build -Doptimize=ReleaseFast
./zig-out/bin/swarmbus lease   /tmp/swarm.shm 1 deploy 5000    # {"acquired":true,"fencing_token":1}
./zig-out/bin/swarmbus lease   /tmp/swarm.shm 2 deploy 5000    # {"acquired":false}
./zig-out/bin/swarmbus release /tmp/swarm.shm 1 deploy
./zig-out/bin/swarmbus claim   /tmp/swarm.shm 1 ci:status green
./zig-out/bin/swarmbus read    /tmp/swarm.shm ci:status        # green (exit 1 if absent)
python3 python/swarmbus.py --self-test
cargo test --manifest-path rust/Cargo.toml
./zig-out/bin/swarmbus bench
```

## Measured

Apple M2 Max, macOS 26.4, Zig 0.16.0 ReleaseFast, 2026-11-19, `swarmbus bench`, three runs:

| What | Workload | Result |
| --- | --- | ---: |
| In-process `Blackboard.publishClaim` | 10,000 calls, one process | 12–14 ns |
| Shared-memory `publishClaim` | 100,000 calls, one process, no contention | 24–30 ns |
| Shared-memory `readClaim` | 100,000 calls | 4–6 ns |
| Shared-memory `tryAcquireLease` | 100,000 calls | ~50 ns |
| **Cross-process round trip** | Parent publishes ping n, child process waits and publishes pong n; both busy-spin; 20,000 rounds | **410–835 ns** |

The cross-process figure assumes both processes are spinning on their own cores. A process
that sleeps or polls between reads adds that delay on top.

## License

MIT
