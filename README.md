# swarmbus

**[Status](STATUS.md) · [Roadmap](ROADMAP.md) · [Contributing](CONTRIBUTING.md) · [Issues](https://github.com/SMC17/swarmbus/issues)**

A shared-memory table that lets separate processes take leases on named resources and publish
small key/value claims, plus an in-process blackboard with a compute-budget auction. Zig 0.16,
with Python and Rust readers of the same memory layout.

Status: **unit-tested** (4 tests). This is a prototype with **known correctness defects**. Do
not use it to coordinate anything that matters until they are fixed.

## What is in it

| Part | What it does |
| --- | --- |
| `src/shm_swarm.zig` (`ShmSwarmBus`) | A file `mmap`ed `MAP_SHARED`, holding 64 fixed slots (64-byte key, 192-byte value). Leases are taken with a compare-and-swap on a per-slot lock word and carry a fencing number from a global counter. Claims are last-writer-wins by that counter. |
| `src/blackboard.zig` (`Blackboard`) | The same ideas in one process, on the heap: LWW claims with merge, leases with TTL, and a greedy compute-budget auction (bids ranked by utility per token). |
| `src/main.zig` | CLI: `lease`, `claim`, `read`, `auction`, `bench`. |
| `python/swarmbus.py`, `rust/` | Read and write the same `ShmHeader` layout from Python (`mmap`) and Rust. |

## Known defects

- **A lease overwrites an unrelated claim.** Lease acquisition reuses any slot whose lock word
  is zero, including slots holding claims. Reproduced in a single process:
  [#2](https://github.com/SMC17/swarmbus/issues/2).
- **Found by code review, not reproduced**
  ([#3](https://github.com/SMC17/swarmbus/issues/3)):
  - A new slot's key is written *after* its lock CAS, so two processes may both be granted
    the same lease. 0 occurrences in 20,000 two-thread trials, but nothing rules it out.
  - Claim writes are not atomic, so readers can see torn values and concurrent writers can
    lose updates. Under concurrency the shared-memory claims are not a CRDT.
  - Fencing numbers are truncated to 32 bits inside the lock word.
- 64 slots in total, shared by leases and claims. Keys longer than 64 bytes and values longer
  than 192 bytes are silently truncated.
- Nothing cleans up a crashed holder's slot except TTL expiry checked by the next acquirer.

## Build

```sh
# Zig 0.16.0
zig build test --summary all       # 4/4
zig build -Doptimize=ReleaseFast
./zig-out/bin/swarmbus lease /tmp/swarm.shm 1 src/main.zig 5000
./zig-out/bin/swarmbus claim /tmp/swarm.shm 1 ci:status green
./zig-out/bin/swarmbus read  /tmp/swarm.shm ci:status
./zig-out/bin/swarmbus bench
```

## Measured

Apple M2 Max, macOS 26.4, Zig 0.16.0 ReleaseFast, commit `e22d3c0`, 2026-11-19:
`swarmbus bench` reports 12–14 ns per `publishClaim` over 10,000 calls.

That number is for the **in-process** `Blackboard`, a heap hash map in one process. Nothing
has measured cross-process latency through the shared-memory bus yet. Earlier versions of
this README claimed "sub-microsecond shared-memory coordination across separate OS processes";
that was never measured.

## License

MIT
