# swarmbus

**[Status](STATUS.md) · [Roadmap](ROADMAP.md) · [Contributing](CONTRIBUTING.md) · [Issues](https://github.com/SMC17/swarmbus/issues) · [Public roadmap](https://github.com/users/SMC17/projects/16)**

**Lock-Free Cross-Process POSIX `mmap` Swarm Bus, LWW-CRDT Blackboard, and Compute-Budget Auction Scheduler.**

Replaces bloated multi-agent JSON-RPC/HTTP chatter with sub-microsecond shared-memory coordination across separate OS processes (coding agents, test runners, Python/Node workers).

## Quickstart
```sh
zig build test
zig build -Doptimize=ReleaseFast
./zig-out/bin/swarmbus lease /tmp/swarm.shm 1 src/main.zig 5000
./zig-out/bin/swarmbus claim /tmp/swarm.shm 1 ci:status green
./zig-out/bin/swarmbus read  /tmp/swarm.shm ci:status
./zig-out/bin/swarmbus bench
```
