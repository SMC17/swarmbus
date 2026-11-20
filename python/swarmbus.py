#!/usr/bin/env python3
"""Python client for the swarmbus shared-memory table (layout v2, `src/shm_swarm.zig`).

Reads are done here, directly on the mapped file, with the same seqlock protocol as Zig:
a read retries while the slot's `seq` is odd or changes underneath it, so it never returns a
half-written value.

Writes (leases, releases, claims) go through the `swarmbus` binary. Python has no atomic
compare-and-swap on shared memory, and a write without one cannot be made safe against
writers in other processes, so this module does not attempt it.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import mmap
import os
import struct
import sys
import tempfile
import time
from typing import Optional

MAGIC: int = 0x325542535741524D  # layout v2
VERSION: int = 2
MAX_SLOTS: int = 64
KEY_CAP: int = 64
VALUE_CAP: int = 192
KIND_LEASE: int = 1
KIND_CLAIM: int = 2
STATE_EMPTY, STATE_INIT, STATE_READY = 0, 1, 2

HEADER_FMT: str = "<QIIQ"  # magic, version, slot_count, counter
SLOT_FMT: str = "<QIIQIIIIQq64s192s"  # seq, state, kind, key_hash, key_len, value_len, owner, pad, fence, expires, key, value
HEADER_SIZE: int = struct.calcsize(HEADER_FMT)  # 24
SLOT_SIZE: int = struct.calcsize(SLOT_FMT)  # 312
TOTAL_SIZE: int = HEADER_SIZE + MAX_SLOTS * SLOT_SIZE

_U64_MASK: int = 0xFFFFFFFFFFFFFFFF
_WY_SECRET: tuple[int, int, int, int] = (
    0xA0761D6478BD642F,
    0xE7037ED1A0B428DB,
    0x8EBC6AF09C88C6E3,
    0x589965CC75374CC3,
)


def _mum(a: int, b: int) -> tuple[int, int]:
    x = (a & _U64_MASK) * (b & _U64_MASK)
    return x & _U64_MASK, (x >> 64) & _U64_MASK


def _mix(a: int, b: int) -> int:
    lo, hi = _mum(a, b)
    return lo ^ hi


def _read_le(data: bytes, offset: int, size: int) -> int:
    return int.from_bytes(data[offset : offset + size], "little")


def wyhash(data: bytes, seed: int = 0) -> int:
    """Bit-exact match for Zig `std.hash.Wyhash.hash(seed, data)`."""
    s0 = (seed ^ _mix(seed ^ _WY_SECRET[0], _WY_SECRET[1])) & _U64_MASK
    state = [s0, s0, s0]
    n = len(data)
    a = 0
    b = 0

    if n <= 16:
        if n >= 4:
            end = n - 4
            quarter = (n >> 3) << 2
            a = (_read_le(data, 0, 4) << 32) | _read_le(data, quarter, 4)
            b = (_read_le(data, end, 4) << 32) | _read_le(data, end - quarter, 4)
        elif n > 0:
            a = (data[0] << 16) | (data[n >> 1] << 8) | data[n - 1]
    else:
        i = 0
        if n >= 48:
            while i + 48 < n:
                for k in range(3):
                    x = _read_le(data, i + 8 * (2 * k), 8)
                    y = _read_le(data, i + 8 * (2 * k + 1), 8)
                    state[k] = _mix(x ^ _WY_SECRET[k + 1], y ^ state[k])
                i += 48
            state[0] ^= state[1] ^ state[2]

        while i + 16 < n:
            state[0] = _mix(
                _read_le(data, i, 8) ^ _WY_SECRET[1],
                _read_le(data, i + 8, 8) ^ state[0],
            )
            i += 16

        a = _read_le(data, n - 16, 8)
        b = _read_le(data, n - 8, 8)

    a ^= _WY_SECRET[1]
    b ^= state[0]
    a, b = _mum(a, b)
    return _mix(a ^ _WY_SECRET[0] ^ n, b ^ _WY_SECRET[1])


def _to_bytes(val: str | bytes) -> bytes:
    return val.encode("utf-8") if isinstance(val, str) else val


def _slot_offset(idx: int) -> int:
    return HEADER_SIZE + idx * SLOT_SIZE


def _binary(explicit: Optional[str] = None) -> str:
    for cand in (explicit, os.environ.get("SWARMBUS_BIN"), os.path.join(os.path.dirname(__file__), "..", "zig-out", "bin", "swarmbus"), shutil.which("swarmbus")):
        if cand and os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    raise FileNotFoundError("swarmbus binary not found: run `zig build` or set SWARMBUS_BIN")


class ShmSwarmBus:
    """Reader for a swarmbus v2 file; writes delegate to the `swarmbus` binary."""

    def __init__(self, path: str, binary: Optional[str] = None) -> None:
        self.path = path
        self._binary = binary
        fd = os.open(path, os.O_RDONLY)
        try:
            if os.fstat(fd).st_size < TOTAL_SIZE:
                raise ValueError(f"{path}: not a swarmbus v2 file (too small)")
            self._mm = mmap.mmap(fd, TOTAL_SIZE, mmap.MAP_SHARED, mmap.PROT_READ)
        finally:
            os.close(fd)
        magic, version, _, _ = struct.unpack_from(HEADER_FMT, self._mm, 0)
        if magic != MAGIC or version != VERSION:
            self._mm.close()
            raise ValueError(f"{path}: not a swarmbus v2 file (magic {magic:#x})")

    @classmethod
    def open_or_create(cls, path: str, binary: Optional[str] = None) -> ShmSwarmBus:
        """Creates the file through the binary (which initialises the header), then maps it."""
        if not os.path.exists(path) or os.path.getsize(path) < TOTAL_SIZE:
            subprocess.run([_binary(binary), "read", path, "_init_"], capture_output=True)
        return cls(path, binary)

    def close(self) -> None:
        if getattr(self, "_mm", None) is not None:
            self._mm.close()
            self._mm = None  # type: ignore[assignment]

    def __enter__(self) -> ShmSwarmBus:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _find(self, kind: int, key: bytes) -> Optional[int]:
        if not key or len(key) > KEY_CAP:
            raise ValueError("key must be 1..64 bytes")
        kh = wyhash(key, kind)
        for i in range(MAX_SLOTS):
            off = _slot_offset((kh % MAX_SLOTS + i) % MAX_SLOTS)
            for _ in range(1_000_000):
                _, state, skind, skh, klen, *_rest = struct.unpack_from(SLOT_FMT, self._mm, off)
                if state != STATE_INIT:
                    break
            if state == STATE_EMPTY:
                return None
            raw_key = struct.unpack_from(SLOT_FMT, self._mm, off)[10]
            if skind == kind and skh == kh and klen == len(key) and raw_key[:klen] == key:
                return off
        return None

    def _snapshot(self, off: int) -> tuple:
        while True:
            s1 = struct.unpack_from("<Q", self._mm, off)[0]
            if s1 & 1:
                continue
            fields = struct.unpack_from(SLOT_FMT, self._mm, off)
            if struct.unpack_from("<Q", self._mm, off)[0] == s1:
                return fields

    def read_claim(self, key: str | bytes) -> Optional[str]:
        off = self._find(KIND_CLAIM, _to_bytes(key))
        if off is None:
            return None
        f = self._snapshot(off)
        if f[8] == 0:  # created, never written
            return None
        return f[11][: f[5]].decode("utf-8", errors="replace")

    def read_lease(self, resource: str | bytes) -> Optional[dict]:
        off = self._find(KIND_LEASE, _to_bytes(resource))
        if off is None:
            return None
        f = self._snapshot(off)
        return {"owner": f[6], "fencing_token": f[8], "expires_ns": f[9]}

    def _run(self, *args: str) -> dict:
        out = subprocess.run([_binary(self._binary), *args], capture_output=True, text=True)
        if out.returncode != 0 and not out.stdout.strip():
            raise RuntimeError(f"swarmbus {args[0]} failed ({out.returncode}): {out.stderr.strip()}")
        return json.loads(out.stdout)

    def publish_claim(self, agent_id: int, key: str, value: str) -> bool:
        return bool(self._run("claim", self.path, str(agent_id), key, value)["published"])

    def try_acquire_lease(self, agent_id: int, resource: str, ttl_ms: int) -> Optional[int]:
        r = self._run("lease", self.path, str(agent_id), resource, str(ttl_ms))
        return r["fencing_token"] if r["acquired"] else None

    def release_lease(self, agent_id: int, resource: str) -> bool:
        return bool(self._run("release", self.path, str(agent_id), resource)["released"])


def _self_test() -> None:
    vectors = [
        (0, b"", 0x409638EE2BDE459),
        (1, b"a", 0xA8412D091B5FE0A9),
        (2, b"abc", 0x32DD92E4B2915153),
        (3, b"message digest", 0x8619124089A3A16B),
        (4, b"abcdefghijklmnopqrstuvwxyz", 0x7A43AFB61D7F5F40),
        (5, b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 0xFF42329B90E50D58),
        (6, b"1234567890" * 8, 0xC39CAB13B115AAD3),
    ]
    for seed, data, expected in vectors:
        got = wyhash(data, seed)
        assert got == expected, f"wyhash mismatch for seed={seed}: {got:#x} != {expected:#x}"

    with tempfile.TemporaryDirectory() as tmpdir:
        shm_path = os.path.join(tmpdir, "test.shm")
        with ShmSwarmBus.open_or_create(shm_path) as bus:
            assert bus.publish_claim(1, "py:status", "ok")
            assert bus.read_claim("py:status") == "ok"
            assert bus.read_claim("py") is None, "keys must match exactly, not by prefix"
            f1 = bus.try_acquire_lease(10, "src/main.zig", ttl_ms=60_000)
            assert f1 is not None
            assert bus.try_acquire_lease(20, "src/main.zig", ttl_ms=60_000) is None
            assert bus.read_lease("src/main.zig")["owner"] == 10
            assert bus.release_lease(10, "src/main.zig")
            f2 = bus.try_acquire_lease(20, "src/main.zig", ttl_ms=60_000)
            assert f2 is not None and f2 > f1
            assert bus.read_claim("py:status") == "ok", "a lease must not overwrite a claim"
    print(json.dumps({"self_test": "ok", "header_size": HEADER_SIZE, "slot_size": SLOT_SIZE, "total_size": TOTAL_SIZE}))


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "--self-test":
        _self_test()
        return 0
    if len(argv) >= 4 and argv[1] == "read":
        with ShmSwarmBus(argv[2]) as bus:
            val = bus.read_claim(argv[3])
        if val is None:
            return 1
        print(val)
        return 0
    print("Usage: swarmbus.py [read <shm_path> <key> | --self-test]   (writes: use the swarmbus binary)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
