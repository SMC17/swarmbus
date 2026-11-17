#!/usr/bin/env python3
"""Lock-Free Cross-Process POSIX mmap Swarm Bus interop for Python.

Matches the exact C-ABI layout of Zig `ShmHeader` / `ShmSlot` (`src/shm_swarm.zig`)
and Rust `ShmHeader` / `ShmSlot` (`rust/src/lib.rs`) so Python, Zig, and Rust
processes can read and write the same shared-memory bus.
"""

from __future__ import annotations

import json
import mmap
import os
import struct
import sys
import tempfile
import time
from typing import Optional

MAGIC: int = 0x535542535741524D  # "SUBSWARM"
MAX_SLOTS: int = 64

HEADER_FMT: str = "<QQ"
SLOT_FMT: str = "<QQqQ64s192s"
HEADER_SIZE: int = struct.calcsize(HEADER_FMT)  # 16 bytes
SLOT_SIZE: int = struct.calcsize(SLOT_FMT)  # 288 bytes
TOTAL_SIZE: int = HEADER_SIZE + MAX_SLOTS * SLOT_SIZE  # 18448 bytes

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


def _unpack_cstr(raw: bytes) -> bytes:
    idx = raw.find(b"\x00")
    return raw if idx == -1 else raw[:idx]


class ShmSwarmBus:
    """POSIX mmap shared-memory Swarm Bus client."""

    def __init__(self, fd: int, mm: mmap.mmap) -> None:
        self._fd = fd
        self._mm = mm

    @classmethod
    def open_or_create(cls, path: str) -> ShmSwarmBus:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o660)
        try:
            st = os.fstat(fd)
            if st.st_size < TOTAL_SIZE:
                os.ftruncate(fd, TOTAL_SIZE)
            mm = mmap.mmap(fd, TOTAL_SIZE, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
        except Exception:
            os.close(fd)
            raise

        magic, _ = struct.unpack_from(HEADER_FMT, mm, 0)
        if magic != MAGIC:
            mm[:TOTAL_SIZE] = b"\x00" * TOTAL_SIZE
            struct.pack_into(HEADER_FMT, mm, 0, MAGIC, 1)
        return cls(fd, mm)

    @classmethod
    def open(cls, path: str) -> ShmSwarmBus:
        fd = os.open(path, os.O_RDWR)
        try:
            mm = mmap.mmap(fd, TOTAL_SIZE, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
        except Exception:
            os.close(fd)
            raise

        magic, _ = struct.unpack_from(HEADER_FMT, mm, 0)
        if magic != MAGIC:
            mm.close()
            os.close(fd)
            raise ValueError(f"invalid swarmbus SHM magic: {magic:#x}")
        return cls(fd, mm)

    def close(self) -> None:
        if self._mm is not None:
            self._mm.close()
            self._mm = None  # type: ignore[assignment]
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def __enter__(self) -> ShmSwarmBus:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _next_seq(self) -> int:
        _, seq = struct.unpack_from(HEADER_FMT, self._mm, 0)
        struct.pack_into("<Q", self._mm, 8, seq + 1)
        return seq

    def read_claim(self, key: str | bytes) -> Optional[str]:
        kbytes = _to_bytes(key)[:64]
        for i in range(MAX_SLOTS):
            _, lamport_ts, _, _, raw_key, raw_val = struct.unpack_from(
                SLOT_FMT, self._mm, _slot_offset(i)
            )
            if lamport_ts > 0 and _unpack_cstr(raw_key) == kbytes:
                return _unpack_cstr(raw_val).decode("utf-8", errors="replace")
        return None

    def list_claims(self) -> dict[str, str]:
        claims: dict[str, str] = {}
        for i in range(MAX_SLOTS):
            _, lamport_ts, _, _, raw_key, raw_val = struct.unpack_from(
                SLOT_FMT, self._mm, _slot_offset(i)
            )
            if lamport_ts > 0:
                k = _unpack_cstr(raw_key).decode("utf-8", errors="replace")
                v = _unpack_cstr(raw_val).decode("utf-8", errors="replace")
                if k:
                    claims[k] = v
        return claims

    def publish_claim(self, agent_id: int, key: str | bytes, value: str | bytes) -> bool:
        del agent_id  # LWW uses monotonic fencing_seq
        kbytes = _to_bytes(key)[:64]
        vbytes = _to_bytes(value)[:192]
        kh = wyhash(kbytes, 0)
        ts = self._next_seq()

        for i in range(MAX_SLOTS):
            off = _slot_offset(i)
            lock_word, lamport_ts, expires_ns, slot_kh, raw_key, _ = struct.unpack_from(
                SLOT_FMT, self._mm, off
            )
            if slot_kh == kh and _unpack_cstr(raw_key) == kbytes:
                if ts > lamport_ts:
                    padded_val = vbytes.ljust(192, b"\x00")
                    struct.pack_into(
                        SLOT_FMT, self._mm, off, lock_word, ts, expires_ns, slot_kh, raw_key, padded_val
                    )
                    return True
                return False

        for i in range(MAX_SLOTS):
            off = _slot_offset(i)
            lock_word, lamport_ts, expires_ns, _, _, _ = struct.unpack_from(
                SLOT_FMT, self._mm, off
            )
            if lamport_ts == 0 and lock_word == 0:
                padded_key = kbytes.ljust(64, b"\x00")
                padded_val = vbytes.ljust(192, b"\x00")
                struct.pack_into(
                    SLOT_FMT, self._mm, off, 0, ts, expires_ns, kh, padded_key, padded_val
                )
                return True

        return False

    def try_acquire_lease(
        self,
        agent_id: int,
        resource: str | bytes,
        now_ns: Optional[int] = None,
        ttl_ns: int = 5_000_000_000,
    ) -> Optional[int]:
        if now_ns is None:
            now_ns = time.time_ns()
        rbytes = _to_bytes(resource)[:64]
        kh = wyhash(rbytes, 0)
        free_idx: Optional[int] = None

        for i in range(MAX_SLOTS):
            off = _slot_offset(i)
            lw, lamport_ts, expires_ns, slot_kh, raw_key, raw_val = struct.unpack_from(
                SLOT_FMT, self._mm, off
            )
            if lw == 0 and lamport_ts == 0 and free_idx is None:
                free_idx = i
                continue
            if lw != 0 and slot_kh == kh and _unpack_cstr(raw_key) == rbytes:
                owner = (lw >> 32) & 0xFFFFFFFF
                if owner != agent_id and expires_ns > now_ns:
                    return None
                fence = self._next_seq()
                next_lw = ((agent_id & 0xFFFFFFFF) << 32) | (fence & 0xFFFFFFFF)
                struct.pack_into(
                    SLOT_FMT, self._mm, off, next_lw, lamport_ts, now_ns + ttl_ns, slot_kh, raw_key, raw_val
                )
                return fence

        if free_idx is not None:
            off = _slot_offset(free_idx)
            _, lamport_ts, _, _, _, raw_val = struct.unpack_from(SLOT_FMT, self._mm, off)
            fence = self._next_seq()
            next_lw = ((agent_id & 0xFFFFFFFF) << 32) | (fence & 0xFFFFFFFF)
            padded_key = rbytes.ljust(64, b"\x00")
            struct.pack_into(
                SLOT_FMT, self._mm, off, next_lw, lamport_ts, now_ns + ttl_ns, kh, padded_key, raw_val
            )
            return fence

        return None


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
        with ShmSwarmBus.open_or_create(shm_path) as bus_a:
            assert bus_a.publish_claim(1, "py:status", "ok")
            f1 = bus_a.try_acquire_lease(10, "src/main.zig", now_ns=1000, ttl_ns=500)
            assert f1 is not None and f1 >= 1
            with ShmSwarmBus.open(shm_path) as bus_b:
                assert bus_b.read_claim("py:status") == "ok"
                assert bus_b.try_acquire_lease(20, "src/main.zig", now_ns=1200, ttl_ns=500) is None
                f2 = bus_b.try_acquire_lease(20, "src/main.zig", now_ns=1600, ttl_ns=500)
                assert f2 is not None and f2 > f1
    print(json.dumps({"self_test": "ok", "header_size": HEADER_SIZE, "slot_size": SLOT_SIZE, "total_size": TOTAL_SIZE}))


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: swarmbus.py [read <shm_path> <key> | claim <shm_path> <agent_id> <key> <value> | lease <shm_path> <agent_id> <resource> <ttl_ms> | --self-test]")
        return 1

    cmd = argv[1]
    if cmd == "--self-test":
        _self_test()
        return 0
    if cmd == "read" and len(argv) >= 4:
        with ShmSwarmBus.open(argv[2]) as bus:
            val = bus.read_claim(argv[3])
            if val is not None:
                print(val)
                return 0
            return 1
    if cmd == "claim" and len(argv) >= 6:
        with ShmSwarmBus.open_or_create(argv[2]) as bus:
            ok = bus.publish_claim(int(argv[3]), argv[4], argv[5])
            print(json.dumps({"published": ok}))
            return 0 if ok else 1
    if cmd == "lease" and len(argv) >= 6:
        with ShmSwarmBus.open_or_create(argv[2]) as bus:
            fence = bus.try_acquire_lease(int(argv[3]), argv[4], ttl_ns=int(argv[5]) * 1_000_000)
            if fence is not None:
                print(json.dumps({"acquired": True, "fencing_token": fence}))
                return 0
            print(json.dumps({"acquired": False}))
            return 1

    print(f"Unknown or incomplete command: {' '.join(argv[1:])}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
