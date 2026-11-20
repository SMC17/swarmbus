//! Read-only Rust client for the swarmbus shared-memory table (layout v2, `src/shm_swarm.zig`).
//!
//! It finds a key the same way Zig does (Wyhash seeded with the slot kind, then linear
//! probing), compares keys exactly, and reads with the seqlock protocol: it retries while the
//! slot's `seq` is odd or changes during the copy. Writes go through the Zig library or the
//! `swarmbus` binary.
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::ptr;
use std::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};

pub const MAGIC: u64 = 0x3255_4253_5741_524D;
pub const VERSION: u32 = 2;
pub const MAX_SLOTS: usize = 64;
pub const KEY_CAP: usize = 64;
pub const VALUE_CAP: usize = 192;
const KIND_LEASE: u32 = 1;
const KIND_CLAIM: u32 = 2;
const STATE_EMPTY: u32 = 0;
const STATE_INIT: u32 = 1;

#[repr(C)]
pub struct ShmSlot {
    pub seq: AtomicU64,
    pub state: AtomicU32,
    pub kind: u32,
    pub key_hash: u64,
    pub key_len: u32,
    pub value_len: u32,
    pub owner_agent: u32,
    _pad: u32,
    pub fence: u64,
    pub expires_ns: i64,
    pub key: [u8; KEY_CAP],
    pub value: [u8; VALUE_CAP],
}

#[repr(C)]
pub struct ShmHeader {
    pub magic: AtomicU64,
    pub version: u32,
    pub slot_count: u32,
    pub counter: AtomicU64,
    pub slots: [ShmSlot; MAX_SLOTS],
}

const _: () = assert!(std::mem::size_of::<ShmSlot>() == 312);

extern "C" {
    fn mmap(addr: *mut std::ffi::c_void, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut std::ffi::c_void;
    fn munmap(addr: *mut std::ffi::c_void, len: usize) -> i32;
}
const PROT_READ: i32 = 0x01;
const MAP_SHARED: i32 = 0x0001;

/// Bit-exact port of Zig `std.hash.Wyhash.hash(seed, data)`.
pub fn wyhash(data: &[u8], seed: u64) -> u64 {
    const S: [u64; 4] = [0xA0761D6478BD642F, 0xE7037ED1A0B428DB, 0x8EBC6AF09C88C6E3, 0x589965CC75374CC3];
    fn mum(a: u64, b: u64) -> (u64, u64) {
        let x = (a as u128) * (b as u128);
        (x as u64, (x >> 64) as u64)
    }
    fn mix(a: u64, b: u64) -> u64 {
        let (lo, hi) = mum(a, b);
        lo ^ hi
    }
    fn le(d: &[u8], o: usize, n: usize) -> u64 {
        let mut v = 0u64;
        for i in 0..n {
            v |= (d[o + i] as u64) << (8 * i);
        }
        v
    }
    let s0 = seed ^ mix(seed ^ S[0], S[1]);
    let mut st = [s0, s0, s0];
    let n = data.len();
    let (mut a, mut b) = (0u64, 0u64);
    if n <= 16 {
        if n >= 4 {
            let end = n - 4;
            let q = (n >> 3) << 2;
            a = (le(data, 0, 4) << 32) | le(data, q, 4);
            b = (le(data, end, 4) << 32) | le(data, end - q, 4);
        } else if n > 0 {
            a = ((data[0] as u64) << 16) | ((data[n >> 1] as u64) << 8) | data[n - 1] as u64;
        }
    } else {
        let mut i = 0;
        if n >= 48 {
            while i + 48 < n {
                for k in 0..3 {
                    let x = le(data, i + 16 * k, 8);
                    let y = le(data, i + 16 * k + 8, 8);
                    st[k] = mix(x ^ S[k + 1], y ^ st[k]);
                }
                i += 48;
            }
            st[0] ^= st[1] ^ st[2];
        }
        while i + 16 < n {
            st[0] = mix(le(data, i, 8) ^ S[1], le(data, i + 8, 8) ^ st[0]);
            i += 16;
        }
        a = le(data, n - 16, 8);
        b = le(data, n - 8, 8);
    }
    a ^= S[1];
    b ^= st[0];
    let (a2, b2) = mum(a, b);
    mix(a2 ^ S[0] ^ n as u64, b2 ^ S[1])
}

#[derive(Debug, PartialEq, Eq)]
pub struct Lease {
    pub owner: u32,
    pub fencing_token: u64,
    pub expires_ns: i64,
}

pub struct RustSwarmBus {
    ptr: *const ShmHeader,
    len: usize,
}

impl RustSwarmBus {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let f = OpenOptions::new().read(true).open(path)?;
        let len = std::mem::size_of::<ShmHeader>();
        if (f.metadata()?.len() as usize) < len {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "not a swarmbus v2 file"));
        }
        let mapped = unsafe { mmap(ptr::null_mut(), len, PROT_READ, MAP_SHARED, f.as_raw_fd(), 0) };
        if mapped == (-1isize) as *mut std::ffi::c_void {
            return Err(std::io::Error::last_os_error());
        }
        let hdr = mapped as *const ShmHeader;
        let ok = unsafe { (*hdr).magic.load(Ordering::Acquire) == MAGIC && (*hdr).version == VERSION };
        if !ok {
            unsafe { munmap(mapped, len) };
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "not a swarmbus v2 file"));
        }
        Ok(Self { ptr: hdr, len })
    }

    fn find(&self, kind: u32, key: &[u8]) -> Option<&ShmSlot> {
        if key.is_empty() || key.len() > KEY_CAP {
            return None;
        }
        let hdr = unsafe { &*self.ptr };
        let kh = wyhash(key, kind as u64);
        for i in 0..MAX_SLOTS as u64 {
            let slot = &hdr.slots[((kh % MAX_SLOTS as u64 + i) % MAX_SLOTS as u64) as usize];
            let mut state = slot.state.load(Ordering::Acquire);
            while state == STATE_INIT {
                std::hint::spin_loop();
                state = slot.state.load(Ordering::Acquire);
            }
            if state == STATE_EMPTY {
                return None;
            }
            let klen = slot.key_len as usize;
            if slot.kind == kind && slot.key_hash == kh && klen == key.len() && &slot.key[..klen] == key {
                return Some(slot);
            }
        }
        None
    }

    /// Copies (value, writer, order, expires) out of a slot under the seqlock.
    fn snapshot(slot: &ShmSlot) -> (Vec<u8>, u32, u64, i64) {
        loop {
            let s1 = slot.seq.load(Ordering::Acquire);
            if s1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let n = (unsafe { ptr::read_volatile(&slot.value_len) } as usize).min(VALUE_CAP);
            let mut v = vec![0u8; n];
            for (i, b) in v.iter_mut().enumerate() {
                *b = unsafe { ptr::read_volatile(&slot.value[i]) };
            }
            let owner = unsafe { ptr::read_volatile(&slot.owner_agent) };
            let order = unsafe { ptr::read_volatile(&slot.fence) };
            let exp = unsafe { ptr::read_volatile(&slot.expires_ns) };
            fence(Ordering::Acquire);
            if slot.seq.load(Ordering::Acquire) == s1 {
                return (v, owner, order, exp);
            }
        }
    }

    pub fn read_claim(&self, key: &str) -> Option<String> {
        let slot = self.find(KIND_CLAIM, key.as_bytes())?;
        let (v, _, order, _) = Self::snapshot(slot);
        if order == 0 {
            return None;
        }
        Some(String::from_utf8_lossy(&v).into_owned())
    }

    pub fn read_lease(&self, resource: &str) -> Option<Lease> {
        let slot = self.find(KIND_LEASE, resource.as_bytes())?;
        let (_, owner, fencing_token, expires_ns) = Self::snapshot(slot);
        Some(Lease { owner, fencing_token, expires_ns })
    }
}

impl Drop for RustSwarmBus {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr as *mut std::ffi::c_void, self.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wyhash_matches_zig_vectors() {
        let v: [(u64, &[u8], u64); 6] = [
            (0, b"", 0x409638EE2BDE459),
            (1, b"a", 0xA8412D091B5FE0A9),
            (2, b"abc", 0x32DD92E4B2915153),
            (3, b"message digest", 0x8619124089A3A16B),
            (4, b"abcdefghijklmnopqrstuvwxyz", 0x7A43AFB61D7F5F40),
            (5, b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 0xFF42329B90E50D58),
        ];
        for (seed, data, want) in v {
            assert_eq!(wyhash(data, seed), want, "seed {seed}");
        }
        assert_eq!(wyhash(&b"1234567890".repeat(8), 6), 0xC39CAB13B115AAD3);
    }

    /// Reads a file written by the Zig binary (built with `zig build` in the repo root).
    #[test]
    fn reads_what_zig_wrote() {
        let bin = concat!(env!("CARGO_MANIFEST_DIR"), "/../zig-out/bin/swarmbus");
        if !std::path::Path::new(bin).exists() {
            eprintln!("skipping: build the Zig binary first (zig build)");
            return;
        }
        let path = std::env::temp_dir().join(format!("swarmbus-rust-{}.shm", std::process::id()));
        let p = path.to_str().unwrap();
        let _ = std::fs::remove_file(&path);
        let run = |args: &[&str]| assert!(std::process::Command::new(bin).args(args).output().unwrap().status.success());
        run(&["claim", p, "7", "poly:status", "zig+rust-ok"]);
        run(&["lease", p, "9", "deploy", "60000"]);
        let bus = RustSwarmBus::open(p).unwrap();
        assert_eq!(bus.read_claim("poly:status").as_deref(), Some("zig+rust-ok"));
        assert_eq!(bus.read_claim("poly"), None, "exact match, not prefix");
        assert_eq!(bus.read_lease("deploy").unwrap().owner, 9);
        let _ = std::fs::remove_file(&path);
    }
}
