use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::ptr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

pub const MAGIC: u64 = 0x5355_4253_5741_524D;
pub const MAX_SLOTS: usize = 64;

#[repr(C)]
pub struct ShmSlot {
    pub lock_word: AtomicU64,
    pub lamport_ts: AtomicU64,
    pub expires_ns: AtomicI64,
    pub key_hash: u64,
    pub key: [u8; 64],
    pub value: [u8; 192],
}

#[repr(C)]
pub struct ShmHeader {
    pub magic: AtomicU64,
    pub fencing_seq: AtomicU64,
    pub slots: [ShmSlot; MAX_SLOTS],
}

extern "C" {
    fn mmap(
        addr: *mut std::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    fn munmap(addr: *mut std::ffi::c_void, len: usize) -> i32;
}

const PROT_READ: i32 = 0x01;
const PROT_WRITE: i32 = 0x02;
const MAP_SHARED: i32 = 0x0001;

pub struct RustSwarmBus {
    ptr: *mut ShmHeader,
    len: usize,
}

impl RustSwarmBus {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let f = OpenOptions::new().read(true).write(true).open(path)?;
        let len = std::mem::size_of::<ShmHeader>();
        let mapped = unsafe {
            mmap(
                ptr::null_mut(),
                len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                f.as_raw_fd(),
                0,
            )
        };
        if mapped == (-1isize) as *mut std::ffi::c_void {
            return Err(std::io::Error::last_os_error());
        }
        let hdr = mapped as *mut ShmHeader;
        if unsafe { (*hdr).magic.load(Ordering::Acquire) } != MAGIC {
            unsafe { munmap(mapped, len) };
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid swarmbus SHM magic",
            ));
        }
        Ok(Self { ptr: hdr, len })
    }

    pub fn read_claim(&self, key: &str) -> Option<String> {
        let hdr = unsafe { &*self.ptr };
        for slot in &hdr.slots {
            if slot.lamport_ts.load(Ordering::Acquire) > 0 && slot.key.starts_with(key.as_bytes()) {
                let end = slot.value.iter().position(|&b| b == 0).unwrap_or(slot.value.len());
                return Some(String::from_utf8_lossy(&slot.value[..end]).into_owned());
            }
        }
        None
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
    fn reads_zig_created_shm_if_present() {
        if let Ok(bus) = RustSwarmBus::open("/tmp/swarm-poly.shm") {
            let val = bus.read_claim("poly:status");
            assert_eq!(val.as_deref(), Some("zig+rust-ok"));
        }
    }
}
