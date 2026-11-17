const std = @import("std");

/// Cross-process POSIX `mmap` shared-memory Swarm Bus.
/// Fixed-layout atomic slot table allowing separate OS processes (Claude, Codex, test runners,
/// Python/Node workers) to acquire fenced resource leases and publish LWW-CRDT claims
/// via shared memory without JSON-RPC or socket serialization overhead.
pub const MAGIC: u64 = 0x5355_4253_5741_524D; // "SUBSWARM"
pub const MAX_SLOTS: usize = 64;

pub const ShmSlot = extern struct {
    /// Packed atomic state: high 32 bits = owner_agent, low 32 bits = seq/generation.
    /// 0 means unlocked / empty.
    lock_word: std.atomic.Value(u64),
    lamport_ts: std.atomic.Value(u64),
    expires_ns: std.atomic.Value(i64),
    key_hash: u64,
    key: [64]u8,
    value: [192]u8,
};

pub const ShmHeader = extern struct {
    magic: std.atomic.Value(u64),
    fencing_seq: std.atomic.Value(u64),
    slots: [MAX_SLOTS]ShmSlot,
};

pub const ShmSwarmBus = struct {
    fd: std.posix.fd_t,
    mapped: []align(std.heap.page_size_min) u8,
    hdr: *ShmHeader,

    pub fn openOrCreate(path: []const u8) !ShmSwarmBus {
        const fd = try std.posix.open(
            path,
            .{ .ACCMODE = .RDWR, .CREAT = true },
            0o660,
        );
        errdefer std.posix.close(fd);

        const total_size = @sizeOf(ShmHeader);
        try std.posix.ftruncate(fd, total_size);

        const mapped = try std.posix.mmap(
            null,
            total_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            fd,
            0,
        );
        errdefer std.posix.munmap(mapped);

        const hdr: *ShmHeader = @ptrCast(@alignCast(mapped.ptr));
        const cur_magic = hdr.magic.load(.acquire);
        if (cur_magic != MAGIC) {
            @memset(mapped, 0);
            hdr.fencing_seq.store(1, .release);
            hdr.magic.store(MAGIC, .release);
        }

        return .{
            .fd = fd,
            .mapped = mapped,
            .hdr = hdr,
        };
    }

    pub fn close(self: *ShmSwarmBus) void {
        std.posix.munmap(self.mapped);
        std.posix.close(self.fd);
    }

    /// Lock-free atomic CAS lease acquisition across OS processes.
    pub fn tryAcquireLease(self: *ShmSwarmBus, agent_id: u32, resource: []const u8, now_ns: i64, ttl_ns: i64) ?u64 {
        const kh = std.hash.Wyhash.hash(0, resource);
        var free_idx: ?usize = null;

        for (&self.hdr.slots, 0..) |*slot, i| {
            const lw = slot.lock_word.load(.acquire);
            if (lw == 0 and free_idx == null) {
                free_idx = i;
                continue;
            }
            if (lw != 0 and slot.key_hash == kh and std.mem.startsWith(u8, &slot.key, resource)) {
                const owner: u32 = @truncate(lw >> 32);
                const exp = slot.expires_ns.load(.acquire);
                if (owner != agent_id and exp > now_ns) {
                    return null; // Contended by active owner
                }
                const fence = self.hdr.fencing_seq.fetchAdd(1, .acq_rel);
                const next_lw: u64 = (@as(u64, agent_id) << 32) | @as(u32, @truncate(fence));
                if (slot.lock_word.cmpxchgStrong(lw, next_lw, .acq_rel, .acquire) == null) {
                    slot.expires_ns.store(now_ns + ttl_ns, .release);
                    return fence;
                }
                return null;
            }
        }

        if (free_idx) |idx| {
            const slot = &self.hdr.slots[idx];
            const fence = self.hdr.fencing_seq.fetchAdd(1, .acq_rel);
            const next_lw: u64 = (@as(u64, agent_id) << 32) | @as(u32, @truncate(fence));
            if (slot.lock_word.cmpxchgStrong(0, next_lw, .acq_rel, .acquire) == null) {
                slot.key_hash = kh;
                @memset(&slot.key, 0);
                const klen = @min(resource.len, slot.key.len);
                @memcpy(slot.key[0..klen], resource[0..klen]);
                slot.expires_ns.store(now_ns + ttl_ns, .release);
                return fence;
            }
        }
        return null;
    }

    pub fn publishClaim(self: *ShmSwarmBus, agent_id: u32, key: []const u8, value: []const u8) bool {
        const kh = std.hash.Wyhash.hash(0, key);
        const ts = self.hdr.fencing_seq.fetchAdd(1, .acq_rel);
        for (&self.hdr.slots) |*slot| {
            if (slot.key_hash == kh and std.mem.startsWith(u8, &slot.key, key)) {
                const cur_ts = slot.lamport_ts.load(.acquire);
                if (ts > cur_ts) {
                    @memset(&slot.value, 0);
                    const vlen = @min(value.len, slot.value.len);
                    @memcpy(slot.value[0..vlen], value[0..vlen]);
                    slot.lamport_ts.store(ts, .release);
                    _ = agent_id;
                    return true;
                }
                return false;
            }
        }
        for (&self.hdr.slots) |*slot| {
            const cur_ts = slot.lamport_ts.load(.acquire);
            if (cur_ts == 0 and slot.lock_word.load(.acquire) == 0) {
                if (slot.lamport_ts.cmpxchgStrong(0, ts, .acq_rel, .acquire) == null) {
                    slot.key_hash = kh;
                    @memset(&slot.key, 0);
                    const klen = @min(key.len, slot.key.len);
                    @memcpy(slot.key[0..klen], key[0..klen]);
                    @memset(&slot.value, 0);
                    const vlen = @min(value.len, slot.value.len);
                    @memcpy(slot.value[0..vlen], value[0..vlen]);
                    _ = agent_id;
                    return true;
                }
            }
        }
        return false;
    }

    pub fn readClaim(self: *const ShmSwarmBus, key: []const u8, out_buf: []u8) ?[]const u8 {
        const kh = std.hash.Wyhash.hash(0, key);
        for (&self.hdr.slots) |*slot| {
            if (slot.key_hash == kh and std.mem.startsWith(u8, &slot.key, key) and slot.lamport_ts.load(.acquire) > 0) {
                const end = std.mem.indexOfScalar(u8, &slot.value, 0) orelse slot.value.len;
                const n = @min(end, out_buf.len);
                @memcpy(out_buf[0..n], slot.value[0..n]);
                return out_buf[0..n];
            }
        }
        return null;
    }
};

test "ShmSwarmBus two independent mmap handles share atomic leases and CRDT claims" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(dir_path);
    const shm_path = try std.fs.path.join(alloc, &.{ dir_path, "swarm.shm" });
    defer alloc.free(shm_path);

    var proc_a = try ShmSwarmBus.openOrCreate(shm_path);
    defer proc_a.close();
    var proc_b = try ShmSwarmBus.openOrCreate(shm_path);
    defer proc_b.close();

    // Process A acquires lease on src/main.zig
    const f1 = proc_a.tryAcquireLease(101, "src/main.zig", 1000, 500).?;
    try std.testing.expect(f1 >= 1);
    // Process B is blocked while lease is active
    try std.testing.expect(proc_b.tryAcquireLease(202, "src/main.zig", 1200, 500) == null);
    // After expiry, Process B acquires higher fencing token
    const f2 = proc_b.tryAcquireLease(202, "src/main.zig", 1600, 500).?;
    try std.testing.expect(f2 > f1);

    // Process A publishes claim, Process B reads immediately via shared mmap
    try std.testing.expect(proc_a.publishClaim(101, "ci:status", "green"));
    var buf: [64]u8 = undefined;
    const val = proc_b.readClaim("ci:status", &buf).?;
    try std.testing.expectEqualStrings("green", val);
}
