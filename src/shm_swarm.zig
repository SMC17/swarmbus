//! Cross-process lease and claim table in a shared memory-mapped file (layout v2).
//!
//! Why it is built this way:
//! - Each key has a fixed place, found by hash with linear probing, and a slot's `state` only
//!   moves forward (empty -> initialising -> ready) and never back. Two processes creating the
//!   same key walk the same probe sequence and meet at the same first empty slot. Only one
//!   compare-and-swap wins it, and the loser waits for `ready` and then sees the key. That
//!   rules out two slots for one key, which is what let two holders share a lease in v1.
//! - Leases and claims are different slot kinds, so a lease can never take over a claim's slot.
//! - Writes to a ready slot hold a per-slot seqlock: `seq` is odd while a writer is inside.
//!   Writers take it with a compare-and-swap; readers retry while it is odd or has changed. A
//!   reader therefore never returns a half-written value, and writers never interleave.
//! - Fencing tokens and claim order come from one 64-bit counter in the header.
//!
//! Known limits: 64 slots that are never freed (release clears a lease's owner but keeps its
//! key); keys up to 64 bytes and values up to 192 bytes (longer is an error, not truncation);
//! a process that dies inside a write (tens of nanoseconds) leaves that slot locked until the
//! file is recreated.
const std = @import("std");

pub const MAGIC: u64 = 0x3255_4253_5741_524D; // "MRAWSBU2" little-endian: layout v2
pub const VERSION: u32 = 2;
pub const MAX_SLOTS: u32 = 64;
pub const KEY_CAP: usize = 64;
pub const VALUE_CAP: usize = 192;

pub const SlotState = enum(u32) { empty = 0, initialising = 1, ready = 2 };
pub const SlotKind = enum(u32) { none = 0, lease = 1, claim = 2 };

pub const ShmSlot = extern struct {
    /// Seqlock: odd while a writer is inside.
    seq: std.atomic.Value(u64),
    state: std.atomic.Value(u32),
    kind: u32,
    key_hash: u64,
    key_len: u32,
    value_len: u32,
    /// Lease holder, or the claim's last writer. 0 means none.
    owner_agent: u32,
    _pad: u32,
    /// Lease: fencing token of the current grant. Claim: order of the stored write.
    fence: u64,
    expires_ns: i64,
    key: [KEY_CAP]u8,
    value: [VALUE_CAP]u8,
};

pub const ShmHeader = extern struct {
    magic: std.atomic.Value(u64),
    version: u32,
    slot_count: u32,
    /// Next fencing token / claim order. Starts at 1.
    counter: std.atomic.Value(u64),
    slots: [MAX_SLOTS]ShmSlot,
};

comptime {
    // Python (`python/swarmbus.py`) and Rust (`rust/src/lib.rs`) hard-code these.
    std.debug.assert(@sizeOf(ShmSlot) == 312);
    std.debug.assert(@offsetOf(ShmHeader, "slots") == 24);
}

pub const Error = error{
    KeyTooLong,
    ValueTooLong,
    EmptyKey,
    InvalidAgent,
    TableFull,
    SlotInitStuck,
    WrongLayout,
    FtruncateFailed,
};

pub const ClaimView = struct { value: []const u8, writer: u32, order: u64 };
pub const LeaseView = struct { owner: u32, fence: u64, expires_ns: i64 };

const spin_limit: usize = 1 << 24;

pub const ShmSwarmBus = struct {
    fd: std.posix.fd_t,
    mapped: []align(std.heap.page_size_min) u8,
    hdr: *ShmHeader,

    pub fn openOrCreate(path: []const u8) !ShmSwarmBus {
        const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDWR, .CREAT = true }, 0o660);
        errdefer _ = std.posix.system.close(fd);

        const total_size = @sizeOf(ShmHeader);
        if (std.posix.errno(std.posix.system.ftruncate(fd, total_size)) != .SUCCESS) return error.FtruncateFailed;

        const prot: std.posix.PROT = if (@typeInfo(std.posix.PROT) == .@"struct")
            .{ .READ = true, .WRITE = true }
        else
            std.posix.PROT.READ | std.posix.PROT.WRITE;
        const mapped = try std.posix.mmap(null, total_size, prot, .{ .TYPE = .SHARED }, fd, 0);
        errdefer std.posix.munmap(mapped);

        const hdr: *ShmHeader = @ptrCast(@alignCast(mapped.ptr));
        const cur = hdr.magic.load(.acquire);
        if (cur == 0) {
            // Fresh (all-zero) file. Racing initialisers write identical values.
            hdr.version = VERSION;
            hdr.slot_count = MAX_SLOTS;
            _ = hdr.counter.cmpxchgStrong(0, 1, .acq_rel, .acquire);
            hdr.magic.store(MAGIC, .release);
        } else if (cur != MAGIC) {
            // A v1 file or something else: refuse rather than reinterpret it.
            return error.WrongLayout;
        }
        return .{ .fd = fd, .mapped = mapped, .hdr = hdr };
    }

    pub fn close(self: *ShmSwarmBus) void {
        std.posix.munmap(self.mapped);
        _ = std.posix.system.close(self.fd);
    }

    fn checkKey(key: []const u8) Error!void {
        if (key.len == 0) return error.EmptyKey;
        if (key.len > KEY_CAP) return error.KeyTooLong;
    }

    fn keyMatches(slot: *const ShmSlot, kind: SlotKind, kh: u64, key: []const u8) bool {
        return slot.kind == @intFromEnum(kind) and slot.key_hash == kh and slot.key_len == key.len and
            std.mem.eql(u8, slot.key[0..key.len], key);
    }

    fn waitReady(slot: *ShmSlot) Error!void {
        var n: usize = 0;
        while (slot.state.load(.acquire) != @intFromEnum(SlotState.ready)) : (n += 1) {
            if (n > spin_limit) return error.SlotInitStuck;
            std.atomic.spinLoopHint();
        }
    }

    /// Finds the slot for (kind, key), or null. Never creates.
    fn find(self: *ShmSwarmBus, kind: SlotKind, key: []const u8) Error!?*ShmSlot {
        try checkKey(key);
        const kh = std.hash.Wyhash.hash(@intFromEnum(kind), key);
        var i: u32 = 0;
        while (i < MAX_SLOTS) : (i += 1) {
            const slot = &self.hdr.slots[@intCast((kh % MAX_SLOTS + i) % MAX_SLOTS)];
            switch (slot.state.load(.acquire)) {
                @intFromEnum(SlotState.empty) => return null,
                @intFromEnum(SlotState.initialising) => {
                    try waitReady(slot);
                    if (keyMatches(slot, kind, kh, key)) return slot;
                },
                else => if (keyMatches(slot, kind, kh, key)) return slot,
            }
        }
        return null;
    }

    /// Finds or creates the slot for (kind, key).
    fn findOrCreate(self: *ShmSwarmBus, kind: SlotKind, key: []const u8) Error!*ShmSlot {
        try checkKey(key);
        const kh = std.hash.Wyhash.hash(@intFromEnum(kind), key);
        var i: u32 = 0;
        while (i < MAX_SLOTS) : (i += 1) {
            const slot = &self.hdr.slots[@intCast((kh % MAX_SLOTS + i) % MAX_SLOTS)];
            if (slot.state.cmpxchgStrong(@intFromEnum(SlotState.empty), @intFromEnum(SlotState.initialising), .acq_rel, .acquire) == null) {
                slot.kind = @intFromEnum(kind);
                slot.key_hash = kh;
                slot.key_len = @intCast(key.len);
                @memset(&slot.key, 0);
                @memcpy(slot.key[0..key.len], key);
                slot.value_len = 0;
                slot.owner_agent = 0;
                slot.fence = 0;
                slot.expires_ns = 0;
                slot.state.store(@intFromEnum(SlotState.ready), .release);
                return slot;
            }
            try waitReady(slot);
            if (keyMatches(slot, kind, kh, key)) return slot;
        }
        return error.TableFull;
    }

    fn lock(slot: *ShmSlot) u64 {
        while (true) {
            const s = slot.seq.load(.acquire);
            if (s & 1 == 0) {
                if (slot.seq.cmpxchgWeak(s, s + 1, .acq_rel, .acquire) == null) return s + 1;
            }
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(slot: *ShmSlot, odd: u64) void {
        slot.seq.store(odd + 1, .release);
    }

    /// Grants the lease on `resource` to `agent_id` (non-zero) if it is free, expired, or
    /// already held by the same agent. Returns the new fencing token, or null if another
    /// agent holds it unexpired.
    pub fn tryAcquireLease(self: *ShmSwarmBus, agent_id: u32, resource: []const u8, now_ns: i64, ttl_ns: i64) Error!?u64 {
        if (agent_id == 0) return error.InvalidAgent;
        const slot = try self.findOrCreate(.lease, resource);
        const s = lock(slot);
        defer unlock(slot, s);
        if (slot.owner_agent != 0 and slot.owner_agent != agent_id and slot.expires_ns > now_ns) return null;
        const fence = self.hdr.counter.fetchAdd(1, .acq_rel);
        slot.owner_agent = agent_id;
        slot.expires_ns = now_ns + ttl_ns;
        slot.fence = fence;
        return fence;
    }

    /// Releases the lease if `agent_id` holds it. Returns whether it did.
    pub fn releaseLease(self: *ShmSwarmBus, agent_id: u32, resource: []const u8) Error!bool {
        const slot = (try self.find(.lease, resource)) orelse return false;
        const s = lock(slot);
        defer unlock(slot, s);
        if (slot.owner_agent != agent_id) return false;
        slot.owner_agent = 0;
        slot.expires_ns = 0;
        return true;
    }

    /// Last-writer-wins by the header counter, which is taken before the slot lock, so a write
    /// that started later always wins. Returns false only if a later write already landed.
    pub fn publishClaim(self: *ShmSwarmBus, agent_id: u32, key: []const u8, value: []const u8) Error!bool {
        if (value.len > VALUE_CAP) return error.ValueTooLong;
        const slot = try self.findOrCreate(.claim, key);
        const order = self.hdr.counter.fetchAdd(1, .acq_rel);
        const s = lock(slot);
        defer unlock(slot, s);
        if (order <= slot.fence) return false;
        @memset(&slot.value, 0);
        @memcpy(slot.value[0..value.len], value);
        slot.value_len = @intCast(value.len);
        slot.owner_agent = agent_id;
        slot.fence = order;
        return true;
    }

    /// Consistent snapshot of a claim, copied into `out_buf`. Null if the key has no claim.
    pub fn readClaim(self: *ShmSwarmBus, key: []const u8, out_buf: []u8) Error!?ClaimView {
        const slot = (try self.find(.claim, key)) orelse return null;
        while (true) {
            const s1 = slot.seq.load(.acquire);
            if (s1 & 1 == 1) {
                std.atomic.spinLoopHint();
                continue;
            }
            const n = @min(@as(usize, @atomicLoad(u32, &slot.value_len, .monotonic)), VALUE_CAP, out_buf.len);
            for (0..n) |i| out_buf[i] = @atomicLoad(u8, &slot.value[i], .monotonic);
            const writer = @atomicLoad(u32, &slot.owner_agent, .monotonic);
            const order = @atomicLoad(u64, &slot.fence, .monotonic);
            if (slot.seq.load(.acquire) == s1) {
                if (order == 0) return null; // created but never written
                return .{ .value = out_buf[0..n], .writer = writer, .order = order };
            }
        }
    }

    pub fn readLease(self: *ShmSwarmBus, resource: []const u8) Error!?LeaseView {
        const slot = (try self.find(.lease, resource)) orelse return null;
        while (true) {
            const s1 = slot.seq.load(.acquire);
            if (s1 & 1 == 1) {
                std.atomic.spinLoopHint();
                continue;
            }
            const v = LeaseView{
                .owner = @atomicLoad(u32, &slot.owner_agent, .monotonic),
                .fence = @atomicLoad(u64, &slot.fence, .monotonic),
                .expires_ns = @atomicLoad(i64, &slot.expires_ns, .monotonic),
            };
            if (slot.seq.load(.acquire) == s1) return v;
        }
    }
};

fn tmpShmPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, name: []const u8) ![]u8 {
    const dir_path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(dir_path);
    return std.fs.path.join(alloc, &.{ dir_path, name });
}

test "two mmap handles share leases and claims" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpShmPath(alloc, &tmp, "swarm.shm");
    defer alloc.free(path);

    var a = try ShmSwarmBus.openOrCreate(path);
    defer a.close();
    var b = try ShmSwarmBus.openOrCreate(path);
    defer b.close();

    const f1 = (try a.tryAcquireLease(101, "src/main.zig", 1000, 500)).?;
    try std.testing.expect((try b.tryAcquireLease(202, "src/main.zig", 1200, 500)) == null);
    const f2 = (try b.tryAcquireLease(202, "src/main.zig", 1600, 500)).?;
    try std.testing.expect(f2 > f1);

    try std.testing.expect(try a.publishClaim(101, "ci:status", "green"));
    var buf: [VALUE_CAP]u8 = undefined;
    const v = (try b.readClaim("ci:status", &buf)).?;
    try std.testing.expectEqualStrings("green", v.value);
    try std.testing.expectEqual(@as(u32, 101), v.writer);
}

test "regression #2: a lease never overwrites a claim" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpShmPath(alloc, &tmp, "swarm.shm");
    defer alloc.free(path);
    var bus = try ShmSwarmBus.openOrCreate(path);
    defer bus.close();

    try std.testing.expect(try bus.publishClaim(1, "ci:status", "green"));
    var i: u32 = 0;
    while (i < 40) : (i += 1) {
        var nb: [16]u8 = undefined;
        _ = (try bus.tryAcquireLease(2, try std.fmt.bufPrint(&nb, "res{d}", .{i}), 0, 1_000_000)).?;
    }
    var buf: [VALUE_CAP]u8 = undefined;
    try std.testing.expectEqualStrings("green", (try bus.readClaim("ci:status", &buf)).?.value);
}

test "keys match exactly, never by prefix; oversized input is an error" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpShmPath(alloc, &tmp, "swarm.shm");
    defer alloc.free(path);
    var bus = try ShmSwarmBus.openOrCreate(path);
    defer bus.close();

    try std.testing.expect(try bus.publishClaim(1, "ci:status", "green"));
    var buf: [VALUE_CAP]u8 = undefined;
    try std.testing.expect((try bus.readClaim("ci", &buf)) == null);
    try std.testing.expectError(error.KeyTooLong, bus.publishClaim(1, "k" ** 65, "v"));
    try std.testing.expectError(error.ValueTooLong, bus.publishClaim(1, "k", "v" ** 193));
    try std.testing.expectError(error.InvalidAgent, bus.tryAcquireLease(0, "r", 0, 1));
    try std.testing.expect(!(try bus.releaseLease(9, "never-leased")));
}

test "release lets another agent take the lease at once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpShmPath(alloc, &tmp, "swarm.shm");
    defer alloc.free(path);
    var bus = try ShmSwarmBus.openOrCreate(path);
    defer bus.close();

    _ = (try bus.tryAcquireLease(1, "deploy", 0, 1_000_000_000)).?;
    try std.testing.expect((try bus.tryAcquireLease(2, "deploy", 1, 1_000)) == null);
    try std.testing.expect(!(try bus.releaseLease(2, "deploy")));
    try std.testing.expect(try bus.releaseLease(1, "deploy"));
    try std.testing.expect((try bus.tryAcquireLease(2, "deploy", 2, 1_000)) != null);
}

const Race = struct {
    path: []const u8,
    go: *std.atomic.Value(u32),
    granted: *std.atomic.Value(u32),
    torn: *std.atomic.Value(u32),

    fn leaser(ctx: Race, agent: u32) void {
        var bus = ShmSwarmBus.openOrCreate(ctx.path) catch return;
        defer bus.close();
        while (ctx.go.load(.acquire) == 0) std.atomic.spinLoopHint();
        if ((bus.tryAcquireLease(agent, "deploy:prod", 0, 1_000_000_000) catch null) != null)
            _ = ctx.granted.fetchAdd(1, .acq_rel);
    }

    fn writer(ctx: Race, agent: u32) void {
        var bus = ShmSwarmBus.openOrCreate(ctx.path) catch return;
        defer bus.close();
        const fill: u8 = @intCast('a' + agent);
        const v = [_]u8{fill} ** VALUE_CAP;
        var i: usize = 0;
        while (i < 2000) : (i += 1) _ = bus.publishClaim(agent, "hot", &v) catch {};
    }

    fn reader(ctx: Race) void {
        var bus = ShmSwarmBus.openOrCreate(ctx.path) catch return;
        defer bus.close();
        var buf: [VALUE_CAP]u8 = undefined;
        var i: usize = 0;
        while (i < 4000) : (i += 1) {
            const v = (bus.readClaim("hot", &buf) catch null) orelse continue;
            for (v.value) |c| if (c != v.value[0]) {
                _ = ctx.torn.fetchAdd(1, .acq_rel);
                break;
            };
        }
    }
};

test "regression #3: one lease holder and no torn claims under contention" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmpShmPath(alloc, &tmp, "race.shm");
    defer alloc.free(path);

    var go = std.atomic.Value(u32).init(0);
    var granted = std.atomic.Value(u32).init(0);
    var torn = std.atomic.Value(u32).init(0);
    const ctx = Race{ .path = path, .go = &go, .granted = &granted, .torn = &torn };

    var round: usize = 0;
    while (round < 200) : (round += 1) {
        tmp.dir.deleteFile(std.testing.io, "race.shm") catch {};
        go.store(0, .release);
        granted.store(0, .release);
        var ts: [4]std.Thread = undefined;
        for (&ts, 0..) |*t, k| t.* = try std.Thread.spawn(.{}, Race.leaser, .{ ctx, @as(u32, @intCast(k + 1)) });
        go.store(1, .release);
        for (ts) |t| t.join();
        try std.testing.expectEqual(@as(u32, 1), granted.load(.acquire));
    }

    var ws: [3]std.Thread = undefined;
    for (&ws, 0..) |*t, k| t.* = try std.Thread.spawn(.{}, Race.writer, .{ ctx, @as(u32, @intCast(k + 1)) });
    var rs: [2]std.Thread = undefined;
    for (&rs) |*t| t.* = try std.Thread.spawn(.{}, Race.reader, .{ctx});
    for (ws) |t| t.join();
    for (rs) |t| t.join();
    try std.testing.expectEqual(@as(u32, 0), torn.load(.acquire));
}
