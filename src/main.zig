const std = @import("std");
pub const shm_swarm = @import("shm_swarm.zig");
pub const blackboard = @import("blackboard.zig");

const usage =
    \\swarmbus — Lock-Free Cross-Process POSIX mmap Swarm Bus, CRDT Blackboard & Compute Auction
    \\
    \\Usage:
    \\  swarmbus lease <shm_path> <agent_id> <resource> <ttl_ms>
    \\  swarmbus claim <shm_path> <agent_id> <key> <value>
    \\  swarmbus read  <shm_path> <key>
    \\  swarmbus bench
    \\
;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const args = try std.process.argsAlloc(alloc);
    if (args.len < 2) {
        try std.fs.File.stdout().writeAll(usage);
        return;
    }

    const cmd = args[1];
    if (std.mem.eql(u8, cmd, "lease") and args.len >= 6) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ttl_ms = try std.fmt.parseInt(i64, args[5], 10);
        const now_ns: i64 = @intCast(std.time.nanoTimestamp());
        const fence = bus.tryAcquireLease(agent_id, args[4], now_ns, ttl_ms * 1_000_000);
        var buf: [128]u8 = undefined;
        const msg = if (fence) |f|
            try std.fmt.bufPrint(&buf, "{{\"acquired\":true,\"fencing_token\":{d}}}\n", .{f})
        else
            try std.fmt.bufPrint(&buf, "{{\"acquired\":false}}\n", .{});
        try std.fs.File.stdout().writeAll(msg);
    } else if (std.mem.eql(u8, cmd, "claim") and args.len >= 6) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ok = bus.publishClaim(agent_id, args[4], args[5]);
        var buf: [64]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "{{\"published\":{}}}\n", .{ok});
        try std.fs.File.stdout().writeAll(msg);
    } else if (std.mem.eql(u8, cmd, "read") and args.len >= 4) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        var vbuf: [192]u8 = undefined;
        if (bus.readClaim(args[3], &vbuf)) |v| {
            try std.fs.File.stdout().writeAll(v);
            try std.fs.File.stdout().writeAll("\n");
        }
    } else if (std.mem.eql(u8, cmd, "bench")) {
        var timer = try std.time.Timer.start();
        var bb = blackboard.Blackboard.init(alloc);
        defer bb.deinit();
        var i: usize = 0;
        while (i < 10_000) : (i += 1) {
            _ = try bb.publishClaim(@intCast(i % 16), "swarm:state", "active");
        }
        const crdt_ns = timer.read() / 10_000;
        var buf: [128]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "{{\"crdt_claim_ns\":{d}}}\n", .{crdt_ns});
        try std.fs.File.stdout().writeAll(msg);
    } else {
        try std.fs.File.stdout().writeAll(usage);
    }
}

test {
    std.testing.refAllDecls(@This());
}
