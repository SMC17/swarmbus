const std = @import("std");
pub const shm_swarm = @import("shm_swarm.zig");
pub const blackboard = @import("blackboard.zig");

const usage =
    \\swarmbus — Lock-Free Cross-Process POSIX mmap Swarm Bus, CRDT Blackboard & Compute Auction
    \\
    \\Usage:
    \\  swarmbus lease   <shm_path> <agent_id> <resource> <ttl_ms>
    \\  swarmbus claim   <shm_path> <agent_id> <key> <value>
    \\  swarmbus read    <shm_path> <key>
    \\  swarmbus auction <token_budget> <max_latency_ms> [<agent_id:utility:tokens:latency_ms> ... | -]
    \\  swarmbus bench
    \\
;

fn parseBidArg(arg: []const u8) !blackboard.ComputeBid {
    var it = std.mem.splitScalar(u8, arg, ':');
    const agent_s = it.next() orelse return error.InvalidBidFormat;
    const util_s = it.next() orelse return error.InvalidBidFormat;
    const tokens_s = it.next() orelse return error.InvalidBidFormat;
    const lat_s = it.next() orelse return error.InvalidBidFormat;
    if (it.next() != null) return error.InvalidBidFormat;

    return .{
        .agent_id = try std.fmt.parseInt(u32, agent_s, 10),
        .utility = try std.fmt.parseFloat(f64, util_s),
        .requested_tokens = try std.fmt.parseInt(u32, tokens_s, 10),
        .est_latency_ms = try std.fmt.parseInt(u32, lat_s, 10),
    };
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.arena.allocator();
    const io = init.io;
    const stdout = std.Io.File.stdout();

    const args = try init.minimal.args.toSlice(alloc);
    if (args.len < 2) {
        try stdout.writeStreamingAll(io, usage);
        return;
    }

    const cmd = args[1];
    if (std.mem.eql(u8, cmd, "lease") and args.len >= 6) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ttl_ms = try std.fmt.parseInt(i64, args[5], 10);
        const now_ns: i64 = @intCast(std.Io.Clock.real.now(io).nanoseconds);
        const fence = bus.tryAcquireLease(agent_id, args[4], now_ns, ttl_ms * 1_000_000);
        var buf: [128]u8 = undefined;
        const msg = if (fence) |f|
            try std.fmt.bufPrint(&buf, "{{\"acquired\":true,\"fencing_token\":{d}}}\n", .{f})
        else
            try std.fmt.bufPrint(&buf, "{{\"acquired\":false}}\n", .{});
        try stdout.writeStreamingAll(io, msg);
    } else if (std.mem.eql(u8, cmd, "claim") and args.len >= 6) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ok = bus.publishClaim(agent_id, args[4], args[5]);
        var buf: [64]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "{{\"published\":{}}}\n", .{ok});
        try stdout.writeStreamingAll(io, msg);
    } else if (std.mem.eql(u8, cmd, "read") and args.len >= 4) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        var vbuf: [192]u8 = undefined;
        if (bus.readClaim(args[3], &vbuf)) |v| {
            try stdout.writeStreamingAll(io, v);
            try stdout.writeStreamingAll(io, "\n");
        }
    } else if (std.mem.eql(u8, cmd, "auction") and args.len >= 4) {
        const token_budget = try std.fmt.parseInt(u32, args[2], 10);
        const max_latency_ms = try std.fmt.parseInt(u32, args[3], 10);

        var bids_slice: []const blackboard.ComputeBid = &.{};
        if (args.len == 4 or (args.len == 5 and std.mem.eql(u8, args[4], "-"))) {
            var stdin_buf: [4096]u8 = undefined;
            var r = std.Io.File.stdin().readerStreaming(io, &stdin_buf);
            const stdin_bytes = try r.interface.allocRemaining(alloc, .limited(1024 * 1024));
            const parsed = try std.json.parseFromSlice([]blackboard.ComputeBid, alloc, stdin_bytes, .{});
            bids_slice = parsed.value;
        } else {
            var bids = std.ArrayListUnmanaged(blackboard.ComputeBid).empty;
            for (args[4..]) |arg| {
                try bids.append(alloc, try parseBidArg(arg));
            }
            bids_slice = bids.items;
        }

        const res = try blackboard.Blackboard.clearBudgetAuction(alloc, bids_slice, token_budget, max_latency_ms);
        defer alloc.free(res.granted_agents);

        var out = std.ArrayListUnmanaged(u8).empty;
        try out.appendSlice(alloc, "{\"granted_agents\":[");
        for (res.granted_agents, 0..) |aid, idx| {
            if (idx > 0) try out.append(alloc, ',');
            var num_buf: [16]u8 = undefined;
            const num_s = try std.fmt.bufPrint(&num_buf, "{d}", .{aid});
            try out.appendSlice(alloc, num_s);
        }
        var tail_buf: [128]u8 = undefined;
        const tail = try std.fmt.bufPrint(
            &tail_buf,
            "],\"total_tokens_allocated\":{d},\"total_utility\":{d:.4}}}\n",
            .{ res.total_tokens_allocated, res.total_utility },
        );
        try out.appendSlice(alloc, tail);
        try stdout.writeStreamingAll(io, out.items);
    } else if (std.mem.eql(u8, cmd, "bench")) {
        const start = std.Io.Clock.awake.now(io);
        var bb = blackboard.Blackboard.init(alloc);
        defer bb.deinit();
        var i: usize = 0;
        while (i < 10_000) : (i += 1) {
            _ = try bb.publishClaim(@intCast(i % 16), "swarm:state", "active");
        }
        const elapsed_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
        const crdt_ns = elapsed_ns / 10_000;
        var buf: [128]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "{{\"crdt_claim_ns\":{d}}}\n", .{crdt_ns});
        try stdout.writeStreamingAll(io, msg);
    } else {
        try stdout.writeStreamingAll(io, usage);
    }
}

test "parseBidArg parses valid colon-delimited bids and rejects malformed ones" {
    const bid = try parseBidArg("11:8.5:500:150");
    try std.testing.expectEqual(@as(u32, 11), bid.agent_id);
    try std.testing.expectEqual(@as(f64, 8.5), bid.utility);
    try std.testing.expectEqual(@as(u32, 500), bid.requested_tokens);
    try std.testing.expectEqual(@as(u32, 150), bid.est_latency_ms);

    try std.testing.expectError(error.InvalidBidFormat, parseBidArg("11:8.5:500"));
    try std.testing.expectError(error.InvalidBidFormat, parseBidArg("11:8.5:500:150:extra"));
}

test {
    std.testing.refAllDecls(@This());
}
