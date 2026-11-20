const std = @import("std");
pub const shm_swarm = @import("shm_swarm.zig");
pub const blackboard = @import("blackboard.zig");

const usage =
    \\swarmbus — cross-process lease and claim table in shared memory, plus an in-process blackboard and auction
    \\
    \\Usage:
    \\  swarmbus lease   <shm_path> <agent_id> <resource> <ttl_ms>   agent_id must be non-zero
    \\  swarmbus release <shm_path> <agent_id> <resource>
    \\  swarmbus claim   <shm_path> <agent_id> <key> <value>         key <= 64 bytes, value <= 192 bytes
    \\  swarmbus read    <shm_path> <key>
    \\  swarmbus auction <token_budget> <max_latency_ms> [<agent_id:utility:tokens:latency_ms> ... | -]
    \\  swarmbus bench   [<shm_path>]                                 in-process, shared-memory and cross-process latency
    \\
;

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.real.now(io).nanoseconds);
}

fn elapsedNs(io: std.Io, start: std.Io.Timestamp) u64 {
    return @intCast(@max(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds, 0));
}

/// Child side of the cross-process round trip: wait for ping n, answer pong n.
fn benchPong(path: []const u8, rounds: u64) !void {
    var bus = try shm_swarm.ShmSwarmBus.openOrCreate(path);
    defer bus.close();
    var buf: [32]u8 = undefined;
    var n: u64 = 1;
    while (n <= rounds) : (n += 1) {
        while (true) {
            if (try bus.readClaim("bench:ping", &buf)) |v| {
                if ((std.fmt.parseInt(u64, v.value, 10) catch 0) == n) break;
            }
            std.atomic.spinLoopHint();
        }
        var nb: [24]u8 = undefined;
        _ = try bus.publishClaim(2, "bench:pong", try std.fmt.bufPrint(&nb, "{d}", .{n}));
    }
}

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
        const fence = try bus.tryAcquireLease(agent_id, args[4], nowNs(io), ttl_ms * 1_000_000);
        var buf: [128]u8 = undefined;
        const msg = if (fence) |f|
            try std.fmt.bufPrint(&buf, "{{\"acquired\":true,\"fencing_token\":{d}}}\n", .{f})
        else
            try std.fmt.bufPrint(&buf, "{{\"acquired\":false}}\n", .{});
        try stdout.writeStreamingAll(io, msg);
    } else if (std.mem.eql(u8, cmd, "release") and args.len >= 5) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ok = try bus.releaseLease(agent_id, args[4]);
        var buf: [64]u8 = undefined;
        try stdout.writeStreamingAll(io, try std.fmt.bufPrint(&buf, "{{\"released\":{}}}\n", .{ok}));
    } else if (std.mem.eql(u8, cmd, "claim") and args.len >= 6) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        const agent_id = try std.fmt.parseInt(u32, args[3], 10);
        const ok = try bus.publishClaim(agent_id, args[4], args[5]);
        var buf: [64]u8 = undefined;
        try stdout.writeStreamingAll(io, try std.fmt.bufPrint(&buf, "{{\"published\":{}}}\n", .{ok}));
    } else if (std.mem.eql(u8, cmd, "read") and args.len >= 4) {
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(args[2]);
        defer bus.close();
        var vbuf: [shm_swarm.VALUE_CAP]u8 = undefined;
        if (try bus.readClaim(args[3], &vbuf)) |v| {
            try stdout.writeStreamingAll(io, v.value);
            try stdout.writeStreamingAll(io, "\n");
        } else {
            std.process.exit(1);
        }
    } else if (std.mem.eql(u8, cmd, "bench-pong") and args.len >= 4) {
        try benchPong(args[2], try std.fmt.parseInt(u64, args[3], 10));
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
        // 1. In-process Blackboard (heap hash map, one process).
        var t = std.Io.Clock.awake.now(io);
        var bb = blackboard.Blackboard.init(alloc);
        defer bb.deinit();
        var i: usize = 0;
        while (i < 10_000) : (i += 1) _ = try bb.publishClaim(@intCast(i % 16), "swarm:state", "active");
        const bb_ns = elapsedNs(io, t) / 10_000;

        // 2. Shared-memory table, one process (no contention).
        const path = if (args.len >= 3) args[2] else "/tmp/swarmbus-bench.shm";
        std.Io.Dir.cwd().deleteFile(io, path) catch {};
        var bus = try shm_swarm.ShmSwarmBus.openOrCreate(path);
        defer bus.close();
        t = std.Io.Clock.awake.now(io);
        i = 0;
        while (i < 100_000) : (i += 1) _ = try bus.publishClaim(1, "swarm:state", "active");
        const claim_ns = elapsedNs(io, t) / 100_000;
        var rb: [shm_swarm.VALUE_CAP]u8 = undefined;
        t = std.Io.Clock.awake.now(io);
        i = 0;
        var sink: usize = 0;
        while (i < 100_000) : (i += 1) sink +%= (try bus.readClaim("swarm:state", &rb)).?.value.len;
        std.mem.doNotOptimizeAway(sink);
        const read_ns = elapsedNs(io, t) / 100_000;
        t = std.Io.Clock.awake.now(io);
        i = 0;
        while (i < 100_000) : (i += 1) _ = try bus.tryAcquireLease(1, "res", @intCast(i), 1_000);
        const lease_ns = elapsedNs(io, t) / 100_000;

        // 3. Cross-process round trip: this process publishes ping n, a child process
        //    (this binary, `bench-pong`) waits for it and publishes pong n.
        const rounds: u64 = 20_000;
        var rounds_buf: [24]u8 = undefined;
        var child = try std.process.spawn(io, .{
            .argv = &.{ args[0], "bench-pong", path, try std.fmt.bufPrint(&rounds_buf, "{d}", .{rounds}) },
        });
        var pb: [32]u8 = undefined;
        t = std.Io.Clock.awake.now(io);
        var n: u64 = 1;
        while (n <= rounds) : (n += 1) {
            var nb: [24]u8 = undefined;
            _ = try bus.publishClaim(1, "bench:ping", try std.fmt.bufPrint(&nb, "{d}", .{n}));
            while (true) {
                if (try bus.readClaim("bench:pong", &pb)) |v| {
                    if ((std.fmt.parseInt(u64, v.value, 10) catch 0) == n) break;
                }
                std.atomic.spinLoopHint();
            }
        }
        const rtt_ns = elapsedNs(io, t) / rounds;
        _ = try child.wait(io);

        var buf: [320]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "{{\"inprocess_blackboard_claim_ns\":{d},\"shm_claim_ns\":{d},\"shm_read_ns\":{d},\"shm_lease_ns\":{d},\"cross_process_round_trip_ns\":{d},\"rounds\":{d}}}\n", .{ bb_ns, claim_ns, read_ns, lease_ns, rtt_ns, rounds });
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
