const std = @import("std");

/// Sub-millisecond shared-state CRDT Blackboard, TTL Lease Manager,
/// and Compute-Budget Auction Scheduler for multi-agent swarms.
pub const Claim = struct {
    key: []const u8,
    value: []const u8,
    lamport_ts: u64,
    agent_id: u32,
};

pub const Lease = struct {
    resource: []const u8,
    owner_agent: u32,
    expires_ns: i128,
    fencing_token: u64,
};

pub const ComputeBid = struct {
    agent_id: u32,
    utility: f64,
    requested_tokens: u32,
    est_latency_ms: u32,
};

pub const AuctionResult = struct {
    granted_agents: []u32,
    total_tokens_allocated: u32,
    total_utility: f64,
};

pub const Blackboard = struct {
    allocator: std.mem.Allocator,
    claims: std.StringHashMapUnmanaged(Claim) = .{},
    leases: std.StringHashMapUnmanaged(Lease) = .{},
    next_fencing_token: u64 = 1,
    lamport_clock: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) Blackboard {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Blackboard) void {
        var cit = self.claims.iterator();
        while (cit.next()) |e| {
            self.allocator.free(e.value_ptr.key);
            self.allocator.free(e.value_ptr.value);
        }
        self.claims.deinit(self.allocator);

        var lit = self.leases.iterator();
        while (lit.next()) |e| {
            self.allocator.free(e.value_ptr.resource);
        }
        self.leases.deinit(self.allocator);
    }

    pub fn publishClaim(self: *Blackboard, agent_id: u32, key: []const u8, value: []const u8) !u64 {
        self.lamport_clock += 1;
        const ts = self.lamport_clock;
        _ = try self.mergeClaim(.{
            .key = key,
            .value = value,
            .lamport_ts = ts,
            .agent_id = agent_id,
        });
        return ts;
    }

    /// Deterministic LWW-CRDT merge: higher (lamport_ts, agent_id) wins.
    pub fn mergeClaim(self: *Blackboard, incoming: Claim) !bool {
        if (incoming.lamport_ts > self.lamport_clock) {
            self.lamport_clock = incoming.lamport_ts;
        }
        if (self.claims.getPtr(incoming.key)) |cur| {
            const wins = (incoming.lamport_ts > cur.lamport_ts) or
                (incoming.lamport_ts == cur.lamport_ts and incoming.agent_id > cur.agent_id);
            if (!wins) return false;
            const new_val = try self.allocator.dupe(u8, incoming.value);
            self.allocator.free(cur.value);
            cur.value = new_val;
            cur.lamport_ts = incoming.lamport_ts;
            cur.agent_id = incoming.agent_id;
            return true;
        } else {
            const k = try self.allocator.dupe(u8, incoming.key);
            errdefer self.allocator.free(k);
            const v = try self.allocator.dupe(u8, incoming.value);
            try self.claims.put(self.allocator, k, .{
                .key = k,
                .value = v,
                .lamport_ts = incoming.lamport_ts,
                .agent_id = incoming.agent_id,
            });
            return true;
        }
    }

    pub fn getClaim(self: *const Blackboard, key: []const u8) ?Claim {
        return self.claims.get(key);
    }

    /// Acquires an exclusive fenced lease on `resource` until `now_ns + ttl_ns`.
    pub fn acquireLease(self: *Blackboard, agent_id: u32, resource: []const u8, now_ns: i128, ttl_ns: i128) !?u64 {
        if (self.leases.getPtr(resource)) |cur| {
            if (cur.expires_ns > now_ns and cur.owner_agent != agent_id) {
                return null; // Contended by active owner
            }
            const tok = self.next_fencing_token;
            self.next_fencing_token += 1;
            cur.owner_agent = agent_id;
            cur.expires_ns = now_ns + ttl_ns;
            cur.fencing_token = tok;
            return tok;
        } else {
            const res_owned = try self.allocator.dupe(u8, resource);
            const tok = self.next_fencing_token;
            self.next_fencing_token += 1;
            try self.leases.put(self.allocator, res_owned, .{
                .resource = res_owned,
                .owner_agent = agent_id,
                .expires_ns = now_ns + ttl_ns,
                .fencing_token = tok,
            });
            return tok;
        }
    }

    pub fn releaseLease(self: *Blackboard, agent_id: u32, resource: []const u8) bool {
        if (self.leases.get(resource)) |cur| {
            if (cur.owner_agent != agent_id) return false;
            const kv = self.leases.fetchRemove(resource).?;
            self.allocator.free(kv.value.resource);
            return true;
        }
        return false;
    }

    /// Clears a compute-budget auction under token and per-step latency caps,
    /// maximizing utility density (`utility / requested_tokens`).
    pub fn clearBudgetAuction(
        allocator: std.mem.Allocator,
        bids: []const ComputeBid,
        token_budget: u32,
        max_latency_ms: u32,
    ) !AuctionResult {
        const sorted = try allocator.dupe(ComputeBid, bids);
        defer allocator.free(sorted);

        std.mem.sort(ComputeBid, sorted, {}, struct {
            fn lessThan(_: void, a: ComputeBid, b: ComputeBid) bool {
                const da = if (a.requested_tokens == 0) 0.0 else a.utility / @as(f64, @floatFromInt(a.requested_tokens));
                const db = if (b.requested_tokens == 0) 0.0 else b.utility / @as(f64, @floatFromInt(b.requested_tokens));
                if (da != db) return da > db;
                return a.agent_id < b.agent_id;
            }
        }.lessThan);

        var winners = std.ArrayListUnmanaged(u32).empty;
        errdefer winners.deinit(allocator);

        var used_tokens: u32 = 0;
        var total_util: f64 = 0.0;
        for (sorted) |b| {
            if (b.requested_tokens == 0 or b.est_latency_ms > max_latency_ms) continue;
            if (used_tokens + b.requested_tokens <= token_budget) {
                try winners.append(allocator, b.agent_id);
                used_tokens += b.requested_tokens;
                total_util += b.utility;
            }
        }
        return .{
            .granted_agents = try winners.toOwnedSlice(allocator),
            .total_tokens_allocated = used_tokens,
            .total_utility = total_util,
        };
    }
};

test "Blackboard CRDT merge, fenced leases, and compute-budget auction" {
    const alloc = std.testing.allocator;
    var bb = Blackboard.init(alloc);
    defer bb.deinit();

    // LWW-CRDT convergence regardless of merge order
    _ = try bb.mergeClaim(.{ .key = "build:status", .value = "failing", .lamport_ts = 5, .agent_id = 1 });
    const stale_applied = try bb.mergeClaim(.{ .key = "build:status", .value = "old", .lamport_ts = 4, .agent_id = 9 });
    try std.testing.expect(!stale_applied);
    try std.testing.expectEqualStrings("failing", bb.getClaim("build:status").?.value);

    const newer_applied = try bb.mergeClaim(.{ .key = "build:status", .value = "green", .lamport_ts = 5, .agent_id = 2 });
    try std.testing.expect(newer_applied);
    try std.testing.expectEqualStrings("green", bb.getClaim("build:status").?.value);

    // Fenced leases prevent concurrent file clobbering
    const t1 = (try bb.acquireLease(1, "src/main.zig", 1000, 500)).?;
    try std.testing.expectEqual(@as(u64, 1), t1);
    try std.testing.expect((try bb.acquireLease(2, "src/main.zig", 1200, 500)) == null);
    // After expiry at t=1600, agent 2 can acquire with monotonically higher fencing token
    const t2 = (try bb.acquireLease(2, "src/main.zig", 1600, 500)).?;
    try std.testing.expect(t2 > t1);

    // Compute-budget auction clears highest utility-per-token agents within budget & latency SLA
    const bids = [_]ComputeBid{
        .{ .agent_id = 10, .utility = 9.0, .requested_tokens = 1000, .est_latency_ms = 200 },
        .{ .agent_id = 11, .utility = 8.0, .requested_tokens = 500, .est_latency_ms = 150 }, // highest density: 0.016
        .{ .agent_id = 12, .utility = 20.0, .requested_tokens = 900, .est_latency_ms = 5000 }, // violates latency SLA
        .{ .agent_id = 13, .utility = 3.0, .requested_tokens = 500, .est_latency_ms = 100 }, // density 0.006
    };
    const res = try Blackboard.clearBudgetAuction(alloc, &bids, 1500, 1000);
    defer alloc.free(res.granted_agents);
    try std.testing.expectEqual(@as(u32, 1500), res.total_tokens_allocated);
    try std.testing.expectEqual(@as(usize, 2), res.granted_agents.len);
    try std.testing.expectEqual(@as(u32, 11), res.granted_agents[0]);
    try std.testing.expectEqual(@as(u32, 10), res.granted_agents[1]);
}
