const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "swarmbus",
        .root_module = root_mod,
    });
    b.installArtifact(exe);

    const tests = b.addTest(.{ .root_module = root_mod });
    const test_step = b.step("test", "Run all swarmbus unit and mmap cross-process tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);
}
