import { Readable } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import type { Queue } from "bullmq";
import type { PrismaClient } from "@prisma/client";
import { enqueueProbeIfMissing, probeJobId, recoverStuckMedia } from "../src/lib/recovery.js";
import { processMedia, type ProcessMediaDeps } from "../src/lib/processing.js";

interface FakeJob {
  id: string;
  state: string;
  data: { mediaId: string };
}

function createFakeQueue(activeJobs: FakeJob[] = []) {
  const jobs = new Map<string, FakeJob>();
  const added: Array<{ name: string; data: unknown; opts: Record<string, unknown> }> = [];
  const removed: string[] = [];
  const queue = {
    getJob: async (id: string) => {
      const job = jobs.get(id);
      if (!job) return undefined;
      return {
        getState: async () => job.state,
        remove: async () => {
          removed.push(id);
          jobs.delete(id);
        },
      };
    },
    getActive: async () => activeJobs.map((job) => ({ data: job.data })),
    add: async (name: string, data: { mediaId: string }, opts: Record<string, unknown>) => {
      added.push({ name, data, opts });
      const id = String(opts.jobId);
      jobs.set(id, { id, state: "waiting", data });
      return { id };
    },
  };
  return { queue: queue as unknown as Queue, jobs, added, removed };
}

function createMockPrisma(options: {
  media?: Record<string, unknown> | null;
  claimCount?: number;
  finalizeCount?: number;
  failCount?: number;
  findManyResult?: Array<{ id: string }>;
}) {
  const calls = {
    mediaUpdateMany: [] as Array<{ where: unknown; data: Record<string, unknown> }>,
    sessionUpdateMany: [] as Array<{ where: unknown; data: Record<string, unknown> }>,
    findManyArgs: [] as unknown[],
  };
  const prisma = {
    mediaAsset: {
      findUnique: async () => options.media ?? null,
      findMany: async (args: unknown) => {
        calls.findManyArgs.push(args);
        return options.findManyResult ?? [];
      },
      updateMany: async (args: { where: unknown; data: Record<string, unknown> }) => {
        calls.mediaUpdateMany.push(args);
        const status = (args.data as { status?: string }).status;
        if (status === "PROCESSING") return { count: options.claimCount ?? 1 };
        if (status === "READY") return { count: options.finalizeCount ?? 1 };
        return { count: options.failCount ?? 1 };
      },
    },
    practiceSession: {
      updateMany: async (args: { where: unknown; data: Record<string, unknown> }) => {
        calls.sessionUpdateMany.push(args);
        return { count: 1 };
      },
    },
    $transaction: async (fn: (tx: unknown) => Promise<void>) => fn(prisma),
  };
  return { prisma: prisma as unknown as PrismaClient, calls };
}

const baseMedia = {
  id: "media-1",
  userId: "user-1",
  sessionId: "session-1",
  status: "UPLOADED",
  objectKey: "users/user-1/sessions/session-1/media-1/a.mp3",
  originalName: "a.mp3",
};

function createProcessDeps(prisma: PrismaClient, overrides: Partial<ProcessMediaDeps> = {}) {
  const probe = vi.fn(async () => ({ durationMs: 12_345n, codec: "mp3", sampleRate: 44_100, channels: 2 }));
  const peaks = vi.fn(async () => [0.1, 0.2, 0.3]);
  const fetchObject = vi.fn(async () => Readable.from([Buffer.from("fake-audio")]));
  const deps: ProcessMediaDeps = {
    prisma,
    log: vi.fn(),
    fetchObject,
    probe,
    peaks,
    ...overrides,
  };
  return { deps, probe, peaks, fetchObject };
}

describe("enqueueProbeIfMissing", () => {
  it("enqueues with a deterministic job id when no job exists", async () => {
    const { queue, added } = createFakeQueue();
    const result = await enqueueProbeIfMissing(queue, "media-1");
    expect(result).toBe("enqueued");
    expect(added).toHaveLength(1);
    expect(added[0]!.name).toBe("probe-media");
    expect(added[0]!.data).toEqual({ mediaId: "media-1" });
    expect(added[0]!.opts.jobId).toBe(probeJobId("media-1"));
  });

  it("is idempotent when an unfinished job already exists", async () => {
    for (const state of ["waiting", "active", "delayed", "prioritized"]) {
      const { queue, jobs, added } = createFakeQueue();
      jobs.set(probeJobId("media-1"), { id: probeJobId("media-1"), state, data: { mediaId: "media-1" } });
      const result = await enqueueProbeIfMissing(queue, "media-1");
      expect(result).toBe("already-queued");
      expect(added).toHaveLength(0);
    }
  });

  it("replaces a finished job so retries are possible", async () => {
    for (const state of ["completed", "failed"]) {
      const { queue, jobs, added, removed } = createFakeQueue();
      jobs.set(probeJobId("media-1"), { id: probeJobId("media-1"), state, data: { mediaId: "media-1" } });
      const result = await enqueueProbeIfMissing(queue, "media-1");
      expect(result).toBe("enqueued");
      expect(removed).toEqual([probeJobId("media-1")]);
      expect(added).toHaveLength(1);
    }
  });
});

describe("recoverStuckMedia", () => {
  const now = new Date("2026-10-04T12:00:00.000Z");

  it("re-enqueues media stuck in UPLOADED/PROCESSING past the threshold", async () => {
    const { prisma, calls } = createMockPrisma({ findManyResult: [{ id: "media-1" }, { id: "media-2" }] });
    const { queue, added } = createFakeQueue();
    const result = await recoverStuckMedia({ prisma, queue, stuckAfterMs: 600_000, now });
    expect(result).toEqual({ candidates: 2, enqueued: 2, skipped: 0 });
    expect(added.map((job) => job.data)).toEqual([{ mediaId: "media-1" }, { mediaId: "media-2" }]);

    const query = calls.findManyArgs[0] as { where: { status: { in: string[] }; updatedAt: { lt: Date } } };
    expect(query.where.status.in).toEqual(["UPLOADED", "PROCESSING"]);
    expect(query.where.updatedAt.lt.toISOString()).toBe("2026-10-04T11:50:00.000Z");
  });

  it("skips media that is actively being processed", async () => {
    const { prisma } = createMockPrisma({ findManyResult: [{ id: "media-1" }, { id: "media-2" }] });
    const active = [{ id: "job-1", state: "active", data: { mediaId: "media-1" } }];
    const { queue, added } = createFakeQueue(active);
    const result = await recoverStuckMedia({ prisma, queue, stuckAfterMs: 600_000, now });
    expect(result).toEqual({ candidates: 2, enqueued: 1, skipped: 1 });
    expect(added.map((job) => job.data)).toEqual([{ mediaId: "media-2" }]);
  });

  it("does not enqueue duplicates on repeated runs", async () => {
    const { prisma } = createMockPrisma({ findManyResult: [{ id: "media-1" }] });
    const { queue, added } = createFakeQueue();
    await recoverStuckMedia({ prisma, queue, stuckAfterMs: 600_000, now });
    const second = await recoverStuckMedia({ prisma, queue, stuckAfterMs: 600_000, now });
    expect(second).toEqual({ candidates: 1, enqueued: 0, skipped: 1 });
    expect(added).toHaveLength(1);
  });

  it("does nothing when no media is stuck", async () => {
    const { prisma } = createMockPrisma({ findManyResult: [] });
    const { queue, added } = createFakeQueue();
    const result = await recoverStuckMedia({ prisma, queue, stuckAfterMs: 600_000, now });
    expect(result).toEqual({ candidates: 0, enqueued: 0, skipped: 0 });
    expect(added).toHaveLength(0);
  });
});

describe("processMedia", () => {
  it("claims only UPLOADED/PROCESSING records and never touches finalized ones", async () => {
    const { prisma, calls } = createMockPrisma({ media: { ...baseMedia, status: "READY" }, claimCount: 0 });
    const { deps, fetchObject, probe, peaks } = createProcessDeps(prisma);
    await processMedia(deps, "media-1");

    expect(calls.mediaUpdateMany).toHaveLength(1);
    const claim = calls.mediaUpdateMany[0]!;
    expect(claim.where).toEqual({ id: "media-1", status: { in: ["UPLOADED", "PROCESSING"] } });
    // 认领失败后不得继续下载、探测或写入任何元数据
    expect(fetchObject).not.toHaveBeenCalled();
    expect(probe).not.toHaveBeenCalled();
    expect(peaks).not.toHaveBeenCalled();
  });

  it("writes READY metadata only while the record is still PROCESSING", async () => {
    const { prisma, calls } = createMockPrisma({ media: { ...baseMedia } });
    const { deps } = createProcessDeps(prisma);
    await processMedia(deps, "media-1");

    expect(calls.mediaUpdateMany).toHaveLength(2);
    const finalize = calls.mediaUpdateMany[1]!;
    expect(finalize.where).toEqual({ id: "media-1", status: "PROCESSING" });
    expect(finalize.data.status).toBe("READY");
    expect(finalize.data.durationMs).toBe(12_345n);
    expect(finalize.data.codec).toBe("mp3");
    expect(finalize.data.peaks).toEqual([0.1, 0.2, 0.3]);
    expect(finalize.data.expiresAt).toBeNull();

    expect(calls.sessionUpdateMany).toHaveLength(1);
    expect(calls.sessionUpdateMany[0]!.where).toEqual({ id: "session-1", userId: "user-1", status: "DRAFT" });
  });

  it("does not move the session when a concurrent worker already finalized the record", async () => {
    const { prisma, calls } = createMockPrisma({ media: { ...baseMedia }, finalizeCount: 0 });
    const { deps } = createProcessDeps(prisma);
    await processMedia(deps, "media-1");
    expect(calls.sessionUpdateMany).toHaveLength(0);
  });

  it("marks FAILED only while still PROCESSING, so a late failure cannot overwrite READY", async () => {
    const { prisma, calls } = createMockPrisma({ media: { ...baseMedia } });
    const { deps, probe } = createProcessDeps(prisma);
    probe.mockRejectedValueOnce(new Error("NO_AUDIO_STREAM"));
    await processMedia(deps, "media-1");

    const failure = calls.mediaUpdateMany.at(-1)!;
    expect(failure.where).toEqual({ id: "media-1", status: "PROCESSING" });
    expect(failure.data.status).toBe("FAILED");
    expect(failure.data.failureCode).toBe("NO_AUDIO_STREAM");
  });
});
