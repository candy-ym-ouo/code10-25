import { describe, expect, it, vi } from "vitest";
import {
  recoverStuckProcessingMedia,
  STUCK_PROCESSING_THRESHOLD_MS,
  type RecoverStuckProcessingDeps,
} from "../src/lib/recovery.js";

interface FakeRow {
  id: string;
  status: string;
  updatedAt: Date;
}

function createFakePrisma(rows: FakeRow[]) {
  const updateMany = vi.fn(
    async (args: { where: { id: string; status: string }; data: { status: string } }) => {
      let count = 0;
      for (const row of rows) {
        if (row.id === args.where.id && row.status === args.where.status) {
          row.status = args.data.status;
          count += 1;
        }
      }
      return { count };
    },
  );
  const findMany = vi.fn(
    async (args: { where: { status: string; updatedAt: { lt: Date } }; take: number }) =>
      rows
        .filter((row) => row.status === args.where.status && row.updatedAt < args.where.updatedAt.lt)
        .sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime())
        .slice(0, args.take)
        .map((row) => ({ id: row.id })),
  );
  return {
    findMany,
    updateMany,
    prisma: { mediaAsset: { findMany, updateMany } } as unknown as RecoverStuckProcessingDeps["prisma"],
  };
}

const NOW = new Date("2026-10-04T12:00:00.000Z");
const stale = new Date(NOW.getTime() - STUCK_PROCESSING_THRESHOLD_MS - 1000);
const fresh = new Date(NOW.getTime() - 1000);

describe("recoverStuckProcessingMedia", () => {
  it("re-enqueues media stuck in PROCESSING beyond the threshold", async () => {
    const rows: FakeRow[] = [{ id: "m1", status: "PROCESSING", updatedAt: stale }];
    const { prisma, updateMany } = createFakePrisma(rows);
    const enqueueProbe = vi.fn(async () => {});

    const result = await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });

    expect(result).toEqual({ scanned: 1, recovered: 1 });
    expect(enqueueProbe).toHaveBeenCalledWith("m1");
    expect(rows[0]!.status).toBe("UPLOADED");
    // 认领与状态重置只允许条件更新，且只写状态列
    for (const call of updateMany.mock.calls) {
      expect(call[0].where.status).toBeDefined();
      expect(Object.keys(call[0].data)).toEqual(["status"]);
    }
  });

  it("never touches READY/FAILED records or fresh PROCESSING records", async () => {
    const rows: FakeRow[] = [
      { id: "ready", status: "READY", updatedAt: stale },
      { id: "failed", status: "FAILED", updatedAt: stale },
      { id: "active", status: "PROCESSING", updatedAt: fresh },
    ];
    const { prisma } = createFakePrisma(rows);
    const enqueueProbe = vi.fn(async () => {});

    const result = await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });

    expect(result).toEqual({ scanned: 0, recovered: 0 });
    expect(enqueueProbe).not.toHaveBeenCalled();
    expect(rows.map((row) => row.status)).toEqual(["READY", "FAILED", "PROCESSING"]);
  });

  it("is idempotent: a second sweep finds nothing to recover", async () => {
    const rows: FakeRow[] = [{ id: "m1", status: "PROCESSING", updatedAt: stale }];
    const { prisma } = createFakePrisma(rows);
    const enqueueProbe = vi.fn(async () => {});

    await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });
    const second = await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });

    expect(second).toEqual({ scanned: 0, recovered: 0 });
    expect(enqueueProbe).toHaveBeenCalledTimes(1);
  });

  it("skips enqueue when the record was finalized concurrently", async () => {
    const rows: FakeRow[] = [{ id: "m1", status: "PROCESSING", updatedAt: stale }];
    const { prisma, updateMany } = createFakePrisma(rows);
    // 模拟并发：认领前记录已被正常流程处理完成
    updateMany.mockImplementationOnce(async () => {
      rows[0]!.status = "READY";
      return { count: 0 };
    });
    const enqueueProbe = vi.fn(async () => {});

    const result = await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });

    expect(result).toEqual({ scanned: 1, recovered: 0 });
    expect(enqueueProbe).not.toHaveBeenCalled();
    expect(rows[0]!.status).toBe("READY");
  });

  it("rolls the claim back when enqueueing fails so the next sweep retries", async () => {
    const rows: FakeRow[] = [{ id: "m1", status: "PROCESSING", updatedAt: stale }];
    const { prisma } = createFakePrisma(rows);
    const enqueueProbe = vi.fn(async () => {
      throw new Error("redis unavailable");
    });

    await expect(recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW })).rejects.toThrow("redis unavailable");
    expect(rows[0]!.status).toBe("PROCESSING");

    enqueueProbe.mockImplementation(async () => {});
    const retry = await recoverStuckProcessingMedia({ prisma, enqueueProbe, now: NOW });
    expect(retry).toEqual({ scanned: 1, recovered: 1 });
    expect(rows[0]!.status).toBe("UPLOADED");
  });
});
