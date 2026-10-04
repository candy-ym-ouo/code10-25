import type { PrismaClient } from "@prisma/client";

// ffprobe 单次探测超时为 120 秒，10 分钟足以判定记录是异常退出残留而非正在处理
export const STUCK_PROCESSING_THRESHOLD_MS = 10 * 60 * 1000;
const RECOVERY_BATCH_SIZE = 100;

export interface RecoverStuckProcessingDeps {
  prisma: Pick<PrismaClient, "mediaAsset">;
  enqueueProbe: (mediaId: string) => Promise<void>;
  now?: Date;
  thresholdMs?: number;
}

export interface RecoverStuckProcessingResult {
  scanned: number;
  recovered: number;
}

/**
 * 把 Worker 异常退出后残留的 PROCESSING 记录安全地重新排队。
 *
 * 安全性与幂等性：
 * - 只通过条件更新（where status = ...）修改状态列，绝不触碰
 *   durationMs / codec / peaks 等已完成的解析元数据；
 * - 认领（PROCESSING -> UPLOADED）是原子操作，并发扫描或记录恰好
 *   处理完成时 count 为 0，直接跳过；
 * - 入队失败会按条件回滚到 PROCESSING，等待下一轮扫描重试，
 *   重复执行整个扫描不会产生额外副作用。
 */
export async function recoverStuckProcessingMedia(
  deps: RecoverStuckProcessingDeps,
): Promise<RecoverStuckProcessingResult> {
  const thresholdMs = deps.thresholdMs ?? STUCK_PROCESSING_THRESHOLD_MS;
  const cutoff = new Date((deps.now ?? new Date()).getTime() - thresholdMs);
  const stuck = await deps.prisma.mediaAsset.findMany({
    where: { status: "PROCESSING", updatedAt: { lt: cutoff } },
    select: { id: true },
    orderBy: { updatedAt: "asc" },
    take: RECOVERY_BATCH_SIZE,
  });

  let recovered = 0;
  for (const media of stuck) {
    const claimed = await deps.prisma.mediaAsset.updateMany({
      where: { id: media.id, status: "PROCESSING" },
      data: { status: "UPLOADED" },
    });
    if (claimed.count === 0) continue;
    try {
      await deps.enqueueProbe(media.id);
      recovered += 1;
    } catch (error) {
      await deps.prisma.mediaAsset.updateMany({
        where: { id: media.id, status: "UPLOADED" },
        data: { status: "PROCESSING" },
      });
      throw error;
    }
  }
  return { scanned: stuck.length, recovered };
}
