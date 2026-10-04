import type { Queue } from "bullmq";
import type { PrismaClient } from "@prisma/client";

export const PROBE_JOB_NAME = "probe-media";

/**
 * 探测任务使用确定性 job id，API 入队与 Worker 恢复扫描共用同一个键，
 * 同一份音频在队列里最多只有一个待执行任务，重复触发不会产生重复任务。
 */
export const probeJobId = (mediaId: string): string => `probe:${mediaId}`;

/** 终态任务可以安全替换；其余状态说明队列里已有同名任务，无需重复入队。 */
const REPLACEABLE_JOB_STATES = new Set(["completed", "failed"]);

/**
 * 幂等入队：队列中已有未完结的同名任务时直接跳过；
 * 同名任务已完结（成功/失败）时先移除再重新入队。
 */
export async function enqueueProbeIfMissing(queue: Queue, mediaId: string): Promise<"enqueued" | "already-queued"> {
  const jobId = probeJobId(mediaId);
  const existing = await queue.getJob(jobId);
  if (existing) {
    const state = await existing.getState();
    if (!REPLACEABLE_JOB_STATES.has(state)) return "already-queued";
    try {
      await existing.remove();
    } catch {
      // 并发的入队方可能已将其移除，继续入队即可，jobId 会兜底去重
    }
  }
  await queue.add(PROBE_JOB_NAME, { mediaId }, {
    jobId,
    attempts: 3,
    backoff: { type: "exponential", delay: 3000 },
    removeOnComplete: 100,
    removeOnFail: 500,
  });
  return "enqueued";
}

export interface RecoverStuckMediaOptions {
  prisma: PrismaClient;
  queue: Queue;
  /** updatedAt 早于该时长的 UPLOADED/PROCESSING 记录视为卡住 */
  stuckAfterMs: number;
  now?: Date;
  /** 单次扫描最多恢复的记录数，避免异常情况下打满队列 */
  limit?: number;
}

export interface RecoverStuckMediaResult {
  candidates: number;
  enqueued: number;
  skipped: number;
}

/**
 * 从数据库恢复卡住的音频：进程异常退出、队列丢失或任务多次 stalled 后，
 * MediaAsset 可能永远停在 UPLOADED/PROCESSING。数据库是业务数据源，
 * 定期扫描并幂等重新入队即可自愈；已经入队或正在处理的记录会被跳过。
 */
export async function recoverStuckMedia(options: RecoverStuckMediaOptions): Promise<RecoverStuckMediaResult> {
  const { prisma, queue, stuckAfterMs } = options;
  const now = options.now ?? new Date();
  const threshold = new Date(now.getTime() - stuckAfterMs);
  const stuck = await prisma.mediaAsset.findMany({
    where: {
      status: { in: ["UPLOADED", "PROCESSING"] },
      updatedAt: { lt: threshold },
    },
    select: { id: true },
    orderBy: { updatedAt: "asc" },
    take: options.limit ?? 100,
  });
  if (stuck.length === 0) return { candidates: 0, enqueued: 0, skipped: 0 };

  // 正在被其他 Worker 处理（例如大文件探测时间较长）的记录不要重复入队
  const activeJobs = await queue.getActive(0, -1);
  const activeMediaIds = new Set(activeJobs.map((job) => String(job.data?.mediaId ?? "")));

  let enqueued = 0;
  let skipped = 0;
  for (const media of stuck) {
    if (activeMediaIds.has(media.id)) {
      skipped += 1;
      continue;
    }
    const result = await enqueueProbeIfMissing(queue, media.id);
    if (result === "enqueued") enqueued += 1;
    else skipped += 1;
  }
  return { candidates: stuck.length, enqueued, skipped };
}
