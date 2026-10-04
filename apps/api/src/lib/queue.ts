import { Queue } from "bullmq";
import { getConfig } from "../config/env.js";
import { getRedis } from "./redis.js";

let queue: Queue | undefined;

export function getMediaQueue(): Queue {
  if (!queue) {
    queue = new Queue("media-processing", { connection: getRedis().duplicate() });
  }
  return queue;
}

/**
 * 探测任务使用确定性 job id，与 Worker 的卡住记录恢复扫描共用同一个键：
 * 同一份音频在队列里最多只有一个未完结任务，重复确认上传/重试/恢复扫描
 * 不会产生重复任务；同名任务已完结（成功/失败）时先移除再重新入队。
 */
export async function enqueueProbe(mediaId: string): Promise<void> {
  const mediaQueue = getMediaQueue();
  const jobId = `probe:${mediaId}`;
  const existing = await mediaQueue.getJob(jobId);
  if (existing) {
    const state = await existing.getState();
    if (state !== "completed" && state !== "failed") return;
    try {
      await existing.remove();
    } catch {
      // 并发的入队方可能已将其移除，继续入队即可，jobId 会兜底去重
    }
  }
  await mediaQueue.add(
    "probe-media",
    { mediaId },
    {
      jobId,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueCleanup(sessionId: string): Promise<void> {
  await getMediaQueue().add(
    "cleanup-session",
    { sessionId },
    {
      jobId: `cleanup:${sessionId}:${Date.now()}`,
      attempts: 5,
      backoff: { type: "exponential", delay: 5000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueExport(exportId: string): Promise<void> {
  await getMediaQueue().add(
    "export-data",
    { exportId },
    {
      jobId: `export:${exportId}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close();
    queue = undefined;
  }
}
