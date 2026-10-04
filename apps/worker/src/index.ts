import { Queue, Worker } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { deleteObject, putObject } from "./lib/s3.js";
import { buildUserExport } from "./lib/export.js";
import { processMedia } from "./lib/processing.js";
import { recoverStuckMedia } from "./lib/recovery.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

const runProcessMedia = (mediaId: string) => processMedia({ prisma, log }, mediaId);

async function cleanupSession(sessionId: string) {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    include: { mediaAssets: { select: { objectKey: true } } },
  });
  if (!session) return;
  try {
    const keys = new Set(session.mediaAssets.map((media) => media.objectKey));
    for (const objectKey of keys) {
      const references = await prisma.mediaAsset.count({ where: { objectKey, sessionId: { not: sessionId } } });
      if (references === 0) await deleteObject(objectKey);
    }
    await prisma.practiceSession.delete({ where: { id: sessionId } });
    log("info", { sessionId }, "session cleanup completed");
  } catch (error) {
    await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    throw error;
  }
}

async function exportData(exportId: string) {
  const task = await prisma.dataExport.findUnique({ where: { id: exportId } });
  if (!task || !task.objectKey) return;
  await prisma.dataExport.update({ where: { id: exportId }, data: { status: "PROCESSING" } });
  try {
    const output = await buildUserExport(task.userId, task.format);
    await putObject(task.objectKey, output.body, output.contentType);
    await prisma.dataExport.update({ where: { id: exportId }, data: { status: "READY", failure: null } });
  } catch (error) {
    await prisma.dataExport.update({
      where: { id: exportId },
      data: { status: "FAILED", failure: error instanceof Error ? error.message.slice(0, 500) : "EXPORT_FAILED" },
    });
    throw error;
  }
}

async function scanOverdueGoals() {
  const startOfToday = new Date();
  startOfToday.setUTCHours(0, 0, 0, 0);
  const result = await prisma.goal.updateMany({
    where: {
      dueDate: { lt: startOfToday },
      status: { in: ["OPEN", "IN_PROGRESS"] },
    },
    data: { status: "MISSED" },
  });
  if (result.count > 0) log("info", { count: result.count }, "overdue goals marked missed");
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") return runProcessMedia(String(job.data.mediaId));
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId));
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    throw new Error(`Unknown job: ${job.name}`);
  },
  { connection: redis, concurrency: config.WORKER_CONCURRENCY },
);

worker.on("failed", (job, error) => log("error", { jobId: job?.id, jobName: job?.name, err: error.message }, "job failed"));
worker.on("error", (error) => log("error", { err: error.message }, "worker error"));

// 恢复扫描：进程异常退出或队列丢失后，数据库里可能残留卡在 UPLOADED/PROCESSING
// 的记录。数据库是业务数据源，定期扫描并幂等重新入队即可自愈。
const recoveryQueue = new Queue("media-processing", { connection: redis.duplicate() });
async function recoverStuckMediaOnce() {
  try {
    const result = await recoverStuckMedia({ prisma, queue: recoveryQueue, stuckAfterMs: config.MEDIA_STUCK_AFTER_MS });
    if (result.enqueued > 0) log("info", { ...result }, "stuck media re-enqueued");
  } catch (error) {
    log("error", { err: error instanceof Error ? error.message : String(error) }, "stuck media recovery failed");
  }
}

const heartbeat = setInterval(async () => {
  await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
}, 10_000);
await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
await scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
await recoverStuckMediaOnce();
const recoveryInterval = setInterval(() => {
  void recoverStuckMediaOnce();
}, config.MEDIA_RECOVERY_INTERVAL_MS);
const overdueInterval = setInterval(() => {
  void scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
}, 24 * 60 * 60_000);

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(recoveryInterval);
  clearInterval(overdueInterval);
  await worker.close();
  await recoveryQueue.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
