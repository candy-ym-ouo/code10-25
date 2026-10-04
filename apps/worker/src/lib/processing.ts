import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import type { PrismaClient } from "@prisma/client";
import { generatePeaks, probeAudio } from "./media.js";
import { getObjectStream } from "./s3.js";

export type WorkerLog = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => void;

export interface ProcessMediaDeps {
  prisma: PrismaClient;
  log: WorkerLog;
  /** 以下依赖用于测试注入，默认走真实实现 */
  fetchObject?: typeof getObjectStream;
  probe?: typeof probeAudio;
  peaks?: typeof generatePeaks;
}

/** 允许被（重新）认领处理的状态；READY/FAILED/CANCELLED 等状态一律不触碰。 */
const CLAIMABLE_STATUSES = ["UPLOADED", "PROCESSING"] as const;

/**
 * 处理一份音频：认领 -> 下载 -> 探测 -> 落库。
 * 所有状态迁移都带前置条件，重复执行、崩溃恢复或并发任务都不会
 * 覆盖已经完成的元数据，也不会把 READY 记录改回 FAILED。
 */
export async function processMedia(deps: ProcessMediaDeps, mediaId: string): Promise<void> {
  const prisma = deps.prisma;
  const fetchObject = deps.fetchObject ?? getObjectStream;
  const probe = deps.probe ?? probeAudio;
  const peaks = deps.peaks ?? generatePeaks;

  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;

  // 条件认领：只有 UPLOADED/PROCESSING 才允许进入处理；
  // 已经是 READY/FAILED 的记录（例如重复任务、迟到的恢复任务）直接跳过。
  const claimed = await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: { in: [...CLAIMABLE_STATUSES] } },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });
  if (claimed.count === 0) {
    deps.log("info", { mediaId, status: media.status }, "media already finalized, skip processing");
    return;
  }

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    const stream = await fetchObject(media.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probeResult, peaksResult] = await Promise.all([probe(localPath), peaks(localPath)]);
    await prisma.$transaction(async (tx) => {
      // 只有仍为 PROCESSING 才允许落库：并发任务若已抢先完成，本次不再覆盖元数据
      const finalized = await tx.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: {
          status: "READY",
          durationMs: probeResult.durationMs,
          codec: probeResult.codec,
          sampleRate: probeResult.sampleRate,
          channels: probeResult.channels,
          peaks: peaksResult,
          processedAt: new Date(),
          expiresAt: null,
          failureCode: null,
          failureMessage: null,
        },
      });
      if (finalized.count === 0) return;
      await tx.practiceSession.updateMany({
        where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      });
    });
    deps.log("info", { mediaId, durationMs: Number(probeResult.durationMs) }, "media probe completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const code = message === "NO_AUDIO_STREAM" ? "NO_AUDIO_STREAM" : message === "INVALID_DURATION" ? "INVALID_DURATION" : "MEDIA_PROBE_FAILED";
    // 失败落库同样带条件：记录若已被并发流程置为 READY，不能被迟到的失败覆盖
    await prisma.mediaAsset.updateMany({
      where: { id: mediaId, status: "PROCESSING" },
      data: {
        status: "FAILED",
        failureCode: code,
        failureMessage: message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试",
        processedAt: new Date(),
      },
    });
    deps.log("error", { mediaId, err: message }, "media probe failed");
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}
