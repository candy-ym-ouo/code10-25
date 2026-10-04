import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { prisma } from "./prisma.js";
import { getObjectStream } from "./s3.js";
import { generatePeaks, probeAudio } from "./media.js";
import { log } from "./log.js";

export async function processMedia(mediaId: string) {
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;
  // 条件认领：只接受待解析/解析中的记录。恢复扫描可能补发重复任务，
  // 已完成（READY）或已终结的记录必须直接跳过，避免覆盖已完成的元数据。
  const claimed = await prisma.mediaAsset.updateMany({
    where: { id: mediaId, status: { in: ["UPLOADED", "PROCESSING"] } },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });
  if (claimed.count === 0) {
    log("info", { mediaId, status: media.status }, "media already finalized, skip probe");
    return;
  }

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    const stream = await getObjectStream(media.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);
    let persisted = false;
    await prisma.$transaction(async (tx) => {
      const finalized = await tx.mediaAsset.updateMany({
        where: { id: mediaId, status: "PROCESSING" },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
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
      persisted = true;
    });
    if (persisted) log("info", { mediaId, durationMs: Number(probe.durationMs) }, "media probe completed");
    else log("info", { mediaId }, "media state changed during probe, result discarded");
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const code = message === "NO_AUDIO_STREAM" ? "NO_AUDIO_STREAM" : message === "INVALID_DURATION" ? "INVALID_DURATION" : "MEDIA_PROBE_FAILED";
    await prisma.mediaAsset.updateMany({
      where: { id: mediaId, status: "PROCESSING" },
      data: {
        status: "FAILED",
        failureCode: code,
        failureMessage: message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试",
        processedAt: new Date(),
      },
    });
    log("error", { mediaId, err: message }, "media probe failed");
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}
