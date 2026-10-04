import { Readable } from "node:stream";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const mediaUpdateMany = vi.fn();
  const txMediaUpdateMany = vi.fn();
  const txSessionUpdateMany = vi.fn();
  return {
    findUnique: vi.fn(),
    mediaUpdateMany,
    txMediaUpdateMany,
    txSessionUpdateMany,
    getObjectStream: vi.fn(),
    probeAudio: vi.fn(),
    generatePeaks: vi.fn(),
  };
});

vi.mock("../src/lib/prisma.js", () => ({
  prisma: {
    mediaAsset: { findUnique: mocks.findUnique, updateMany: mocks.mediaUpdateMany },
    $transaction: async (callback: (tx: unknown) => Promise<unknown>) =>
      callback({
        mediaAsset: { updateMany: mocks.txMediaUpdateMany },
        practiceSession: { updateMany: mocks.txSessionUpdateMany },
      }),
  },
}));
vi.mock("../src/lib/s3.js", () => ({ getObjectStream: mocks.getObjectStream }));
vi.mock("../src/lib/media.js", () => ({ probeAudio: mocks.probeAudio, generatePeaks: mocks.generatePeaks }));
vi.mock("../src/lib/log.js", () => ({ log: vi.fn() }));

import { processMedia } from "../src/lib/process-media.js";

const baseMedia = {
  id: "m1",
  userId: "u1",
  sessionId: "s1",
  status: "UPLOADED",
  objectKey: "users/u1/sessions/s1/m1/a.wav",
  originalName: "a.wav",
};

beforeEach(() => {
  vi.clearAllMocks();
  mocks.getObjectStream.mockResolvedValue(Readable.from([Buffer.from("fake-audio")]));
  mocks.probeAudio.mockResolvedValue({ durationMs: 1234n, codec: "pcm_s16le", sampleRate: 44100, channels: 1 });
  mocks.generatePeaks.mockResolvedValue([0.1, 0.2]);
});

describe("processMedia", () => {
  it("skips records that are already finalized instead of overwriting them", async () => {
    mocks.findUnique.mockResolvedValue({ ...baseMedia, status: "READY" });
    mocks.mediaUpdateMany.mockResolvedValue({ count: 0 });

    await processMedia("m1");

    expect(mocks.mediaUpdateMany).toHaveBeenCalledWith({
      where: { id: "m1", status: { in: ["UPLOADED", "PROCESSING"] } },
      data: { status: "PROCESSING", failureCode: null, failureMessage: null },
    });
    expect(mocks.getObjectStream).not.toHaveBeenCalled();
    expect(mocks.probeAudio).not.toHaveBeenCalled();
    expect(mocks.txMediaUpdateMany).not.toHaveBeenCalled();
  });

  it("writes probe metadata only through a conditional finalize", async () => {
    mocks.findUnique.mockResolvedValue(baseMedia);
    mocks.mediaUpdateMany.mockResolvedValue({ count: 1 });
    mocks.txMediaUpdateMany.mockResolvedValue({ count: 1 });
    mocks.txSessionUpdateMany.mockResolvedValue({ count: 1 });

    await processMedia("m1");

    const finalize = mocks.txMediaUpdateMany.mock.calls[0]![0];
    expect(finalize.where).toEqual({ id: "m1", status: "PROCESSING" });
    expect(finalize.data).toMatchObject({ status: "READY", durationMs: 1234n, codec: "pcm_s16le", peaks: [0.1, 0.2] });
    expect(mocks.txSessionUpdateMany).toHaveBeenCalledWith({
      where: { id: "s1", userId: "u1", status: "DRAFT" },
      data: { status: "IN_REVIEW", version: { increment: 1 } },
    });
  });

  it("discards the probe result when the record changed state concurrently", async () => {
    mocks.findUnique.mockResolvedValue(baseMedia);
    mocks.mediaUpdateMany.mockResolvedValue({ count: 1 });
    mocks.txMediaUpdateMany.mockResolvedValue({ count: 0 });

    await processMedia("m1");

    expect(mocks.txMediaUpdateMany).toHaveBeenCalledTimes(1);
    expect(mocks.txSessionUpdateMany).not.toHaveBeenCalled();
  });

  it("marks FAILED only when the record is still PROCESSING", async () => {
    mocks.findUnique.mockResolvedValue(baseMedia);
    mocks.mediaUpdateMany.mockResolvedValue({ count: 1 });
    mocks.probeAudio.mockRejectedValue(new Error("NO_AUDIO_STREAM"));

    await processMedia("m1");

    const failure = mocks.mediaUpdateMany.mock.calls[1]![0];
    expect(failure.where).toEqual({ id: "m1", status: "PROCESSING" });
    expect(failure.data).toMatchObject({ status: "FAILED", failureCode: "NO_AUDIO_STREAM" });
    expect(mocks.txMediaUpdateMany).not.toHaveBeenCalled();
  });
});
