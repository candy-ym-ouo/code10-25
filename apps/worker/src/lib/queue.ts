import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "../config/env.js";

let queue: Queue | undefined;

function getMediaQueue(): Queue {
  if (!queue) {
    const connection = new Redis(getConfig().REDIS_URL, {
      lazyConnect: true,
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
    });
    queue = new Queue("media-processing", { connection });
  }
  return queue;
}

export async function enqueueRecoveryProbe(mediaId: string): Promise<void> {
  await getMediaQueue().add(
    "probe-media",
    { mediaId },
    {
      jobId: `probe-recovery:${mediaId}:${Date.now()}`,
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
