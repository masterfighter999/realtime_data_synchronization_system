import { redis } from '../infrastructure/redis';
import { runWithRedisBreaker } from '../infrastructure/circuitBreaker';
import { logger } from '../shared/logger';

const CLIENTS_METRIC_KEY = 'metrics:connected_clients';
const EVENTS_PUBLISHED_PREFIX = 'metrics:events_published:';

/**
 * Increments the global client connection counter.
 */
export async function incrementClientCount(): Promise<number> {
  try {
    return await runWithRedisBreaker(() => redis.incr(CLIENTS_METRIC_KEY));
  } catch (error: any) {
    logger.warn({ err: error.message }, 'Failed to increment client count');
    return 0;
  }
}

/**
 * Decrements the global client connection counter, capping it at 0.
 */
export async function decrementClientCount(): Promise<number> {
  try {
    const count = await runWithRedisBreaker(() => redis.decr(CLIENTS_METRIC_KEY));
    if (count < 0) {
      await runWithRedisBreaker(() => redis.set(CLIENTS_METRIC_KEY, 0));
      return 0;
    }
    return count;
  } catch (error: any) {
    logger.warn({ err: error.message }, 'Failed to decrement client count');
    return 0;
  }
}

/**
 * Gets the current count of connected clients.
 */
export async function getClientCount(): Promise<number> {
  try {
    const val = await runWithRedisBreaker(() => redis.get(CLIENTS_METRIC_KEY));
    return val ? parseInt(val, 10) : 0;
  } catch {
    return 0;
  }
}

/**
 * Logs an event publication to calculate events-per-second throughput.
 */
export async function recordPublishedEvent(): Promise<void> {
  const currentSecond = Math.floor(Date.now() / 1000);
  const key = `${EVENTS_PUBLISHED_PREFIX}${currentSecond}`;
  try {
    await runWithRedisBreaker(async () => {
      await redis.pipeline()
        .incr(key)
        .expire(key, 10) // Expire after 10 seconds to avoid memory leaks
        .exec();
    });
  } catch (error: any) {
    logger.warn({ err: error.message }, 'Failed to record published event metric');
  }
}

/**
 * Computes the throughput (events per second) for the preceding second.
 */
export async function getEventsPerSecond(): Promise<number> {
  const previousSecond = Math.floor(Date.now() / 1000) - 1;
  const key = `${EVENTS_PUBLISHED_PREFIX}${previousSecond}`;
  try {
    const count = await runWithRedisBreaker(() => redis.get(key));
    return count ? parseInt(count, 10) : 0;
  } catch {
    return 0;
  }
}
