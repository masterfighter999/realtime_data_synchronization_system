import { Order } from '@prisma/client';
import { redis } from '../infrastructure/redis';
import { runWithRedisBreaker } from '../infrastructure/circuitBreaker';
import { logger } from '../shared/logger';

const CACHE_TTL_SECONDS = 300; // 5 minutes TTL for demonstration
const CACHE_KEY_PREFIX = 'order:';

/**
 * Fetches an order from the Redis cache.
 * Returns null on cache miss or Redis error.
 */
export async function getCachedOrder(id: number): Promise<Order | null> {
  const cacheKey = `${CACHE_KEY_PREFIX}${id}`;
  try {
    const cached = await runWithRedisBreaker(() => redis.get(cacheKey));
    if (cached) {
      logger.debug({ id }, 'Cache HIT for order');
      // Revive dates during parsing
      const order = JSON.parse(cached);
      order.updated_at = new Date(order.updated_at);
      return order as Order;
    }
  } catch (error: any) {
    logger.warn({ err: error.message, id }, 'Failed to get cached order, falling back');
  }
  logger.debug({ id }, 'Cache MISS for order');
  return null;
}

/**
 * Stores an order in the Redis cache.
 */
export async function setCachedOrder(order: Order): Promise<void> {
  const cacheKey = `${CACHE_KEY_PREFIX}${order.id}`;
  try {
    await runWithRedisBreaker(() =>
      redis.set(cacheKey, JSON.stringify(order), 'EX', CACHE_TTL_SECONDS)
    );
    logger.debug({ id: order.id }, 'Populated order cache');
  } catch (error: any) {
    logger.warn({ err: error.message, id: order.id }, 'Failed to write order cache');
  }
}

/**
 * Invalidates an order cache entry.
 */
export async function invalidateOrderCache(id: number): Promise<void> {
  const cacheKey = `${CACHE_KEY_PREFIX}${id}`;
  try {
    await runWithRedisBreaker(() => redis.del(cacheKey));
    logger.debug({ id }, 'Invalidated order cache');
  } catch (error: any) {
    logger.error({ err: error.message, id }, 'Failed to invalidate order cache');
  }
}
