import Redis from 'ioredis';
import { config } from '../shared/config';
import { logger } from '../shared/logger';

export function createRedisClient(): Redis {
  const client = new Redis(config.redisUrl, {
    maxRetriesPerRequest: null,
    retryStrategy(times) {
      // Exponential retry strategy capped at 2 seconds
      const delay = Math.min(times * 100, 2000);
      return delay;
    },
  });

  client.on('error', (err) => {
    logger.error({ err }, 'Redis connection error');
  });

  client.on('connect', () => {
    logger.info('Redis connected');
  });

  return client;
}

export const redis = createRedisClient();
export const pubRedis = createRedisClient();
export const subRedis = createRedisClient();
