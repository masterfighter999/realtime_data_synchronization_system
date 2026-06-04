import { RateLimiterRedis } from 'rate-limiter-flexible';
import { redis } from '../infrastructure/redis';
import { Request, Response, NextFunction } from 'express';
import { logger } from './logger';

// 1. IP-based limiter: 100 requests per 15 minutes
export const ipLimiter = new RateLimiterRedis({
  storeClient: redis,
  keyPrefix: 'rl:ip:',
  points: 100,
  duration: 15 * 60,
  blockDuration: 60 * 5, // block for 5 minutes if limit hit
});

// 2. Client-based limiter: 30 requests per minute
export const clientLimiter = new RateLimiterRedis({
  storeClient: redis,
  keyPrefix: 'rl:client:',
  points: 30,
  duration: 60,
});

/**
 * Higher-order middleware to enforce rate limits.
 */
export function rateLimitMiddleware(limiter: RateLimiterRedis, useIp = false) {
  return async (req: Request, res: Response, next: NextFunction) => {
    // Determine limit identifier (username or IP address)
    const key = useIp ? (req.ip || 'unknown-ip') : (req.user?.username || req.ip || 'unknown-user');

    try {
      const rateLimitRes = await limiter.consume(key);
      res.setHeader('X-RateLimit-Limit', limiter.points);
      res.setHeader('X-RateLimit-Remaining', rateLimitRes.remainingPoints);
      next();
    } catch (rejected: any) {
      if (rejected instanceof Error) {
        // If Redis connectivity fails, fail open and log error to prevent system lockout
        logger.error({ err: rejected.message, key }, 'Rate limiter Redis error, failing open');
        return next();
      }

      // Limit reached, reject request
      res.setHeader('Retry-After', Math.ceil(rejected.msBeforeNext / 1000));
      res.setHeader('X-RateLimit-Limit', limiter.points);
      res.setHeader('X-RateLimit-Remaining', 0);
      return res.status(429).json({
        error: 'Too Many Requests',
        message: 'Rate limit exceeded. Please try again later.',
      });
    }
  };
}
