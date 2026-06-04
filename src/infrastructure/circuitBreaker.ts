import CircuitBreaker from 'opossum';
import { logger } from '../shared/logger';

const breakerOptions = {
  timeout: 3000,                // 3s timeout
  errorThresholdPercentage: 50, // Open if 50% operations fail
  resetTimeout: 10000,          // Wait 10s before attempting half-open state
};

// Generic helper to create circuit breakers
export function createBreaker<TI extends any[], TO>(
  action: (...args: TI) => Promise<TO>,
  name: string
): CircuitBreaker<TI, TO> {
  const breaker = new CircuitBreaker(action, {
    ...breakerOptions,
    name,
  });

  breaker.on('open', () => {
    logger.error({ name }, `Circuit Breaker [${name}] is now OPEN`);
  });

  breaker.on('halfOpen', () => {
    logger.warn({ name }, `Circuit Breaker [${name}] is now HALF-OPEN`);
  });

  breaker.on('close', () => {
    logger.info({ name }, `Circuit Breaker [${name}] is now CLOSED (functioning normally)`);
  });

  return breaker;
}

// 1. PostgreSQL Circuit Breaker
const dbBreaker = createBreaker(
  async <T>(operation: () => Promise<T>): Promise<T> => {
    return operation();
  },
  'PostgreSQL-Database'
);

export async function runWithDbBreaker<T>(operation: () => Promise<T>): Promise<T> {
  return dbBreaker.fire(operation) as Promise<T>;
}

// 2. Redis Circuit Breaker
const redisBreaker = createBreaker(
  async <T>(operation: () => Promise<T>): Promise<T> => {
    return operation();
  },
  'Redis-Cache'
);

export async function runWithRedisBreaker<T>(operation: () => Promise<T>): Promise<T> {
  return redisBreaker.fire(operation) as Promise<T>;
}

// Export breakers for metrics retrieval
export { dbBreaker, redisBreaker };
