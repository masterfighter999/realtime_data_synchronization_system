import { prisma } from '../infrastructure/db';
import { runWithDbBreaker } from '../infrastructure/circuitBreaker';
import { logger } from '../shared/logger';

export interface LagMetrics {
  outboxLag: number;
  oldestUnprocessedSeconds: number;
}

/**
 * Calculates CDC lag from PostgreSQL outbox table.
 * Fallbacks to zeroes if database is down/unavailable.
 */
export async function getLagMetrics(): Promise<LagMetrics> {
  try {
    return await runWithDbBreaker(async () => {
      const outboxLag = await prisma.outboxEvent.count({
        where: { processed: false },
      });

      const oldestUnprocessed = await prisma.outboxEvent.findFirst({
        where: { processed: false },
        orderBy: { created_at: 'asc' },
        select: { created_at: true },
      });

      let oldestUnprocessedSeconds = 0;
      if (oldestUnprocessed) {
        const diffMs = Date.now() - new Date(oldestUnprocessed.created_at).getTime();
        oldestUnprocessedSeconds = Math.max(0, Math.floor(diffMs / 1000));
      }

      return {
        outboxLag,
        oldestUnprocessedSeconds,
      };
    });
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to compute outbox lag metrics');
    return {
      outboxLag: -1,
      oldestUnprocessedSeconds: -1,
    };
  }
}
