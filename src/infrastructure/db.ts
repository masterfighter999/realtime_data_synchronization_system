import { PrismaClient } from '@prisma/client';
import { logger } from '../shared/logger';
import { config } from '../shared/config';

export const prisma = new PrismaClient({
  log: config.nodeEnv === 'development'
    ? [
        { emit: 'event', level: 'query' },
        { emit: 'stdout', level: 'info' },
        { emit: 'stdout', level: 'warn' },
        { emit: 'stdout', level: 'error' },
      ]
    : [
        { emit: 'stdout', level: 'error' },
      ],
});

if (config.nodeEnv === 'development') {
  (prisma as any).$on('query', (e: any) => {
    logger.debug({ query: e.query, params: e.params, duration: `${e.duration}ms` }, 'Prisma Query');
  });
}
