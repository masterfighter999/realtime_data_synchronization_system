import Redlock from 'redlock';
import { prisma } from '../infrastructure/db';
import { redis } from '../infrastructure/redis';
import { producer, connectProducer } from '../infrastructure/kafka';
import { runWithDbBreaker } from '../infrastructure/circuitBreaker';
import { recordPublishedEvent } from '../monitoring/redisMetrics';
import { SerializedEvent } from '../events/eventTypes';
import { logger } from '../shared/logger';
import { config } from '../shared/config';

const redlock = new Redlock([redis], {
  driftFactor: 0.01,
  retryCount: 0, // Fail immediately if lease is held by another replica
  retryDelay: 200,
  retryJitter: 50,
  automaticExtensionThreshold: 500,
});

const LOCK_KEY = 'locks:outbox_publisher';
const LOCK_TTL_MS = 10000;
const BATCH_SIZE = 10;
const POLLING_INTERVAL_EMPTY_MS = 1000;
const POLLING_INTERVAL_BUSY_MS = 50;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Migrates a failed event to the Dead Letter Queue (DLQ).
 */
async function moveToDLQ(event: any, reason: string): Promise<void> {
  try {
    await runWithDbBreaker(() =>
      prisma.deadLetterEvent.create({
        data: {
          event_id: event.event_id,
          aggregate_type: event.aggregate_type,
          aggregate_id: event.aggregate_id,
          event_type: event.event_type,
          payload: event.payload as any,
          created_at: event.created_at,
          sequence_number: event.sequence_number,
          event_version: event.event_version,
          failure_reason: reason,
        },
      })
    );
    logger.error({ eventId: event.event_id, reason }, 'Event migrated to DLQ due to publication failure');
  } catch (error: any) {
    logger.fatal({ err: error.message, eventId: event.event_id }, 'Failed to record event to Dead Letter Queue');
  }
}

/**
 * Fetches and publishes a batch of unprocessed events.
 */
async function processOutboxBatch(): Promise<number> {
  const events = await runWithDbBreaker(() =>
    prisma.outboxEvent.findMany({
      where: { processed: false },
      orderBy: { sequence_number: 'asc' },
      take: BATCH_SIZE,
    })
  );

  if (events.length === 0) {
    return 0;
  }

  logger.debug({ count: events.length }, 'Fetched unprocessed outbox events batch');

  for (const event of events) {
    const serializedEvent: SerializedEvent = {
      eventId: event.event_id,
      aggregateType: event.aggregate_type,
      aggregateId: event.aggregate_id,
      eventType: event.event_type,
      sequenceNumber: event.sequence_number.toString(),
      createdAt: event.created_at.toISOString(),
      eventVersion: event.event_version,
      payload: event.payload,
    };

    const retryKey = `outbox:retries:${event.event_id}`;

    try {
      // Publish event to Kafka topic
      await producer.send({
        topic: 'order_events',
        messages: [
          {
            key: event.event_id,
            value: JSON.stringify(serializedEvent),
          },
        ],
      });

      // Mark processed in PostgreSQL
      await runWithDbBreaker(() =>
        prisma.outboxEvent.update({
          where: { event_id: event.event_id },
          data: { processed: true },
        })
      );

      // Record metric
      await recordPublishedEvent();
      await redis.del(retryKey);

      logger.info({ eventId: event.event_id, seq: serializedEvent.sequenceNumber }, 'Event published to Kafka');
    } catch (error: any) {
      logger.error({ err: error.message, eventId: event.event_id }, 'Failed to publish event to Kafka, retrying');
      
      try {
        const attempts = await redis.incr(retryKey);
        await redis.expire(retryKey, 3600); // 1 hr TTL

        if (attempts >= 3) {
          await moveToDLQ(event, `Failed to publish to Kafka after ${attempts} attempts. Error: ${error.message}`);
          await runWithDbBreaker(() =>
            prisma.outboxEvent.update({
              where: { event_id: event.event_id },
              data: { processed: true },
            })
          );
          await redis.del(retryKey);
        }
      } catch (redisErr: any) {
        logger.error({ err: redisErr.message }, 'Failed to update retry counter in Redis/Valkey');
        throw error;
      }
    }
  }

  return events.length;
}

/**
 * CDC Publisher main loop.
 */
async function startPublisher() {
  logger.info('CDC Outbox Publisher service starting...');
  
  // Establish Kafka connection
  await connectProducer();

  while (true) {
    let lock;
    try {
      lock = await redlock.acquire([LOCK_KEY], LOCK_TTL_MS);
      logger.debug('Acquired publisher lease lock');

      const processedCount = await processOutboxBatch();

      const sleepDelay = processedCount > 0 ? POLLING_INTERVAL_BUSY_MS : POLLING_INTERVAL_EMPTY_MS;
      await sleep(sleepDelay);
    } catch (error: any) {
      if (error.name === 'LockError') {
        logger.debug('Lease lock held by another publisher instance, sleeping...');
        await sleep(POLLING_INTERVAL_EMPTY_MS);
      } else {
        logger.error({ err: error.message }, 'Unexpected error in publisher loop, sleeping...');
        await sleep(5000); // Back off 5 seconds
      }
    } finally {
      if (lock) {
        try {
          await lock.release();
          logger.debug('Released publisher lease lock');
        } catch (releaseErr: any) {
          // Lock might have expired, ignore
        }
      }
    }
  }
}

// Start CDC publisher loop
startPublisher().catch((err) => {
  logger.fatal({ err: err.message }, 'CDC Outbox Publisher failed critically');
  process.exit(1);
});

// Handle graceful shutdown
const shutdown = async () => {
  logger.info('Shutting down CDC Publisher, disconnecting Kafka producer...');
  try {
    await producer.disconnect();
  } catch (e) {}
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
