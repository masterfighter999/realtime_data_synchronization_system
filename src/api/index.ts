import express from 'express';
import { prisma } from '../infrastructure/db';
import { runWithDbBreaker, dbBreaker, redisBreaker } from '../infrastructure/circuitBreaker';
import { getCachedOrder, setCachedOrder } from '../cache/orderCache';
import { OrderService } from './orderService';
import { authMiddleware, requireAdmin, signToken } from '../auth/jwt';
import { rateLimitMiddleware, ipLimiter, clientLimiter } from '../shared/rateLimiter';
import { getLagMetrics } from '../monitoring/lagMonitor';
import { getClientCount, getEventsPerSecond } from '../monitoring/redisMetrics';
import { serializeBigInt } from '../shared/utils';
import { config } from '../shared/config';
import { logger } from '../shared/logger';

const app = express();
app.use(express.json());

// Apply global IP-based rate limit
app.use(rateLimitMiddleware(ipLimiter, true));

/**
 * POST /auth/login (Mock Auth Endpoint)
 */
app.post('/auth/login', (req, res) => {
  const { username, role } = req.body;
  if (!username) {
    return res.status(400).json({ error: 'Username is required' });
  }

  const userRole = role === 'admin' ? 'admin' : 'customer';
  const token = signToken({ username, role: userRole });
  logger.info({ username, role: userRole }, 'Mock login token issued');
  return res.json({ token });
});

/**
 * POST /orders (Create Order)
 */
app.post('/orders', authMiddleware, rateLimitMiddleware(clientLimiter), async (req, res) => {
  const { customer_name, product_name, status } = req.body;
  if (!customer_name || !product_name || !status) {
    return res.status(400).json({ error: 'Missing required order fields' });
  }

  // Prevent users from creating orders for other customers unless they are admin
  if (req.user?.role !== 'admin' && req.user?.username !== customer_name) {
    return res.status(403).json({ error: 'Forbidden: Cannot create order for another customer' });
  }

  try {
    const order = await OrderService.createOrder({ customer_name, product_name, status });
    return res.status(201).json(serializeBigInt(order));
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to create order');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

/**
 * GET /orders/snapshot (Snapshot API)
 */
app.get('/orders/snapshot', authMiddleware, rateLimitMiddleware(clientLimiter), async (req, res) => {
  try {
    const result = await runWithDbBreaker(async () => {
      return prisma.$transaction(async (tx) => {
        // Find maximum sequence number currently recorded
        const maxSeqAgg = await tx.outboxEvent.aggregate({
          _max: { sequence_number: true },
        });
        const lastSequence = maxSeqAgg._max.sequence_number
          ? maxSeqAgg._max.sequence_number.toString()
          : '0';

        // Read active orders
        const orders = await tx.order.findMany({
          where: req.user?.role === 'admin' ? {} : { customer_name: req.user?.username },
          orderBy: { id: 'asc' },
        });

        return { lastSequence, orders };
      });
    });

    return res.json(serializeBigInt(result));
  } catch (error: any) {
    logger.error({ err: error.message }, 'Snapshot retrieval failed');
    return res.status(500).json({ error: 'Failed to retrieve snapshot', message: error.message });
  }
});

/**
 * GET /events/replay (Replay API)
 */
app.get('/events/replay', authMiddleware, rateLimitMiddleware(clientLimiter), async (req, res) => {
  const fromStr = req.query.from;
  if (!fromStr) {
    return res.status(400).json({ error: 'Missing required parameter: from' });
  }

  try {
    const fromSeq = BigInt(fromStr as string);
    const events = await runWithDbBreaker(async () => {
      return prisma.outboxEvent.findMany({
        where: { sequence_number: { gt: fromSeq } },
        orderBy: { sequence_number: 'asc' },
      });
    });

    // Filter events to maintain customer isolation (authorization propagation)
    const filteredEvents = events.filter((e) => {
      if (req.user?.role === 'admin') return true;
      const payload: any = e.payload;
      const customer = payload?.customer_name || payload?.customerName;
      return customer === req.user?.username;
    });

    return res.json(serializeBigInt(filteredEvents));
  } catch (error: any) {
    logger.error({ err: error.message }, 'Event replay failed');
    return res.status(500).json({ error: 'Failed to replay events', message: error.message });
  }
});

/**
 * GET /orders/:id (Get Order - Cache Aside)
 */
app.get('/orders/:id', authMiddleware, rateLimitMiddleware(clientLimiter), async (req, res) => {
  const orderId = parseInt(req.params.id, 10);
  if (isNaN(orderId)) {
    return res.status(400).json({ error: 'Invalid order ID' });
  }

  try {
    // 1. Check Redis cache
    let order = await getCachedOrder(orderId);

    // 2. Query DB on cache miss
    if (!order) {
      order = await runWithDbBreaker(() =>
        prisma.order.findUnique({ where: { id: orderId } })
      );

      if (order) {
        await setCachedOrder(order);
      }
    }

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // 3. Authorize access
    if (req.user?.role !== 'admin' && req.user?.username !== order.customer_name) {
      return res.status(403).json({ error: 'Forbidden: Access to this order is restricted' });
    }

    return res.json(serializeBigInt(order));
  } catch (error: any) {
    logger.error({ err: error.message, orderId }, 'Error retrieving order');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

/**
 * PATCH /orders/:id (Update Order)
 */
app.patch('/orders/:id', authMiddleware, rateLimitMiddleware(clientLimiter), async (req, res) => {
  const orderId = parseInt(req.params.id, 10);
  if (isNaN(orderId)) {
    return res.status(400).json({ error: 'Invalid order ID' });
  }

  try {
    // Fetch order first to check permissions
    const existing = await runWithDbBreaker(() =>
      prisma.order.findUnique({ where: { id: orderId } })
    );

    if (!existing) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (req.user?.role !== 'admin' && req.user?.username !== existing.customer_name) {
      return res.status(403).json({ error: 'Forbidden: Cannot update other customers\' orders' });
    }

    const { customer_name, product_name, status } = req.body;
    const updated = await OrderService.updateOrder(orderId, {
      customer_name,
      product_name,
      status,
    });

    return res.json(serializeBigInt(updated));
  } catch (error: any) {
    logger.error({ err: error.message, orderId }, 'Error updating order');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

/**
 * DELETE /orders/:id (Delete Order - Admin Only)
 */
app.delete('/orders/:id', authMiddleware, requireAdmin, rateLimitMiddleware(clientLimiter), async (req, res) => {
  const orderId = parseInt(req.params.id, 10);
  if (isNaN(orderId)) {
    return res.status(400).json({ error: 'Invalid order ID' });
  }

  try {
    const deleted = await OrderService.deleteOrder(orderId);
    return res.json(serializeBigInt(deleted));
  } catch (error: any) {
    logger.error({ err: error.message, orderId }, 'Error deleting order');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

/**
 * GET /dlq (Dead Letter Queue - Admin Only)
 */
app.get('/dlq', authMiddleware, requireAdmin, rateLimitMiddleware(clientLimiter), async (req, res) => {
  try {
    const dlqEvents = await runWithDbBreaker(() =>
      prisma.deadLetterEvent.findMany({
        orderBy: { failed_at: 'desc' },
      })
    );
    return res.json(serializeBigInt(dlqEvents));
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to fetch DLQ events');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

/**
 * GET /metrics (Monitoring metrics)
 */
app.get('/metrics', async (req, res) => {
  try {
    const lagMetrics = await getLagMetrics();
    const clients = await getClientCount();
    const eps = await getEventsPerSecond();

    const metrics = {
      outboxLag: lagMetrics.outboxLag,
      oldestUnprocessedSeconds: lagMetrics.oldestUnprocessedSeconds,
      connectedClients: clients,
      eventsPerSecond: eps,
      circuitBreakers: {
        postgres: dbBreaker.opened ? 'OPEN' : dbBreaker.halfOpen ? 'HALF-OPEN' : 'CLOSED',
        redis: redisBreaker.opened ? 'OPEN' : redisBreaker.halfOpen ? 'HALF-OPEN' : 'CLOSED',
      },
    };

    return res.json(metrics);
  } catch (error: any) {
    logger.error({ err: error.message }, 'Failed to retrieve metrics');
    return res.status(500).json({ error: 'Internal Server Error', message: error.message });
  }
});

// Start API Server
app.listen(config.port, () => {
  logger.info(`Express API Server listening on port ${config.port} in ${config.nodeEnv} mode`);
});
