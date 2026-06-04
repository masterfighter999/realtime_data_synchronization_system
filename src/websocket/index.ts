import { createServer } from 'http';
import { Server, Socket } from 'socket.io';
import { consumer } from '../infrastructure/kafka';
import { verifyToken, UserPayload } from '../auth/jwt';
import { upgradeEventToV2, SerializedEvent } from '../events/eventTypes';
import { incrementClientCount, decrementClientCount } from '../monitoring/redisMetrics';
import { logger } from '../shared/logger';
import { config } from '../shared/config';

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: '*',
  },
});

const MAX_PENDING_ACK_SIZE = 5;
const socketQueues = new Map<string, { eventId: string; timestamp: number }[]>();
const socketBackpressureState = new Map<string, boolean>();

// Socket.IO Connection Handshake Middleware for JWT Verification
io.use((socket, next) => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;

  if (!token) {
    logger.warn('Socket connection rejected: Missing JWT token');
    return next(new Error('Authentication error: Token required'));
  }

  try {
    const decoded = verifyToken(token as string);
    socket.data.user = decoded;
    next();
  } catch (error: any) {
    logger.warn({ err: error.message }, 'Socket connection rejected: Invalid JWT token');
    return next(new Error('Authentication error: Invalid token'));
  }
});

io.on('connection', async (socket: Socket) => {
  const user = socket.data.user as UserPayload;
  logger.info({ id: socket.id, username: user.username, role: user.role }, 'Socket client connected');

  // Track connection metric in Valkey
  await incrementClientCount();

  // Initialize socket buffers for backpressure tracking
  socketQueues.set(socket.id, []);
  socketBackpressureState.set(socket.id, false);

  // Place socket in rooms for targeted routing
  if (user.role === 'admin') {
    socket.join('room:admin');
  }
  socket.join(`room:customer:${user.username}`);

  socket.on('disconnect', async () => {
    logger.info({ id: socket.id, username: user.username }, 'Socket client disconnected');
    await decrementClientCount();
    socketQueues.delete(socket.id);
    socketBackpressureState.delete(socket.id);
  });
});

/**
 * Sends an event to a specific socket connection while respecting backpressure thresholds.
 */
function sendEventToSocketWithBackpressure(socket: Socket, event: SerializedEvent) {
  const queue = socketQueues.get(socket.id) || [];
  const isBackpressured = socketBackpressureState.get(socket.id) || false;

  if (queue.length >= MAX_PENDING_ACK_SIZE) {
    if (!isBackpressured) {
      socketBackpressureState.set(socket.id, true);
      logger.warn({ socketId: socket.id, username: socket.data.user?.username }, 'Socket backpressure triggered, dropping updates');
      socket.emit('warning', {
        type: 'CLIENT_BACKPRESSURE',
        message: 'Rate of updates exceeds capacity. Some events were dropped. Please synchronize via Replay/Snapshot API.',
      });
    }
    // Drop the event
    return;
  }

  // Queue event id to wait for client acknowledgment
  queue.push({ eventId: event.eventId, timestamp: Date.now() });
  socketQueues.set(socket.id, queue);

  socket.emit('order_event', event, (ack: any) => {
    // Ack received: remove from queue
    const currentQueue = socketQueues.get(socket.id) || [];
    const index = currentQueue.findIndex((item) => item.eventId === event.eventId);
    if (index !== -1) {
      currentQueue.splice(index, 1);
      socketQueues.set(socket.id, currentQueue);
    }

    // Resolve backpressure warning flag if the client clears its backlog
    if (currentQueue.length < MAX_PENDING_ACK_SIZE && socketBackpressureState.get(socket.id)) {
      socketBackpressureState.set(socket.id, false);
      logger.info({ socketId: socket.id }, 'Socket backpressure cleared');
    }
  });
}

/**
 * Initializes and starts the Kafka consumer subscription.
 */
async function startKafkaSubscription() {
  const topic = 'order_events';
  try {
    logger.info('Connecting Kafka Consumer...');
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    logger.info({ topic }, 'Kafka Consumer subscribed to topic');

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;

        try {
          const rawEvent = JSON.parse(message.value.toString()) as SerializedEvent;

          // Schema Evolution: upgrade payload format to V2 before routing
          const upgradedEvent = upgradeEventToV2(rawEvent);

          // Get target customer from payload
          const customerName = upgradedEvent.payload?.customerName || upgradedEvent.payload?.customer_name;

          // Route to Admins
          const adminSocketIds = io.sockets.adapter.rooms.get('room:admin');
          if (adminSocketIds) {
            for (const socketId of adminSocketIds) {
              const socket = io.sockets.sockets.get(socketId);
              if (socket) {
                sendEventToSocketWithBackpressure(socket, upgradedEvent);
              }
            }
          }

          // Route to specific customer
          if (customerName) {
            const customerSocketIds = io.sockets.adapter.rooms.get(`room:customer:${customerName}`);
            if (customerSocketIds) {
              for (const socketId of customerSocketIds) {
                const socket = io.sockets.sockets.get(socketId);
                if (socket && socket.data.user?.role !== 'admin') {
                  sendEventToSocketWithBackpressure(socket, upgradedEvent);
                }
              }
            }
          }
        } catch (parseError: any) {
          logger.error({ err: parseError.message }, 'Failed to process event from Kafka topic');
        }
      },
    });
  } catch (error: any) {
    logger.fatal({ err: error.message }, 'Kafka subscription failed critically');
    process.exit(1);
  }
}

// Start consumer and server
startKafkaSubscription();
httpServer.listen(config.wsPort, () => {
  logger.info(`WebSocket Gateway listening on port ${config.wsPort}`);
});

// Handle graceful shutdown
const shutdown = async () => {
  logger.info('Shutting down WebSocket Gateway, disconnecting Kafka consumer...');
  try {
    await consumer.disconnect();
  } catch (e) {}
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
