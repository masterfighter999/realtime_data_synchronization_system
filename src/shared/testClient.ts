import { io as ioClient } from 'socket.io-client';
import { logger } from './logger';
import { config } from './config';

const API_BASE = `http://localhost:${config.port}`;
const WS_BASE = `http://localhost:${config.wsPort}`;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runTest() {
  logger.info('==================================================');
  logger.info('   Starting Real-Time System Integration Test   ');
  logger.info('==================================================');

  // 1. Authenticate users
  logger.info('Step 1: Authenticating test accounts...');
  
  const adminLogin = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: 'admin_user', role: 'admin' }),
  });
  const { token: adminToken } = await adminLogin.json() as any;
  logger.info('Admin logged in successfully');

  const customerLogin = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: 'customer_a', role: 'customer' }),
  });
  const { token: customerToken } = await customerLogin.json() as any;
  logger.info('Customer A logged in successfully');

  // 2. Create Order (Verify Transactional Outbox)
  logger.info('\nStep 2: Creating order via Express API (Transactional Outbox)...');
  const createRes = await fetch(`${API_BASE}/orders`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${adminToken}`,
    },
    body: JSON.stringify({
      customer_name: 'customer_a',
      product_name: 'Premium Gaming Laptop',
      status: 'pending',
    }),
  });
  const createdOrder = await createRes.json() as any;
  logger.info({ order: createdOrder }, 'Order created successfully');

  // 3. Read Order (Verify Cache Aside)
  logger.info('\nStep 3: Reading order (verifying Cache-Aside)...');
  const readRes1 = await fetch(`${API_BASE}/orders/${createdOrder.id}`, {
    headers: { 'Authorization': `Bearer ${customerToken}` },
  });
  const readOrder1 = await readRes1.json() as any;
  logger.info({ order: readOrder1 }, 'Cache-aside read 1 (miss -> populate)');

  const readRes2 = await fetch(`${API_BASE}/orders/${createdOrder.id}`, {
    headers: { 'Authorization': `Bearer ${customerToken}` },
  });
  const readOrder2 = await readRes2.json() as any;
  logger.info({ order: readOrder2 }, 'Cache-aside read 2 (hit from cache)');

  // 4. WebSocket connection & Live Stream
  logger.info('\nStep 4: Connecting Socket.IO client (Customer A)...');
  const socket = ioClient(WS_BASE, {
    query: { token: customerToken },
    transports: ['websocket'],
  });

  const receivedEvents: any[] = [];
  socket.on('connect', () => {
    logger.info('Socket.IO client connected successfully!');
  });

  socket.on('order_event', (event, ack) => {
    logger.info({ event }, 'WebSocket live stream received event');
    receivedEvents.push(event);
    if (ack) ack(); // Send acknowledgment
  });

  socket.on('warning', (warning) => {
    logger.warn({ warning }, 'Received socket warning from gateway');
  });

  await sleep(2000); // Wait for connection

  // 5. Update Order (Verify live event broadcast)
  logger.info('\nStep 5: Updating order status (Verify live event broadcast)...');
  const updateRes = await fetch(`${API_BASE}/orders/${createdOrder.id}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${customerToken}`,
    },
    body: JSON.stringify({ status: 'processing' }),
  });
  const updatedOrder = await updateRes.json() as any;
  logger.info({ order: updatedOrder }, 'Order updated via API');

  await sleep(2000); // Wait for CDC worker to poll and publish

  // 6. Simulate Disconnection & Replay
  logger.info('\nStep 6: Simulating client disconnection and state replay reconciliation...');
  logger.info('Disconnecting socket...');
  socket.disconnect();
  await sleep(1000);

  // Read current snapshot
  const snapshotRes = await fetch(`${API_BASE}/orders/snapshot`, {
    headers: { 'Authorization': `Bearer ${customerToken}` },
  });
  const snapshot = await snapshotRes.json() as any;
  logger.info({ lastSequence: snapshot.lastSequence }, 'Fetched order snapshot');

  // Create another order while disconnected
  logger.info('Creating a new order while client is offline...');
  await fetch(`${API_BASE}/orders`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${adminToken}`,
    },
    body: JSON.stringify({
      customer_name: 'customer_a',
      product_name: '4K Ultra-Wide Monitor',
      status: 'pending',
    }),
  });

  await sleep(2000); // Wait for CDC worker

  // Reconnect and replay missed events
  logger.info('Re-establishing socket connection and replaying missed events...');
  const newSocket = ioClient(WS_BASE, {
    query: { token: customerToken },
    transports: ['websocket'],
  });

  newSocket.on('order_event', (event, ack) => {
    logger.info({ event }, 'WebSocket (reconnected) received live event');
    if (ack) ack();
  });

  newSocket.on('warning', (warning) => {
    logger.warn({ warning }, 'WebSocket (reconnected) received warning');
  });

  // Call replay endpoint using the sequence number from our previous snapshot
  const replayRes = await fetch(`${API_BASE}/events/replay?from=${snapshot.lastSequence}`, {
    headers: { 'Authorization': `Bearer ${customerToken}` },
  });
  const missedEvents = await replayRes.json() as any;
  logger.info({ count: missedEvents.length, events: missedEvents }, 'Replayed missed events successfully');

  // 7. Verify Backpressure & Flow Control
  logger.info('\nStep 7: Testing Backpressure & Flow Control...');
  logger.info('Connecting a client that will block / ignore acknowledgments...');
  
  const slowSocket = ioClient(WS_BASE, {
    query: { token: customerToken },
    transports: ['websocket'],
  });

  // Intercept events and DO NOT call the ack callback (simulating backpressure)
  slowSocket.on('order_event', (event, ack) => {
    logger.info({ eventId: event.eventId }, 'Slow socket received event, ignoring ACK callback');
    // ack() is NOT called here
  });

  slowSocket.on('warning', (warning) => {
    logger.warn({ warning }, 'Slow socket received warning');
  });

  await sleep(2000);

  logger.info('Triggering multiple rapid order updates to flood the client buffer...');
  for (let i = 1; i <= 8; i++) {
    await fetch(`${API_BASE}/orders/${createdOrder.id}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${customerToken}`,
      },
      body: JSON.stringify({ status: `step_batch_${i}` }),
    });
    await sleep(200);
  }

  await sleep(3000);

  // Clean up
  logger.info('\nStep 8: Cleaning up test clients...');
  newSocket.disconnect();
  slowSocket.disconnect();

  // Print metrics
  logger.info('\nStep 9: Querying server metrics...');
  const metricsRes = await fetch(`${API_BASE}/metrics`);
  const metrics = await metricsRes.json() as any;
  logger.info({ metrics }, 'Server metrics state');

  logger.info('==================================================');
  logger.info('   Integration Test Completed Successfully!     ');
  logger.info('==================================================');
}

runTest().catch((err) => {
  logger.error({ err: err.message }, 'Integration test failed');
});
