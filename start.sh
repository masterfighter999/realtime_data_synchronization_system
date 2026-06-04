#!/bin/sh

echo "Starting WebSocket Gateway on port 3001..."
node dist/websocket/index.js &

echo "Starting CDC Outbox Publisher..."
node dist/outbox/index.js &

echo "Starting Express REST API on port 3000..."
exec node dist/api/index.js
