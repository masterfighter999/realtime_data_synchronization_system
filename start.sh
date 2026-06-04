#!/bin/sh

echo "Starting CDC Outbox Publisher..."
node dist/outbox/index.js &

echo "Starting Express REST API & WebSocket Server..."
exec node dist/api/index.js
