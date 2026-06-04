export interface OutboxPayloadV1 {
  id: number;
  customer_name: string;
  product_name: string;
  status: string;
  updated_at: string;
}

// V2 payload: structure updated (e.g., camelCase conversion, field additions)
export interface OutboxPayloadV2 {
  id: number;
  customerName: string;
  productName: string;
  status: string;
  updatedAt: string;
  processedAt: string;
  schemaVersion: number;
}

export interface SerializedEvent {
  eventId: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  sequenceNumber: string;
  createdAt: string;
  eventVersion: number;
  payload: any;
}

/**
 * Transforms a V1 event payload to a V2 event payload if needed.
 * This demonstrates schema evolution and compatibility handlers.
 */
export function upgradeEventToV2(event: SerializedEvent): SerializedEvent {
  if (event.eventVersion === 2) {
    return event;
  }

  if (event.eventVersion === 1) {
    const v1Payload = event.payload as OutboxPayloadV1;
    const v2Payload: OutboxPayloadV2 = {
      id: v1Payload.id,
      customerName: v1Payload.customer_name,
      productName: v1Payload.product_name,
      status: v1Payload.status,
      updatedAt: v1Payload.updated_at,
      processedAt: new Date().toISOString(),
      schemaVersion: 2,
    };

    return {
      ...event,
      eventVersion: 2,
      payload: v2Payload,
    };
  }

  return event;
}
